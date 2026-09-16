const express = require('express');
const db = require('../db');

const router = express.Router();

const FREE_BASE = (process.env.FREE_BACKEND_URL || 'https://api.groq.com/openai/v1').replace(/\/$/, '');

// Global rate limit gate — holds requests when backend is rate limited
const rateLimitGate = { blockedUntil: 0, releaseSlot: 0, queued: 0 };
const MAX_QUEUED = 6;        // total queue cap
const MAX_QUEUED_PER_USER = 2; // per-user cap — prevents one user from filling all slots
const perUserQueued = {};    // userId -> current queued count

async function waitForRateLimit(isStream, res, userId) {
  const now = Date.now();
  if (rateLimitGate.blockedUntil <= now && rateLimitGate.releaseSlot <= now) return;

  // Per-user cap: if this user already has 2 slots queued, reject fast
  const userQ = perUserQueued[userId] || 0;
  if (userQ >= MAX_QUEUED_PER_USER) {
    return 'overloaded';
  }

  // Global cap: reject if no slots left at all
  if (rateLimitGate.queued >= MAX_QUEUED) {
    return 'overloaded';
  }

  // Claim a staggered slot (1s apart per request after gate opens)
  rateLimitGate.queued++;
  perUserQueued[userId] = (perUserQueued[userId] || 0) + 1;
  rateLimitGate.releaseSlot = Math.max(rateLimitGate.blockedUntil, rateLimitGate.releaseSlot) + 1000;
  const totalWait = rateLimitGate.releaseSlot - now;

  console.log(`Rate gate: holding for ${Math.ceil(totalWait/1000)}s (${rateLimitGate.queued} total, user ${userId}: ${perUserQueued[userId]})...`);

  if (isStream && !res.headersSent) {
    res.setHeader('content-type', 'text/event-stream');
    res.setHeader('cache-control', 'no-cache');
    res.setHeader('connection', 'keep-alive');
    res.status(200);
  }
  if (isStream) {
    const iv = setInterval(() => {
      try { if (!res.writableEnded) res.write(': ping\n\n'); }
      catch (_) { clearInterval(iv); }
    }, 5000);
    await new Promise(r => setTimeout(r, totalWait));
    clearInterval(iv);
  } else {
    await new Promise(r => setTimeout(r, totalWait));
  }
  rateLimitGate.queued--;
  perUserQueued[userId] = Math.max(0, (perUserQueued[userId] || 1) - 1);
}

// Auto-refreshing token manager for Open WebUI
const tokenCache = {
  token: process.env.FREE_BACKEND_KEY || '',
  expiresAt: 0,
};

// Parse JWT expiry without a library
function jwtExpiry(token) {
  try {
    const payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString());
    return (payload.exp || 0) * 1000;
  } catch (_) { return 0; }
}

async function getKey() {
  const email = process.env.WEBUI_EMAIL;
  const password = process.env.WEBUI_PASSWORD;
  const loginUrl = process.env.WEBUI_LOGIN_URL || (FREE_BASE.replace(/\/api.*$/, '') + '/api/v1/auths/signin');

  // If no auto-refresh creds, just return static key
  if (!email || !password) return process.env.FREE_BACKEND_KEY || '';

  // Refresh if token expires within 1 hour
  const now = Date.now();
  if (tokenCache.token && tokenCache.expiresAt > now + 3600_000) {
    return tokenCache.token;
  }

  try {
    const resp = await fetch(loginUrl, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });
    if (!resp.ok) throw new Error(`Login failed: ${resp.status}`);
    const data = await resp.json();
    tokenCache.token = data.token;
    tokenCache.expiresAt = jwtExpiry(data.token) || (now + 25 * 24 * 3600_000);
    console.log('Token refreshed, expires:', new Date(tokenCache.expiresAt).toISOString());
    return tokenCache.token;
  } catch (err) {
    console.error('Token refresh failed:', err.message);
    return tokenCache.token; // fall back to cached
  }
}

// Map Claude model names → backend model
function mapModel(claudeModel) {
  if (!claudeModel) return 'claude-opus-5';
  const m = claudeModel.toLowerCase();
  if (m.includes('haiku')) return 'claude-fable-5';
  if (m.includes('fable')) return 'claude-fable-5';
  if (m.includes('sonnet')) return 'claude-sonnet-5';
  return 'claude-opus-5';
}

function extractToken(req) {
  const auth = req.headers['authorization'] || '';
  if (auth.startsWith('Bearer ')) return auth.slice(7).trim();
  return req.headers['x-api-key'] || '';
}

function maybeResetWindow(row) {
  if (!row.reset_interval_hours) return row;
  const lastReset = new Date(row.last_reset_at);
  const windowMs = row.reset_interval_hours * 3600 * 1000;
  if (new Date() - lastReset >= windowMs) {
    db.prepare(`UPDATE tokens SET requests_used = 0, tokens_used = 0, last_reset_at = datetime('now') WHERE id = ?`).run(row.id);
    return { ...row, requests_used: 0, tokens_used: 0 };
  }
  return row;
}

function validateToken(raw) {
  if (!raw) return { error: 'Missing API token', status: 401 };
  let row = db.prepare('SELECT * FROM tokens WHERE token = ?').get(raw);
  if (!row) return { error: 'Invalid API token', status: 401 };
  if (!row.enabled) return { error: 'Token is disabled', status: 403 };
  if (row.expires_at && new Date(row.expires_at) < new Date()) {
    return { error: 'Token has expired', status: 403 };
  }
  row = maybeResetWindow(row);
  if (row.request_limit !== null && row.requests_used >= row.request_limit) {
    return { error: 'Request limit reached for this window', status: 429 };
  }
  if (row.token_limit !== null && row.tokens_used >= row.token_limit) {
    return { error: 'Token usage limit reached for this window', status: 429 };
  }
  return { row };
}

const TOOL_RESULT_LIMIT = 2000;        // chars — old tool results get capped at this
const TOOL_RESULT_LIMIT_RECENT = 8000; // chars — recent tool results (last 10 msgs) capped here
const RECENT_MSGS = 10;                // boundary between old and recent
const MAX_TOOL_DESC_LENGTH = 500;      // chars — truncate long tool descriptions

// Strip broken Unicode surrogates that cause "no low surrogate" JSON errors
function sanitizeString(s) {
  if (typeof s !== 'string') return s;
  return s.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, '�');
}

// Convert Anthropic messages + system → OpenAI messages
function toOpenAIMessages(system, messages) {
  const result = [];
  if (system) {
    let sysText;
    if (typeof system === 'string') {
      sysText = system;
    } else if (Array.isArray(system)) {
      sysText = system.map(b => (typeof b === 'string' ? b : b.text || '')).join('\n');
    } else {
      sysText = String(system);
    }
    result.push({ role: 'system', content: sanitizeString(sysText) });
  }

  for (let i = 0; i < messages.length; i++) {
    const msg = messages[i];
    const isRecent = i >= messages.length - RECENT_MSGS;

    if (typeof msg.content === 'string') {
      result.push({ role: msg.role, content: sanitizeString(msg.content) });
      continue;
    }
    if (!Array.isArray(msg.content)) {
      result.push({ role: msg.role, content: sanitizeString(String(msg.content || '')) });
      continue;
    }

    const textBlocks = msg.content.filter(b => b.type === 'text');
    const toolUseBlocks = msg.content.filter(b => b.type === 'tool_use');
    const toolResultBlocks = msg.content.filter(b => b.type === 'tool_result');

    if (toolResultBlocks.length > 0) {
      for (const block of toolResultBlocks) {
        let content = typeof block.content === 'string'
          ? block.content
          : (Array.isArray(block.content) ? block.content.map(b => b.text || '').join('') : '');
        const limit = isRecent ? TOOL_RESULT_LIMIT_RECENT : TOOL_RESULT_LIMIT;
        if (content.length > limit) {
          content = content.slice(0, limit) + '\n[...truncated]';
        }
        result.push({ role: 'tool', tool_call_id: block.tool_use_id, content: sanitizeString(content) });
      }
      if (textBlocks.length > 0) {
        result.push({ role: msg.role, content: sanitizeString(textBlocks.map(b => b.text).join('')) });
      }
    } else if (toolUseBlocks.length > 0) {
      result.push({
        role: 'assistant',
        content: sanitizeString(textBlocks.map(b => b.text).join('')) || null,
        tool_calls: toolUseBlocks.map(block => ({
          id: block.id,
          type: 'function',
          function: { name: block.name, arguments: sanitizeString(JSON.stringify(block.input)) },
        })),
      });
    } else {
      result.push({ role: msg.role, content: sanitizeString(textBlocks.map(b => b.text).join('')) });
    }
  }
  return result;
}

// === Built-in tool execution (web_search, web_fetch) ===

const BUILTIN_TOOL_NAMES = new Set(['web_search', 'web_fetch']);

function isBuiltinTool(tool) {
  if (BUILTIN_TOOL_NAMES.has(tool.name)) return true;
  const t = tool.type || '';
  return t.startsWith('web_search') || t.startsWith('web_fetch');
}

function separateTools(tools) {
  if (!tools || !tools.length) return { userTools: [], builtinTools: [] };
  const userTools = [], builtinTools = [];
  for (const t of tools) (isBuiltinTool(t) ? builtinTools : userTools).push(t);
  return { userTools, builtinTools };
}

async function execWebSearch(query) {
  if (process.env.BRAVE_SEARCH_API_KEY) {
    try {
      const resp = await fetch(
        `https://api.search.brave.com/res/v1/web/search?q=${encodeURIComponent(query)}&count=8`,
        {
          headers: { 'Accept': 'application/json', 'X-Subscription-Token': process.env.BRAVE_SEARCH_API_KEY },
          signal: AbortSignal.timeout(12000),
        }
      );
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      const results = data.web?.results || [];
      if (!results.length) return 'No results found.';
      return results.slice(0, 6).map((r, i) =>
        `[${i+1}] ${r.title}\nURL: ${r.url}\n${r.description || ''}`
      ).join('\n\n');
    } catch (err) {
      console.error('Brave search error:', err.message);
    }
  }
  // DuckDuckGo instant answers fallback
  try {
    const resp = await fetch(
      `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`,
      { signal: AbortSignal.timeout(10000) }
    );
    const data = await resp.json();
    const parts = [];
    if (data.Heading) parts.push(data.Heading);
    if (data.AbstractText) parts.push(data.AbstractText);
    if (data.Answer) parts.push(`Answer: ${data.Answer}`);
    for (const t of (data.RelatedTopics || []).filter(t => t.Text).slice(0, 6)) {
      parts.push(`• ${t.Text}${t.FirstURL ? '\n  ' + t.FirstURL : ''}`);
    }
    if (!parts.length) return `No results for "${query}". Add BRAVE_SEARCH_API_KEY to Railway env for full web search (free at brave.com/search/api).`;
    return parts.join('\n\n');
  } catch (err) {
    return `Search unavailable: ${err.message}`;
  }
}

async function execWebFetch(url) {
  try {
    const resp = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; ResearchBot/1.0)' },
      signal: AbortSignal.timeout(20000),
    });
    if (!resp.ok) return `Failed to fetch ${url}: HTTP ${resp.status}`;
    const ct = resp.headers.get('content-type') || '';
    if (ct.includes('application/json')) {
      const t = await resp.text();
      return t.slice(0, 8000) + (t.length > 8000 ? '\n[...truncated]' : '');
    }
    const html = await resp.text();
    const text = html
      .replace(/<script[\s\S]*?<\/script>/gi, '')
      .replace(/<style[\s\S]*?<\/style>/gi, '')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/gi, ' ').replace(/&amp;/gi, '&').replace(/&lt;/gi, '<')
      .replace(/&gt;/gi, '>').replace(/&quot;/gi, '"').replace(/&#39;/gi, "'")
      .replace(/\s+/g, ' ').trim();
    return text.slice(0, 8000) + (text.length > 8000 ? '\n[...truncated]' : '');
  } catch (err) {
    return `Fetch failed: ${err.message}`;
  }
}

// Execute builtin tools in a loop until model returns a final text answer
async function runToolLoop(messages, baseBody, builtinTools, claudeModel) {
  const MAX_ROUNDS = 5;
  let totalInputTokens = 0, totalOutputTokens = 0;

  for (let round = 0; round <= MAX_ROUNDS; round++) {
    const body = JSON.stringify({ ...baseBody, messages, stream: false });
    const resp = await fetch(`${FREE_BASE}/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'authorization': `Bearer ${await getKey()}` },
      body,
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw Object.assign(new Error(text), { status: resp.status });
    }
    const data = await resp.json();
    const anthropicResp = toAnthropicResponse(data, claudeModel);
    totalInputTokens += anthropicResp.usage?.input_tokens || 0;
    totalOutputTokens += anthropicResp.usage?.output_tokens || 0;

    const toolUses = anthropicResp.content.filter(b => b.type === 'tool_use');
    const builtinCalls = toolUses.filter(tu => isBuiltinTool({ name: tu.name, type: tu.name }));
    const userCalls = toolUses.filter(tu => !builtinCalls.includes(tu));

    // No tool calls, or user tool calls → return as-is (client handles user tools)
    if (!toolUses.length || anthropicResp.stop_reason !== 'tool_use' || userCalls.length > 0 || round === MAX_ROUNDS) {
      anthropicResp.usage = { input_tokens: totalInputTokens, output_tokens: totalOutputTokens };
      return anthropicResp;
    }

    // Add assistant's tool_call message
    const textBlock = anthropicResp.content.find(b => b.type === 'text');
    messages = [...messages, {
      role: 'assistant',
      content: textBlock?.text || null,
      tool_calls: builtinCalls.map(tu => ({
        id: tu.id, type: 'function',
        function: { name: tu.name, arguments: JSON.stringify(tu.input) },
      })),
    }];

    // Execute each builtin tool and add results
    for (const tu of builtinCalls) {
      let result;
      if (tu.name === 'web_search' || tu.name.includes('search')) {
        const q = tu.input?.query || tu.input?.q || tu.input?.search_query || JSON.stringify(tu.input);
        console.log(`  → web_search round=${round}: "${q}"`);
        result = await execWebSearch(q);
      } else if (tu.name === 'web_fetch' || tu.name.includes('fetch')) {
        const u = tu.input?.url || tu.input?.URL || JSON.stringify(tu.input);
        console.log(`  → web_fetch round=${round}: ${u}`);
        result = await execWebFetch(u);
      } else {
        result = `Tool "${tu.name}" not supported.`;
      }
      messages.push({ role: 'tool', tool_call_id: tu.id, content: result });
    }
  }
}

// Emit a complete Anthropic response object as SSE events
function emitAnthropicResponseAsStream(res, anthropicResp) {
  if (res.writableEnded) return;
  res.write(`event: message_start\ndata: ${JSON.stringify({
    type: 'message_start',
    message: {
      id: anthropicResp.id, type: 'message', role: 'assistant', model: anthropicResp.model,
      content: [], stop_reason: null, stop_sequence: null,
      usage: { input_tokens: anthropicResp.usage?.input_tokens || 0, output_tokens: 0 },
    },
  })}\n\n`);
  res.write(`event: ping\ndata: ${JSON.stringify({ type: 'ping' })}\n\n`);
  let idx = 0;
  for (const block of anthropicResp.content) {
    if (block.type === 'text') {
      res.write(`event: content_block_start\ndata: ${JSON.stringify({ type:'content_block_start', index:idx, content_block:{type:'text',text:''} })}\n\n`);
      res.write(`event: content_block_delta\ndata: ${JSON.stringify({ type:'content_block_delta', index:idx, delta:{type:'text_delta', text:block.text} })}\n\n`);
      res.write(`event: content_block_stop\ndata: ${JSON.stringify({ type:'content_block_stop', index:idx })}\n\n`);
    } else if (block.type === 'tool_use') {
      res.write(`event: content_block_start\ndata: ${JSON.stringify({ type:'content_block_start', index:idx, content_block:{type:'tool_use', id:block.id, name:block.name, input:{}} })}\n\n`);
      res.write(`event: content_block_delta\ndata: ${JSON.stringify({ type:'content_block_delta', index:idx, delta:{type:'input_json_delta', partial_json:JSON.stringify(block.input)} })}\n\n`);
      res.write(`event: content_block_stop\ndata: ${JSON.stringify({ type:'content_block_stop', index:idx })}\n\n`);
    }
    idx++;
  }
  res.write(`event: message_delta\ndata: ${JSON.stringify({
    type: 'message_delta',
    delta: { stop_reason: anthropicResp.stop_reason, stop_sequence: null },
    usage: { input_tokens: anthropicResp.usage?.input_tokens || 0, output_tokens: anthropicResp.usage?.output_tokens || 0 },
  })}\n\n`);
  res.write(`event: message_stop\ndata: ${JSON.stringify({ type: 'message_stop' })}\n\n`);
  res.end();
}

// Convert Anthropic tools → OpenAI tools
function toOpenAITools(tools) {
  if (!tools || !tools.length) return undefined;
  return tools.map(t => {
    let desc = t.description || '';
    if (desc.length > MAX_TOOL_DESC_LENGTH) {
      desc = desc.slice(0, MAX_TOOL_DESC_LENGTH) + '...';
    }
    return {
      type: 'function',
      function: {
        name: t.name,
        description: desc,
        parameters: t.input_schema || { type: 'object', properties: {} },
      },
    };
  });
}

// Convert OpenAI response → Anthropic response
function toAnthropicResponse(data, claudeModel) {
  const choice = data.choices?.[0];
  const message = choice?.message || {};
  const content = [];

  if (message.content) content.push({ type: 'text', text: message.content });
  if (message.tool_calls) {
    for (const tc of message.tool_calls) {
      let input = {};
      try { input = JSON.parse(tc.function.arguments); } catch (_) {}
      content.push({ type: 'tool_use', id: tc.id, name: tc.function.name, input });
    }
  }

  const fr = choice?.finish_reason;
  const stopReason = fr === 'tool_calls' ? 'tool_use' : fr === 'length' ? 'max_tokens' : 'end_turn';

  return {
    id: `msg_${Date.now()}`,
    type: 'message',
    role: 'assistant',
    model: claudeModel,
    content,
    stop_reason: stopReason,
    stop_sequence: null,
    usage: {
      input_tokens: data.usage?.prompt_tokens || 0,
      output_tokens: data.usage?.completion_tokens || 0,
    },
  };
}

// Convert OpenAI SSE chunk → Anthropic SSE events (generator)
function* toAnthropicEvents(chunk, state) {
  // OpenAI sends a final usage-only chunk with empty choices — capture tokens, emit nothing
  if (chunk.usage && (!chunk.choices || chunk.choices.length === 0)) {
    state.finalInputTokens = chunk.usage.prompt_tokens || 0;
    state.finalOutputTokens = chunk.usage.completion_tokens || 0;
    return;
  }
  const choice = chunk.choices?.[0];
  if (!choice) return;
  const delta = choice.delta || {};

  if (!state.started) {
    state.started = true;
    state.blockIndex = 0;
    state.blockOpen = false;
    state.blockType = null;
    state.toolCallBuffers = {};
    state.toolCallIndexMap = {};
    state.outputTokens = 0;

    yield `event: message_start\ndata: ${JSON.stringify({
      type: 'message_start',
      message: {
        id: `msg_${Date.now()}`,
        type: 'message',
        role: 'assistant',
        model: state.claudeModel,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: chunk.usage?.prompt_tokens || 0, output_tokens: 0 },
      },
    })}\n\n`;
    yield `event: ping\ndata: ${JSON.stringify({ type: 'ping' })}\n\n`;

    if (delta.content !== undefined && delta.content !== null && delta.content !== '') {
      yield `event: content_block_start\ndata: ${JSON.stringify({
        type: 'content_block_start', index: 0,
        content_block: { type: 'text', text: '' },
      })}\n\n`;
      state.blockOpen = true;
      state.blockType = 'text';
    }
  }

  if (delta.content) {
    // If a tool_use block is currently open, close it before opening a text block
    if (state.blockOpen && state.blockType === 'tool_use') {
      yield `event: content_block_stop\ndata: ${JSON.stringify({ type: 'content_block_stop', index: state.blockIndex })}\n\n`;
      state.blockIndex++;
      state.blockOpen = false;
    }
    if (!state.blockOpen) {
      yield `event: content_block_start\ndata: ${JSON.stringify({
        type: 'content_block_start', index: state.blockIndex,
        content_block: { type: 'text', text: '' },
      })}\n\n`;
      state.blockOpen = true;
      state.blockType = 'text';
    }
    yield `event: content_block_delta\ndata: ${JSON.stringify({
      type: 'content_block_delta', index: state.blockIndex,
      delta: { type: 'text_delta', text: delta.content },
    })}\n\n`;
    state.outputTokens++;
  }

  if (delta.tool_calls) {
    for (const tc of delta.tool_calls) {
      const idx = tc.index ?? 0;
      if (!state.toolCallBuffers[idx]) {
        if (state.blockOpen && state.blockType === 'text') {
          yield `event: content_block_stop\ndata: ${JSON.stringify({ type: 'content_block_stop', index: state.blockIndex })}\n\n`;
          state.blockIndex++;
          state.blockOpen = false;
        }
        state.toolCallBuffers[idx] = { id: tc.id || '', name: tc.function?.name || '', args: '' };
        state.toolCallIndexMap[idx] = state.blockIndex;
        yield `event: content_block_start\ndata: ${JSON.stringify({
          type: 'content_block_start', index: state.blockIndex,
          content_block: { type: 'tool_use', id: tc.id || `tool_${idx}`, name: tc.function?.name || '', input: {} },
        })}\n\n`;
        state.blockOpen = true;
        state.blockType = 'tool_use';
      }
      if (tc.function?.name && !state.toolCallBuffers[idx].name) {
        state.toolCallBuffers[idx].name = tc.function.name;
      }
      if (tc.function?.arguments) {
        state.toolCallBuffers[idx].args += tc.function.arguments;
        yield `event: content_block_delta\ndata: ${JSON.stringify({
          type: 'content_block_delta', index: state.toolCallIndexMap[idx],
          delta: { type: 'input_json_delta', partial_json: tc.function.arguments },
        })}\n\n`;
      }
    }
  }

  if (choice.finish_reason) {
    if (state.blockOpen) {
      yield `event: content_block_stop\ndata: ${JSON.stringify({ type: 'content_block_stop', index: state.blockIndex })}\n\n`;
      state.blockOpen = false;
    }
    const fr = choice.finish_reason;
    const stopReason = fr === 'tool_calls' ? 'tool_use' : fr === 'length' ? 'max_tokens' : 'end_turn';
    const outputTokens = chunk.usage?.completion_tokens || state.outputTokens || 0;
    const inputTokens = chunk.usage?.prompt_tokens || 0;

    yield `event: message_delta\ndata: ${JSON.stringify({
      type: 'message_delta',
      delta: { stop_reason: stopReason, stop_sequence: null },
      usage: { input_tokens: inputTokens, output_tokens: outputTokens },
    })}\n\n`;
    yield `event: message_stop\ndata: ${JSON.stringify({ type: 'message_stop' })}\n\n`;

    state.done = true;
    state.finalInputTokens = inputTokens;
    state.finalOutputTokens = outputTokens;
  }
}

function sendError(res, isStream, statusCode, message, claudeModel) {
  if (!res.headersSent) {
    return res.status(statusCode).json({ type: 'error', error: { type: 'api_error', message } });
  }
  // Headers already sent (SSE mode) — send error as stream event
  try {
    if (!res.writableEnded) {
      res.write(`event: message_start\ndata: ${JSON.stringify({ type:'message_start', message:{ id:`msg_${Date.now()}`, type:'message', role:'assistant', model:claudeModel||'unknown', content:[], stop_reason:null, usage:{input_tokens:0,output_tokens:0} } })}\n\n`);
      res.write(`event: content_block_start\ndata: ${JSON.stringify({ type:'content_block_start', index:0, content_block:{type:'text',text:''} })}\n\n`);
      res.write(`event: content_block_delta\ndata: ${JSON.stringify({ type:'content_block_delta', index:0, delta:{type:'text_delta', text:`[Error: ${message.slice(0, 200)}]`} })}\n\n`);
      res.write(`event: content_block_stop\ndata: ${JSON.stringify({ type:'content_block_stop', index:0 })}\n\n`);
      res.write(`event: message_delta\ndata: ${JSON.stringify({ type:'message_delta', delta:{stop_reason:'end_turn'}, usage:{output_tokens:0} })}\n\n`);
      res.write(`event: message_stop\ndata: ${JSON.stringify({ type:'message_stop' })}\n\n`);
      res.end();
    }
  } catch (_) { try { res.end(); } catch (__) {} }
}

router.use(async (req, res) => {
  const raw = extractToken(req);
  const { error, status, row } = validateToken(raw);
  if (error) return res.status(status).json({ error });

  // Lightweight logging — estimate body size without serializing the whole thing
  const msgCount = req.body?.messages?.length || 0;
  const toolCount = req.body?.tools?.length || 0;
  console.log(`[${req.path}] model=${req.body?.model} msgs=${msgCount} tools=${toolCount}`);

  // Models list — return fake Claude model list
  if (req.path === '/models' || req.path === '/models/') {
    return res.json({
      data: [
        { type: 'model', id: 'claude-opus-4-5',  display_name: 'Claude Opus',   created_at: '2025-01-01T00:00:00Z' },
        { type: 'model', id: 'claude-sonnet-4-5', display_name: 'Claude Sonnet', created_at: '2025-01-01T00:00:00Z' },
        { type: 'model', id: 'claude-haiku-4-5',  display_name: 'Claude Haiku',  created_at: '2025-01-01T00:00:00Z' },
        { type: 'model', id: 'claude-fable-5-1',  display_name: 'Claude Fable',  created_at: '2025-01-01T00:00:00Z' },
        { type: 'model', id: 'claude-opus-5',     display_name: 'Claude Opus 5', created_at: '2025-01-01T00:00:00Z' },
        { type: 'model', id: 'claude-sonnet-5',   display_name: 'Claude Sonnet 5', created_at: '2025-01-01T00:00:00Z' },
      ],
      has_more: false,
    });
  }

  const body = req.body;

  // count_tokens — return a fake estimate so clients don't error
  if (req.path === '/messages/count_tokens') {
    let charCount = 0;
    const msgs = body.messages || [];
    for (const m of msgs) {
      if (typeof m.content === 'string') charCount += m.content.length;
      else if (Array.isArray(m.content)) {
        for (const b of m.content) charCount += (b.text || '').length;
      }
    }
    return res.json({ input_tokens: Math.ceil(charCount / 4) });
  }

  // Only translate /messages
  if (req.path !== '/messages' && req.path !== '/messages/') {
    return res.status(404).json({ type: 'error', error: { type: 'not_found_error', message: 'Not found' } });
  }
  const claudeModel = body.model || 'claude-opus-4-5';
  const isStream = body.stream === true;

  // Auto-truncate: keep last 40 messages when conversation gets too large
  const MAX_MSGS = 40;
  let msgs = body.messages || [];
  if (msgs.length > MAX_MSGS) {
    let kept = msgs.slice(-MAX_MSGS);
    // Ensure slice starts at a user message — orphaned tool/assistant messages
    // at the start cause OpenAI to reject the request with an invalid sequence error
    while (kept.length > 0 && kept[0].role !== 'user') kept.shift();
    if (kept.length === 0) kept = msgs.slice(-2); // fallback: keep last 2
    kept.unshift({ role: 'user', content: '[Note: Earlier conversation history was auto-truncated to keep context manageable.]' });
    kept.splice(1, 0, { role: 'assistant', content: 'Understood. I\'ll continue from the recent context.' });
    msgs = kept;
    console.log(`Auto-truncated: ${body.messages.length} → ${msgs.length} messages`);
  }

  const openAIBody = {
    model: mapModel(claudeModel),
    messages: toOpenAIMessages(body.system, msgs),
    max_tokens: body.max_tokens || 4096,
    stream: isStream,
  };
  if (body.temperature !== undefined) openAIBody.temperature = body.temperature;
  if (body.top_p !== undefined) openAIBody.top_p = body.top_p;
  const { userTools, builtinTools } = separateTools(body.tools);
  const openAITools = toOpenAITools(userTools);
  if (openAITools) openAIBody.tools = openAITools;
  if (isStream) openAIBody.stream_options = { include_usage: true };

  // Log actual forwarded body size (after truncation)
  const fwdBody = JSON.stringify(openAIBody);
  const fwdKB = (Buffer.byteLength(fwdBody, 'utf8') / 1024).toFixed(1);
  console.log(`  → forwarding: model=${openAIBody.model} msgs=${openAIBody.messages.length} tools=${openAIBody.tools?.length || 0} fwdSize=${fwdKB}KB`);

  try {
    // Wait if global rate limit is active (set by a previous request hitting the limit)
    const gateResult = await waitForRateLimit(isStream, res, row.id);
    if (gateResult === 'overloaded') {
      console.log(`Queue full — returning 529 (user ${row.id})`);
      return sendError(res, isStream, 529, 'Server busy, please retry in a moment', claudeModel);
    }

    // Tool loop: intercept web_search/web_fetch, execute them here, return final answer
    if (builtinTools.length > 0) {
      if (isStream && !res.headersSent) {
        res.setHeader('content-type', 'text/event-stream');
        res.setHeader('cache-control', 'no-cache');
        res.setHeader('connection', 'keep-alive');
        res.status(200);
      }
      const keepAlive = isStream ? setInterval(() => {
        try { if (!res.writableEnded) res.write(': ping\n\n'); }
        catch (_) { clearInterval(keepAlive); }
      }, 5000) : null;
      try {
        const anthropicResp = await runToolLoop(openAIBody.messages, openAIBody, builtinTools, claudeModel);
        if (keepAlive) clearInterval(keepAlive);
        db.prepare('UPDATE tokens SET requests_used = requests_used + 1 WHERE id = ?').run(row.id);
        logUsage(row.id, claudeModel, anthropicResp.usage?.input_tokens || 0, anthropicResp.usage?.output_tokens || 0);
        if (isStream) {
          if (!res.writableEnded) emitAnthropicResponseAsStream(res, anthropicResp);
        } else {
          res.status(200).json(anthropicResp);
        }
      } catch (toolErr) {
        if (keepAlive) clearInterval(keepAlive);
        console.error('Tool loop error:', toolErr.message);
        sendError(res, isStream, toolErr.status || 502, toolErr.message, claudeModel);
      }
      return;
    }

    async function fetchUpstream() {
      return fetch(`${FREE_BASE}/chat/completions`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'authorization': `Bearer ${await getKey()}`,
        },
        body: fwdBody,
      });
    }

    let upstream = await fetchUpstream();

    if (!upstream.ok) {
      const errText = await upstream.text();
      const isRateLimit = upstream.status === 429 || (upstream.status === 400 && errText.includes('RateLimitError'));
      const isTimeout = upstream.status === 524 || upstream.status === 504 || upstream.status === 502 || upstream.status === 503;

      if (isRateLimit) {
        rateLimitGate.blockedUntil = Date.now() + 65_000;
        console.log('Rate limit hit — global gate set for 65s');
        await waitForRateLimit(isStream, res, row.id);
        upstream = await fetchUpstream();
        if (!upstream.ok) {
          const retryErr = await upstream.text();
          const cleanRetryErr = retryErr.includes('<html') ? `Backend error ${upstream.status}` : retryErr.slice(0, 300);
          console.error('Upstream error after retry:', upstream.status, cleanRetryErr);
          return sendError(res, isStream, upstream.status, cleanRetryErr, claudeModel);
        }
      } else if (isTimeout) {
        // Backend timeout — wait 8s and retry once before giving up
        console.log(`Backend timeout ${upstream.status} — retrying in 8s...`);
        await new Promise(r => setTimeout(r, 8000));
        upstream = await fetchUpstream();
        if (!upstream.ok) {
          const retryText = await upstream.text();
          const cleanErr = retryText.includes('<html') ? `Backend timeout (${upstream.status}) — please retry your message` : retryText.slice(0, 300);
          console.error('Upstream timeout after retry:', upstream.status);
          return sendError(res, isStream, 503, cleanErr, claudeModel);
        }
      } else {
        const cleanErr = errText.includes('<html') ? `Backend error ${upstream.status}` : errText.slice(0, 300);
        console.error('Upstream error:', upstream.status, cleanErr);
        return sendError(res, isStream, upstream.status, cleanErr, claudeModel);
      }
    }

    db.prepare('UPDATE tokens SET requests_used = requests_used + 1 WHERE id = ?').run(row.id);

    if (isStream) {
      if (!res.headersSent) {
        res.setHeader('content-type', 'text/event-stream');
        res.setHeader('cache-control', 'no-cache');
        res.setHeader('connection', 'keep-alive');
        res.status(200);
      }

      const keepAlive = setInterval(() => {
        try { if (!res.writableEnded) res.write(': ping\n\n'); }
        catch (_) { clearInterval(keepAlive); }
      }, 5000);

      const reader = upstream.body.getReader();
      const decoder = new TextDecoder();
      const state = { claudeModel, started: false };

      // Abort upstream reader if client disconnects
      req.on('close', () => {
        try { reader.cancel(); } catch (_) {}
      });
      let buffer = '';

      const pump = async () => {
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop();
            for (const line of lines) {
              if (!line.startsWith('data: ')) continue;
              const payload = line.slice(6).trim();
              if (payload === '[DONE]') continue;
              let chunk;
              try { chunk = JSON.parse(payload); } catch (_) { continue; }
              for (const event of toAnthropicEvents(chunk, state)) {
                if (res.writableEnded) break;
                res.write(event);
              }
            }
          }
          // Process any remaining data in buffer after stream ends
          if (buffer.trim().startsWith('data: ')) {
            const payload = buffer.trim().slice(6).trim();
            if (payload && payload !== '[DONE]') {
              try {
                const chunk = JSON.parse(payload);
                for (const event of toAnthropicEvents(chunk, state)) {
                  if (res.writableEnded) break;
                  res.write(event);
                }
              } catch (_) {}
            }
          }
          clearInterval(keepAlive);
          // If stream ended but we never emitted message_stop, emit it now
          if (!state.done && !res.writableEnded) {
            if (state.started) {
              if (state.blockOpen) {
                res.write(`event: content_block_stop\ndata: ${JSON.stringify({ type:'content_block_stop', index:state.blockIndex })}\n\n`);
              }
              res.write(`event: message_delta\ndata: ${JSON.stringify({ type:'message_delta', delta:{stop_reason:'end_turn', stop_sequence:null}, usage:{input_tokens:state.finalInputTokens||0, output_tokens:state.finalOutputTokens||state.outputTokens||0} })}\n\n`);
              res.write(`event: message_stop\ndata: ${JSON.stringify({ type:'message_stop' })}\n\n`);
            }
          }
          res.end();
          logUsage(row.id, claudeModel, state.finalInputTokens || 0, state.finalOutputTokens || 0);
        } catch (err) {
          clearInterval(keepAlive);
          console.error('Stream error:', err.message);
          try {
            if (!res.writableEnded) {
              if (!state.started) {
                res.write(`event: message_start\ndata: ${JSON.stringify({ type:'message_start', message:{ id:`msg_${Date.now()}`, type:'message', role:'assistant', model:claudeModel, content:[], stop_reason:null, usage:{input_tokens:0,output_tokens:0} } })}\n\n`);
                res.write(`event: content_block_start\ndata: ${JSON.stringify({ type:'content_block_start', index:0, content_block:{type:'text',text:''} })}\n\n`);
                res.write(`event: content_block_delta\ndata: ${JSON.stringify({ type:'content_block_delta', index:0, delta:{type:'text_delta', text:'[Connection dropped. Please resend your message.]'} })}\n\n`);
                res.write(`event: content_block_stop\ndata: ${JSON.stringify({ type:'content_block_stop', index:0 })}\n\n`);
              } else if (state.blockOpen) {
                res.write(`event: content_block_stop\ndata: ${JSON.stringify({ type:'content_block_stop', index:state.blockIndex })}\n\n`);
              }
              res.write(`event: message_delta\ndata: ${JSON.stringify({ type:'message_delta', delta:{stop_reason:'end_turn'}, usage:{output_tokens:state.outputTokens||0} })}\n\n`);
              res.write(`event: message_stop\ndata: ${JSON.stringify({ type:'message_stop' })}\n\n`);
              res.end();
            }
          } catch (_) { try { res.end(); } catch (__) {} }
        }
      };
      pump().catch(err => console.error('Unhandled pump error:', err.message));
    } else {
      const data = await upstream.json();
      const anthropicResp = toAnthropicResponse(data, claudeModel);
      res.status(200).json(anthropicResp);
      logUsage(row.id, claudeModel, anthropicResp.usage.input_tokens, anthropicResp.usage.output_tokens);
    }
  } catch (err) {
    console.error('Proxy error:', err.message);
    if (!res.headersSent) {
      res.status(502).json({ type: 'error', error: { type: 'api_error', message: err.message } });
    } else if (!res.writableEnded) {
      res.end();
    }
  }
});

function logUsage(tokenId, model, inputTokens, outputTokens) {
  const total = inputTokens + outputTokens;
  if (total > 0) {
    db.prepare('UPDATE tokens SET tokens_used = tokens_used + ? WHERE id = ?').run(total, tokenId);
  }
  db.prepare('INSERT INTO usage_log (token_id, model, input_tokens, output_tokens) VALUES (?, ?, ?, ?)')
    .run(tokenId, model || null, inputTokens, outputTokens);
}

module.exports = router;
