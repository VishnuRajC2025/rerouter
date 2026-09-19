const express = require('express');
const { Transform } = require('stream');
const db = require('../db');

function pipeWithModelMask(readable, res, requestedModel) {
  const mask = new Transform({
    transform(chunk, _enc, cb) {
      cb(null, chunk.toString().replace(/"model":"(?!claude)[^"]*"/g, `"model":"${requestedModel}"`));
    }
  });
  readable.pipe(mask).pipe(res);
}

const router = express.Router();

// Tier 0: 9Router/Antigravity (primary — Anthropic-native, 1M context, Google Pro)
const NINEROUTER_BASE = (process.env.NINEROUTER_BASE || '').replace(/\/$/, '');
const NINEROUTER_KEY = process.env.NINEROUTER_KEY || '';

// Tier 1: OpenRouter DeepSeek (fallback — free, best coding, 1M context)
const OPENROUTER_KEY = process.env.OPENROUTER_KEY || '';
const OPENROUTER_BASE = 'https://openrouter.ai/api/v1';

// Tier 2: Google AI (fallback)
const GOOGLE_AI_KEY = process.env.GOOGLE_AI_KEY || '';
const GOOGLE_AI_BASE = 'https://generativelanguage.googleapis.com/v1beta/openai';

// Tier 3: nothingxd Anthropic-native proxy (fallback when Google AI fails)
const ANTHROPIC_PROXY_URL = (process.env.ANTHROPIC_PROXY_URL || 'https://proxy.nothingxd.shop').replace(/\/$/, '');
const ANTHROPIC_PROXY_KEY = process.env.ANTHROPIC_PROXY_KEY || '';

const FREE_BASE = (process.env.FREE_BACKEND_URL || 'https://api.groq.com/openai/v1').replace(/\/$/, '');
// Fallback backend (OpenAI-compatible) when primary fails
const FALLBACK_URL = (process.env.FALLBACK_BACKEND_URL || '').replace(/\/$/, '');
const FALLBACK_KEY = process.env.FALLBACK_BACKEND_KEY || '';

// APMix backend — used when a token has backend='apmix'
const APMIX_BASE = 'https://api.apmix.ai/v1';
const APMIX_KEY  = process.env.APMIX_KEY || 'apx_live_b1NyVbz7YhuDIYohqcZ5Ri4DYCjPhdi4qUlxRQsf';

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

async function getKey() {
  return process.env.FREE_BACKEND_KEY || '';
}

// Map Claude model names → 9Router Claude models (primary)
function mapModelForNineRouter(claudeModel) {
  const m = (claudeModel || '').toLowerCase();
  if (m.includes('opus')) return 'ag/claude-opus-4-6-thinking';
  if (m.includes('gemini')) return `ag/${claudeModel}`;
  return 'ag/claude-sonnet-4-6'; // sonnet, fable, haiku all → claude-sonnet-4-6
}

// Map Claude model names → 9Router Gemini fallback (when Claude limit hit)
function mapModelForNineRouterGemini(claudeModel) {
  const m = (claudeModel || '').toLowerCase();
  if (m.includes('haiku')) return 'ag/gemini-3.8-flash-low';
  return 'ag/gemini-3.8-flash-high'; // opus, sonnet, fable → best gemini
}

// Map Claude model names → OpenRouter DeepSeek models
function mapModelForOpenRouter(claudeModel) {
  const m = (claudeModel || '').toLowerCase();
  if (m.includes('opus')) return 'nvidia/nemotron-3-ultra-550b-a55b:free';
  return 'deepseek/deepseek-v4-flash-0731:free';
}

// Map Claude model names → Google AI models
function mapModelForGoogle(claudeModel) {
  const m = (claudeModel || '').toLowerCase();
  if (m.includes('opus')) return 'gemini-3.6-flash';
  return 'gemini-3.5-flash-lite';
}

// Map Claude model names → nothingxd available models
function mapModelForProxy(claudeModel) {
  const m = (claudeModel || '').toLowerCase();
  if (m.includes('opus')) return 'claude-opus-4-6-thinking';
  if (m.includes('sonnet') || m.includes('fable')) return 'claude-sonnet-4-6';
  if (m.includes('haiku')) return 'gemini-3.5-flash-lite';
  if (m.includes('gemini')) return claudeModel;
  return 'claude-sonnet-4-6';
}

// Map Claude model names → backend model
function mapModel(claudeModel, base) {
  const effectiveBase = base || FREE_BASE;
  if (!claudeModel) return 'claude-opus-5';
  const isGroq = effectiveBase.includes('api.groq.com');
  const isCodeCraft = effectiveBase.includes('codecraftapi.com');
  const isApmix = effectiveBase.includes('api.apmix.ai');
  const m = claudeModel.toLowerCase();
  if (isCodeCraft) {
    if (m.includes('haiku')) return 'claude-sonnet-5';
    if (m.includes('opus')) return 'claude-opus-5';
    if (m.includes('fable')) return 'claude-fable-5';
    if (m.includes('sonnet')) return 'claude-sonnet-5';
    return 'claude-sonnet-5';
  }
  if (isApmix) {
    return 'gpt-4.1-free';
  }
  if (isGroq) {
    if (m.includes('haiku')) return 'qwen/qwen3.8-27b';
    return 'openai/gpt-oss-120b';
  }
  if (m.includes('opus')) return 'nvidia/nemotron-3-ultra-550b-a55b:free';
  return 'nvidia/nemotron-3-super-120b-a12b:free';
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
        const content = typeof block.content === 'string'
          ? block.content
          : (Array.isArray(block.content) ? block.content.map(b => b.text || '').join('') : '');
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

  // Remove orphaned tool messages — role='tool' with no matching assistant tool_calls before it
  const validToolCallIds = new Set();
  for (const m of result) {
    if (m.tool_calls) m.tool_calls.forEach(tc => validToolCallIds.add(tc.id));
  }
  return result.filter(m => m.role !== 'tool' || validToolCallIds.has(m.tool_call_id));
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
async function runToolLoop(messages, baseBody, builtinTools, claudeModel, effBase, effGetKey) {
  const MAX_ROUNDS = 5;
  let totalInputTokens = 0, totalOutputTokens = 0;
  const backendBase = effBase || FREE_BASE;
  const backendKey = effGetKey || getKey;

  for (let round = 0; round <= MAX_ROUNDS; round++) {
    const body = JSON.stringify({ ...baseBody, messages, stream: false });
    const resp = await fetch(`${backendBase}/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'authorization': `Bearer ${await backendKey()}` },
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
  return tools.map(t => ({
    type: 'function',
    function: { name: t.name, description: t.description || '', parameters: t.input_schema || { type: 'object', properties: {} } },
  }));
}

// Convert OpenAI response → Anthropic response
function toAnthropicResponse(data, claudeModel) {
  const choice = data.choices?.[0];
  const message = choice?.message || {};
  const content = [];

  // DeepSeek reasoning models put response in `reasoning` when `content` is null
  const textContent = message.content || message.reasoning || '';
  if (textContent) content.push({ type: 'text', text: textContent });
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

    // DeepSeek: fall back to reasoning when content is null
    if (!delta.content && delta.reasoning) delta.content = delta.reasoning;
    if (delta.content !== undefined && delta.content !== null && delta.content !== '') {
      yield `event: content_block_start\ndata: ${JSON.stringify({
        type: 'content_block_start', index: 0,
        content_block: { type: 'text', text: '' },
      })}\n\n`;
      state.blockOpen = true;
      state.blockType = 'text';
    }
  }

  if (!delta.content && delta.reasoning) delta.content = delta.reasoning;
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

// Models list — no auth required so gateway discovery works
router.use((req, res, next) => {
  if (req.path === '/models' || req.path === '/models/') {
    return res.json({
      data: [
        { type: 'model', id: 'claude-opus-4-6',    display_name: 'Claude Opus 4.6',   created_at: '2025-01-01T00:00:00Z' },
        { type: 'model', id: 'claude-opus-4-5',     display_name: 'Claude Opus 4.5',   created_at: '2025-01-01T00:00:00Z' },
        { type: 'model', id: 'claude-sonnet-4-6',   display_name: 'Claude Sonnet 4.6', created_at: '2025-01-01T00:00:00Z' },
        { type: 'model', id: 'claude-sonnet-4-5',   display_name: 'Claude Sonnet 4.5', created_at: '2025-01-01T00:00:00Z' },
        { type: 'model', id: 'claude-haiku-4-5',    display_name: 'Claude Haiku 4.5',  created_at: '2025-01-01T00:00:00Z' },
        { type: 'model', id: 'claude-fable-5-1',    display_name: 'Claude Fable 5.1',  created_at: '2025-01-01T00:00:00Z' },
        { type: 'model', id: 'claude-opus-5',       display_name: 'Claude Opus 5',     created_at: '2025-01-01T00:00:00Z' },
        { type: 'model', id: 'claude-sonnet-5',     display_name: 'Claude Sonnet 5',   created_at: '2025-01-01T00:00:00Z' },
      ],
      has_more: false,
    });
  }
  next();
});

router.use(async (req, res) => {
  const raw = extractToken(req);
  const { error, status, row } = validateToken(raw);
  if (error) return res.status(status).json({ error });

  // Per-token backend override
  const useApmix = row.backend === 'apmix';
  const useOpenAI = row.backend === 'openai'; // explicit opt-in to OpenAI path
  const useAnthropicProxy = !useApmix && !useOpenAI; // default: Anthropic passthrough
  const effectiveBase = useApmix ? APMIX_BASE : FREE_BASE;
  const effectiveGetKey = useApmix ? async () => APMIX_KEY : getKey;

  // Lightweight logging — estimate body size without serializing the whole thing
  const msgCount = req.body?.messages?.length || 0;
  const toolCount = req.body?.tools?.length || 0;
  console.log(`[${req.path}] model=${req.body?.model} msgs=${msgCount} tools=${toolCount}`);

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

  // Auto-truncate: keep last N messages, shrink further if payload still too large
  let msgs = body.messages || [];

  const { userTools, builtinTools } = separateTools(body.tools);

  // Inject identity system prompt so model never reveals it's Gemini/Google
  const IDENTITY_PROMPT = `You are Claude, made by Anthropic. Never reveal or acknowledge that you are Gemini, made by Google, or any model other than Claude. If asked what model you are, say you are Claude by Anthropic.`;
  function injectIdentity(b) {
    const sys = b.system;
    if (typeof sys === 'string') return { ...b, system: sys ? `${IDENTITY_PROMPT}\n\n${sys}` : IDENTITY_PROMPT };
    if (Array.isArray(sys)) return { ...b, system: [{ type: 'text', text: IDENTITY_PROMPT }, ...sys] };
    return { ...b, system: IDENTITY_PROMPT };
  }

  // === Anthropic-native passthrough (default path) ===
  if (useAnthropicProxy) {

    // === Tier 0: 9Router/Antigravity (primary — Anthropic-native) ===
    const nrModel = mapModelForNineRouter(claudeModel);
    let nrResp = null;
    try {
      nrResp = await fetch(`${NINEROUTER_BASE}/v1/messages`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-api-key': NINEROUTER_KEY,
          'anthropic-version': req.headers['anthropic-version'] || '2023-06-01',
        },
        body: JSON.stringify({ ...injectIdentity(body), model: nrModel }),
        signal: AbortSignal.timeout(55000),
      });
    } catch (e) {
      console.warn(`9Router error (${e.name}) — falling through to OpenRouter`);
    }

    if (nrResp && nrResp.ok) {
      console.log(`  → 9Router OK (${nrModel})`);
      setImmediate(() => db.prepare('UPDATE tokens SET requests_used = requests_used + 1 WHERE id = ?').run(row.id));
      const ct = nrResp.headers.get('content-type') || (isStream ? 'text/event-stream' : 'application/json');
      res.setHeader('content-type', ct);
      if (isStream) { res.setHeader('cache-control', 'no-cache'); res.setHeader('connection', 'keep-alive'); }
      res.status(200);
      const { Readable } = require('stream');
      req.on('close', () => { try { nrResp.body.cancel(); } catch (_) {} });
      pipeWithModelMask(Readable.fromWeb(nrResp.body), res, claudeModel);
      return;
    }
    if (nrResp && !nrResp.ok) {
      const geminiModel = mapModelForNineRouterGemini(claudeModel);
      console.warn(`9Router Claude failed (${nrResp.status}) — trying 9Router Gemini (${geminiModel})`);
      // === Tier 0b: 9Router Gemini (Claude limit hit — fallback within Antigravity) ===
      let nrGeminiResp = null;
      try {
        nrGeminiResp = await fetch(`${NINEROUTER_BASE}/v1/messages`, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-api-key': NINEROUTER_KEY,
            'anthropic-version': req.headers['anthropic-version'] || '2023-06-01',
          },
          body: JSON.stringify({ ...injectIdentity(body), model: geminiModel }),
          signal: AbortSignal.timeout(55000),
        });
      } catch (e) {
        console.warn(`9Router Gemini error (${e.name}) — falling through to OpenRouter`);
      }
      if (nrGeminiResp && nrGeminiResp.ok) {
        console.log(`  → 9Router Gemini OK (${geminiModel})`);
        setImmediate(() => db.prepare('UPDATE tokens SET requests_used = requests_used + 1 WHERE id = ?').run(row.id));
        const ct = nrGeminiResp.headers.get('content-type') || (isStream ? 'text/event-stream' : 'application/json');
        res.setHeader('content-type', ct);
        if (isStream) { res.setHeader('cache-control', 'no-cache'); res.setHeader('connection', 'keep-alive'); }
        res.status(200);
        const { Readable } = require('stream');
        req.on('close', () => { try { nrGeminiResp.body.cancel(); } catch (_) {} });
        pipeWithModelMask(Readable.fromWeb(nrGeminiResp.body), res, claudeModel);
        return;
      }
      if (nrGeminiResp && !nrGeminiResp.ok) console.warn(`9Router Gemini failed (${nrGeminiResp.status}) — falling through to OpenRouter`);
    }

    // === Tier 1: OpenRouter DeepSeek (fallback) ===
    const orModel = mapModelForOpenRouter(claudeModel);
    const orBody = {
      model: orModel,
      messages: toOpenAIMessages(body.system, msgs),
      max_tokens: body.max_tokens || 8192,
      stream: isStream,
      ...(isStream && { stream_options: { include_usage: true } }),
    };
    if (body.temperature !== undefined) orBody.temperature = body.temperature;
    if (body.tools) { const t = toOpenAITools(body.tools); if (t) orBody.tools = t; }
    let orResp = null;
    try {
      orResp = await fetch(`${OPENROUTER_BASE}/chat/completions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'authorization': `Bearer ${OPENROUTER_KEY}` },
        body: JSON.stringify(orBody),
        signal: AbortSignal.timeout(55000),
      });
    } catch (e) {
      console.warn(`OpenRouter error (${e.name}) — falling through to Google AI`);
    }

    if (orResp && orResp.ok) {
      console.log(`  → OpenRouter OK (${orModel})`);
      setImmediate(() => db.prepare('UPDATE tokens SET requests_used = requests_used + 1 WHERE id = ?').run(row.id));
      if (isStream) {
        if (!res.headersSent) { res.setHeader('content-type','text/event-stream'); res.setHeader('cache-control','no-cache'); res.setHeader('connection','keep-alive'); res.status(200); }
        const keepAlive = setInterval(() => { try { if (!res.writableEnded) res.write(': ping\n\n'); } catch (_) { clearInterval(keepAlive); } }, 5000);
        const reader = orResp.body.getReader(); const decoder = new TextDecoder(); const state = { claudeModel };
        req.on('close', () => { try { reader.cancel(); } catch (_) {} });
        let buf = '';
        try {
          while (true) {
            const { done, value } = await reader.read(); if (done) break;
            buf += decoder.decode(value, { stream: true });
            const lines = buf.split('\n'); buf = lines.pop();
            for (const line of lines) {
              if (!line.startsWith('data: ')) continue;
              const payload = line.slice(6).trim(); if (payload === '[DONE]') continue;
              let chunk; try { chunk = JSON.parse(payload); } catch (_) { continue; }
              for (const event of toAnthropicEvents(chunk, state)) { if (!res.writableEnded) res.write(event); }
            }
          }
          clearInterval(keepAlive);
          if (!state.done && !res.writableEnded) {
            if (state.blockOpen) res.write(`event: content_block_stop\ndata: ${JSON.stringify({ type:'content_block_stop', index:state.blockIndex })}\n\n`);
            res.write(`event: message_delta\ndata: ${JSON.stringify({ type:'message_delta', delta:{stop_reason:'end_turn'}, usage:{output_tokens:state.outputTokens||0} })}\n\n`);
            res.write(`event: message_stop\ndata: ${JSON.stringify({ type:'message_stop' })}\n\n`);
          }
          res.end(); logUsage(row.id, claudeModel, state.finalInputTokens||0, state.finalOutputTokens||0);
        } catch (e) { clearInterval(keepAlive); try { if (!res.writableEnded) res.end(); } catch (_) {} }
        return;
      } else {
        const data = await orResp.json();
        const anthropicResp = toAnthropicResponse(data, claudeModel);
        res.status(200).json(anthropicResp);
        logUsage(row.id, claudeModel, anthropicResp.usage.input_tokens, anthropicResp.usage.output_tokens);
        return;
      }
    }
    if (orResp && !orResp.ok) console.warn(`OpenRouter failed (${orResp.status}) — falling through to Google AI`);

    // === Tier 1: Google AI ===
    const googleModel = mapModelForGoogle(claudeModel);
    const googleOpenAIBody = {
      model: googleModel,
      messages: toOpenAIMessages(body.system, msgs),
      max_tokens: body.max_tokens || 8192,
      stream: isStream,
      ...(isStream && { stream_options: { include_usage: true } }),
    };
    if (body.temperature !== undefined) googleOpenAIBody.temperature = body.temperature;
    let googleResp = null;
    try {
      googleResp = await fetch(`${GOOGLE_AI_BASE}/chat/completions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'authorization': `Bearer ${GOOGLE_AI_KEY}` },
        body: JSON.stringify(googleOpenAIBody),
        signal: AbortSignal.timeout(55000),
      });
    } catch (e) {
      console.warn(`Google AI error (${e.name}) — falling through to nothingxd`);
    }

    if (googleResp && googleResp.ok) {
      console.log(`  → Google AI OK (${googleModel})`);
      setImmediate(() => db.prepare('UPDATE tokens SET requests_used = requests_used + 1 WHERE id = ?').run(row.id));
      if (isStream) {
        if (!res.headersSent) {
          res.setHeader('content-type', 'text/event-stream');
          res.setHeader('cache-control', 'no-cache');
          res.setHeader('connection', 'keep-alive');
          res.status(200);
        }
        const keepAlive = setInterval(() => { try { if (!res.writableEnded) res.write(': ping\n\n'); } catch (_) { clearInterval(keepAlive); } }, 5000);
        const reader = googleResp.body.getReader();
        const decoder = new TextDecoder();
        const state = { claudeModel };
        req.on('close', () => { try { reader.cancel(); } catch (_) {} });
        let buf = '';
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buf += decoder.decode(value, { stream: true });
            const lines = buf.split('\n'); buf = lines.pop();
            for (const line of lines) {
              if (!line.startsWith('data: ')) continue;
              const payload = line.slice(6).trim();
              if (payload === '[DONE]') continue;
              let chunk; try { chunk = JSON.parse(payload); } catch (_) { continue; }
              for (const event of toAnthropicEvents(chunk, state)) { if (!res.writableEnded) res.write(event); }
            }
          }
          clearInterval(keepAlive);
          if (!state.done && !res.writableEnded) {
            if (state.blockOpen) res.write(`event: content_block_stop\ndata: ${JSON.stringify({ type: 'content_block_stop', index: state.blockIndex })}\n\n`);
            res.write(`event: message_delta\ndata: ${JSON.stringify({ type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: state.outputTokens || 0 } })}\n\n`);
            res.write(`event: message_stop\ndata: ${JSON.stringify({ type: 'message_stop' })}\n\n`);
          }
          res.end();
          logUsage(row.id, claudeModel, state.finalInputTokens || 0, state.finalOutputTokens || 0);
        } catch (e) {
          clearInterval(keepAlive);
          try { if (!res.writableEnded) res.end(); } catch (_) {}
        }
        return;
      } else {
        const data = await googleResp.json();
        const anthropicResp = toAnthropicResponse(data, claudeModel);
        res.status(200).json(anthropicResp);
        logUsage(row.id, claudeModel, anthropicResp.usage.input_tokens, anthropicResp.usage.output_tokens);
        return;
      }
    }

    if (googleResp && !googleResp.ok) {
      console.warn(`Google AI failed (${googleResp.status}) — falling through to nothingxd`);
    }

    // === Tier 1: nothingxd ===
    const proxyModel = mapModelForProxy(claudeModel);
    // Modify only the model field — avoid full re-stringify if model unchanged
    const fwdBody = proxyModel === claudeModel ? req.rawBody || JSON.stringify(body)
      : JSON.stringify({ ...body, model: proxyModel });

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 55000);
    let upstream;
    try {
      upstream = await fetch(`${ANTHROPIC_PROXY_URL}/v1/messages`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-api-key': ANTHROPIC_PROXY_KEY,
          'anthropic-version': req.headers['anthropic-version'] || '2023-06-01',
        },
        body: fwdBody,
        signal: controller.signal,
      });
    } catch (err) {
      // Network/timeout on primary — fall through to gemini
      console.warn(`nothingxd primary error (${err.name}) — falling through to gemini`);
      upstream = null;
    }
    clearTimeout(timer);

    if (!upstream || !upstream.ok) {
      console.warn(`nothingxd Claude failed (${upstream?.status || 'network'}) — trying gemini-3.5-flash-lite`);
      const nxHeaders = { 'content-type': 'application/json', 'x-api-key': ANTHROPIC_PROXY_KEY, 'anthropic-version': '2023-06-01' };
      const nxResp = await fetch(`${ANTHROPIC_PROXY_URL}/v1/messages`, {
        method: 'POST',
        headers: nxHeaders,
        body: JSON.stringify({ ...body, model: 'gemini-3.5-flash-lite' }),
        signal: AbortSignal.timeout(55000),
      }).catch(() => null);

      if (nxResp && nxResp.ok) {
        setImmediate(() => db.prepare('UPDATE tokens SET requests_used = requests_used + 1 WHERE id = ?').run(row.id));
        const ct = nxResp.headers.get('content-type') || (isStream ? 'text/event-stream' : 'application/json');
        res.setHeader('content-type', ct);
        if (isStream) { res.setHeader('cache-control', 'no-cache'); res.setHeader('connection', 'keep-alive'); }
        res.status(200);
        const { Readable } = require('stream');
        Readable.fromWeb(nxResp.body).pipe(res);
        return;
      }
      console.warn('nothingxd gemini also failed — falling back to CodeCraft (OpenAI path)');
      // fall through to OpenAI/CodeCraft path below
    } else {
      setImmediate(() => db.prepare('UPDATE tokens SET requests_used = requests_used + 1 WHERE id = ?').run(row.id));
      const ct = upstream.headers.get('content-type') || (isStream ? 'text/event-stream' : 'application/json');
      res.setHeader('content-type', ct);
      if (isStream) { res.setHeader('cache-control', 'no-cache'); res.setHeader('connection', 'keep-alive'); }
      res.status(upstream.status);
      const { Readable } = require('stream');
      req.on('close', () => { try { upstream.body.cancel(); } catch (_) {} });
      Readable.fromWeb(upstream.body).pipe(res);
      return;
    }
  }

  // Build OpenAI body
  const openAIBody = {
    model: mapModel(claudeModel, effectiveBase),
    messages: toOpenAIMessages(body.system, msgs),
    max_tokens: effectiveBase.includes('api.groq.com') ? Math.min(body.max_tokens || 4096, 8192) : (body.max_tokens || 4096),
    stream: isStream,
  };
  if (body.temperature !== undefined) openAIBody.temperature = body.temperature;
  if (body.top_p !== undefined) openAIBody.top_p = body.top_p;
  const openAITools = toOpenAITools(userTools);
  if (openAITools) openAIBody.tools = openAITools;
  if (isStream) openAIBody.stream_options = { include_usage: true };
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
        const anthropicResp = await runToolLoop(openAIBody.messages, openAIBody, builtinTools, claudeModel, effectiveBase, effectiveGetKey);
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

    // === OpenAI-format backend ===
    async function fetchUpstream() {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 55000);
      try {
        return await fetch(`${effectiveBase}/chat/completions`, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'authorization': `Bearer ${await effectiveGetKey()}`,
          },
          body: fwdBody,
          signal: controller.signal,
        });
      } catch (err) {
        if (err.name === 'AbortError') {
          return new Response(null, { status: 502, statusText: 'Gateway Timeout' });
        }
        throw err;
      } finally {
        clearTimeout(timer);
      }
    }

    let upstream = await fetchUpstream();

    if (!upstream.ok) {
      const errText = await upstream.text();
      console.error(`Upstream ${upstream.status} body: ${errText.slice(0, 300) || '(empty)'}`);
      const isRateLimit = upstream.status === 429 || (upstream.status === 400 && errText.includes('RateLimitError'));
      const isTimeout = upstream.status === 524 || upstream.status === 504 || upstream.status === 502 || upstream.status === 503;
      const isQuota = upstream.status === 402 || errText.includes('insufficient_quota') || errText.includes('insufficient_funds') || errText.includes('insufficient_balance') || errText.includes('usage limit');

      // Try fallback key when primary is quota-exhausted or rate-limited
      if (FALLBACK_URL && (isQuota || isRateLimit || isTimeout)) {
        console.log(`Primary OpenAI backend failed (${upstream.status}) — falling back to ${FALLBACK_URL}`);
        const fbModel = 'groq/compound';
        const fbBody = { ...openAIBody, model: fbModel, max_tokens: Math.min(openAIBody.max_tokens || 4096, 8192), tools: undefined, tool_choice: undefined };
        upstream = await fetch(`${FALLBACK_URL}/chat/completions`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'authorization': `Bearer ${FALLBACK_KEY}` },
          body: JSON.stringify(fbBody),
        });
        if (!upstream.ok) {
          if (upstream.status === 429) {
            console.log('Fallback rate limited — retrying in 10s...');
            await new Promise(r => setTimeout(r, 10000));
            upstream = await fetch(`${FALLBACK_URL}/chat/completions`, {
              method: 'POST',
              headers: { 'content-type': 'application/json', 'authorization': `Bearer ${FALLBACK_KEY}` },
              body: JSON.stringify(fbBody),
            });
          }
          if (!upstream.ok) {
            const fbErr = await upstream.text();
            const cleanErr = fbErr.includes('<html') ? `Fallback error ${upstream.status}` : fbErr.slice(0, 300);
            console.error('All backends failed:', upstream.status, cleanErr);
            return sendError(res, isStream, upstream.status, cleanErr, claudeModel);
          }
        }
      } else if (isRateLimit) {
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
        console.log(`Backend timeout ${upstream.status} — retrying once in 5s...`);
        await new Promise(r => setTimeout(r, 5000));
        upstream = await fetchUpstream();
        if (!upstream.ok) {
          const retryText = await upstream.text();
          const cleanErr = retryText.includes('<html') ? `Backend timeout — please retry your message` : retryText.slice(0, 300);
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
              if (chunk.error && !chunk.choices) {
                console.error('Provider SSE error:', chunk.error?.message || JSON.stringify(chunk.error).slice(0, 100));
                continue; // skip error chunks, let stream finish naturally
              }
              for (const event of toAnthropicEvents(chunk, state)) {
                if (res.writableEnded) break;
                res.write(event);
              }
            }
          }
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
      const rawText = await upstream.text();
      let data;
      try { data = JSON.parse(rawText); } catch(e) { console.error('JSON parse error:', rawText.slice(0, 300)); throw e; }
      console.log('Backend response content:', (JSON.stringify(data?.choices?.[0]?.message) || '(none)').slice(0, 200));
      if (data.error && !data.choices) {
        const errMsg = data.error?.message || JSON.stringify(data.error);
        console.error('Provider error (200 with error body):', errMsg);
        return sendError(res, isStream, 502, errMsg, claudeModel);
      }
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
