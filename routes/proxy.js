const express = require('express');
const db = require('../db');

const router = express.Router();

const FREE_BASE = (process.env.FREE_BACKEND_URL || 'https://api.groq.com/openai/v1').replace(/\/$/, '');

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

// Convert Anthropic messages + system → OpenAI messages
function toOpenAIMessages(system, messages) {
  const result = [];
  if (system) result.push({ role: 'system', content: system });

  for (const msg of messages) {
    if (typeof msg.content === 'string') {
      result.push({ role: msg.role, content: msg.content });
      continue;
    }
    if (!Array.isArray(msg.content)) {
      result.push({ role: msg.role, content: String(msg.content || '') });
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
        result.push({ role: 'tool', tool_call_id: block.tool_use_id, content });
      }
      if (textBlocks.length > 0) {
        result.push({ role: msg.role, content: textBlocks.map(b => b.text).join('') });
      }
    } else if (toolUseBlocks.length > 0) {
      result.push({
        role: 'assistant',
        content: textBlocks.map(b => b.text).join('') || null,
        tool_calls: toolUseBlocks.map(block => ({
          id: block.id,
          type: 'function',
          function: { name: block.name, arguments: JSON.stringify(block.input) },
        })),
      });
    } else {
      result.push({ role: msg.role, content: textBlocks.map(b => b.text).join('') });
    }
  }
  return result;
}

// Convert Anthropic tools → OpenAI tools
function toOpenAITools(tools) {
  if (!tools || !tools.length) return undefined;
  return tools.map(t => ({
    type: 'function',
    function: {
      name: t.name,
      description: t.description || '',
      parameters: t.input_schema || { type: 'object', properties: {} },
    },
  }));
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
    id: data.id || `msg_${Date.now()}`,
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
        id: chunk.id || `msg_${Date.now()}`,
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
      usage: { output_tokens: outputTokens },
    })}\n\n`;
    yield `event: message_stop\ndata: ${JSON.stringify({ type: 'message_stop' })}\n\n`;
    yield `data: [DONE]\n\n`;

    state.done = true;
    state.finalInputTokens = inputTokens;
    state.finalOutputTokens = outputTokens;
  }
}

router.use(async (req, res) => {
  const raw = extractToken(req);
  const { error, status, row } = validateToken(raw);
  if (error) return res.status(status).json({ error });

  // Log request size to diagnose 32MB issue
  const bodyStr = JSON.stringify(req.body);
  const bodyBytes = Buffer.byteLength(bodyStr, 'utf8');
  console.log(`[${req.path}] model=${req.body?.model} msgs=${req.body?.messages?.length} tools=${req.body?.tools?.length || 0} bodySize=${(bodyBytes/1024).toFixed(1)}KB`);

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

  // Only translate /messages
  if (req.path !== '/messages' && req.path !== '/messages/') {
    return res.status(404).json({ type: 'error', error: { type: 'not_found_error', message: 'Not found' } });
  }

  const body = req.body;
  const claudeModel = body.model || 'claude-opus-4-5';
  const isStream = body.stream === true;

  // Auto-truncate: keep last 40 messages when conversation gets too large
  const MAX_MSGS = 40;
  let msgs = body.messages || [];
  if (msgs.length > MAX_MSGS) {
    const kept = msgs.slice(-MAX_MSGS);
    // Inject a note so the model knows context was trimmed
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
  const openAITools = toOpenAITools(body.tools);
  if (openAITools) openAIBody.tools = openAITools;
  if (isStream) openAIBody.stream_options = { include_usage: true };

  try {
    const upstream = await fetch(`${FREE_BASE}/chat/completions`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'authorization': `Bearer ${await getKey()}`,
      },
      body: JSON.stringify(openAIBody),
    });

    db.prepare('UPDATE tokens SET requests_used = requests_used + 1 WHERE id = ?').run(row.id);

    if (!upstream.ok) {
      const errText = await upstream.text();
      console.error('Upstream error:', upstream.status, errText);
      return res.status(upstream.status).json({
        type: 'error',
        error: { type: 'api_error', message: `Upstream error: ${errText}` },
      });
    }

    if (isStream) {
      res.setHeader('content-type', 'text/event-stream');
      res.setHeader('cache-control', 'no-cache');
      res.setHeader('connection', 'keep-alive');
      res.status(200);

      // Send keep-alive pings every 5s so Claude Code doesn't time out
      // while the model is thinking before its first token
      const keepAlive = setInterval(() => res.write(': ping\n\n'), 5000);

      const reader = upstream.body.getReader();
      const decoder = new TextDecoder();
      const state = { claudeModel, started: false };
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
                res.write(event);
              }
            }
          }
          clearInterval(keepAlive);
          res.end();
          logUsage(row.id, claudeModel, state.finalInputTokens || 0, state.finalOutputTokens || 0);
        } catch (err) {
          clearInterval(keepAlive);
          console.error('Stream error:', err.message);
          // Send a clean message_stop so Claude Code doesn't hang
          if (!res.writableEnded) {
            if (!state.started) {
              // Nothing sent yet — send minimal valid Anthropic error response
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
        }
      };
      pump();
    } else {
      const data = await upstream.json();
      const anthropicResp = toAnthropicResponse(data, claudeModel);
      res.status(200).json(anthropicResp);
      logUsage(row.id, claudeModel, anthropicResp.usage.input_tokens, anthropicResp.usage.output_tokens);
    }
  } catch (err) {
    console.error('Proxy error:', err);
    res.status(502).json({ type: 'error', error: { type: 'api_error', message: err.message } });
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
