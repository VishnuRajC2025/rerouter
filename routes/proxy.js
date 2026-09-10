const express = require('express');
const fetch = require('node-fetch');
const db = require('../db');

const router = express.Router();
const ANTHROPIC_BASE = (process.env.ANTHROPIC_BASE_URL || 'https://api.anthropic.com').replace(/\/$/, '');

function extractToken(req) {
  const auth = req.headers['authorization'] || '';
  if (auth.startsWith('Bearer ')) return auth.slice(7).trim();
  return req.headers['x-api-key'] || '';
}

function maybeResetWindow(row) {
  if (!row.reset_interval_hours) return row;

  const lastReset = new Date(row.last_reset_at);
  const windowMs = row.reset_interval_hours * 3600 * 1000;
  const now = new Date();

  if (now - lastReset >= windowMs) {
    db.prepare(`
      UPDATE tokens SET requests_used = 0, tokens_used = 0, last_reset_at = datetime('now') WHERE id = ?
    `).run(row.id);
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

  // Auto-reset usage if the window has elapsed
  row = maybeResetWindow(row);

  if (row.request_limit !== null && row.requests_used >= row.request_limit) {
    return { error: 'Request limit reached for this window', status: 429 };
  }
  if (row.token_limit !== null && row.tokens_used >= row.token_limit) {
    return { error: 'Token usage limit reached for this window', status: 429 };
  }

  return { row };
}

// Forward any path under /v1 to Anthropic
router.all('/*', async (req, res) => {
  const raw = extractToken(req);
  const { error, status, row } = validateToken(raw);
  if (error) return res.status(status).json({ error });

  const targetUrl = ANTHROPIC_BASE + '/v1' + req.path;

  const forwardHeaders = {
    'content-type': req.headers['content-type'] || 'application/json',
    'anthropic-version': req.headers['anthropic-version'] || '2023-06-01',
    'authorization': `Bearer ${process.env.ANTHROPIC_AUTH_TOKEN}`,
  };
  if (req.headers['anthropic-beta']) {
    forwardHeaders['anthropic-beta'] = req.headers['anthropic-beta'];
  }

  let body;
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    body = JSON.stringify(req.body);
  }

  try {
    const upstream = await fetch(targetUrl, {
      method: req.method,
      headers: forwardHeaders,
      body,
    });

    // Increment request counter immediately
    db.prepare('UPDATE tokens SET requests_used = requests_used + 1 WHERE id = ?').run(row.id);

    const isStream = req.body?.stream === true;

    if (isStream) {
      res.status(upstream.status);
      upstream.headers.forEach((v, k) => {
        if (!['content-encoding', 'transfer-encoding', 'connection'].includes(k)) {
          res.setHeader(k, v);
        }
      });

      let inputTokens = 0;
      let outputTokens = 0;
      const chunks = [];

      upstream.body.on('data', (chunk) => {
        res.write(chunk);
        chunks.push(chunk.toString());
      });

      upstream.body.on('end', () => {
        res.end();
        for (const chunk of chunks) {
          for (const line of chunk.split('\n')) {
            if (line.startsWith('data: ') && !line.includes('[DONE]')) {
              try {
                const data = JSON.parse(line.slice(6));
                if (data.type === 'message_start' && data.message?.usage) {
                  inputTokens += data.message.usage.input_tokens || 0;
                }
                if (data.type === 'message_delta' && data.usage) {
                  outputTokens += data.usage.output_tokens || 0;
                }
              } catch (_) {}
            }
          }
        }
        logUsage(row.id, req.body?.model, inputTokens, outputTokens);
      });
    } else {
      const data = await upstream.json();
      res.status(upstream.status).json(data);

      if (upstream.ok && data.usage) {
        logUsage(row.id, data.model, data.usage.input_tokens || 0, data.usage.output_tokens || 0);
      }
    }
  } catch (err) {
    console.error('Proxy error:', err);
    res.status(502).json({ error: 'Upstream request failed', detail: err.message });
  }
});

function logUsage(tokenId, model, inputTokens, outputTokens) {
  const total = inputTokens + outputTokens;
  if (total > 0) {
    db.prepare('UPDATE tokens SET tokens_used = tokens_used + ? WHERE id = ?').run(total, tokenId);
  }
  db.prepare(`
    INSERT INTO usage_log (token_id, model, input_tokens, output_tokens)
    VALUES (?, ?, ?, ?)
  `).run(tokenId, model || null, inputTokens, outputTokens);
}

module.exports = router;
