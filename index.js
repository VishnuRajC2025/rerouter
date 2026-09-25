require('dotenv').config();
const express = require('express');
const app = express();

process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection (caught):', err?.message || err);
});
process.on('uncaughtException', (err) => {
  console.error('Uncaught exception (caught):', err?.message || err);
});

if (!process.env.ANTHROPIC_AUTH_TOKEN) {
  console.error('ERROR: ANTHROPIC_AUTH_TOKEN is not set in .env');
  process.exit(1);
}

if (!process.env.ADMIN_KEY) {
  console.error('ERROR: ADMIN_KEY is not set in .env');
  process.exit(1);
}

app.use(express.json({ limit: '50mb', verify: (req, _res, buf) => { req.rawBody = buf; } }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
// Handle malformed JSON bodies (e.g. from Claude Code health checks) — don't crash
app.use((err, req, res, next) => {
  if (err.type === 'entity.parse.failed') { req.body = {}; return next(); }
  next(err);
});
app.use(express.static('public'));

// Admin routes — protected by ADMIN_KEY header or ?key= query param
app.use('/admin', (req, res, next) => {
  const key = req.headers['x-admin-key'] || req.query.key;
  if (key !== process.env.ADMIN_KEY) {
    return res.status(401).json({ error: 'Invalid admin key' });
  }
  next();
});
app.use('/admin', require('./routes/admin'));

// Proxy — mirrors Anthropic's /v1/* paths
app.use('/v1', require('./routes/proxy'));

// Per-user stats page — authenticated by their own token
app.get('/stats', (req, res) => {
  const raw = (req.headers['authorization'] || '').replace('Bearer ', '').trim()
    || req.query.token || '';
  if (!raw) return res.status(401).send('Provide your token as ?token=sk-rr-...');

  const db = require('./db');
  const row = db.prepare('SELECT * FROM tokens WHERE token = ?').get(raw);
  if (!row) return res.status(401).send('Invalid token');

  const now = new Date();
  let windowResetAt = null, daysLeft = null;
  if (row.reset_interval_hours && row.last_reset_at) {
    const next = new Date(new Date(row.last_reset_at).getTime() + row.reset_interval_hours * 3600000);
    windowResetAt = next.toUTCString();
  }
  if (row.expires_at) {
    daysLeft = Math.max(0, Math.ceil((new Date(row.expires_at) - now) / 86400000));
  }

  const logs = db.prepare(
    'SELECT model, input_tokens, output_tokens, created_at FROM usage_log WHERE token_id = ? ORDER BY created_at DESC LIMIT 20'
  ).all(row.id);

  res.send(`<!DOCTYPE html><html><head><meta charset="utf-8">
<title>My Usage — ${row.name}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  body{font-family:system-ui,sans-serif;background:#0f0f0f;color:#e5e5e5;max-width:600px;margin:40px auto;padding:0 20px}
  h1{font-size:1.4rem;margin-bottom:4px}
  .sub{color:#888;font-size:.85rem;margin-bottom:24px}
  .card{background:#1a1a1a;border:1px solid #2a2a2a;border-radius:10px;padding:20px;margin-bottom:16px}
  .row{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #222}
  .row:last-child{border-bottom:none}
  .label{color:#888;font-size:.9rem}
  .value{font-weight:600}
  .ok{color:#4ade80}.warn{color:#facc15}.bad{color:#f87171}
  table{width:100%;border-collapse:collapse;font-size:.85rem}
  th{text-align:left;color:#888;padding:6px 8px;border-bottom:1px solid #2a2a2a}
  td{padding:6px 8px;border-bottom:1px solid #1f1f1f}
</style></head><body>
<h1>My Usage</h1>
<div class="sub">Token: ${raw.slice(0, 12)}...</div>

<div class="card">
  <div class="row"><span class="label">Name</span><span class="value">${row.name}</span></div>
  <div class="row"><span class="label">Status</span><span class="value ${row.enabled ? 'ok' : 'bad'}">${row.enabled ? 'Active' : 'Disabled'}</span></div>
  <div class="row"><span class="label">Days left</span><span class="value ${daysLeft > 3 ? 'ok' : 'warn'}">${daysLeft !== null ? daysLeft + ' days' : '—'}</span></div>
  <div class="row"><span class="label">Requests used</span><span class="value">${row.requests_used}</span></div>
  <div class="row"><span class="label">Tokens used (window)</span><span class="value">${row.tokens_used.toLocaleString()}${row.token_limit ? ' / ' + row.token_limit.toLocaleString() : ''}</span></div>
  <div class="row"><span class="label">Window resets</span><span class="value">${windowResetAt || '—'}</span></div>
</div>

<div class="card">
  <b>Recent requests</b>
  <table><tr><th>Time</th><th>Model</th><th>In</th><th>Out</th></tr>
  ${logs.map(l => `<tr><td>${l.created_at.slice(0,16)}</td><td>${l.model || '—'}</td><td>${l.input_tokens}</td><td>${l.output_tokens}</td></tr>`).join('')}
  ${logs.length === 0 ? '<tr><td colspan="4" style="color:#888">No requests yet</td></tr>' : ''}
  </table>
</div>
</body></html>`);
});

// Health check
app.get('/', (req, res) => {
  res.json({ status: 'ok' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Rerouter running on http://localhost:${PORT}`);
  console.log(`Admin API: http://localhost:${PORT}/admin/tokens  (header: x-admin-key)`);
  console.log(`Proxy API: http://localhost:${PORT}/v1/messages   (header: x-api-key or Authorization: Bearer)`);
});
