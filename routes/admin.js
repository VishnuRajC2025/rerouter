const express = require('express');
const { v4: uuidv4 } = require('uuid');
const crypto = require('crypto');
const db = require('../db');

const router = express.Router();

function generateToken() {
  return 'sk-rr-' + crypto.randomBytes(24).toString('hex');
}

function daysFromNow(days) {
  const d = new Date();
  d.setDate(d.getDate() + days);
  return d.toISOString();
}

function formatToken(row) {
  const now = new Date();
  let windowResetAt = null;
  let windowSecondsLeft = null;
  let expired = false;
  let daysLeft = null;

  if (row.reset_interval_hours && row.last_reset_at) {
    const lastReset = new Date(row.last_reset_at);
    const nextReset = new Date(lastReset.getTime() + row.reset_interval_hours * 3600 * 1000);
    windowResetAt = nextReset.toISOString();
    windowSecondsLeft = Math.max(0, Math.floor((nextReset - now) / 1000));
  }

  if (row.expires_at) {
    const exp = new Date(row.expires_at);
    expired = exp < now;
    daysLeft = Math.max(0, Math.ceil((exp - now) / (1000 * 60 * 60 * 24)));
  }

  return {
    id: row.id,
    name: row.name,
    token: row.token,
    enabled: !!row.enabled,
    expired,
    days_left: daysLeft,
    expires_at: row.expires_at,
    token_limit: row.token_limit,
    tokens_used: row.tokens_used,
    reset_interval_hours: row.reset_interval_hours,
    window_resets_at: windowResetAt,
    window_seconds_left: windowSecondsLeft,
    requests_used: row.requests_used,
    created_at: row.created_at,
  };
}

// Create token
router.post('/tokens', (req, res) => {
  const { name, expires_in_days, token_limit, reset_interval_hours } = req.body;
  if (!name) return res.status(400).json({ error: 'name is required' });
  if (!expires_in_days || expires_in_days < 1) return res.status(400).json({ error: 'expires_in_days is required' });

  const id = uuidv4();
  const token = generateToken();
  const expires_at = daysFromNow(expires_in_days);

  db.prepare(`
    INSERT INTO tokens (id, name, token, token_limit, reset_interval_hours, expires_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(id, name, token, token_limit ?? null, reset_interval_hours ?? 5, expires_at);

  const row = db.prepare('SELECT * FROM tokens WHERE id = ?').get(id);
  res.status(201).json(formatToken(row));
});

// List all tokens
router.get('/tokens', (req, res) => {
  const tokens = db.prepare('SELECT * FROM tokens ORDER BY created_at DESC').all();
  res.json(tokens.map(formatToken));
});

// Get script to give to customer
router.get('/tokens/:id/script', (req, res) => {
  const row = db.prepare('SELECT * FROM tokens WHERE id = ?').get(req.params.id);
  if (!row) return res.status(404).json({ error: 'Token not found' });

  const baseUrl = process.env.PUBLIC_URL || `http://localhost:${process.env.PORT || 3000}`;

  const script = `@echo off
if not exist "%USERPROFILE%\\.claude" mkdir "%USERPROFILE%\\.claude"
(
echo {
echo   "env": {
echo     "ANTHROPIC_AUTH_TOKEN": "${row.token}",
echo     "ANTHROPIC_BASE_URL": "${baseUrl}",
echo     "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"
echo   }
echo }
) > "%USERPROFILE%\\.claude\\settings.json"
echo Done! Claude Code is configured.
pause`;

  res.setHeader('Content-Type', 'text/plain');
  res.setHeader('Content-Disposition', `attachment; filename="setup-${row.name.replace(/\s+/g, '-')}.bat"`);
  res.send(script);
});

// Enable token
router.post('/tokens/:id/enable', (req, res) => {
  const info = db.prepare('UPDATE tokens SET enabled = 1 WHERE id = ?').run(req.params.id);
  if (info.changes === 0) return res.status(404).json({ error: 'Token not found' });
  res.json({ message: 'Token enabled' });
});

// Disable token
router.post('/tokens/:id/disable', (req, res) => {
  const info = db.prepare('UPDATE tokens SET enabled = 0 WHERE id = ?').run(req.params.id);
  if (info.changes === 0) return res.status(404).json({ error: 'Token not found' });
  res.json({ message: 'Token disabled' });
});

// Delete token
router.delete('/tokens/:id', (req, res) => {
  const info = db.prepare('DELETE FROM tokens WHERE id = ?').run(req.params.id);
  if (info.changes === 0) return res.status(404).json({ error: 'Token not found' });
  res.json({ message: 'Token deleted' });
});

module.exports = router;
