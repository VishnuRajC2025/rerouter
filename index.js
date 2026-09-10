require('dotenv').config();
const express = require('express');
const app = express();

if (!process.env.ANTHROPIC_AUTH_TOKEN) {
  console.error('ERROR: ANTHROPIC_AUTH_TOKEN is not set in .env');
  process.exit(1);
}

if (!process.env.ADMIN_KEY) {
  console.error('ERROR: ADMIN_KEY is not set in .env');
  process.exit(1);
}

app.use(express.json());
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

// Health check
app.get('/', (req, res) => {
  res.json({ status: 'ok', message: 'Anthropic API Rerouter' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Rerouter running on http://localhost:${PORT}`);
  console.log(`Admin API: http://localhost:${PORT}/admin/tokens  (header: x-admin-key)`);
  console.log(`Proxy API: http://localhost:${PORT}/v1/messages   (header: x-api-key or Authorization: Bearer)`);
});
