const Database = require('better-sqlite3');
const path = require('path');

const dbPath = process.env.DB_PATH || path.join(__dirname, 'rerouter.db');
const db = new Database(dbPath);

db.exec(`
  CREATE TABLE IF NOT EXISTS tokens (
    id                   TEXT PRIMARY KEY,
    name                 TEXT NOT NULL,
    token                TEXT UNIQUE NOT NULL,
    enabled              INTEGER NOT NULL DEFAULT 1,
    request_limit        INTEGER DEFAULT NULL,
    token_limit          INTEGER DEFAULT NULL,
    requests_used        INTEGER NOT NULL DEFAULT 0,
    tokens_used          INTEGER NOT NULL DEFAULT 0,
    reset_interval_hours INTEGER DEFAULT NULL,
    last_reset_at        TEXT NOT NULL DEFAULT (datetime('now')),
    created_at           TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at           TEXT DEFAULT NULL
  );

  CREATE TABLE IF NOT EXISTS usage_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id      TEXT NOT NULL,
    model         TEXT,
    input_tokens  INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (token_id) REFERENCES tokens(id)
  );
`);

// Add new columns to existing DB if upgrading
const cols = db.pragma('table_info(tokens)').map(c => c.name);
if (!cols.includes('reset_interval_hours')) {
  db.exec(`ALTER TABLE tokens ADD COLUMN reset_interval_hours INTEGER DEFAULT NULL`);
}
if (!cols.includes('last_reset_at')) {
  db.exec(`ALTER TABLE tokens ADD COLUMN last_reset_at TEXT NOT NULL DEFAULT (datetime('now'))`);
}

module.exports = db;
