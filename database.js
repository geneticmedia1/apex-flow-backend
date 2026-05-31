const Database = require("better-sqlite3");
const path = require("path");

const dbPath = path.join(__dirname, "apexflow.db");
const db = new Database(dbPath);

console.log("SQLite database connected.");

db.exec(`
CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  time TEXT,
  symbol TEXT,
  action TEXT,
  setup TEXT,
  price REAL,
  regime TEXT,
  volatility TEXT
);

CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT,
  side TEXT,
  entryPrice REAL,
  exitPrice REAL,
  pnl REAL,
  entryTime TEXT,
  exitTime TEXT,
  status TEXT
);

CREATE TABLE IF NOT EXISTS runtime_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  time TEXT,
  category TEXT,
  severity TEXT,
  message TEXT,
  symbol TEXT,
  action TEXT,
  setup TEXT,
  payload TEXT
);
`);

function saveSignal(signal) {
  const stmt = db.prepare(`
    INSERT INTO signals
    (time, symbol, action, setup, price, regime, volatility)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  stmt.run(
    signal.time,
    signal.symbol,
    signal.action,
    signal.setup,
    signal.price,
    signal.regime,
    signal.volatility
  );
}

function saveTrade(trade) {
  const stmt = db.prepare(`
    INSERT INTO trades
    (symbol, side, entryPrice, exitPrice, pnl, entryTime, exitTime, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  stmt.run(
    trade.symbol,
    trade.side,
    trade.entryPrice,
    trade.exitPrice,
    trade.pnl,
    trade.entryTime,
    trade.exitTime,
    trade.status
  );
}

function loadSignals() {
  const stmt = db.prepare(
    `SELECT * FROM signals ORDER BY id DESC LIMIT 100`
  );
  return stmt.all();
}

function loadTrades() {
  const stmt = db.prepare(
    `SELECT * FROM trades ORDER BY id DESC`
  );
  return stmt.all();
}

function saveRuntimeEvent(event) {
  const stmt = db.prepare(`
    INSERT INTO runtime_events
    (time, category, severity, message, symbol, action, setup, payload)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  stmt.run(
    event.time || new Date().toISOString(),
    event.category || "SYSTEM",
    event.severity || "INFO",
    event.message || "Runtime event",
    event.symbol || null,
    event.action || null,
    event.setup || null,
    JSON.stringify(event.payload || {})
  );
}

function loadRuntimeEvents() {
  const stmt = db.prepare(
    `SELECT * FROM runtime_events ORDER BY id DESC LIMIT 150`
  );

  return stmt.all().map((event) => ({
    ...event,
    payload: event.payload ? JSON.parse(event.payload) : {},
  }));
}

module.exports = {
  db,
  saveSignal,
  saveTrade,
  loadSignals,
  loadTrades,
  saveRuntimeEvent,
  loadRuntimeEvents,
};