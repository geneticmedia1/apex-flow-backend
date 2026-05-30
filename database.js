const sqlite3 = require("sqlite3").verbose();
const path = require("path");

const dbPath = path.join(__dirname, "apexflow.db");

const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error("Database connection failed:", err.message);
  } else {
    console.log("SQLite database connected.");
  }
});

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS signals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      time TEXT,
      symbol TEXT,
      action TEXT,
      setup TEXT,
      price REAL,
      regime TEXT,
      volatility TEXT
    )
  `);

  db.run(`
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
    )
  `);
});

function saveSignal(signal) {
  db.run(
    `INSERT INTO signals
    (time, symbol, action, setup, price, regime, volatility)
    VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [
      signal.time,
      signal.symbol,
      signal.action,
      signal.setup,
      signal.price,
      signal.regime,
      signal.volatility,
    ]
  );
}

function saveTrade(trade) {
  db.run(
    `INSERT INTO trades
    (symbol, side, entryPrice, exitPrice, pnl, entryTime, exitTime, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      trade.symbol,
      trade.side,
      trade.entryPrice,
      trade.exitPrice,
      trade.pnl,
      trade.entryTime,
      trade.exitTime,
      trade.status,
    ]
  );
}

function loadSignals() {
  return new Promise((resolve, reject) => {
    db.all(
      `SELECT * FROM signals ORDER BY id DESC LIMIT 100`,
      [],
      (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      }
    );
  });
}

function loadTrades() {
  return new Promise((resolve, reject) => {
    db.all(
      `SELECT * FROM trades ORDER BY id DESC`,
      [],
      (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      }
    );
  });
}

module.exports = {
  db,
  saveSignal,
  saveTrade,
  loadSignals,
  loadTrades,
};
