const express = require("express");
const cors = require("cors");
const http = require("http");
const { Server } = require("socket.io");

const {
  processSignal,
  restoreTradeHistory,
  getAccount,
  getActiveTrade,
  getActiveTrades,
  getActiveTradeListForDashboard,
  getTradeHistory,
  getRejectedSignals,
  getBrokerStatus,
  getPortfolioSummary,
  getAnalytics,
  getPositionManagement,
  getPositionJournal,
  getPositionTelemetry,
  getRiskStatus,
  synchronizeExchangeBalances,
  synchronizeExchangePositions,
  detectPortfolioDrift,
  runExchangeHealthCheck,
  getExchangeSynchronizationRuntime,
  synchronizeEntireExchangeRuntime,
  processSignalIntelligence,
  getSignalIntelligenceRuntime,
  orchestrateExecution,
  getExecutionOrchestratorRuntime,
  runAutonomousRecoveryCycle,
  getAutonomousRuntimeStatus,

  armExecutionEngine,
  disarmExecutionEngine,
  activateEmergencyHalt,
  resetPaperState,
  restartRuntimeSystems,
  resetRuntimeRecoverySystems,
  getRuntimeControlStatus,
  getAutoCloseConfig,
  updateAutoCloseConfig,
  forceCloseTrade,
  runAutoCloseCheck,
} = require("./tradeEngine");

const { saveSignal, loadSignals, saveRuntimeEvent, loadRuntimeEvents, clearDatabase } = require("./database");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

app.use(cors());
app.use(express.json());

let latestSignal = null;
let signalHistory = [];
let runtimeEventHistory = [];
let runtimeEventDedup = {};

async function restoreDatabaseState() {
  console.log("Loading previous database state...");

  try {
    signalHistory = await loadSignals();

    if (signalHistory.length > 0) {
      latestSignal = signalHistory[0];
    }

    console.log(`Signals restored: ${signalHistory.length}`);

    runtimeEventHistory = await loadRuntimeEvents();
    console.log(`Runtime events restored: ${runtimeEventHistory.length}`);

    await restoreTradeHistory();

    console.log("Backend recovery complete.");
  } catch (err) {
    console.error("Database recovery failed:", err.message);
  }
}


function createRuntimeEvent(category, severity, message, payload = {}) {
  const normalizedCategory = String(category || "SYSTEM").toUpperCase();
  const normalizedSeverity = String(severity || "INFO").toUpperCase();
  const normalizedMessage = String(message || "Runtime event");
  const dedupKey = `${normalizedCategory}|${normalizedSeverity}|${normalizedMessage}`;
  const nowMs = Date.now();
  const existingDedup = runtimeEventDedup[dedupKey];

  if (existingDedup && nowMs - existingDedup.lastSeen < 60000) {
    existingDedup.count += 1;
    existingDedup.lastSeen = nowMs;

    if (existingDedup.event) {
      existingDedup.event.duplicateCount = existingDedup.count;
      existingDedup.event.payload = {
        ...(existingDedup.event.payload || {}),
        duplicateCount: existingDedup.count,
        duplicateSuppressed: true,
      };
    }

    return existingDedup.event;
  }

  const event = {
    time: new Date().toISOString(),
    category: normalizedCategory,
    severity: normalizedSeverity,
    message: normalizedMessage,
    symbol: payload.symbol || payload.signal?.symbol || null,
    action: payload.action || payload.signal?.action || null,
    setup: payload.setup || payload.signal?.setup || null,
    payload,
  };

  runtimeEventHistory.unshift(event);

  if (runtimeEventHistory.length > 150) {
    runtimeEventHistory.pop();
  }

  try {
    saveRuntimeEvent(event);
  } catch (err) {
    console.error("Runtime event save failed:", err.message);
  }

  io.emit("runtime-event", event);

  runtimeEventDedup[dedupKey] = {
    lastSeen: nowMs,
    count: 1,
    event,
  };

  return event;
}

function emitLifecycleEvents(signal, processResult) {
  if (!signal) return;

  createRuntimeEvent(
    "SIGNAL",
    signal.action === "SELL" ? "WARNING" : "SUCCESS",
    `${signal.symbol} ${signal.action} ${signal.setup || "signal"} received`,
    { signal }
  );

  if (!processResult) return;

  if (!processResult.accepted) {
    createRuntimeEvent(
      "RISK",
      "WARNING",
      `${signal.symbol} ${signal.action} rejected: ${processResult.reason || "Risk check failed"}`,
      { signal, result: processResult }
    );
    return;
  }

  if (processResult.action === "OPENED") {
    createRuntimeEvent(
      "EXECUTION",
      "SUCCESS",
      `${signal.symbol} paper ${processResult.trade?.side || "LONG"} opened at ${processResult.trade?.entryPrice || signal.price}`,
      { signal, result: processResult }
    );

    createRuntimeEvent(
      "LIFECYCLE",
      "SUCCESS",
      `${signal.symbol} lifecycle moved SIGNAL → OPEN`,
      { signal, result: processResult }
    );
  }

  if (processResult.action === "CLOSED") {
    const pnl = Number(processResult.trade?.pnl || 0);
    createRuntimeEvent(
      "EXECUTION",
      pnl >= 0 ? "SUCCESS" : "WARNING",
      `${signal.symbol} position closed · PnL ${pnl.toFixed(2)}`,
      { signal, result: processResult }
    );

    createRuntimeEvent(
      "LIFECYCLE",
      pnl >= 0 ? "SUCCESS" : "WARNING",
      `${signal.symbol} lifecycle moved OPEN → CLOSED`,
      { signal, result: processResult }
    );
  }

  if (processResult.action === "AUTO_CLOSED") {
    const closedCount = Array.isArray(processResult.autoClose?.closed)
      ? processResult.autoClose.closed.length
      : 0;

    createRuntimeEvent(
      "EXECUTION",
      "WARNING",
      `${signal.symbol} auto-close protection closed ${closedCount} position${closedCount === 1 ? "" : "s"}`,
      { signal, result: processResult }
    );
  }

}

function emitDashboardUpdates(signal = null, processResult = null) {
  if (signal) {
    io.emit("new-signal", signal);
  }

  if (processResult && !processResult.accepted) {
    io.emit("signal-rejected", processResult);
  }

  io.emit("account-update", getAccount());
  io.emit("active-trade-update", getActiveTrade());
  io.emit("active-trades-update", getActiveTradeListForDashboard());
  io.emit("position-management-update", getPositionManagement());
  io.emit("position-journal-update", getPositionJournal());
  io.emit("position-telemetry-update", getPositionTelemetry());
  io.emit("trade-history-update", getTradeHistory());
  io.emit("rejected-signals-update", getRejectedSignals());
  io.emit("broker-update", getBrokerStatus());
  io.emit("portfolio-update", getPortfolioSummary());
  io.emit("analytics-update", getAnalytics());
  io.emit("risk-update", getRiskStatus());
  io.emit("runtime-events-update", runtimeEventHistory);
  io.emit("auto-close-update", getAutoCloseConfig());
}

function performPaperReset() {
  signalHistory = [];
  runtimeEventHistory = [];
  runtimeEventDedup = {};
  latestSignal = null;

  if (typeof clearDatabase === "function") {
    clearDatabase();
  }

  const resetResult = resetPaperState();

  const resetPayload = {
    ok: true,
    message: "Paper state reset",
    reset: resetResult,
    account: {
      balance: 10000,
      equity: 10000,
      realizedBalance: 10000,
      unrealizedPnl: 0,
      wins: 0,
      losses: 0,
      totalTrades: 0,
      activeTradeCount: 0,
    },
    activeTrade: null,
    activeTrades: [],
    tradeHistory: [],
    rejectedSignals: [],
  };

  io.emit("paper-reset", resetPayload);
  io.emit("runtime-events-update", runtimeEventHistory);
  emitDashboardUpdates();

  setTimeout(() => {
    io.emit("paper-reset", resetPayload);
    emitDashboardUpdates();
  }, 300);

  setTimeout(() => {
    io.emit("paper-reset", resetPayload);
    emitDashboardUpdates();
  }, 1200);

  return resetPayload;
}

app.post("/webhook", (req, res) => {
  const SECRET = "T-bot Apex Flow Automation v1";

  if (req.body.secret !== SECRET) {
    console.log("INVALID SECRET");
    return res.status(403).send("Forbidden");
  }

  const signal = {
    time: new Date().toISOString(),
    symbol: req.body.symbol || req.body.ticker || "BTCUSD",
    action: String(req.body.action || "UNKNOWN").toUpperCase(),
    setup: req.body.setup || "NONE",
    price: Number(req.body.price || 0),
    quantity:
      req.body.quantity !== undefined
        ? Number(req.body.quantity)
        : req.body.qty !== undefined
          ? Number(req.body.qty)
          : undefined,
    stopLoss:
      req.body.stopLoss !== undefined
        ? Number(req.body.stopLoss)
        : req.body.stop !== undefined
          ? Number(req.body.stop)
          : undefined,
    takeProfit:
      req.body.takeProfit !== undefined
        ? Number(req.body.takeProfit)
        : req.body.target !== undefined
          ? Number(req.body.target)
          : undefined,
    trail:
      req.body.trail !== undefined
        ? Number(req.body.trail)
        : undefined,
    equityPct:
      req.body.equity_pct !== undefined
        ? Number(req.body.equity_pct)
        : undefined,
    exitMode: req.body.exit_mode || null,
    regime: req.body.regime || "NEUTRAL",
    volatility: req.body.volatility || "NORMAL",
  };

  latestSignal = signal;

  signalHistory.unshift(signal);

  if (signalHistory.length > 100) {
    signalHistory.pop();
  }

  saveSignal(signal);

  const processResult = processSignal(signal);

  emitLifecycleEvents(signal, processResult);
  emitDashboardUpdates(signal, processResult);

    res.status(200).json({    ok: processResult.accepted,
    result: processResult,
    signal,
    account: getAccount(),
    positions: getPositionManagement(),
    positionJournal: getPositionJournal(),
    telemetry: getPositionTelemetry(),
    risk: getRiskStatus(),
  });
});

app.get("/", (req, res) => {
  res.send("Apex Flow backend running");
});

app.get("/status", (req, res) => {
  const brokerStatus = getBrokerStatus();

  res.json({
    status: "online",
    backendOnline: true,
    webhookHealthy: true,
    signalsStored: signalHistory.length,
    latestSignal,
    lastSignalTime: latestSignal?.time || null,
    brokerMode: brokerStatus?.mode || "paper",
    brokerConnected: brokerStatus?.connected !== false,
  });
});

app.get("/signals", (req, res) => {
  res.json(signalHistory);
});

app.get("/runtime-events", (req, res) => {
  res.json(runtimeEventHistory);
});

app.get("/account", (req, res) => {
  res.json(getAccount());
});

app.get("/active-trade", (req, res) => {
  res.json(getActiveTrade());
});

app.get("/active-trades", (req, res) => {
  res.json(getActiveTradeListForDashboard());
});

app.get("/active-trades-map", (req, res) => {
  res.json(getActiveTrades());
});

app.get("/positions", (req, res) => {
  res.json(getPositionManagement());
});

app.get("/position-journal", (req, res) => {
  res.json(getPositionJournal());
});

app.get("/telemetry", (req, res) => {
  res.json(getPositionTelemetry());
});

app.get("/trades", (req, res) => {
  res.json(getTradeHistory());
});

app.get("/rejected-signals", (req, res) => {
  res.json(getRejectedSignals());
});

app.get("/broker", (req, res) => {
  res.json(getBrokerStatus());
});

app.get("/portfolio", (req, res) => {
  res.json(getPortfolioSummary());
});

app.get("/analytics", (req, res) => {
  res.json(getAnalytics());
});

app.get("/risk", (req, res) => {
  res.json(getRiskStatus());
});

app.get("/exchange-sync", async (req, res) => {
  const runtime =
    await synchronizeEntireExchangeRuntime();

  res.json(runtime);
});

app.get("/exchange-balances", async (req, res) => {
  const balances =
    await synchronizeExchangeBalances();

  res.json(balances);
});

app.get("/exchange-positions", async (req, res) => {
  const positions =
    await synchronizeExchangePositions();

  res.json(positions);
});

app.get("/exchange-health", (req, res) => {
  res.json(runExchangeHealthCheck());
});

app.get("/exchange-runtime", (req, res) => {
  res.json(getExchangeSynchronizationRuntime());
});

app.get("/exchange-reconciliation", (req, res) => {
  res.json(detectPortfolioDrift());
});

app.get("/signal-intelligence", (req, res) => {
  res.json(
    getSignalIntelligenceRuntime()
  );
});

app.post("/signal-analysis", (req, res) => {
  const result =
    processSignalIntelligence(req.body || {});

  res.json(result);
});

app.get("/execution-orchestrator", (req, res) => {
  res.json(
    getExecutionOrchestratorRuntime()
  );
});

app.post("/execution-orchestrate", (req, res) => {
  const result =
    orchestrateExecution(req.body || {});

  res.json(result);
});

app.get("/runtime-monitor", (req, res) => {
  res.json(
    getAutonomousRuntimeStatus()
  );
});

app.get("/runtime-recovery", (req, res) => {
  res.json(
    runAutonomousRecoveryCycle()
  );
});



/* =====================================================
   PHASE 10.4 — RUNTIME CONTROL ROUTES
===================================================== */

app.get("/runtime-controls", (req, res) => {
  res.json(
    getRuntimeControlStatus()
  );
});

app.post("/runtime/arm", (req, res) => {
  res.json(
    armExecutionEngine()
  );
});

app.post("/runtime/disarm", (req, res) => {
  res.json(
    disarmExecutionEngine()
  );
});

app.post("/runtime/emergency-halt", (req, res) => {
  res.json(
    activateEmergencyHalt()
  );
});

app.post("/runtime/restart", (req, res) => {
  res.json(
    restartRuntimeSystems()
  );
});

app.post("/runtime/recovery/reset", (req, res) => {
  res.json(
    resetRuntimeRecoverySystems()
  );
});

app.post("/admin/reset-paper", (req, res) => {
  res.json(performPaperReset());
});



/* =====================================================
   COMPATIBILITY ADMIN ROUTES
===================================================== */


app.get("/auto-close", (req, res) => {
  res.json(getAutoCloseConfig());
});

app.post("/auto-close", (req, res) => {
  const config = updateAutoCloseConfig(req.body || {});

  createRuntimeEvent(
    "SYSTEM",
    config.enabled ? "SUCCESS" : "INFO",
    `Auto-close protection ${config.enabled ? "enabled" : "disabled"}`,
    { config }
  );

  emitDashboardUpdates();

  res.json({
    ok: true,
    config,
  });
});

app.post("/force-close", (req, res) => {
  const symbol = req.body?.symbol || "BTCUSD";
  const price = req.body?.price;
  const reason = req.body?.reason || "MANUAL_FORCE_CLOSE";

  const result = forceCloseTrade(symbol, price, reason);

  createRuntimeEvent(
    "EXECUTION",
    result.accepted ? "WARNING" : "INFO",
    result.accepted
      ? `${result.symbol} manually force-closed from command centre`
      : `${result.symbol} force-close skipped: ${result.reason}`,
    { result }
  );

  emitDashboardUpdates(null, result);

  res.json({
    ok: result.accepted,
    result,
    account: getAccount(),
    positions: getPositionManagement(),
  });
});

app.get("/force-close", (req, res) => {
  const symbol = req.query.symbol || "BTCUSD";
  const price = req.query.price;
  const reason = req.query.reason || "MANUAL_FORCE_CLOSE_BROWSER";

  const result = forceCloseTrade(symbol, price, reason);

  createRuntimeEvent(
    "EXECUTION",
    result.accepted ? "WARNING" : "INFO",
    result.accepted
      ? `${result.symbol} manually force-closed from browser endpoint`
      : `${result.symbol} force-close skipped: ${result.reason}`,
    { result }
  );

  emitDashboardUpdates(null, result);

  res.json({
    ok: result.accepted,
    result,
    account: getAccount(),
    positions: getPositionManagement(),
  });
});

app.get("/reset-paper", (req, res) => {
  res.json(performPaperReset());
});

app.get("/clear-database", (req, res) => {
  res.json(performPaperReset());
});


const PORT = process.env.PORT || 3000;

restoreDatabaseState().then(() => {
  server.listen(PORT, () => {
    console.log(`Apex Flow backend running on ${PORT}`);
  });
});

setInterval(() => {
  const autoCloseResult = runAutoCloseCheck();

  if (autoCloseResult.enabled && autoCloseResult.closed.length > 0) {
    autoCloseResult.closed.forEach((result) => {
      createRuntimeEvent(
        "EXECUTION",
        "WARNING",
        `${result.symbol} auto-close protection triggered: ${result.reason}`,
        { result }
      );
    });
  }

  io.emit("account-update", getAccount());
  io.emit("active-trades-update", getActiveTradeListForDashboard());
  io.emit("position-management-update", getPositionManagement());
  io.emit("position-journal-update", getPositionJournal());
  io.emit("position-telemetry-update", getPositionTelemetry());
  io.emit("portfolio-update", getPortfolioSummary());
  io.emit("risk-update", getRiskStatus());
  io.emit("runtime-events-update", runtimeEventHistory);
  io.emit("auto-close-update", getAutoCloseConfig());
}, 5000);