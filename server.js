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
  restartRuntimeSystems,
  resetRuntimeRecoverySystems,
  getRuntimeControlStatus,
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
  const event = {
    time: new Date().toISOString(),
    category: String(category || "SYSTEM").toUpperCase(),
    severity: String(severity || "INFO").toUpperCase(),
    message,
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
  io.emit("position-telemetry-update", getPositionTelemetry());
  io.emit("trade-history-update", getTradeHistory());
  io.emit("rejected-signals-update", getRejectedSignals());
  io.emit("broker-update", getBrokerStatus());
  io.emit("portfolio-update", getPortfolioSummary());
  io.emit("analytics-update", getAnalytics());
  io.emit("risk-update", getRiskStatus());
  io.emit("runtime-events-update", runtimeEventHistory);
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
    telemetry: getPositionTelemetry(),
    risk: getRiskStatus(),
  });
});

app.get("/", (req, res) => {
  res.send("Apex Flow backend running");
});

app.get("/status", (req, res) => {
  res.json({
    status: "online",
    signalsStored: signalHistory.length,
    latestSignal,
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
  signalHistory = [];
  latestSignal = null;

  resetPaperState();

  if (typeof clearDatabase === "function") {
    clearDatabase();
  }

  emitDashboardUpdates();

  res.json({
    ok: true,
    message: "Paper state reset",
  });
});

const PORT = process.env.PORT || 3000;

restoreDatabaseState().then(() => {
  server.listen(PORT, () => {
    console.log(`Apex Flow backend running on ${PORT}`);
  });
});

setInterval(() => {
  io.emit("account-update", getAccount());
  io.emit("active-trades-update", getActiveTradeListForDashboard());
  io.emit("position-management-update", getPositionManagement());
  io.emit("position-telemetry-update", getPositionTelemetry());
  io.emit("portfolio-update", getPortfolioSummary());
  io.emit("risk-update", getRiskStatus());
  io.emit("runtime-events-update", runtimeEventHistory);
}, 5000);