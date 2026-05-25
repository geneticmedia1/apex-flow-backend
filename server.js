const express = require("express");
const cors = require("cors");

const app = express();

app.use(cors());
app.use(express.json());

// ============================================
// SIGNAL STORAGE
// ============================================

let latestSignal = null;
let signalHistory = [];

app.post("/webhook", (req, res) => {

    const SECRET = "T-bot Apex Flow Automation v1";

    // CHECK SECRET
    if (req.body.secret !== SECRET) {

        console.log("INVALID SECRET");

        return res.status(403).send("Forbidden");
    }

    // RESPOND IMMEDIATELY
    res.sendStatus(200);

    // LOG AFTER RESPONSE
    console.log("VALID WEBHOOK RECEIVED:");
    console.log(req.body);
    const signal = {
  time: new Date().toISOString(),
  symbol: req.body.symbol || "BTCUSD",
  action: req.body.action || "UNKNOWN",
  setup: req.body.setup || "NONE",
  price: req.body.price || 0,
  regime: req.body.regime || "NEUTRAL",
  volatility: req.body.volatility || "NORMAL",
};

latestSignal = signal;

signalHistory.unshift(signal);

if (signalHistory.length > 100) {
  signalHistory.pop();
}
});

app.get("/", (req, res) => {
    res.send("Bot running");
});

// ============================================
// STATUS ENDPOINT
// ============================================

app.get("/status", (req, res) => {
  res.json({
    status: "online",
    bot: "Apex Flow",
    signalsStored: signalHistory.length,
    latestSignal,
  });
});

// ============================================
// SIGNAL HISTORY ENDPOINT
// ============================================

app.get("/signals", (req, res) => {
  res.json(signalHistory);
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
    console.log(`Server running on ${PORT}`);
});