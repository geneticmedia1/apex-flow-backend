const BrokerInterface = require("./brokerInterface");

class BinanceBroker extends BrokerInterface {
  constructor() {
    super();

    this.connected = false;
    this.mode = "disabled";
  }

  connect() {
    console.log("Binance broker placeholder loaded.");
    console.log("Live trading is disabled until API keys and safety checks are added.");

    this.connected = false;
  }

  getBalance() {
    return {
      mode: this.mode,
      connected: this.connected,
      message: "Binance balance unavailable. Live mode disabled.",
    };
  }

  placeOrder(order) {
    console.log("Binance order blocked:", order);

    return {
      success: false,
      blocked: true,
      reason: "Live Binance execution is disabled for safety.",
      order,
    };
  }

  closePosition(symbol) {
    console.log(`Binance close blocked for ${symbol}`);

    return {
      success: false,
      blocked: true,
      reason: "Live Binance close is disabled for safety.",
      symbol,
    };
  }

  getOpenPositions() {
    return [];
  }
}

module.exports = BinanceBroker;
