const BrokerInterface = require("./brokerInterface");

class PaperBroker extends BrokerInterface {
  constructor() {
    super();

    this.startingBalance = 10000;
    this.balance = this.startingBalance;
    this.positions = [];
  }

  connect() {
    console.log("Paper broker connected");
  }

  getBalance() {
    return this.balance;
  }

  getEquity() {
    // v11.0.4: paper broker balance is cash/realized equity.
    // Open-position PnL is calculated by tradeEngine, not by adding full notional value here.
    return Number(this.balance.toFixed(2));
  }

  placeOrder(order) {
    const price = Number(order.price || order.entryPrice || 0);
    const requestedQuantity = Number(order.quantity || 0);
    const allocationDollars = Number(order.allocationDollars || 0);

    const quantity = requestedQuantity > 0
      ? requestedQuantity
      : price > 0 && allocationDollars > 0
        ? allocationDollars / price
        : 0;

    const normalizedOrder = {
      ...order,
      quantity: Number(quantity.toFixed(6)),
      entryPrice: Number((order.entryPrice || price).toFixed ? (order.entryPrice || price).toFixed(2) : (order.entryPrice || price)),
      price,
    };

    console.log("Paper order executed:", normalizedOrder);

    this.positions.push(normalizedOrder);

    return {
      success: true,
      order: normalizedOrder,
    };
  }

  closePosition(symbol) {
    this.positions = this.positions.filter(
      (position) => position.symbol !== symbol
    );

    console.log(`Closed paper position for ${symbol}`);
  }

  reset() {
    this.balance = this.startingBalance;
    this.positions = [];
  }

  getOpenPositions() {
    return this.positions;
  }
}

module.exports = PaperBroker;
