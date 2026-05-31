const BrokerInterface = require("./brokerinterface");

class PaperBroker extends BrokerInterface {
  constructor() {
    super();

    this.balance = 10000;

    this.positions = [];
  }

  connect() {
    console.log("Paper broker connected");
  }

  getBalance() {
    return this.balance;
  }

  placeOrder(order) {
    console.log("Paper order executed:", order);

    this.positions.push(order);

    return {
      success: true,
      order,
    };
  }

  closePosition(symbol) {
    this.positions = this.positions.filter(
      (position) => position.symbol !== symbol
    );

    console.log(`Closed paper position for ${symbol}`);
  }

  getOpenPositions() {
    return this.positions;
  }
}

module.exports = PaperBroker;
