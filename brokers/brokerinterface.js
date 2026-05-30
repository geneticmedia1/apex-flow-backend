class BrokerInterface {
  connect() {
    throw new Error("connect() not implemented");
  }

  getBalance() {
    throw new Error("getBalance() not implemented");
  }

  placeOrder(order) {
    throw new Error("placeOrder() not implemented");
  }

  closePosition(symbol) {
    throw new Error("closePosition() not implemented");
  }

  getOpenPositions() {
    throw new Error("getOpenPositions() not implemented");
  }
}

module.exports = BrokerInterface;