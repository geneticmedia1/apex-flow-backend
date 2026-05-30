// ============================================
// ANALYTICS ENGINE
// ============================================

function buildEquityCurve(trades, startingBalance = 10000) {
  let equity = startingBalance;

  return trades
    .slice()
    .reverse()
    .map((trade, index) => {
      equity += trade.pnl;

      return {
        trade: index + 1,
        equity: Number(equity.toFixed(2)),
        pnl: Number(trade.pnl.toFixed(2)),
        symbol: trade.symbol,
      };
    });
}

function calculateAnalytics(trades, account) {
  const totalTrades = trades.length;

  const wins = trades.filter(
    (trade) => trade.pnl > 0
  ).length;

  const losses = trades.filter(
    (trade) => trade.pnl <= 0
  ).length;

  const grossProfit = trades
    .filter((trade) => trade.pnl > 0)
    .reduce((sum, trade) => sum + trade.pnl, 0);

  const grossLoss = Math.abs(
    trades
      .filter((trade) => trade.pnl < 0)
      .reduce((sum, trade) => sum + trade.pnl, 0)
  );

  const winRate =
    totalTrades > 0
      ? (wins / totalTrades) * 100
      : 0;

  const profitFactor =
    grossLoss > 0
      ? grossProfit / grossLoss
      : grossProfit;

  // ============================================
  // EQUITY CURVE
  // ============================================

  const equityCurve = buildEquityCurve(trades);

  // ============================================
  // MAX DRAWDOWN
  // ============================================

  let peak = 10000;
  let maxDrawdown = 0;

  equityCurve.forEach((point) => {
    if (point.equity > peak) {
      peak = point.equity;
    }

    const drawdown =
      ((peak - point.equity) / peak) * 100;

    if (drawdown > maxDrawdown) {
      maxDrawdown = drawdown;
    }
  });

  // ============================================
  // SYMBOL PERFORMANCE
  // ============================================

  const symbolStats = {};

  trades.forEach((trade) => {
    if (!symbolStats[trade.symbol]) {
      symbolStats[trade.symbol] = {
        trades: 0,
        pnl: 0,
        wins: 0,
        losses: 0,
      };
    }

    symbolStats[trade.symbol].trades++;

    symbolStats[trade.symbol].pnl += trade.pnl;

    if (trade.pnl > 0) {
      symbolStats[trade.symbol].wins++;
    } else {
      symbolStats[trade.symbol].losses++;
    }
  });

  return {
    totalTrades,
    wins,
    losses,

    winRate: Number(winRate.toFixed(2)),

    grossProfit: Number(grossProfit.toFixed(2)),

    grossLoss: Number(grossLoss.toFixed(2)),

    profitFactor: Number(
      profitFactor.toFixed(2)
    ),

    maxDrawdown: Number(
      maxDrawdown.toFixed(2)
    ),

    currentEquity: Number(
      account.balance.toFixed(2)
    ),

    equityCurve,

    symbolStats,
  };
}

module.exports = {
  calculateAnalytics,
};