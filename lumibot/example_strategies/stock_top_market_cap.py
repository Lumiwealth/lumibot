from datetime import datetime

from lumibot.strategies.strategy import Strategy

"""
Strategy Description

Each month, buys the largest companies in a universe by market cap, in equal weights.
Market cap on each date is shares outstanding from the latest SEC filing public on that
date times the price that day, so a backtest never sees a later filing.

The universe is a list you choose. A list of today's index members has survivorship bias
in a backtest (it leaves out companies that were removed from the index).
"""


class TopMarketCap(Strategy):
    parameters = {
        "universe": ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "BRK.B", "LLY", "AVGO", "JPM", "XOM", "WMT"],
        "top_n": 5,
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.vars.month = None

    def on_trading_iteration(self):
        now = self.get_datetime()
        if self.vars.month == (now.year, now.month):
            return  # rebalance once a month
        self.vars.month = (now.year, now.month)

        # One row per company: shares outstanding from the latest filing public today x today's price.
        table = self.fundamentals.get_metrics_table(self.parameters["universe"], ["market_cap"])
        market_caps = table["market_cap"].astype(float).dropna()  # no filing or no price yet: left out
        targets = list(market_caps.nlargest(self.parameters["top_n"]).index)
        if not targets:
            self.log_message("No company has a market cap yet (no filing or no price). Holding.")
            return
        self.log_message(f"Largest by market cap: {targets}")
        rebalance_equal_weight(self, targets)


def rebalance_equal_weight(strategy, targets):
    """Sell what is not a target, then bring each target to an equal share of the portfolio."""
    for position in strategy.get_positions():
        symbol = position.asset.symbol
        if position.asset.asset_type == "stock" and symbol not in targets and position.quantity > 0:
            strategy.submit_order(strategy.create_order(symbol, position.quantity, "sell"))
    budget = strategy.get_portfolio_value() / len(targets)
    for symbol in targets:
        price = strategy.get_last_price(symbol)
        if not price:
            continue
        held = strategy.get_position(symbol)
        change = int(budget // price) - (int(held.quantity) if held else 0)
        if change:
            strategy.submit_order(strategy.create_order(symbol, abs(change), "buy" if change > 0 else "sell"))


if __name__ == "__main__":
    from lumibot.backtesting import YahooDataBacktesting

    TopMarketCap.backtest(YahooDataBacktesting, datetime(2020, 1, 1), datetime(2024, 12, 31), benchmark_asset="SPY")
