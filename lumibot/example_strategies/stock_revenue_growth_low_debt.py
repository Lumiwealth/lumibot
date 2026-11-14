from datetime import datetime

from lumibot.example_strategies.stock_top_market_cap import rebalance_equal_weight
from lumibot.strategies.strategy import Strategy

"""
Strategy Description

Each month, screens a universe for companies whose revenue grew more than 20% over the
last twelve months and whose debt is under half of equity, then holds the fastest growers
in equal weights. Every number comes from SEC filings that were public on that date. A
company that did not file a number (for example no debt line) is left out, never assumed
to pass.
"""


class RevenueGrowthLowDebt(Strategy):
    parameters = {
        "universe": ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "LLY", "AVGO", "COST", "NFLX", "AMD", "CRM"],
        "min_revenue_growth": 0.20,
        "max_debt_to_equity": 0.5,
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

        columns = ["revenue_growth_yoy", "debt_to_equity"]
        table = self.fundamentals.get_metrics_table(self.parameters["universe"], columns)
        table = table[columns].astype(float).dropna()  # a missing number is left out, never counted as zero
        passing = table[
            (table["revenue_growth_yoy"] > self.parameters["min_revenue_growth"])
            & (table["debt_to_equity"] < self.parameters["max_debt_to_equity"])
        ]
        targets = list(passing["revenue_growth_yoy"].nlargest(self.parameters["top_n"]).index)
        if not targets:
            self.log_message("No company passes the screen this month. Moving to cash.")
            self.sell_all()
            return
        self.log_message(f"Fast growth, low debt: {targets}")
        rebalance_equal_weight(self, targets)


if __name__ == "__main__":
    from lumibot.backtesting import YahooDataBacktesting

    RevenueGrowthLowDebt.backtest(YahooDataBacktesting, datetime(2020, 1, 1), datetime(2024, 12, 31), benchmark_asset="SPY")
