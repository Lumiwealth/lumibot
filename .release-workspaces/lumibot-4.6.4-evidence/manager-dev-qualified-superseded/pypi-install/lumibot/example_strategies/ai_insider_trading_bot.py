"""Insider Trading Bot.

Buys more of the stocks whose CEOs and directors are buying their own company's
shares, and less of the stocks they are selling. A research agent reads the
insider trade reports executives file with the SEC. A trading agent then moves
the account toward the stocks insiders are buying.
"""

from lumibot.strategies import Strategy


class InsiderTradingBot(Strategy):
    parameters = {"watchlist": ["AAPL", "MSFT", "JPM", "BAC", "XOM", "CVX", "PFE", "INTC", "F", "KO"]}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "For each stock in the watchlist, look up the insider trades its executives reported to the "
                "SEC in the last 30 days. Count only real buys and sells on the open market, not stock "
                "awards, gifts, or option exercises. Report the dollars bought and sold for each stock and "
                "who traded. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Hold every stock in the watchlist. Put more money in the stocks insiders are buying and less "
                "in the stocks they are selling. Never short."
            ),
        )

    def on_trading_iteration(self):
        facts = {"watchlist": self.parameters["watchlist"]}
        research = self.agents["researcher"].run(task_prompt="Find this month's insider buys and sells.", context=facts)
        self.agents["trader"].run(
            task_prompt="Lean the account toward insider buying.", context={**facts, "research": research.summary}
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        InsiderTradingBot.backtest(YahooDataBacktesting)
    else:
        InsiderTradingBot().run_live()
