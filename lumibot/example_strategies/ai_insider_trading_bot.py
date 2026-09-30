"""Insider Trading Bot.

Buys more of the stocks whose CEOs and directors are buying their own company's
shares, and less of the stocks they are selling. A research agent reads the
insider trade reports that executives must file with the SEC. A trading agent
then moves the account toward the stocks insiders are buying.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class InsiderTradingBot(Strategy):
    parameters = {"watchlist": ["AAPL", "MSFT", "JPM", "BAC", "XOM", "CVX", "PFE", "INTC", "F", "KO"]}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "You track insider trades. For each ticker in the watchlist, call "
                "get_filings(symbol, form='4', limit=20) and open the reports from the last 30 days with "
                "get_filing_document. Code P is an insider buying shares on the open market with their own "
                "money. Code S is an insider selling. Ignore stock awards, gifts, option exercises, tax "
                "withholding, and sales under a pre-set 10b5-1 plan. For each ticker, report the dollars "
                "bought, the dollars sold, and who traded (CEO, CFO, director). Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You own the watchlist stocks. Start with equal weights. Give more weight to stocks that "
                "insiders are buying and less to stocks with large insider sales. Weights add up to about "
                "100% of the account. Move the account to those weights. Only trade watchlist stocks and "
                "never short."
            ),
        )

    def on_trading_iteration(self):
        facts = {"watchlist": self.parameters["watchlist"]}
        research = self.agents["researcher"].run(
            task_prompt="Find this month's insider buys and sells.", context=facts
        )
        self.agents["trader"].run(
            task_prompt="Lean the account toward insider buying.",
            context={**facts, "research": research.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        InsiderTradingBot.backtest(YahooDataBacktesting, datetime(2026, 1, 5), datetime(2026, 2, 13))
    else:
        InsiderTradingBot().run_live()
