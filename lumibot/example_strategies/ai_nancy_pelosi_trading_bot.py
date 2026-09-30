"""Nancy Pelosi Stock Trading Bot.

Copies the stock trades Nancy Pelosi reports to Congress. A research agent opens a
real web browser, searches the House Clerk website for her newest trade reports,
and reads them. A trading agent then buys and sells to match her trades.
Change "last_name" to copy any other member of the House.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class NancyPelosiTradingBot(Strategy):
    parameters = {"last_name": "Pelosi"}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            allow_network=True,
            system_prompt=(
                "You find the latest stock trades of a member of Congress. Open a browser session and go to "
                "https://disclosures-clerk.house.gov/FinancialDisclosure. Click Search, type the last name "
                "from the context, pick the filing year, and press Search. Search this year and last year. "
                "Each 'PTR' row is a trade report. Get each report's PDF link and read it with http_request. "
                "In a backtest, or if the browser is not available, call house_public_disclosures(last_name) "
                "for each year instead; it only returns reports that were public by today. "
                "A trade line shows the ticker in parentheses, P for a buy or S for a sell, and a dollar "
                "range. Skip options [OP], gifts, and exchanges. Skip any report filed after today's date. "
                "Return one line per ticker: dollars bought, dollars sold (use range midpoints), and the "
                "date of the newest report. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You copy the member's stock portfolio from the research. For each ticker, net dollars are "
                "dollars bought minus dollars sold. Give every ticker with positive net dollars a weight "
                "equal to its net dollars divided by the total of all positive net dollars. Move the account "
                "to those weights. Sell any holding whose net is zero or negative. Only buy stocks, never "
                "options, and never short. If the research found no trades, do nothing."
            ),
        )

    def on_trading_iteration(self):
        facts = {"last_name": self.parameters["last_name"]}
        research = self.agents["researcher"].run(
            task_prompt="Find the member's newest stock trades.", context=facts
        )
        self.agents["trader"].run(
            task_prompt="Copy the member's portfolio.",
            context={**facts, "research": research.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        NancyPelosiTradingBot.backtest(YahooDataBacktesting, datetime(2026, 1, 20), datetime(2026, 2, 13))
    else:
        NancyPelosiTradingBot().run_live()
