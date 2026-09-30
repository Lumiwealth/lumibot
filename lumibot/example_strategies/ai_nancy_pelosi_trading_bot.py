"""Nancy Pelosi Stock Trading Bot.

Copies the stock trades Nancy Pelosi reports to Congress. A research agent reads
her trade reports on the House Clerk website. A planning agent turns them into a
portfolio. A trading agent places the orders. Change "last_name" to copy any
other member of the House.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class NancyPelosiTradingBot(Strategy):
    parameters = {"last_name": "Pelosi", "max_weight": 0.25}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            # No web in backtests: the live House website also shows reports filed after the test date.
            allow_network=not self.is_backtesting,
            system_prompt=(
                "You find the stock trades a member of Congress reported in the last 12 months. Open a browser "
                "session, go to https://disclosures-clerk.house.gov/FinancialDisclosure, click Search, type the "
                "last name from the context, and search this year and last year. Each 'PTR' row is a trade "
                "report: read its PDF link with http_request. If you have no browser, call "
                "house_public_disclosures(last_name, year, asset_mode) for this year and last year, once with "
                "asset_mode 'stock' and once with 'option'; it only returns reports public by today. "
                "Count shares bought (P) and call options bought as buys of that stock, and shares sold (S) as "
                "sells. Use the middle of each dollar range. Skip gifts, donations, exchanges, puts, funds, "
                "and private companies. Return one line per ticker with dollars bought and dollars sold, then "
                "the date of the newest report. Your memory notes show what you reported before: if the "
                "newest report is one you already reported, reply only NOTHING NEW. Do not trade."
            ),
        )
        self.agents.create(
            name="planner",
            allow_trading=False,
            system_prompt=(
                "You turn a member's trades into a target portfolio. For each ticker, net dollars are dollars "
                "bought minus dollars sold. Keep tickers with positive net dollars and weight each by its net "
                "dollars divided by the total. No stock may be more than max_weight of the account: give what "
                "you cut to the other stocks in proportion, and keep cash only when every stock is at the cap. "
                "Return one line per ticker with its target percent of the account. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You move the account to the target percents in the plan. Sell anything not in the plan and "
                "trim anything above its target first, then buy. Only trade stocks, never options, never short, "
                "and never spend more cash than you have. Check that every order filled."
            ),
        )

    def on_trading_iteration(self):
        facts = {"last_name": self.parameters["last_name"], "max_weight": self.parameters["max_weight"]}
        research = self.agents["researcher"].run(task_prompt="Find the member's stock trades.", context=facts)
        if "NOTHING NEW" in (research.summary or ""):
            return  # No new report since the last rebalance.
        plan = self.agents["planner"].run(
            task_prompt="Build the target portfolio.", context={**facts, "research": research.summary}
        )
        self.agents["trader"].run(task_prompt="Rebalance to the plan.", context={"plan": plan.summary})


if __name__ == "__main__":
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        NancyPelosiTradingBot.backtest(YahooDataBacktesting, datetime(2026, 1, 2), datetime(2026, 9, 25))
    else:
        NancyPelosiTradingBot().run_live()
