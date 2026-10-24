"""Nancy Pelosi Stock Trading Bot.

Estimates Nancy Pelosi's disclosed household stock portfolio as of each trading
date, then mirrors its weights. Disclosure delays and value ranges mean this
is an estimate, not her exact current account.
"""

from lumibot.strategies import Strategy


class NancyPelosiTradingBot(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            allow_network=True,
            system_prompt=(
                "Reconstruct Nancy Pelosi's entire disclosed household stock portfolio as of as_of, not a list of "
                "recent purchases. In backtests as_of is simulated time; never use a later disclosure. Start with "
                "the newest annual holdings snapshot available then. Its year-end valuation date is different from "
                "its filing date: apply all disclosed transactions AFTER THE SNAPSHOT DATE, including those filed "
                "before the annual report. Check the filing list for new reports and amendments every day. "
                "Use official reports at https://disclosures-clerk.house.gov and cite their URLs and filing dates; "
                "you may find them through other public sources. Disclose estimated availability dates. "
                "Keep a full ledger in your answer: snapshot date, reports already applied, latest filing checked, "
                "each ticker, estimated shares or value, assumptions and target percentage. Apply a transaction "
                "once, in transaction-date order; an amendment corrects the original, not another trade. A partial "
                "sale reduces a position; only a full sale removes it. Exercised calls add shares; account for "
                "splits and gifts. Include publicly traded partnership units; skip unexercised options, funds, "
                "bonds, private companies and real estate. Estimate unknown quantities from disclosed ranges and "
                "contemporaneous prices, mark remaining shares at prices available as of as_of, and explain "
                "uncertainty. Normalize stock weights to 100%. Never replace missing evidence with a favorite-stock "
                "basket. Reuse your last complete ledger when nothing changed, but return the FULL portfolio "
                "again. On research failure say what failed, retain the prior ledger and label it stale; a missing "
                "report or lookup failure is not evidence of a sale or an empty portfolio. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Mirror the complete estimated stock portfolio in the research as of as_of. From cash, buy the "
                "whole basket on the first iteration; do not wait for a new Pelosi trade. Inspect actual positions, "
                "pending orders, prices and cash. Size to this account's value, not her account. Sell confirmed "
                "removed holdings and overweight shares before buying missing or underweight holdings; verify "
                "fills and available cash before dependent buys. Leave weights within 2 percentage points of "
                "target alone. Respect whole-share rounding and explain unfilled orders or unaffordable targets. "
                "Never short or overspend. If research failed or is incomplete, compare previous_research and "
                "preserve existing holdings; do not interpret an error or 'nothing new' as a liquidation signal. "
                "Report target versus actual weights, fills, remaining cash and any unresolved mismatch."
            ),
        )

    def on_trading_iteration(self):
        as_of = self.get_datetime().isoformat()
        previous = self.vars.get("last_research", "none yet")
        research = self.agents["researcher"].run(
            task_prompt="Reconstruct the full portfolio as of as_of and check for newly available disclosures.",
            context={"as_of": as_of, "your_last_answer": previous},
        )
        self.vars.set("last_research", research.summary)
        self.agents["trader"].run(
            task_prompt="Match the account to the complete portfolio, including the initial purchases.",
            context={"as_of": as_of, "research": research.summary, "previous_research": previous},
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        NancyPelosiTradingBot.backtest(YahooDataBacktesting)
    else:
        NancyPelosiTradingBot().run_live()
