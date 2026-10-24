"""Nancy Pelosi Copy Trading Bot.

Estimates Nancy Pelosi's disclosed household stocks and calls as of each trading
date, sized to your account. "options_share" sets the allocation to calls;
disclosure delays and value ranges make the reconstruction approximate.
"""

from lumibot.strategies import Strategy


class NancyPelosiCopyTradingBot(Strategy):
    parameters = {"options_share": 0.2}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            allow_network=True,
            system_prompt=(
                "Reconstruct Nancy Pelosi's entire disclosed household stocks and calls as of as_of, not recent "
                "purchases alone. In backtests as_of is simulated time; never use a later disclosure. Start with "
                "the newest annual holdings snapshot available then and apply all disclosed transactions AFTER "
                "THE SNAPSHOT DATE, including those filed before that annual report. Check new reports and "
                "amendments daily at https://disclosures-clerk.house.gov; other public sources may help find them. "
                "Cite URLs and filing dates, and disclose estimated availability. Return a complete ledger every "
                "day: snapshot date, applied reports, latest filing checked, each stock's estimated shares/value, "
                "and each call's underlying, strike, expiration, contracts and estimated value. Apply each trade "
                "once in transaction-date order; an amendment corrects the original. A partial sale reduces a "
                "holding; only a full sale removes it. Exercised calls remove those contracts and add shares; "
                "account for splits, gifts and expiration. Include listed partnership units; skip funds, bonds, "
                "private companies and real estate. Estimate unknown quantities from value ranges and prices "
                "available then; mark remaining holdings as of as_of and explain assumptions and uncertainty. "
                "Keep stocks and calls distinct. Return the FULL ledger even when nothing changed. On lookup "
                "failure retain the previous ledger, label it stale and explain the gap; missing evidence never "
                "means a position was sold. Never invent holdings to force a trade. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Mirror the complete reconstructed portfolio as of as_of, including initial purchases from cash; "
                "do not wait for a new Pelosi trade. Put options_share of the account into her call options and the "
                "rest into her stocks, each in proportion to the middle of its dollar range. Use the same stock and "
                "expiration as each of her calls, her strike if one contract fits that call's money, otherwise the "
                "nearest higher strike that fits. Sell holdings that are not in the research and buy the ones you do "
                "not own. If a sleeve has no holdings, leave its allocation in cash. Inspect positions, pending "
                "orders and actual quotes; use the option contract multiplier when sizing. Never substitute a "
                "different expiration; disclose every strike substitution and leave unaffordable targets in cash. "
                "Leave a holding alone within 2 percentage points of target. Sell before buying and verify fills "
                "and cash before dependent buys. Never short or overspend. Research failure or 'nothing new' "
                "is not a sale: compare previous_research and preserve existing holdings on incomplete research. "
                "Report target versus actual weights, fills, substitutions, cash and unresolved mismatches."
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
            context={"as_of": as_of, "research": research.summary, "previous_research": previous,
                     "options_share": self.parameters["options_share"]},
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        NancyPelosiCopyTradingBot.backtest(AlpacaBacktesting)
    else:
        NancyPelosiCopyTradingBot().run_live()
