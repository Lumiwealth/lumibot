"""AI-owned bullish vertical with sparse research and inexpensive monitoring.

Entry opportunity, expiration, intended hold and attention frequency are separate
choices. One researcher informs one trader; the trader chooses and submits every
order. Historical trade-bar simulation does not prove historical bid/ask liquidity.
"""
from lumibot.strategies import Strategy


class AIBullCallSpreadStrategy(Strategy):
    parameters = {"symbol": "SPY", "model": None}

    def initialize(self):
        self.sleeptime = "1M"
        self.minutes_before_closing = 15
        self.agents.create(
            name="researcher", allow_trading=False, allow_network=True, model=self.parameters["model"],
            reasoning_effort="high", system_prompt=(
                "Research bullish exposure through a defined-risk call vertical on the supplied ETF. "
                "The investor wants ongoing bullish exposure, with a capped loss, rather than market timing. "
                "Use dated trend, volatility and relevant event evidence at the strategy time to inform "
                "30-60-day exposure, strike selection and management. Distinguish material thesis risks "
                "from ordinary uncertainty. Leave account checks, contract comparisons, exact-leg quotes/Greeks "
                "and package pricing to the trader; do not duplicate its execution checklist. "
                "Recommend an intended holding/roll plan and what would invalidate the thesis. Refresh "
                "only the evidence requested by the attention packet; reuse still-relevant dated work. "
                "Do not trade or invent unavailable evidence."
            ),
        )
        self.agents.create(
            name="trader", allow_trading=True, model=self.parameters["model"],
            reasoning_effort="high", system_prompt=(
                "You alone own contracts, quantity, risk, prices, entry, management, recovery and exit. "
                "Follow the shared options guidance. Hold at most one bullish net-debit call vertical on the "
                "supplied ETF: buy the lower-strike call and sell the higher-strike call, equal quantities "
                "and one listed expiration. Do not substitute naked options or a ratio spread. "
                "The investor wants to establish and maintain bullish exposure within the risk limit. "
                "Review opportunities roughly weekly; an existing healthy spread is held rather than "
                "repurchased. Do your own contract and execution assessment before deciding whether a "
                "suitable spread exists. Ordinary mixed headlines or a weak technical signal do not add "
                "an unrequested market-timing condition. Decline for a concrete unsuitable thesis, risk "
                "or execution constraint, and explain it. Missing execution details in the research "
                "packet are your work to obtain, not a prerequisite the researcher failed. Choose strikes, width, "
                "expiry, integer package count, limit and management conditions from actual evidence. "
                "Compare at most three useful spreads. Maximum debit loss including estimated fees "
                "must fit 2% of current account value. "
                "Obtain exact-leg quotes and useful Greeks, assess the spread and net exposure, then "
                "submit the complete package atomically with a net limit. Minute-bar backtests permit "
                "the data source's usable last-trade proxy when historical bid/ask is unavailable. "
                "Check each leg with options_evaluate_market and require usable_for_limit_pricing. "
                "Missing historical bid/ask alone is not an unmet permission. Disclose the approximation; "
                "never invent quotes or call this executable liquidity proof. "
                "Live orders require actual quotes. A healthy holding need not trigger new research daily. "
                "Choose and record a sensible profit/loss/thesis policy and intended holding duration; "
                "retain cheap risk observations between reviews. Reassess before time decay or assignment "
                "becomes inappropriate; close or roll atomically no later than 14 calendar days before "
                "expiration, earlier if the selected risk policy requires it. Manage assigned shares and "
                "pending obligations before another entry. Do not change the bullish vertical mandate. "
                "Reconcile exact working orders and partial fills before another action. Use attention_watch "
                "for meaningful underlying/package/quantity/order changes and sparse review times, with "
                "stable operation and returned order identities. Leave the appropriate review_scope for "
                "the next setup, a needed research update or management using still-relevant saved evidence. "
                "The shared team dispatcher supplies unchanged dated answers. "
                "Do not nest synchronous agent calls or add ten-minute AI heartbeats. Remove "
                "obsolete watches after confirmed closure, and schedule a legitimate next opportunity. "
                "Report actual resulting state and chosen assumptions without claiming unobserved fills."
            ),
        )

    def before_market_closes(self):
        if not self.vars.get("attention_started", False):
            self.agents.attention.watch(owner="lifecycle", watch_id="initial_review",
                operation_id="initial_setup", metric="time", comparison="gte",
                value=self.get_datetime().isoformat(), reason="First scheduled setup opportunity",
                review_scope="setup")
            self.vars.set("attention_started", True)
        self.on_trading_iteration()

    def on_trading_iteration(self):
        self.agents.attention.poll()
        self.agents.attention.dispatch_team(decision_agent="trader", research_agents=["researcher"],
            context={"symbol": self.parameters["symbol"]})

    def on_strategy_end(self):
        self.agents.attention.join()

    def on_abrupt_closing(self):
        self.agents.attention.join()


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        AIBullCallSpreadStrategy.backtest(AlpacaBacktesting)
    else:
        AIBullCallSpreadStrategy().run_live()
