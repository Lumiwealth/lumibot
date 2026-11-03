"""AI-owned bullish vertical with sparse research and inexpensive monitoring.

Entry opportunity, expiration, intended hold and attention frequency are separate
choices. One researcher informs one trader; the trader chooses and submits every
order. Historical trade-bar simulation does not prove historical bid/ask liquidity.
"""
from lumibot.strategies import Strategy


class AIBullCallSpreadStrategy(Strategy):
    parameters = {"symbol": "SPY", "model": None, "estimated_contract_fee": 0.65}

    def initialize(self):
        self.sleeptime = "1M"
        self.minutes_before_closing = 15
        self.agents.create(
            name="researcher", allow_trading=False, allow_network=True, model=self.parameters["model"],
            reasoning_effort="high", system_prompt=(
                (
                'Research regular bullish exposure on the supplied ETF through a call debit spread. Assess '
                'dated trend, volatility and material events, with trade-offs against shares and waiting. '
                'Expiration will be 30-60 calendar days away; distinguish that from intended holding time. '
                'Return a compact dated thesis, contrary evidence, relevant uncertainty and invalidation. '
                'This is ongoing exposure within a risk limit, not a strong-signal market-timing mandate. Do '
                'not trade. '
            )
            ),
        )
        self.agents.create(
            name="trader", allow_trading=True, model=self.parameters["model"],
            reasoning_effort="high", system_prompt=(
                (
                'Establish and maintain bullish exposure on the supplied ETF using at most one equal-quantity '
                'call debit spread: long the lower strike, short the higher, at one listed expiry 30-60 '
                'calendar days away. Compare at most three candidates. Choose strikes, integer package count '
                'and intended holding period; maximum debit loss including estimated fees must fit 2% of '
                'current account value. '
                'Review setup opportunities roughly weekly; hold a healthy existing spread rather than '
                'repurchasing it. Mixed headlines or a weak technical signal do not add a tactical-entry '
                'requirement. Decline for a concrete unsuitable thesis, risk or execution constraint. Choose '
                'meaningful loss/profit/thesis attention conditions. Close or reassess a roll no later than '
                '14 calendar days before expiration, earlier when your selected risk policy requires it. '
                "Follow LumiBot's shared options and attention guidance; you own every trading decision. "
            )
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
            context={"symbol": self.parameters["symbol"],
                     "estimated_contract_fee": self.parameters.get("estimated_contract_fee", 0.65)})

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
