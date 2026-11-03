"""AI-owned 2:1 long-call stock replacement with inexpensive observations.

Two lower-strike long calls and one higher-strike short call retain net long
upside. Offsetting extrinsic premium does not mean free exposure or zero debit.
The sole trader owns construction, sizing, pricing, management and every order.
"""
from lumibot.strategies import Strategy


class AICallStockReplacementStrategy(Strategy):
    parameters = {"symbol": "SPY", "model": None, "estimated_contract_fee": 0.65}

    def initialize(self):
        self.sleeptime = "1M"
        self.minutes_before_closing = 15
        self.agents.create(
            name="researcher", allow_trading=False, allow_network=True, model=self.parameters["model"],
            reasoning_effort="high", system_prompt=(
                (
                'Research ongoing bullish stock-like exposure on the supplied underlying through a two- '
                'long/one-short call package. Assess dated trend, volatility, earnings/dividend risks and '
                'trade-offs against shares or a vertical. Expiration will be 90-180 calendar days away; '
                'distinguish that from intended holding time. Return a compact dated thesis, contrary '
                'evidence, uncertainty and invalidation. This is ongoing exposure within a loss limit, not a '
                'high-conviction tactical-entry mandate. Do not trade. '
            )
            ),
        )
        self.agents.create(
            name="trader", allow_trading=True, model=self.parameters["model"],
            reasoning_effort="high", system_prompt=(
                (
                'Establish and maintain bullish stock-like exposure on the supplied underlying using at most '
                'one package with exactly two lower-strike long calls per one higher-strike short call at one '
                'listed expiry 90-180 calendar days away. Compare at most three candidates. Aim for stock- '
                'like net delta and low net extrinsic cost; a premium offset is not free exposure. You choose '
                'strikes, integer package count and intended holding period. Maximum debit loss including '
                'estimated fees must fit 5% of current account value. '
                'Review a healthy holding roughly monthly; hold it rather than repurchasing it. An incomplete '
                'calendar or ordinary uncertainty is not a strong-signal entry requirement. Decline for a '
                'concrete unsuitable thesis, risk or execution constraint. Choose meaningful loss/thesis '
                'attention conditions. Close or reassess a roll no later than 30 calendar days before '
                'expiration, earlier for your risk policy or assignment/dividend concerns. After an unfilled '
                "canceled entry, allow one fresh setup attempt by the next regular session. Follow LumiBot's "
                'shared options and attention guidance; you own every trading decision. '
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
                     "estimated_contract_fee": self.parameters.get("estimated_contract_fee", 0.65),
                     "estimated_contract_fee_units": "USD per contract per side"})

    def on_strategy_end(self):
        self.agents.attention.join()

    def on_abrupt_closing(self):
        self.agents.attention.join()


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        AICallStockReplacementStrategy.backtest(AlpacaBacktesting)
    else:
        AICallStockReplacementStrategy().run_live()
