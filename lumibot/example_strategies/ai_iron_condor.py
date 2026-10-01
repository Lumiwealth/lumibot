"""AI Iron Condor.

Every afternoon at 3:45 PM the AI sells a SPY iron condor that expires the next
trading day and lets it expire. Between those runs, plain Python checks SPY every
5 minutes for free and wakes the AI only when SPY runs too close to one of the
short strikes, so the AI can close the condor early.
"""

from lumibot.strategies import Strategy


class AIIronCondorStrategy(Strategy):
    parameters = {"symbol": "SPY", "stop_at": 0.4}

    def initialize(self):
        self.sleeptime = "5M"  # how often plain Python checks the stop (no AI call)
        self.minutes_before_closing = 15  # before_market_closes runs at 3:45 PM
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You trade SPY iron condors that expire the next trading day. When it is time to open one, "
                "first check the VIX: if it closed above 25 yesterday, do not trade today. Otherwise sell an "
                "iron condor on SPY that expires the next trading day: sell a put and a call near 0.14 delta, "
                "and buy a put and a call one dollar farther out to cap the loss. Size it so the most we can "
                "lose is about 3% of the account. Then hold it until it expires. If we ever hold SPY shares "
                "because an option was exercised, sell them."
            ),
        )

    def before_market_closes(self):
        self.agents["trader"].run(task_prompt="It is 3:45 PM. Open today's iron condor.")

    def on_trading_iteration(self):
        # The stop, in plain Python: how far SPY has moved from the middle of the condor toward a short strike.
        shorts = [p.asset.strike for p in self.get_positions() if p.asset.asset_type == "option" and p.quantity < 0]
        if len(shorts) != 2 or any(order.is_active() for order in self.get_orders()):
            return
        middle, half_width = (max(shorts) + min(shorts)) / 2, (max(shorts) - min(shorts)) / 2
        price = self.get_last_price(self.parameters["symbol"])
        if price and abs(price - middle) >= self.parameters["stop_at"] * half_width:
            self.agents["trader"].run(
                task_prompt=f"SPY is at {price}, close to one of our short strikes. Close the whole iron condor now."
            )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        AIIronCondorStrategy.backtest(AlpacaBacktesting)
    else:
        AIIronCondorStrategy().run_live()
