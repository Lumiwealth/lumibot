"""One strategy that uses a custom indicator, in a backtest and live.

The strategy buys when the Donchian midline turns up and the up/down ratio is
above 50, and sells when the ratio drops below 50. The same class runs in a
backtest and against a live broker; only how you start it changes. This is a
teaching example, not a recommendation.

Backtest with free Yahoo data::

    python -m lumibot.example_indicators.indicator_strategy
"""

from __future__ import annotations

from lumibot.entities import Asset
from lumibot.strategies import Strategy

# Importing the modules registers their indicators.
from lumibot.example_indicators import donchian_midline, up_down_ratio  # noqa: F401


class CustomIndicatorStrategy(Strategy):
    parameters = {"symbol": "SPY", "length": 20}

    def initialize(self):
        self.sleeptime = "1D"
        self.vars.previous_mid = None
        self.vars.log = []

    def on_trading_iteration(self):
        asset = Asset(self.parameters["symbol"])
        length = self.parameters["length"]
        mid = self.indicators.get("donchian_mid", asset, length=length)
        ratio = self.indicators.get("up_down_ratio", asset, length=14)
        if not mid or not ratio or mid["mid"] is None or ratio["ratio"] is None:
            return  # still warming up
        rising = self.vars.previous_mid is not None and mid["mid"] > self.vars.previous_mid
        self.vars.previous_mid = mid["mid"]
        self.vars.log.append((self.get_datetime(), mid["mid"], ratio["ratio"]))

        position = self.get_position(asset)
        if rising and ratio["ratio"] > 50 and position is None:
            quantity = int(self.get_cash() // self.get_last_price(asset))
            if quantity > 0:
                self.submit_order(self.create_order(asset, quantity, "buy"))
        elif ratio["ratio"] < 50 and position is not None and position.quantity > 0:
            self.submit_order(self.create_order(asset, position.quantity, "sell"))


if __name__ == "__main__":  # pragma: no cover - needs network for Yahoo data
    from datetime import datetime

    from lumibot.backtesting import YahooDataBacktesting

    CustomIndicatorStrategy.run_backtest(YahooDataBacktesting, datetime(2024, 1, 1), datetime(2024, 12, 31))
