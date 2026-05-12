from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from lumibot.entities import Asset
from lumibot.strategies.strategy_executor import StrategyExecutor


@dataclass
class _DummyBroker:
    IS_BACKTESTING_BROKER: bool = True


class _DummyStrategy:
    def __init__(self):
        self.broker = _DummyBroker()
        self.hide_trades = True
        self.portfolio_value = 0.0

    def on_trading_iteration(self):
        return None

    def on_filled_order(self, position, order, price, quantity, multiplier):
        return None

    def send_discord_message(self, message, silent=False):
        return None


@dataclass
class _DummyPosition:
    asset: Asset


class _DummyOrder:
    def __init__(self, asset: Asset, side: str):
        self.asset = asset
        self.side = side

    def is_buy_order(self) -> bool:
        return self.side.lower() == "buy"


def test_strategy_executor_filled_order_does_not_divide_by_zero():
    strategy = _DummyStrategy()
    executor = StrategyExecutor(strategy)

    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    position = _DummyPosition(asset=asset)
    order = _DummyOrder(asset=asset, side="buy")

    # Regression: event notifications should not crash just because portfolio_value is zero.
    executor._on_filled_order(position, order, price=100.0, quantity=Decimal("1"), multiplier=1)
