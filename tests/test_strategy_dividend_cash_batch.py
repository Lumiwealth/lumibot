from datetime import datetime
from decimal import Decimal

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Position
from lumibot.strategies.strategy import Strategy


class _DividendBatchStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        pass


def test_update_cash_with_dividends_batches_single_cash_update(monkeypatch):
    data_source = PandasDataBacktesting(
        datetime_start=datetime(2025, 1, 1),
        datetime_end=datetime(2025, 1, 3),
        show_progress_bar=False,
    )
    broker = BacktestingBroker(data_source=data_source)
    strategy = _DividendBatchStrategy(broker=broker)

    asset_a = Asset("AAPL")
    asset_b = Asset("MSFT")
    strategy._set_cash_position(100.0)
    broker._filled_positions.append(Position(strategy=strategy.name, asset=asset_a, quantity=Decimal("2")))
    broker._filled_positions.append(Position(strategy=strategy.name, asset=asset_b, quantity=Decimal("3")))

    monkeypatch.setattr(strategy, "get_yesterday_dividends", lambda assets: {asset_a: 1.5, asset_b: 2.0})

    calls = {"count": 0}
    original_set_cash_position = strategy._set_cash_position

    def _counting_set_cash_position(value):
        calls["count"] += 1
        return original_set_cash_position(value)

    monkeypatch.setattr(strategy, "_set_cash_position", _counting_set_cash_position)

    result = strategy._update_cash_with_dividends()

    assert result == 109.0
    assert calls["count"] == 1
