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


def _ex_date_strategy(monkeypatch, dividends):
    data_source = PandasDataBacktesting(
        datetime_start=datetime(2026, 6, 19),
        datetime_end=datetime(2026, 6, 26),
        show_progress_bar=False,
    )
    broker = BacktestingBroker(data_source=data_source)
    strategy = _DividendBatchStrategy(broker=broker)
    strategy._set_cash_position(100_000.0)
    monkeypatch.setattr(
        strategy,
        "get_yesterday_dividends",
        lambda assets: {a: dividends.get((strategy.get_datetime().date().isoformat(), a.symbol), 0.0) for a in assets},
    )
    return broker, strategy


def test_position_bought_on_the_ex_date_does_not_receive_that_days_dividend(monkeypatch):
    """Release gate, 2026-09-25: a position bought ON the ex-date still received that day's dividend,
    because _update_cash_with_dividends runs before every iteration and the position existed by the
    next one (SEH Simple: XBI bought 2026-06-22 11:31, credited 82 x 0.138 = $11.32). Only shares
    held when the day began are entitled."""
    import pytz

    ny = pytz.timezone("America/New_York")
    xbi = Asset("XBI")
    broker, strategy = _ex_date_strategy(monkeypatch, {("2026-06-22", "XBI"): 0.138})

    broker._update_datetime(ny.localize(datetime(2026, 6, 22, 9, 30)))
    strategy._update_cash_with_dividends()  # first check of the ex-date: nothing held
    broker._filled_positions.append(Position(strategy=strategy.name, asset=xbi, quantity=Decimal("82")))
    for minute in (31, 32, 33):
        broker._update_datetime(ny.localize(datetime(2026, 6, 22, 11, minute)))
        strategy._update_cash_with_dividends()

    assert strategy.cash == 100_000.0


def test_position_held_into_the_ex_date_is_paid_once_on_the_shares_held_at_the_start(monkeypatch):
    import pytz

    ny = pytz.timezone("America/New_York")
    xle = Asset("XLE")
    broker, strategy = _ex_date_strategy(monkeypatch, {("2026-06-22", "XLE"): 0.80})
    broker._filled_positions.append(Position(strategy=strategy.name, asset=xle, quantity=Decimal("100")))

    broker._update_datetime(ny.localize(datetime(2026, 6, 22, 9, 30)))
    strategy._update_cash_with_dividends()
    assert strategy.cash == 100_080.0

    # Buying 50 more on the ex-date adds no dividend; the credit is not repeated either.
    broker.get_tracked_position(strategy.name, xle)._quantity = Decimal("150")
    broker._update_datetime(ny.localize(datetime(2026, 6, 22, 14, 0)))
    strategy._update_cash_with_dividends()
    assert strategy.cash == 100_080.0
