from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy


class _DummyIbkrCryptoStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"
        self.include_cash_positions = True

    def on_trading_iteration(self):
        return


def test_ibkr_crypto_non_marketable_limit_does_not_fill_until_crossed(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    idx = pd.date_range("2025-01-01 00:00", periods=3, freq="1min", tz="America/New_York")
    df = pd.DataFrame(
        {
            "open": [100.0, 90.0, 90.0],
            "high": [101.0, 91.0, 91.0],
            "low": [99.0, 89.0, 89.0],
            "close": [100.0, 90.0, 90.0],
            # At t0: bid=99 ask=101. Buy limit @ 90 is NOT marketable and should not fill.
            # At t1: ask drops to 90 (<= limit), so it should fill.
            "bid": [99.0, 89.0, 89.0],
            "ask": [101.0, 90.0, 90.0],
            "volume": [1_000, 1_000, 1_000],
        },
        index=idx,
    )

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=idx[0].to_pydatetime(),
        datetime_end=(idx[-1] + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _DummyIbkrCryptoStrategy(
        broker=broker,
        budget=10_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    data_source.get_historical_prices_between_dates(
        (base, quote),
        timestep="minute",
        quote=quote,
        start_date=idx[0].to_pydatetime(),
        end_date=idx[-1].to_pydatetime(),
    )

    # Do NOT call `broker._update_datetime(idx[0])` here: the broker guard logic will treat a
    # non-advancing timestamp as an error and bump the clock forward, which would make this
    # test assert against the wrong bar.
    buy_limit = strategy.create_order(
        base,
        Decimal("1"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=Decimal("90"),
        quote=quote,
    )
    strategy.submit_order(buy_limit)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert not buy_limit.is_filled()

    broker._update_datetime(broker.datetime + timedelta(minutes=1))
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert buy_limit.is_filled()
    assert buy_limit.get_fill_price() == pytest.approx(90.0, rel=1e-12)
