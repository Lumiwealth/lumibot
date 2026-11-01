"""Daily strategy cadence must not turn stock market fills into historical closes."""
import pandas as pd
import pytest

from lumibot.entities import Asset, Data, Order, Quote
from scripts.agent_eval_production_fixture import ProductionFixture
from scripts.run_agent_evals import build_fixture


def _execution_fixture():
    production = ProductionFixture(build_fixture("flat_stock_account"))
    source = production.broker.data_source
    asset = Asset("AAPL")
    key = source.find_asset_in_data_store(asset)
    frame = source._data_store[key].df.copy()
    current_day = pd.Timestamp(source.get_datetime()).normalize()
    for column in ("open", "high", "low", "close"):
        frame[column] = 100.0
        frame.loc[frame.index >= current_day, column] = 99.0
    frame = frame.drop(columns=["bid", "ask"], errors="ignore")
    source._data_store[key] = Data(asset, frame, timestep="minute", quote=production.strategy.quote_asset)
    return production


def test_daily_cadence_market_fill_uses_loaded_current_minute_and_keeps_history_completed():
    production = _execution_fixture()
    try:
        source = production.broker.data_source
        source._timestep = "day"
        asset = Asset("AAPL")
        history = source.get_historical_prices(asset, 2, timestep="day").df
        assert history.index[-1].date() < source.get_datetime().date()
        assert history.iloc[-1].open == 100
        assert production.strategy.get_last_price(asset) == 99
        order = production.strategy.create_order(asset, 1, "buy", order_type="market")
        production.strategy.submit_order(order)
        production.settle()
        assert order.is_filled()
        assert order.avg_fill_price == 99
        after = source.get_historical_prices(asset, 2, timestep="day").df
        pd.testing.assert_frame_equal(history, after)
    finally:
        production.close()


def test_daily_only_stock_and_nonmarket_timestep_selection_is_preserved():
    production = _execution_fixture()
    try:
        asset = Asset("AAPL")
        source = production.broker.data_source
        market = Order(production.strategy.name, asset, 1, "buy", order_type="market")
        limit = Order(production.strategy.name, asset, 1, "buy", order_type="limit", limit_price=98)
        assert production.broker._resolve_order_fill_timestep(limit, "day") == "day"
        store = source._data_store
        source._data_store = {}
        assert production.broker._resolve_order_fill_timestep(market, "day") == "day"
        source._data_store = store
    finally:
        production.close()


@pytest.mark.parametrize("native_quantity,native_unit,expected", [(1, "hour", "1 hour"), (5, "minute", "5 minute")])
def test_resolution_preserves_loaded_native_interval_and_quote_identity(native_quantity, native_unit, expected):
    production = _execution_fixture()
    try:
        broker = production.broker
        source = broker.data_source
        asset = Asset("AAPL")
        usd, eur = Asset("USD", asset_type="forex"), Asset("EUR", asset_type="forex")
        data = source._data_store[source.find_asset_in_data_store(asset)]
        data._native_timestep_quantity = native_quantity
        data._native_timestep_unit = native_unit
        source._data_store = {(asset, usd, native_unit): data}
        order = Order(production.strategy.name, asset, 1, "buy", order_type="market", quote=usd)
        assert broker._resolve_order_fill_timestep(order, "day") == expected
        order.quote = eur
        assert broker._resolve_order_fill_timestep(order, "day") == "day"
    finally:
        production.close()


def test_missing_current_minute_does_not_fill_from_previous_daily_bar():
    production = _execution_fixture()
    try:
        source = production.broker.data_source
        source._timestep = "day"
        asset = Asset("AAPL")
        key = source.find_asset_in_data_store(asset)
        data = source._data_store[key]
        # No observation exists at the execution clock. Construct the actual
        # shorter source series rather than changing one of Data's cached arrays.
        frame = data.df[data.df.index < source.get_datetime()].copy()
        source._data_store[key] = Data(asset, frame, timestep="minute", quote=production.strategy.quote_asset)
        order = production.strategy.create_order(asset, 1, "buy", order_type="market")
        production.strategy.submit_order(order)
        production.settle()
        assert not order.is_filled()
        assert order.is_active()
    finally:
        production.close()


def test_daily_quote_cannot_override_current_intraday_stock_execution(monkeypatch):
    production = _execution_fixture()
    try:
        source = production.broker.data_source
        source._timestep = "day"
        asset = Asset("AAPL")
        old_quote = Quote(asset=asset, price=200, bid=199, ask=200, raw_data={
            "bar_timestamp": pd.Timestamp("2026-08-10 09:30", tz="America/New_York"),
            "bar_timestep": "day",
        })
        monkeypatch.setattr(production.broker, "get_quote", lambda *args, **kwargs: old_quote)
        order = production.strategy.create_order(asset, 1, "buy", order_type="market")
        production.strategy.submit_order(order)
        production.settle()
        assert order.is_filled()
        assert order.avg_fill_price == 99
    finally:
        production.close()
