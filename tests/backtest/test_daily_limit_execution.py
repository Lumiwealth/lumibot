"""Daily cadence must not execute a new limit against yesterday's quote."""
from datetime import timedelta

import pandas as pd
import pytest

from lumibot.entities import Asset, Data, Position, Quote, TradingFee
from scripts.agent_eval_production_fixture import ProductionFixture
from scripts.run_agent_evals import build_fixture


@pytest.mark.parametrize("observation", ["previous", "missing", "future"])
@pytest.mark.parametrize("side,prior,opening,limit", [("buy", 100., 105., 101.), ("sell", 110., 95., 100.)])
def test_daily_limit_waits_for_current_price_and_reconciles_cash(monkeypatch, observation, side, prior, opening, limit):
    production = ProductionFixture(build_fixture("flat_stock_account"))
    try:
        source = production.broker.data_source
        now = source.get_datetime()
        asset = Asset("QZX")
        previous = now - timedelta(days=1)
        later = now + timedelta(minutes=1)
        frame = pd.DataFrame(
            {"open": [prior, opening, 100.5], "high": [prior, opening, 100.5],
             "low": [prior, opening, 100.5], "close": [prior, opening, 100.5],
             "volume": [1000, 1000, 1000]},
            index=pd.DatetimeIndex([previous, now, later]),
        )
        source._data_store[(asset, production.strategy.quote_asset)] = Data(
            asset, frame, timestep="minute", quote=production.strategy.quote_asset)
        source._timestep = "day"
        # Same fields returned by PandasData.get_quote: source observation time
        # is distinct from the time at which a quote was requested.
        quote_time = {"previous": previous, "missing": None, "future": now + timedelta(days=1)}[observation]
        stale = Quote(asset=asset, price=prior, bid=prior, ask=prior, raw_data={
            "bar_timestamp": quote_time, "bar_timestep": "day"})
        monkeypatch.setattr(production.broker, "get_quote", lambda *args, **kwargs: stale)
        production.strategy.buy_trading_fees = [TradingFee(percent_fee=0.001)]
        production.strategy.sell_trading_fees = [TradingFee(percent_fee=0.001)]
        if side == "sell":
            production.broker._filled_positions.append(Position(production.strategy.name, asset, 3))
        starting_cash = float(production.strategy.get_cash())
        history = source.get_historical_prices(asset, 1, timestep="day").df.copy()
        order = production.strategy.create_order(asset, 3, side, order_type="limit", limit_price=limit)
        production.strategy.submit_order(order)
        production.settle()
        assert not order.is_filled()
        assert order.is_active()
        assert float(production.strategy.get_cash()) == starting_cash
        position = production.strategy.get_position(asset)
        assert (0 if position is None else position.quantity) == (3 if side == "sell" else 0)
        pd.testing.assert_frame_equal(history, source.get_historical_prices(asset, 1, timestep="day").df)

        production.broker._update_datetime(later)
        production.settle()
        assert order.is_filled()
        assert order.avg_fill_price == 100.5
        position = production.strategy.get_position(asset)
        assert (0 if position is None else position.quantity) == (0 if side == "sell" else 3)
        change = 301.5 if side == "sell" else -301.5
        assert float(production.strategy.get_cash()) == pytest.approx(starting_cash + change - 0.3015)
    finally:
        production.close()


def test_daily_limit_does_not_fill_when_loaded_intraday_execution_is_missing(monkeypatch):
    production = ProductionFixture(build_fixture("flat_stock_account"))
    try:
        source = production.broker.data_source
        asset = Asset("QZM")
        previous = source.get_datetime() - timedelta(days=1)
        frame = pd.DataFrame({key: [100.] for key in ("open", "high", "low", "close", "volume")},
                             index=pd.DatetimeIndex([previous]))
        source._data_store[(asset, production.strategy.quote_asset)] = Data(
            asset, frame, timestep="minute", quote=production.strategy.quote_asset)
        source._timestep = "day"
        stale = Quote(asset=asset, bid=100, ask=100, raw_data={"bar_timestamp": previous, "bar_timestep": "day"})
        monkeypatch.setattr(production.broker, "get_quote", lambda *args, **kwargs: stale)
        cash = production.strategy.get_cash()
        order = production.strategy.create_order(asset, 3, "buy", order_type="limit", limit_price=101)
        production.strategy.submit_order(order)
        production.settle()
        assert order.is_active() and not order.is_filled()
        assert production.strategy.get_cash() == cash
    finally:
        production.close()


@pytest.mark.parametrize("close_hour", [13, 16])
def test_daily_only_limit_does_not_read_previous_quote_or_future_session_low(monkeypatch, close_hour):
    production = ProductionFixture(build_fixture("flat_stock_account"))
    try:
        source = production.broker.data_source
        now = pd.Timestamp(source.get_datetime()).tz_convert("America/New_York")
        asset = Asset("QZY")
        previous = now.normalize() - timedelta(days=1)
        frame = pd.DataFrame(
            {"open": [100., 105.], "high": [100., 110.], "low": [100., 99.],
             "close": [100., 108.], "volume": [1000, 1000]},
            index=pd.DatetimeIndex([previous, now.normalize()]),
        )
        source._data_store[(asset, production.strategy.quote_asset)] = Data(
            asset, frame, timestep="day", quote=production.strategy.quote_asset)
        source._timestep = "day"
        opening = now.normalize() + timedelta(hours=9, minutes=30)
        closing = now.normalize() + timedelta(hours=close_hour)
        source.datetime_end = closing
        production.broker.initialize_market_calendars(pd.DataFrame(
            {"market_open": [opening]}, index=pd.DatetimeIndex([closing])))
        stale = Quote(asset=asset, price=100, bid=100, ask=100, raw_data={
            "bar_timestamp": previous, "bar_timestep": "day"})
        monkeypatch.setattr(production.broker, "get_quote", lambda *args, **kwargs: stale)
        cash = float(production.strategy.get_cash())
        order = production.strategy.create_order(asset, 3, "buy", order_type="limit", limit_price=101)
        production.strategy.submit_order(order)
        production.settle()
        assert not order.is_filled()
        assert float(production.strategy.get_cash()) == cash
        # Once the whole daily bar is known, its range can establish a touch.
        # This is a close-time OHLC convention, not an invented intraday quote.
        production.broker._update_datetime(closing)
        assert production.broker.datetime == closing
        production.settle()
        assert order.is_filled()
        assert order.avg_fill_price == 101
        assert production.strategy.get_position(asset).quantity == 3
    finally:
        production.close()


def test_daily_limit_eval_keeps_daily_cadence_after_strategy_initialization():
    production = ProductionFixture(build_fixture("stock_daily_limit_current_execution"))
    try:
        source = production.broker.data_source
        assert source._timestep == "day"
        asset = Asset("QZX")
        order = production.strategy.create_order(asset, 3, "buy", order_type="limit", limit_price=101)
        assert production.broker._resolve_order_fill_timestep(order, source._timestep) == "minute"
        quote = production.broker.get_quote(asset, quote=order.quote)
        assert pd.Timestamp(quote.raw_data["bar_timestamp"]).date() < source.get_datetime().date()
        assert not production.broker._quote_matches_current_execution_bar(quote, order)
        production.strategy.submit_order(order)
        production.settle()
        assert order.is_active() and not order.is_filled()
        assert production.strategy.get_cash() == 100_000
        assert production.strategy.get_position(asset) is None
    finally:
        production.close()
