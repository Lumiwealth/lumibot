"""Estimated option fills use the expiration's volatility smile when its contracts are loaded.

An option order that has no real quote and no real trade on its fill bar fills at a model
estimate marked ``price_source="estimated"``. When minute trade bars of enough other contracts
of the same expiration are already in memory, the estimate comes from a volatility smile fitted
across those contracts (calls and puts separately). Otherwise it comes from the contract's own
last trade, exactly as before. Real quotes and real trades on the fill bar always win.

All prices here are synthetic and generated from a known volatility curve, so the right answer
is known exactly.
"""

from datetime import date, datetime
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy
from lumibot.tools.option_price_estimate import black_scholes_forward_price
from lumibot.traders import Trader

QUOTE = Asset("USD", asset_type=Asset.AssetType.FOREX)
EXPIRY = date(2025, 2, 21)
START = pd.Timestamp("2025-01-13 09:30")
NY = "America/New_York"
SPOT = 590.0
FILL_MINUTE = 25
STOCK = Asset("XYZ", asset_type=Asset.AssetType.STOCK)
INDEX = Asset("XYZX", asset_type=Asset.AssetType.INDEX)


def _option(strike, right, underlying=None):
    if underlying is None:
        return Asset("XYZ", asset_type=Asset.AssetType.OPTION, expiration=EXPIRY, strike=strike, right=right)
    return Asset(
        underlying.symbol,
        asset_type=Asset.AssetType.OPTION,
        expiration=EXPIRY,
        strike=strike,
        right=right,
        underlying_asset=underlying,
    )


def _years(minute):
    when = (START + pd.Timedelta(minutes=minute)).tz_localize(NY)
    expiry = pd.Timestamp(datetime(EXPIRY.year, EXPIRY.month, EXPIRY.day, 16, 0)).tz_localize(NY)
    return (expiry - when).total_seconds() / (365.0 * 86400.0)


def _skew(strike):
    """The 'true' volatility curve of the synthetic market: higher for lower strikes."""
    return 0.20 + 0.60 * np.log(SPOT / strike)


def _true_price(strike, right, minute, forward_factor=1.0, vol=None):
    vol = _skew(strike) if vol is None else vol
    is_call = right == "call"
    return float(black_scholes_forward_price(SPOT * forward_factor, strike, _years(minute), vol, is_call))


def _data(asset, prints, volume=10):
    index = [START + pd.Timedelta(minutes=m) for m in sorted(prints)]
    opens = [prints[m] for m in sorted(prints)]
    volumes = volume if isinstance(volume, list) else [volume] * len(opens)
    df = pd.DataFrame(
        {"open": opens, "high": opens, "low": opens, "close": opens, "volume": volumes},
        index=pd.DatetimeIndex(index),
    )
    return Data(asset=asset, df=df, quote=QUOTE, timestep="minute", timezone=NY)


def _fill(datas, target, side=Order.OrderSide.SELL, underlying=STOCK):
    """Run one market order for ``target`` submitted at FILL_MINUTE; return (minute, price, source)."""
    pandas_data = {(data.asset, QUOTE): data for data in datas}
    pandas_data[(underlying, QUOTE)] = _data(underlying, {m: SPOT for m in range(60)})
    data_source = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=datetime(2025, 1, 13, 9, 30),
        datetime_end=datetime(2025, 1, 13, 10, 30),
        show_progress_bar=False,
        market="24/7",
    )
    data_source.load_data()
    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    start = START.tz_localize(NY)

    class OneOrder(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.include_cash_positions = True
            self.order = None
            self.fill = None

        def on_trading_iteration(self):
            if self.order is None and self.get_datetime() >= start + pd.Timedelta(minutes=FILL_MINUTE):
                self.order = self.submit_order(self.create_order(target, Decimal("1"), side, quote=QUOTE))

        def on_filled_order(self, position, order, price, quantity, multiplier):
            minute = int((self.get_datetime() - start).total_seconds() // 60)
            self.fill = (minute, float(price), order.identifier)

    strategy = OneOrder(broker=broker, budget=1_000_000.0, analyze_backtest=False)
    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)
    assert strategy.fill is not None, "the order did not fill"
    # The price source of a fill is recorded in the trade log.
    log = strategy.broker._trade_event_log_df
    fills = log[log["status"] == "fill"]
    source = dict(zip(fills["identifier"], fills["price_source"]))[strategy.fill[2]]
    return strategy.fill[0], strategy.fill[1], source


TARGET = _option(565, "put")
STALE_VOL = 0.45  # the target's only trade, at minute 2, was at a volatility the market has left
PUT_STRIKES = [540, 545, 550, 555, 560, 570, 575, 580, 585]


def _target_data():
    return _data(TARGET, {2: _true_price(565, "put", 2, vol=STALE_VOL)})


def _put_chain(minute=20, strikes=PUT_STRIKES):
    return [_data(_option(k, "put"), {minute: _true_price(k, "put", minute)}) for k in strikes]


def test_estimate_follows_the_chain_smile_when_the_expiration_is_loaded():
    truth = _true_price(565, "put", FILL_MINUTE)
    own_trade_model = _true_price(565, "put", FILL_MINUTE, vol=STALE_VOL)
    assert own_trade_model - truth > 3.0  # the two models disagree by dollars here

    minute, price, source = _fill([_target_data()] + _put_chain(), TARGET)

    assert (minute, source) == (FILL_MINUTE, "estimated")
    # Nine neighbours traded five minutes ago on the true curve. The target keeps a small pull
    # toward its own older trade (a contract keeps its volatility at its strike), so the
    # estimate lands within a dollar of the truth instead of 15 dollars away.
    assert price == pytest.approx(truth, abs=0.90)
    assert abs(price - truth) < abs(price - own_trade_model) / 5


def test_estimate_is_unchanged_when_the_chain_is_not_loaded():
    alone = _fill([_target_data()], TARGET)
    few = _fill([_target_data()] + _put_chain(strikes=[555, 560, 570, 575]), TARGET)

    own_trade_model = _true_price(565, "put", FILL_MINUTE, vol=STALE_VOL)
    assert alone[2] == few[2] == "estimated"
    assert alone[1] == pytest.approx(own_trade_model, abs=0.02)
    assert few[1] == alone[1]  # four other contracts are not a chain: same price to the cent


def test_estimate_never_reads_trades_after_the_fill_bar():
    future = [_data(_option(k, "put"), {FILL_MINUTE + 5: 3 * _true_price(k, "put", 30)}) for k in PUT_STRIKES]
    alone = _fill([_target_data()], TARGET)
    with_future = _fill([_target_data()] + future, TARGET)
    assert with_future == alone


def test_bars_with_zero_volume_are_not_trades():
    silent = [
        _data(_option(k, "put"), {20: 3 * _true_price(k, "put", 20)}, volume=0) for k in PUT_STRIKES
    ]
    alone = _fill([_target_data()], TARGET)
    assert _fill([_target_data()] + silent, TARGET) == alone


def test_a_real_trade_on_the_fill_bar_still_wins():
    traded = _data(TARGET, {2: 9.99, FILL_MINUTE: 7.77})
    minute, price, source = _fill([traded] + _put_chain(), TARGET)
    assert (minute, price, source) == (FILL_MINUTE, 7.77, "trade")


def test_calls_and_puts_are_fitted_separately():
    # Calls trade on a curve 5 volatility points above the puts (as a wrong carry assumption
    # would make them look). A put estimate must follow the puts only.
    calls = [
        _data(_option(k, "call"), {20: _true_price(k, "call", 20, vol=_skew(k) + 0.05)}) for k in PUT_STRIKES
    ]
    truth = _true_price(565, "put", FILL_MINUTE)
    _, price, source = _fill([_target_data()] + _put_chain() + calls, TARGET)
    assert source == "estimated"
    assert price == pytest.approx(truth, abs=0.90)


def test_index_options_price_in_the_money_contracts_from_the_parity_forward():
    # European index options: the forward is 0.6% above the index level. Calls and puts trade at
    # the same strikes in the same minute, so put-call parity reveals the forward, and a deep
    # in-the-money call that last traded long ago is priced from the liquid put side.
    factor = 1.006
    strikes = [560, 570, 580, 585, 590, 595, 600, 610, 620]
    datas = []
    for k in strikes:
        for right in ("call", "put"):
            contract = _option(k, right, INDEX)
            datas.append(_data(contract, {20: _true_price(k, right, 20, forward_factor=factor)}))
    target = _option(540, "call", INDEX)
    datas.append(_data(target, {2: _true_price(540, "call", 2, forward_factor=factor, vol=STALE_VOL)}))
    datas.append(_data(_option(540, "put", INDEX), {20: _true_price(540, "put", 20, forward_factor=factor)}))

    truth = _true_price(540, "call", FILL_MINUTE, forward_factor=factor)
    no_forward = _true_price(540, "call", FILL_MINUTE)
    assert truth - no_forward > 2.5  # ignoring the forward misprices this call by dollars

    _, price, source = _fill(datas, target, side=Order.OrderSide.BUY, underlying=INDEX)
    assert source == "estimated"
    assert price == pytest.approx(truth, abs=0.60)
