"""The option fill and valuation path gives identical results while doing less repeated work.

Why: a profiled two-year option spread backtest spent about half of its loop in the fill path,
re-reading the same contract and underlying bars many times per bar (once per pending leg,
once per estimate step, once per valuation). Those reads cannot change within one bar, so the
broker now reads each (contract, length) once per bar and reuses it.

Invariant under test: the trade log and every per-minute account value are exactly the same as
before the change (fingerprints captured from the code before the change), and the number of
history reads made by the broker's fill path goes down. Counts, never seconds.

All prices are synthetic: a moving underlying and sparse option prints so estimates, resting
limit packages and valuation marks are all exercised.
"""

import hashlib
import math
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy
from lumibot.tools.option_price_estimate import black_scholes_forward_price
from lumibot.traders import Trader

QUOTE = Asset("USD", asset_type=Asset.AssetType.FOREX)
UNDERLYING = Asset("XYZ", asset_type=Asset.AssetType.STOCK)
EXPIRY = date(2025, 2, 21)
START = pd.Timestamp("2025-01-13 09:30")
MINUTES = 150
NY = "America/New_York"
VOL = 0.30


def _option(strike, right):
    return Asset("XYZ", asset_type=Asset.AssetType.OPTION, expiration=EXPIRY, strike=strike, right=right)


SHORT_PUT = _option(570.0, "put")
LONG_PUT = _option(560.0, "put")
SHORT_CALL = _option(610.0, "call")
LONG_CALL = _option(620.0, "call")
# Each contract prints only every few minutes, at different minutes.
PRINT_EVERY = {SHORT_PUT: 7, LONG_PUT: 11, SHORT_CALL: 13, LONG_CALL: 17}


def _spot(minute):
    return 590.0 + 6.0 * math.sin(minute / 9.0) + 0.05 * minute


def _years(minute):
    when = (START + pd.Timedelta(minutes=minute)).tz_localize(NY)
    expiry = pd.Timestamp(datetime(EXPIRY.year, EXPIRY.month, EXPIRY.day, 16, 0)).tz_localize(NY)
    return (expiry - when).total_seconds() / (365.0 * 86400.0)


def _price(asset, minute):
    is_call = str(asset.right).lower().startswith("c")
    vol = VOL + 0.0005 * (minute % 5)
    return round(float(black_scholes_forward_price(_spot(minute), float(asset.strike), _years(minute), vol, is_call)), 2)


def _data(asset, minutes, price_of):
    values = [price_of(m) for m in minutes]
    df = pd.DataFrame(
        {"open": values, "high": values, "low": values, "close": values, "volume": [10] * len(values)},
        index=pd.DatetimeIndex([START + pd.Timedelta(minutes=m) for m in minutes]),
    )
    return Data(asset=asset, df=df, quote=QUOTE, timestep="minute", timezone=NY)


def _run():
    pandas_data = {(UNDERLYING, QUOTE): _data(UNDERLYING, list(range(MINUTES)), lambda m: round(_spot(m), 2))}
    for asset, every in PRINT_EVERY.items():
        minutes = [m for m in range(MINUTES) if m % every == 1]
        pandas_data[(asset, QUOTE)] = _data(asset, minutes, lambda m, a=asset: _price(a, m))
    data_source = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=datetime(2025, 1, 13, 9, 30),
        datetime_end=datetime(2025, 1, 13, 12, 0),
        show_progress_bar=False,
        market="24/7",
    )
    data_source.load_data()
    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())

    # Count the history reads the broker's order-fill path makes (valuation counted apart).
    reads = {"n": 0, "valuation": 0, "inside": 0, "valuing": 0}
    original_read = data_source.get_historical_prices
    original_recent = broker._recent_real_bars
    original_marks = broker.option_marks_for_valuation

    def counted_read(*args, **kwargs):
        if reads["inside"]:
            reads["valuation" if reads["valuing"] else "n"] += 1
        return original_read(*args, **kwargs)

    def counted_marks(*args, **kwargs):
        reads["valuing"] += 1
        try:
            return original_marks(*args, **kwargs)
        finally:
            reads["valuing"] -= 1

    def counted_recent(*args, **kwargs):
        reads["inside"] += 1
        try:
            return original_recent(*args, **kwargs)
        finally:
            reads["inside"] -= 1

    data_source.get_historical_prices = counted_read
    broker._recent_real_bars = counted_recent
    broker.option_marks_for_valuation = counted_marks
    start = START.tz_localize(NY)

    class Condor(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.include_cash_positions = True
            self.opened_at = None
            self.package = None
            self.resting = None
            self.values = []

        def on_trading_iteration(self):
            minute = int((self.get_datetime() - start).total_seconds() // 60)
            if self.resting is None:
                # Two packages whose limits are never reached: repriced on every bar for the whole
                # run, like a strategy waiting for its price.
                self.resting = []
                for limit in (60.0, 70.0):
                    legs = [
                        self.create_order(SHORT_PUT, Decimal("1"), Order.OrderSide.SELL, quote=QUOTE),
                        self.create_order(LONG_PUT, Decimal("1"), Order.OrderSide.BUY, quote=QUOTE),
                    ]
                    self.resting.append(self.submit_orders(legs, is_multileg=True, order_type="credit", price=limit))
            if self.package is None and minute % 40 == 3:
                legs = [
                    self.create_order(LONG_PUT, Decimal("1"), Order.OrderSide.BUY, quote=QUOTE),
                    self.create_order(SHORT_PUT, Decimal("2"), Order.OrderSide.SELL, quote=QUOTE),
                    self.create_order(SHORT_CALL, Decimal("2"), Order.OrderSide.SELL, quote=QUOTE),
                    self.create_order(LONG_CALL, Decimal("1"), Order.OrderSide.BUY, quote=QUOTE),
                ]
                # A credit limit that rests for a while before the net reaches it.
                self.package = self.submit_orders(legs, is_multileg=True, order_type="credit", price=4.0)
                self.opened_at = None
            elif self.package is not None and self.package[0].is_filled() and self.opened_at is None:
                self.opened_at = minute
            elif self.opened_at is not None and minute - self.opened_at >= 12:
                closing = [
                    self.create_order(
                        o.asset,
                        o.quantity,
                        Order.OrderSide.SELL if o.is_buy_order() else Order.OrderSide.BUY,
                        quote=QUOTE,
                    )
                    for o in self.package
                ]
                self.submit_orders(closing, is_multileg=True)
                self.package = None
                self.opened_at = None
            elif self.package is not None and not self.package[0].is_filled() and minute % 40 == 30:
                for o in self.package:
                    if o.is_active():
                        self.cancel_order(o)
                self.package = None
            self.values.append((minute, repr(float(self.get_portfolio_value())), repr(float(self.get_cash()))))

    strategy = Condor(broker=broker, budget=1_000_000.0, analyze_backtest=False)
    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)
    return strategy, broker, reads["n"], reads["valuation"]


def _fingerprints(strategy, broker):
    log = broker._trade_event_log_df
    columns = [c for c in ("time", "symbol", "strike", "type", "status", "side", "price", "filled_quantity",
                           "price_source") if c in log.columns]
    trade_text = log[columns].astype(str).to_csv(index=False)
    value_text = "\n".join(",".join(map(str, row)) for row in strategy.values)
    return (
        hashlib.sha256(trade_text.encode()).hexdigest(),
        hashlib.sha256(value_text.encode()).hexdigest(),
        int((log["status"] == "fill").sum()),
    )


# Captured on origin/version/4.6.86 at 6ca6c83b2, before the per-bar read reuse.
EXPECTED_TRADE_LOG_SHA = "0d380ec89c5b1e9fc656f9e271039b2e50da3bedc6cf9390f611e1ed00ecbbcf"
EXPECTED_VALUES_SHA = "7b1b81997c70e953916f8e3d2636dd6eb23de44b82f126181d839bcb348e751c"
EXPECTED_FILLS = 20
FILL_READS_BEFORE = 2241
VALUATION_READS_BEFORE = 991


def test_fill_and_valuation_results_are_identical_and_history_reads_drop():
    strategy, broker, reads, valuation_reads = _run()
    trade_sha, values_sha, fills = _fingerprints(strategy, broker)
    print(f"\nTRADE={trade_sha}\nVALUES={values_sha}\nFILLS={fills}\nREADS={reads}\nVALUATION={valuation_reads}")
    counts = getattr(broker, "_option_fill_price_counts", {}) or {}
    assert counts.get("estimated", 0) >= 1, "the run must exercise estimated fills"
    assert fills >= 8, "the run must open and close packages"
    assert trade_sha == EXPECTED_TRADE_LOG_SHA
    assert values_sha == EXPECTED_VALUES_SHA
    assert fills == EXPECTED_FILLS
    # At most half the order-fill history reads of the old code; valuation never reads more.
    assert reads * 2 <= FILL_READS_BEFORE
    assert valuation_reads <= VALUATION_READS_BEFORE
