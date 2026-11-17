"""Every option leg of a backtest account is valued from the same moment.

An option contract is marked from real trades. Two legs of one position rarely print in the same
bar, so the account used to be valued with one leg at this bar's trade and the other at a trade
from hours or a day earlier, while the underlying had moved in between. The account value then
swung by tens of percent for one row and came back (a call ratio spread read -56.6% and then
+57.4% inside one day, with a spread "worth" more than the distance between its strikes).

Rule under test: a leg that has a real trade on the valuation bar is valued at that real trade.
A leg that has none is valued at a model estimate for this moment (underlying price now,
volatility from the leg's own last real trade), counted as estimated. A price from an earlier
bar is never used as if it were current.

All prices are synthetic, generated from one known volatility, so the right answer is exact.
"""

from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy
from lumibot.tools.option_price_estimate import black_scholes_forward_price
from lumibot.traders import Trader

QUOTE = Asset("USD", asset_type=Asset.AssetType.FOREX)
STOCK = Asset("XYZ", asset_type=Asset.AssetType.STOCK)
EXPIRY = date(2025, 2, 21)
START = pd.Timestamp("2025-01-13 09:30")
NY = "America/New_York"
VOL = 0.30
SPOT_BEFORE = 590.0
SPOT_AFTER = 620.0
GAP_MINUTE = 20  # the underlying jumps here
OPEN_MINUTE = 5  # both legs trade here and the position is opened
CHECK_MINUTE = 25
SOLD = Asset("XYZ", asset_type=Asset.AssetType.OPTION, expiration=EXPIRY, strike=560.0, right="call")
BOUGHT = Asset("XYZ", asset_type=Asset.AssetType.OPTION, expiration=EXPIRY, strike=590.0, right="call")


def _spot(minute):
    return SPOT_BEFORE if minute < GAP_MINUTE else SPOT_AFTER


def _years(minute):
    when = (START + pd.Timedelta(minutes=minute)).tz_localize(NY)
    expiry = pd.Timestamp(datetime(EXPIRY.year, EXPIRY.month, EXPIRY.day, 16, 0)).tz_localize(NY)
    return (expiry - when).total_seconds() / (365.0 * 86400.0)


def _true_price(asset, minute):
    return float(black_scholes_forward_price(_spot(minute), float(asset.strike), _years(minute), VOL, True))


def _data(asset, prints):
    minutes = sorted(prints)
    values = [prints[m] for m in minutes]
    df = pd.DataFrame(
        {"open": values, "high": values, "low": values, "close": values, "volume": [10] * len(values)},
        index=pd.DatetimeIndex([START + pd.Timedelta(minutes=m) for m in minutes]),
    )
    return Data(asset=asset, df=df, quote=QUOTE, timestep="minute", timezone=NY)


def _run(sold_prints, bought_prints):
    """Open -1 SOLD / +2 BOUGHT at OPEN_MINUTE; return (value at CHECK_MINUTE, cash, broker)."""
    pandas_data = {
        (SOLD, QUOTE): _data(SOLD, sold_prints),
        (BOUGHT, QUOTE): _data(BOUGHT, bought_prints),
        (STOCK, QUOTE): _data(STOCK, {m: _spot(m) for m in range(60)}),
    }
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

    class RatioSpread(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.sent = False
            self.seen = {}

        def on_trading_iteration(self):
            minute = int((self.get_datetime() - start).total_seconds() // 60)
            if not self.sent and minute >= OPEN_MINUTE:
                self.sent = True
                self.submit_order(self.create_order(SOLD, Decimal("1"), Order.OrderSide.SELL_TO_OPEN, quote=QUOTE))
                self.submit_order(self.create_order(BOUGHT, Decimal("2"), Order.OrderSide.BUY_TO_OPEN, quote=QUOTE))
            self.seen[minute] = (float(self.get_portfolio_value()), float(self.get_cash()))

    strategy = RatioSpread(broker=broker, budget=1_000_000.0, analyze_backtest=False)
    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)
    assert CHECK_MINUTE in strategy.seen, "the backtest did not reach the check minute"
    assert len(strategy.broker.get_tracked_positions(strategy._name)) >= 2, "the spread was not opened"
    value, cash = strategy.seen[CHECK_MINUTE]
    return value, cash, strategy.broker


def test_leg_without_a_trade_on_this_bar_is_valued_for_this_moment_not_from_its_old_trade():
    """The bought leg prints now; the sold leg last printed before the underlying jumped 30 points."""
    sold_prints = {OPEN_MINUTE: _true_price(SOLD, OPEN_MINUTE)}
    bought_prints = {OPEN_MINUTE: _true_price(BOUGHT, OPEN_MINUTE), CHECK_MINUTE: _true_price(BOUGHT, CHECK_MINUTE)}

    value, cash, broker = _run(sold_prints, bought_prints)

    sold_now = _true_price(SOLD, CHECK_MINUTE)
    bought_now = _true_price(BOUGHT, CHECK_MINUTE)
    sold_stale = _true_price(SOLD, OPEN_MINUTE)
    # The stale mark and the right mark differ by dollars per share: the bug is not subtle.
    assert sold_now - sold_stale > 20.0
    wrong = cash + (-1 * sold_stale + 2 * bought_now) * 100
    right = cash + (-1 * sold_now + 2 * bought_now) * 100
    assert wrong - right > 2_000

    assert value == pytest.approx(right, abs=5.0)
    # The estimate is labeled: it is counted, and real marks are counted separately.
    counts = broker.option_valuation_mark_counts()
    assert counts.get("estimated", 0) >= 1
    assert counts.get("real", 0) >= 1


def test_legs_that_both_trade_on_this_bar_keep_their_real_prices():
    """Opposite case: real trades on the valuation bar always win, even when a model disagrees."""
    off_model = 1.37  # real prints need not agree with any model
    sold_real = _true_price(SOLD, CHECK_MINUTE) + off_model
    bought_real = _true_price(BOUGHT, CHECK_MINUTE) - off_model
    sold_prints = {OPEN_MINUTE: _true_price(SOLD, OPEN_MINUTE), CHECK_MINUTE: sold_real}
    bought_prints = {OPEN_MINUTE: _true_price(BOUGHT, OPEN_MINUTE), CHECK_MINUTE: bought_real}

    value, cash, _broker = _run(sold_prints, bought_prints)

    assert value == pytest.approx(cash + (-1 * sold_real + 2 * bought_real) * 100, abs=0.5)


def test_underlying_that_did_not_move_leaves_a_quiet_leg_at_its_last_trade():
    """No jump, no print since the open: the estimate for now equals the last trade (less a little time)."""
    global SPOT_AFTER
    saved = SPOT_AFTER
    SPOT_AFTER = SPOT_BEFORE
    try:
        sold_prints = {OPEN_MINUTE: _true_price(SOLD, OPEN_MINUTE)}
        bought_prints = {OPEN_MINUTE: _true_price(BOUGHT, OPEN_MINUTE)}
        value, cash, _broker = _run(sold_prints, bought_prints)
        last_trades = cash + (-1 * sold_prints[OPEN_MINUTE] + 2 * bought_prints[OPEN_MINUTE]) * 100
        assert value == pytest.approx(last_trades, abs=5.0)
    finally:
        SPOT_AFTER = saved


# ----------------------------------------------------------------------------------------------
# Daily bars. A day's bar has no time of day: its open is the first trade whenever that happened,
# so two legs' "opens" can be hours apart. The day's bar is a real mark only once the session has
# closed. These tests drive the broker rule directly with made-up bars.
# ----------------------------------------------------------------------------------------------

DAY_SOLD = Asset("XYZ", asset_type=Asset.AssetType.OPTION, expiration=date(2025, 3, 21), strike=450.0, right="call")
DAY_BOUGHT = Asset("XYZ", asset_type=Asset.AssetType.OPTION, expiration=date(2025, 3, 21), strike=485.0, right="call")
DAY_VOL = 0.50
YESTERDAY = pd.Timestamp("2025-02-18", tz=NY)
TODAY = pd.Timestamp("2025-02-19", tz=NY)
SPOT_YESTERDAY_CLOSE = 540.0
SPOT_TODAY_OPEN = 520.0


def _day_years(when):
    expiry = pd.Timestamp(datetime(2025, 3, 21, 16, 0)).tz_localize(NY)
    return (expiry - when).total_seconds() / (365.0 * 86400.0)


def _day_price(asset, spot, when):
    return float(black_scholes_forward_price(spot, float(asset.strike), _day_years(when), DAY_VOL, True))


def _bars(rows):
    index = pd.DatetimeIndex([ts for ts, _open, _close in rows])
    return pd.DataFrame({"open": [o for _t, o, _c in rows], "close": [c for _t, _o, c in rows]}, index=index)


def _daily_broker(monkeypatch, now, phase, frames, spot):
    data_source = PandasDataBacktesting(
        pandas_data={(STOCK, QUOTE): _data(STOCK, {0: 1.0})},
        datetime_start=datetime(2025, 1, 13, 9, 30),
        datetime_end=datetime(2025, 1, 13, 10, 30),
        show_progress_bar=False,
        market="24/7",
    )
    broker = BacktestingBroker(data_source=data_source)
    data_source._timestep = "day"
    monkeypatch.setattr(data_source, "get_datetime", lambda *a, **k: now.to_pydatetime())
    monkeypatch.setattr(data_source, "get_last_price", lambda asset, **k: spot)
    monkeypatch.setattr(broker, "_daily_session_phase", lambda _now: phase)
    monkeypatch.setattr(broker, "_quote_price_now", lambda _order: None)

    def recent(asset, _quote, length):
        frame = frames[asset]
        frame = frame[[ts.date() <= now.date() for ts in frame.index]]  # as the real lookup does on daily bars
        return frame.iloc[-length:] if len(frame) else None

    monkeypatch.setattr(broker, "_recent_real_bars", recent)
    return broker


def _daily_frames(sold_today=None, bought_today=None):
    close_time = YESTERDAY + pd.Timedelta(hours=16)
    sold = [(YESTERDAY, 1.0, _day_price(DAY_SOLD, SPOT_YESTERDAY_CLOSE, close_time))]
    bought = [(YESTERDAY, 1.0, _day_price(DAY_BOUGHT, SPOT_YESTERDAY_CLOSE, close_time))]
    if sold_today:
        sold.append((TODAY, *sold_today))
    if bought_today:
        bought.append((TODAY, *bought_today))
    stock = [
        (YESTERDAY - pd.Timedelta(days=4), 535.0, 535.0),
        (YESTERDAY, 538.0, SPOT_YESTERDAY_CLOSE),
        (TODAY, SPOT_TODAY_OPEN, 525.0),
    ]
    return {DAY_SOLD: _bars(sold), DAY_BOUGHT: _bars(bought), STOCK: _bars(stock)}


def test_daily_bars_first_trades_of_the_day_are_not_this_moments_mark(monkeypatch):
    """At the open both legs have a bar for today, but those first trades happened at unknown,
    different times: 100.71 and 38.00 put a 35-wide spread at 62.71. Both legs are valued from
    the underlying's opening price instead."""
    now = TODAY + pd.Timedelta(hours=9, minutes=30)
    frames = _daily_frames(sold_today=(100.71, 80.17), bought_today=(38.00, 42.72))
    broker = _daily_broker(monkeypatch, now, "open", frames, SPOT_TODAY_OPEN)

    marks = broker.option_marks_for_valuation([DAY_SOLD, DAY_BOUGHT], QUOTE)

    assert marks[DAY_SOLD] == pytest.approx(_day_price(DAY_SOLD, SPOT_TODAY_OPEN, now), abs=0.25)
    assert marks[DAY_BOUGHT] == pytest.approx(_day_price(DAY_BOUGHT, SPOT_TODAY_OPEN, now), abs=0.25)
    assert 0.0 <= marks[DAY_SOLD] - marks[DAY_BOUGHT] <= 485.0 - 450.0  # a spread cannot exceed its width
    assert broker.option_valuation_mark_counts() == {"estimated": 2}


def test_daily_bars_after_the_close_use_the_days_real_closing_trades(monkeypatch):
    now = TODAY + pd.Timedelta(hours=16)
    frames = _daily_frames(sold_today=(100.71, 80.17), bought_today=(38.00, 42.72))
    broker = _daily_broker(monkeypatch, now, "closed", frames, 525.0)

    assert broker.option_marks_for_valuation([DAY_SOLD, DAY_BOUGHT], QUOTE) == {}
    assert broker.option_valuation_mark_counts() == {"real": 2}


def test_daily_bars_before_the_open_never_read_todays_bar_and_keep_last_sessions_trades(monkeypatch):
    """Before the open nothing has traded today. A leg that traded in the last session keeps that
    real close; a leg whose last trade is days old is valued for the last session's close."""
    now = TODAY + pd.Timedelta(hours=8, minutes=30)
    stale_day = YESTERDAY - pd.Timedelta(days=4)
    frames = _daily_frames(sold_today=(100.71, 80.17), bought_today=(38.00, 42.72))
    # The stale leg last traded when the underlying closed at 535, at the same volatility.
    frames[DAY_SOLD] = _bars([
        (stale_day, 1.0, _day_price(DAY_SOLD, 535.0, stale_day + pd.Timedelta(hours=16))),
        (TODAY, 100.71, 80.17),
    ])
    broker = _daily_broker(monkeypatch, now, "before", frames, SPOT_YESTERDAY_CLOSE)

    marks = broker.option_marks_for_valuation([DAY_SOLD, DAY_BOUGHT], QUOTE)

    assert DAY_BOUGHT not in marks  # traded in the last session: its real close stands
    assert marks[DAY_SOLD] == pytest.approx(_day_price(DAY_SOLD, SPOT_YESTERDAY_CLOSE, now), abs=0.5)
    assert broker.option_valuation_mark_counts() == {"estimated": 1, "real": 1}


def test_trade_below_its_no_volatility_value_is_valued_without_time_value(monkeypatch):
    """A last trade that no volatility explains (it printed when the underlying was elsewhere)
    carried no time value: the leg is valued at what it is worth in the money now."""
    now = TODAY + pd.Timedelta(hours=9, minutes=30)
    frames = _daily_frames()
    frames[DAY_SOLD] = _bars([(YESTERDAY, 1.0, 80.0)])  # below 540 - 450 = 90
    broker = _daily_broker(monkeypatch, now, "open", frames, SPOT_TODAY_OPEN)

    marks = broker.option_marks_for_valuation([DAY_SOLD], QUOTE)

    assert marks[DAY_SOLD] == pytest.approx(SPOT_TODAY_OPEN - 450.0, abs=0.01)
