"""
Sim-time safety regression tests for `Strategy.get_last_price()`.

Context — production incident, 2026-04-17:
    An IBKR Stock Alpha Picks backtest (window 2022-07-01 → 2022-08-01) called
    `self.get_last_price(symbol)` during rebalance and got back the CURRENT
    (2026-04-17) market price instead of the 2022-07-01 close:

        COP:  get_last_price returned $97.43   real 2022-07-01 close: $90.98
        AMR:  get_last_price returned $136.76  real 2022-07-01 close: $124.87
        NUE:  get_last_price returned $135.80  real 2022-07-01 close: $105.64
        VLO:  get_last_price returned $110.77  real 2022-07-01 close: $107.98

    The actual historical-bars fill path was fine (fills happened at the
    correct 2022 prices a moment later) — only the daily-cadence shortcut in
    `Strategy.get_last_price()` was polluted. Position sizing ran on the
    look-ahead/future prices, under-investing every symbol on day 1.

Root cause:
    `strategy.py:get_last_price()` contained a "daily-optimization shortcut"
    that called:

        bars = self.get_historical_prices(
            asset, length=2, timestep="day", timeshift=-1,
            quote=..., exchange=...,
        )
        result = float(bars.df["close"].iloc[-1])

    `Data.get_bars(dt, length=2, timestep="day", timeshift=-1)` computes

        end_row   = iter_count + 1 - timeshift = iter_count + 2
        start_row = end_row - length           = iter_count

    so the slice is `[iter_count, iter_count + 1]` — i.e. the bar AT sim_time
    AND THE NEXT BAR AFTER IT. Taking `iloc[-1]` returned that next-bar close.
    In a backtest the full history already exists, so that next bar could be
    anything up to the last row of the frame. When the shared S3 IBKR daily
    cache contained a bar stamped at real-now (e.g. from a concurrent live
    process, or a prior buggy run), `iloc[-1]` returned today's close.

Fix (strategy.py):
    Changed the shortcut to `length=1, timeshift=0` which slices exactly one
    row at `iter_count` — the bar at (or immediately before) sim_time. No
    look-ahead, no dependence on what's beyond sim_time in the frame.

These tests lock that fix in place. Every scenario that matters:
    1. Polluted frame with a row stamped past sim_time → MUST return the
       pre-sim-time bar, never the post-sim-time bar.
    2. Dense daily frame, every sim_time → MUST return sim_time's own close
       (or the most recent completed close before sim_time for intraday).
    3. Sim_time before any bar → MUST fall through (empty frame → broker).
    4. Direct reproduction of the Alpha Picks 2026-04-17 symptom.
    5. A guard test that pins the bug-pattern params so the old
       `length=2, timeshift=-1` cannot silently return.

If any of these fail, the shortcut has regressed. Do NOT loosen the
assertions — fix the code.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import pytz

from lumibot.entities import Asset, Bars, Data


NY = pytz.timezone("America/New_York")


def _make_daily_data(asset: Asset, rows: list[tuple[str, float]]) -> Data:
    """Build a `Data(timestep='day')` object from `(timestamp_str, close)` pairs.

    All OHLC columns are filled with the provided close to keep the fixture
    minimal and focused on close-price assertions.
    """
    idx = pd.DatetimeIndex([ts for ts, _ in rows]).tz_localize(NY)
    closes = [c for _, c in rows]
    df = pd.DataFrame(
        {
            "open": closes,
            "high": closes,
            "low": closes,
            "close": closes,
            "volume": [1000] * len(closes),
        },
        index=idx,
    )
    data = Data(asset, df, timestep="day")
    data.repair_times_and_fill(df.index)
    return data


class _StubBrokerGetLastPrice(Exception):
    """Raised by the stub broker to prove the shortcut fell through."""


def _make_strategy_stub(
    data: Data,
    sim_time: datetime,
    asset_type_source_name: str = "IbkrRESTBacktesting",
):
    """Build a minimal stand-in for `Strategy` that exercises the shortcut path.

    We avoid pulling the whole Strategy class because it requires a Broker +
    DataSource initialization chain. Instead, we bind just enough methods to
    the Strategy's unbound `get_last_price` so the shortcut and fallback
    branches both execute with realistic semantics:

      - `is_backtesting` / broker flags → route through the daily shortcut
      - `get_historical_prices(...)` → returns `Bars` built from `data.get_bars`
      - `broker.get_last_price(...)` → raises `_StubBrokerGetLastPrice` so any
        fall-through is visible (we can assert "shortcut returned" vs "broker
        was called" directly)
    """
    from lumibot.strategies.strategy import Strategy

    strat = MagicMock(spec=Strategy)
    strat.is_backtesting = True
    strat.quote_asset = Asset("USD", "forex")
    strat.logger = MagicMock()
    strat.log_message = MagicMock()
    strat._last_price_request_cache = None
    strat._last_price_request_cache_datetime = None

    # Broker: datetime = sim_time, raises on get_last_price so we can tell
    # whether the shortcut path or the fallback path produced the result.
    broker = MagicMock()
    broker.datetime = sim_time
    broker.IS_BACKTESTING_BROKER = True
    broker.get_last_price = MagicMock(side_effect=_StubBrokerGetLastPrice())
    # data_source.__class__.__name__ feeds `_supports_daily_last_price_optimization`.
    data_source_type = type(asset_type_source_name, (), {})
    broker.data_source = data_source_type()
    strat.broker = broker

    # sleeptime 1D triggers `_should_use_daily_last_price == True`.
    strat._sleeptime = "1D"
    strat.sleeptime = "1D"

    # `_sanitize_user_asset` is a no-op for an already-Asset input.
    strat._sanitize_user_asset.side_effect = lambda a: a

    # Route get_historical_prices through the real Data object so assertions
    # reflect the same slice math the production call would hit.
    def _get_historical_prices(asset, length, timestep=None, timeshift=None, **kwargs):
        if timeshift is None:
            timeshift = 0
        df = data.get_bars(sim_time, length=length, timestep=timestep or "day", timeshift=timeshift)
        if df is None or df.empty:
            return None
        bars = Bars(df, source="test", asset=asset, quote=kwargs.get("quote"))
        return bars

    strat.get_historical_prices.side_effect = _get_historical_prices

    # Bind the real methods we care about so the shortcut logic executes.
    strat._should_use_daily_last_price = Strategy._should_use_daily_last_price.__get__(strat, Strategy)
    strat._supports_daily_last_price_optimization = Strategy._supports_daily_last_price_optimization.__get__(
        strat, Strategy
    )
    strat._get_sleeptime_seconds = Strategy._get_sleeptime_seconds.__get__(strat, Strategy)
    strat.get_last_price = Strategy.get_last_price.__get__(strat, Strategy)
    return strat


def _run_shortcut(data: Data, asset: Asset, sim_time: datetime):
    """Invoke `Strategy.get_last_price(asset)` with the real shortcut path.

    Returns `(shortcut_result, broker_was_called)` so tests can assert both the
    answer and whether the fallback was reached.
    """
    strat = _make_strategy_stub(data, sim_time)
    with patch("lumibot.strategies.strategy.IS_BACKTESTING", True):
        try:
            result = strat.get_last_price(asset)
        except _StubBrokerGetLastPrice:
            # Shortcut fell through and the stub broker was invoked.
            return None, True
    broker_called = strat.broker.get_last_price.called
    return result, broker_called


class TestPollutedFrameDoesNotLeakFutureBars:
    """Production symptom from the 2026-04-17 incident.

    The shared IBKR daily cache sometimes contains a row stamped at real-now
    (from a concurrent live process, or from an earlier buggy run that
    persisted today's bar). The sim-time safety invariant is non-negotiable:
    no matter what's in the frame past sim_time, `get_last_price` MUST return
    the last completed bar at or before sim_time — never a future bar.
    """

    def test_cop_2022_sim_time_does_not_return_today_close(self):
        # Exact Alpha Picks symptom: a frame whose only bars are 2022-06-30
        # (the last completed bar before sim_time) and 2026-04-17 (poisoned
        # wall-clock row). `get_last_price(sim_time=2022-07-01)` must return
        # the 2022-06-30 close, never the 2026-04-17 close.
        asset = Asset("COP", "stock")
        data = _make_daily_data(asset, [("2022-06-30 16:00", 90.98), ("2026-04-17 16:00", 97.43)])

        sim_time = NY.localize(datetime(2022, 7, 1, 10, 30))
        result, _ = _run_shortcut(data, asset, sim_time)

        assert result == pytest.approx(90.98), (
            "Sim-time-safety regression: `Strategy.get_last_price` returned the "
            "2026-04-17 poisoned bar instead of the 2022-06-30 close. This is "
            "the exact Alpha Picks 2026-04-17 incident — do NOT add a filter, "
            "find out why the shortcut is reading past sim_time."
        )

    def test_every_2022_sim_time_returns_pre_sim_bar_with_polluted_frame(self):
        # The symptom was insensitive to *where* in 2022 the sim_time was — it
        # returned the poisoned bar for every call. Pin that invariant: any
        # sim_time between the two bars must still resolve to the 2022 close.
        asset = Asset("NUE", "stock")
        data = _make_daily_data(asset, [("2022-06-30 16:00", 105.64), ("2026-04-17 16:00", 135.80)])

        for sim_time in [
            NY.localize(datetime(2022, 7, 1, 0, 0)),
            NY.localize(datetime(2022, 7, 15, 12, 0)),
            NY.localize(datetime(2023, 3, 1, 9, 30)),
            NY.localize(datetime(2025, 12, 31, 15, 59)),
        ]:
            result, _ = _run_shortcut(data, asset, sim_time)
            assert result == pytest.approx(105.64), (
                f"Sim-time {sim_time}: expected 2022 bar (105.64), got {result}. "
                f"Shortcut is leaking future bars from the frame."
            )

    def test_dense_frame_returns_bar_at_sim_time(self):
        # Healthy frame (no poisoning): 5 consecutive trading days, sim_time
        # lands between day 2 and day 3 — must return day 2's close.
        asset = Asset("SPY", "stock")
        closes = [("2022-07-01 16:00", 100.0), ("2022-07-05 16:00", 101.0), ("2022-07-06 16:00", 102.0),
                  ("2022-07-07 16:00", 103.0), ("2022-07-08 16:00", 104.0)]
        data = _make_daily_data(asset, closes)

        # Sim-time Tuesday 2022-07-05 10:30 AM: day 2 bar (close=101 stamped
        # at 16:00) hasn't completed yet, so the last completed bar is day 1.
        sim_time = NY.localize(datetime(2022, 7, 5, 10, 30))
        result, _ = _run_shortcut(data, asset, sim_time)
        assert result == pytest.approx(100.0)

        # Sim-time Tuesday 2022-07-05 at 16:00 (exact close): iter_count points
        # at day 2's bar, shortcut must return day 2's close (101), NEVER day 3.
        sim_time = NY.localize(datetime(2022, 7, 5, 16, 0))
        result, _ = _run_shortcut(data, asset, sim_time)
        assert result == pytest.approx(101.0), (
            "At sim_time = day 2 close, shortcut returned day 3 close — "
            "look-ahead bias regression (`timeshift=-1` is back)."
        )


class TestSimTimeBeforeAnyBar:
    """Edge case: sim_time is earlier than the first bar in the data."""

    def test_shortcut_falls_through_to_broker(self):
        # Data starts 2022-06-30; sim_time is 2022-06-01 (before any bar). The
        # shortcut must NOT fabricate a value from the frame — it must return
        # empty bars and let the broker path answer.
        asset = Asset("COP", "stock")
        data = _make_daily_data(asset, [("2022-06-30 16:00", 90.98), ("2022-07-01 16:00", 92.0)])
        sim_time = NY.localize(datetime(2022, 6, 1, 10, 30))

        _, broker_called = _run_shortcut(data, asset, sim_time)
        assert broker_called, (
            "Shortcut must fall through to broker when no bar at-or-before "
            "sim_time exists. A short-circuit here would return stale/future "
            "data with no upstream source to correct it."
        )


class TestDataGetBarsSliceMath:
    """Whitebox pins on `Data.get_bars` for daily series.

    The shortcut delegates to `Data.get_bars(...)`. The fix only works because
    `Data.get_bars(length=1, timeshift=0, timestep='day')` returns exactly the
    single bar at `iter_count` (the bar at or before sim_time). If
    `Data.get_bars`'s slice math ever changes, the shortcut silently breaks
    back into look-ahead — so pin both the safe and the banned slice shapes
    directly at the primitive.
    """

    def test_length_1_timeshift_0_returns_only_sim_time_bar(self):
        asset = Asset("SPY", "stock")
        data = _make_daily_data(
            asset,
            [
                ("2022-06-30 16:00", 100.0),
                ("2022-07-01 16:00", 101.0),
                ("2022-07-05 16:00", 102.0),
                ("2022-07-06 16:00", 103.0),
            ],
        )

        # Sim-time before 2022-07-01 close → iter_count points at 2022-06-30.
        sim_time = NY.localize(datetime(2022, 7, 1, 10, 30))
        out = data.get_bars(sim_time, length=1, timestep="day", timeshift=0)
        assert out is not None and len(out) == 1
        assert float(out["close"].iloc[-1]) == pytest.approx(100.0), (
            "length=1, timeshift=0 must return the last-completed bar at or "
            "before sim_time. Any other value indicates get_bars slice math "
            "has changed."
        )

    def test_length_2_timeshift_negative_one_is_look_ahead_unsafe(self):
        # This test DOCUMENTS that the old parameter combination produces a
        # slice extending past sim_time. It is intentionally the bug-pattern:
        # if get_bars' math ever changes and this test passes with the safe
        # value (100.0) instead of 101.0, it means slice semantics shifted and
        # the shortcut's invariant may need re-checking.
        asset = Asset("SPY", "stock")
        data = _make_daily_data(
            asset,
            [
                ("2022-06-30 16:00", 100.0),
                ("2022-07-01 16:00", 101.0),
                ("2022-07-05 16:00", 102.0),
            ],
        )

        sim_time = NY.localize(datetime(2022, 7, 1, 10, 30))
        out = data.get_bars(sim_time, length=2, timestep="day", timeshift=-1)
        assert out is not None and len(out) == 2
        # Slice is [iter_count, iter_count+1] — the SECOND row is the
        # 2022-07-01 bar, whose close is AFTER sim_time 10:30. `iloc[-1]`
        # returning 101.0 is the proof of look-ahead. If this assertion ever
        # starts failing with 100.0, get_bars' semantics changed and the
        # shortcut fix may need a follow-up.
        assert float(out["close"].iloc[-1]) == pytest.approx(101.0), (
            "get_bars(length=2, timeshift=-1, timestep='day') is no longer "
            "look-ahead-unsafe. This is the parameter combination that caused "
            "the Alpha Picks 2026-04-17 incident — verify the shortcut fix "
            "still holds before relaxing."
        )


class TestShortcutParamsAreSimTimeSafe:
    """Whitebox guard on the shortcut call signature itself.

    The shortcut MUST NOT request `timeshift=-1` or `length=2` again — those
    are the specific parameters that caused the incident. If someone ever
    restores them (e.g. from a "revert recent changes" or a merge conflict
    resolution that drops our fix), this test fails loudly before the code
    reaches CI acceptance backtests.
    """

    def test_shortcut_uses_length_1_timeshift_0(self):
        captured: dict[str, object] = {}

        class CapturingBars:
            def __init__(self, df):
                self.df = df

        asset = Asset("COP", "stock")
        data = _make_daily_data(asset, [("2022-06-30 16:00", 90.98), ("2026-04-17 16:00", 97.43)])
        sim_time = NY.localize(datetime(2022, 7, 1, 10, 30))

        strat = _make_strategy_stub(data, sim_time)

        def _capture(asset, length, timestep=None, timeshift=None, **kwargs):
            # Record the exact (length, timestep, timeshift) the shortcut uses.
            captured["length"] = length
            captured["timestep"] = timestep
            captured["timeshift"] = timeshift
            # Return a valid-looking Bars so the shortcut short-circuits here.
            idx = pd.DatetimeIndex(["2022-06-30 16:00"]).tz_localize(NY)
            df = pd.DataFrame({"close": [90.98]}, index=idx)
            return CapturingBars(df)

        strat.get_historical_prices.side_effect = _capture

        with patch("lumibot.strategies.strategy.IS_BACKTESTING", True):
            strat.get_last_price(asset)

        assert captured["length"] == 1, (
            f"Shortcut called `length={captured.get('length')}` — must be 1 to "
            "avoid reaching past sim_time. `length=2` combined with any "
            "non-zero `timeshift` was the Alpha Picks 2026-04-17 bug."
        )
        assert captured["timestep"] == "day"
        # `timeshift` omitted is equivalent to `timeshift=0`. Both are safe.
        assert captured["timeshift"] in (None, 0), (
            f"Shortcut called `timeshift={captured.get('timeshift')}` — must be 0 "
            "or omitted. Any negative timeshift produces a `slice(x, x+|ts|+1)` "
            "that walks forward past sim_time. Never re-introduce `timeshift=-1`."
        )


class TestForwardFillWhenNoSimTimeBar:
    """When the shortcut's length=1 slice returns empty (no bar at sim_time),
    Rob's requirement is: forward-fill from the last prior known bar rather
    than returning None.

    Rationale: None breaks the rebalance logic (strategy skips the buy). At
    sim_time=00:00 on a 24/7 market, there's no day bar at 00:00 but
    2022-06-30 16:00 has a valid close that's a perfectly good "last known
    price" for mark-to-market purposes.
    """

    def test_forward_fill_returns_last_prior_bar_when_length1_empty(self):
        # Frame has a 2022-06-30 bar but sim_time 2022-07-01 00:00 might
        # not exactly hit it depending on iter_count semantics. The shortcut
        # should forward-fill with 2022-06-30's close.
        asset = Asset("AMR", "stock")
        data = _make_daily_data(
            asset,
            [
                ("2022-06-28 16:00", 120.0),
                ("2022-06-29 16:00", 122.0),
                ("2022-06-30 16:00", 124.87),
            ],
        )
        sim_time = NY.localize(datetime(2022, 7, 1, 0, 0))

        result, broker_called = _run_shortcut(data, asset, sim_time)

        # Either the shortcut forward-fills (124.87) or falls through.
        # The unacceptable outcome is returning None when there's plainly a
        # 2022-06-30 bar available.
        if not broker_called:
            assert result == pytest.approx(124.87), (
                f"Shortcut returned {result} for sim_time 2022-07-01 00:00 "
                f"when 2022-06-30 close (124.87) was the obvious forward-fill "
                f"candidate."
            )
