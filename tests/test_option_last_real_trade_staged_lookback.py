"""The option estimate's "last real trade" looks in the newest few bars before the full lookback.

Why: the fill estimate asked every data source for about a week of minute bars (1,950) just to
take the newest real trade. On data sources that load history lazily (Alpaca backtests load a
contract from the lookup month on), that request forced loading the contract's whole history
from the backtest start: one shared-cache file per elapsed month. The newest few bars are the
newest part of the full lookback, so looking there first gives the same trade.

Invariant: the answer equals the full-lookback answer for every bar pattern (a trade in the last
hour, an older trade, bad prints, a print at the current minute, no trade at all), and the full
lookback is requested only when the short one has no usable trade.
"""

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from lumibot.backtesting.backtesting_broker import BacktestingBroker

NOW = pd.Timestamp("2026-08-04 15:00", tz="America/New_York")


def _broker(closes_by_minutes_ago, timestep="minute"):
    """A broker whose data source returns the newest ``length`` bars of a fixed series."""
    unit = pd.Timedelta(days=1) if timestep == "day" else pd.Timedelta(minutes=1)
    stamps = [NOW - unit * ago for ago in sorted(closes_by_minutes_ago, reverse=True)]
    closes = [closes_by_minutes_ago[ago] for ago in sorted(closes_by_minutes_ago, reverse=True)]
    series = pd.DataFrame({"close": closes, "open": closes}, index=pd.DatetimeIndex(stamps))
    requested = []

    def recent(asset, quote, length):
        requested.append(int(length))
        frame = series.iloc[-int(length):] if len(series) else series
        return None if frame.empty else frame

    broker = SimpleNamespace(
        data_source=SimpleNamespace(_timestep=timestep),
        datetime=NOW,
        OPTION_IV_LOOKBACK_MINUTE_BARS=BacktestingBroker.OPTION_IV_LOOKBACK_MINUTE_BARS,
        OPTION_IV_LOOKBACK_DAY_BARS=BacktestingBroker.OPTION_IV_LOOKBACK_DAY_BARS,
        OPTION_IV_FIRST_LOOK_MINUTE_BARS=BacktestingBroker.OPTION_IV_FIRST_LOOK_MINUTE_BARS,
        OPTION_IV_FIRST_LOOK_DAY_BARS=BacktestingBroker.OPTION_IV_FIRST_LOOK_DAY_BARS,
        _recent_real_bars=recent,
    )
    for name in ("_coerce_price", "_is_invalid_price", "_bars_match_now", "_bar_matches_now"):
        setattr(broker, name, getattr(BacktestingBroker, name).__get__(broker))
    broker._last_real_trade = BacktestingBroker._last_real_trade.__get__(broker)
    return broker, requested


def _full_lookback_answer(closes_by_minutes_ago, timestep="minute"):
    """The pre-change rule: newest usable trade within the full lookback, current bar excluded."""
    lookback = (BacktestingBroker.OPTION_IV_LOOKBACK_DAY_BARS if timestep == "day"
                else BacktestingBroker.OPTION_IV_LOOKBACK_MINUTE_BARS)
    usable = [ago for ago in sorted(closes_by_minutes_ago)
              if ago != 0 and closes_by_minutes_ago[ago] == closes_by_minutes_ago[ago]
              and closes_by_minutes_ago[ago] > 0]
    newest = sorted(closes_by_minutes_ago)[:lookback]
    usable = [ago for ago in usable if ago in newest]
    return usable[0] if usable else None


CASES = {
    "trade a few minutes ago": {0: 2.0, 3: 1.9, 400: 1.5},
    "only older trades": {200: 1.7, 900: 1.4, 1800: 1.2},
    "bad recent prints": {1: 0.0, 2: float("nan"), 120: 1.1},
    "only the current bar": {0: 2.5},
    "nothing": {},
    "an old trade, the only bar": {2500: 1.0},
    "trade older than the first look": {**{ago: float("nan") for ago in range(1, 80)}, 300: 1.3},
}


@pytest.mark.parametrize("name", list(CASES))
def test_same_trade_as_the_full_lookback(name):
    closes = CASES[name]
    broker, requested = _broker(closes)
    got = broker._last_real_trade(object(), object())
    expected_ago = _full_lookback_answer(closes)
    if expected_ago is None:
        assert got is None
    else:
        assert got is not None
        assert got[0] == NOW - pd.Timedelta(minutes=expected_ago)
        assert got[1] == closes[expected_ago]
    first = BacktestingBroker.OPTION_IV_FIRST_LOOK_MINUTE_BARS
    assert requested[0] == first
    # Lookbacks count bars (data sources return the newest N bars), not minutes.
    found_early = expected_ago is not None and sorted(closes).index(expected_ago) < first
    assert (BacktestingBroker.OPTION_IV_LOOKBACK_MINUTE_BARS in requested) == (not found_early)


def test_daily_bars_look_back_a_few_days_first():
    broker, requested = _broker({1: 3.0, 20: 2.0}, timestep="day")
    got = broker._last_real_trade(object(), object())
    assert got[1] == 3.0
    assert requested == [BacktestingBroker.OPTION_IV_FIRST_LOOK_DAY_BARS]
    assert np.isfinite(got[1])
