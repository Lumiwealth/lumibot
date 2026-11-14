"""Intraday futures lookbacks must count trading bars, not wall-clock time.

Regression: a 15-minute continuous-futures strategy asked for 63 bars at the
start of a backtest and got 59. The four missing bars were the CME daily
maintenance hour (17:00-18:00 ET) inside the lookback, because the fetch window
was sized as 63 x 15 wall-clock minutes. The shortfall was then reported as
"required historical data unavailable" and the run was marked failed.

The fixture holds real recorded IBKR 15-minute micro Nasdaq bars (CME, Sep and
Oct 2026): maintenance hours, a weekend and the Labor Day early close are real
gaps in it. Nothing here fabricates or fills bars.
"""

import datetime as dt
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
import pytz

from lumibot.backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.tools.ibkr_history_health import (
    ibkr_history_health_snapshot,
    reset_ibkr_history_health_for_testing,
)

FIXTURE = Path(__file__).parent / "fixtures" / "ibkr_session_gaps" / "mnq_15minute_cme_2026.csv"
NY = pytz.timezone("America/New_York")


def _recorded_bars() -> pd.DataFrame:
    frame = pd.read_csv(FIXTURE, index_col="timestamp")
    frame.index = pd.to_datetime(frame.index, utc=True)
    frame["missing"] = False
    return frame


def _provider(frame: pd.DataFrame):
    """Answer like the provider: only recorded bars inside the requested window."""

    def get_price_data(*, start_dt, end_dt, **_kwargs):
        start = pd.Timestamp(start_dt).tz_convert("UTC")
        end = pd.Timestamp(end_dt).tz_convert("UTC")
        return frame.loc[(frame.index >= start) & (frame.index <= end)].copy()

    return get_price_data


def _request(now: dt.datetime, length: int, frame: pd.DataFrame, *, run_start: dt.datetime = None):
    reset_ibkr_history_health_for_testing()
    run_start = run_start or now
    source = InteractiveBrokersRESTBacktesting(
        datetime_start=run_start, datetime_end=run_start + dt.timedelta(days=1), exchange="CME"
    )
    source._update_datetime(now)
    asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
    with patch("lumibot.tools.ibkr_helper.get_price_data", side_effect=_provider(frame)):
        bars = source.get_historical_prices(asset, length, "15minute")
    source.record_history_requirement(asset=asset, timestep="15minute", requested_bars=length, bars=bars)
    return bars, ibkr_history_health_snapshot()["required_failures"]


@pytest.mark.parametrize(
    "now,length",
    [
        # The reported run: start of backtest, maintenance hour inside the lookback.
        (NY.localize(dt.datetime(2026, 10, 1, 0, 0)), 63),
        # Lookback crosses the weekend closure (Fri 17:00 to Sun 18:00).
        (NY.localize(dt.datetime(2026, 9, 7, 2, 0)), 63),
        # Lookback crosses the Labor Day early close plus maintenance hours.
        (NY.localize(dt.datetime(2026, 9, 8, 10, 0)), 120),
    ],
)
def test_session_breaks_do_not_shorten_the_lookback(now, length):
    bars, failures = _request(now, length, _recorded_bars())

    assert bars is not None
    assert len(bars.df) == length
    assert failures == []
    # Every returned bar is a real recorded bar that closed before "now".
    recorded = _recorded_bars().index
    assert bars.df.index.tz_convert("UTC").isin(recorded).all()
    assert bars.df.index.max() < now


def test_truly_missing_bars_are_still_reported():
    # The provider has nothing before the session that opened Sep 30 18:00:
    # only 24 real bars exist before the run start. That is a genuine shortfall.
    frame = _recorded_bars()
    frame = frame.loc[frame.index >= pd.Timestamp("2026-09-30 18:00", tz=NY)]

    bars, failures = _request(NY.localize(dt.datetime(2026, 10, 1, 0, 0)), 63, frame)

    assert bars is None or len(bars.df) < 63
    assert len(failures) == 1
    assert failures[0]["reason"] == "insufficient_required_history"
    assert failures[0]["requested_bars"] == 63


@pytest.mark.parametrize(
    "now,length",
    [
        (NY.localize(dt.datetime(2026, 10, 1, 0, 0)), 63),
        (NY.localize(dt.datetime(2026, 9, 7, 2, 0)), 63),
    ],
)
def test_routed_ibkr_futures_lookback_counts_trading_bars(monkeypatch, now, length):
    """The routed (multi-provider) data source has the same contract."""
    from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
    import lumibot.tools.ibkr_helper as ibkr_helper

    frame = _recorded_bars()
    monkeypatch.setattr(ibkr_helper, "get_price_data", _provider(frame))
    reset_ibkr_history_health_for_testing()
    source = RoutedBacktestingPandas(
        datetime_start=now, datetime_end=now + dt.timedelta(days=1),
        show_progress_bar=False, log_backtest_progress_to_file=False,
    )
    source.load_data()
    source._update_datetime(now)
    asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)

    bars = source.get_historical_prices(asset, length, "15minute")
    source.record_history_requirement(asset=asset, timestep="15minute", requested_bars=length, bars=bars)

    assert bars is not None and len(bars.df) == length
    assert ibkr_history_health_snapshot()["required_failures"] == []
