"""The first IBKR intraday page of a stock walk ends at the last session close, not in closed time.

A minute walk usually ends at midnight New York time (the end of the backtest or prefetch
window). Its first page used to end there too, so it reached back only to about 07:20 and
split the last session over two pages: the first two sessions took three requests. Anchoring
the first page at the previous session close fetches the same bars with one page per session.
"""

import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.tools import ibkr_helper as h

NY = "America/New_York"


@pytest.fixture(autouse=True)
def _frozen_equity_actions(monkeypatch):
    from lumibot.tools.ibkr_equity_basis import EquityBasisSnapshot

    ledger = pd.DataFrame(columns=["Dividends", "Stock Splits"])
    basis = EquityBasisSnapshot.from_verified_actions(
        ledger, as_of=pd.Timestamp("2026-10-10T17:00Z"), coverage_through="2026-10-09"
    )
    monkeypatch.setattr(h, "_get_current_equity_basis", lambda _: basis)


def _session_minutes(start_utc, end_utc, outside_rth):
    """Every minute an extended (04:00-20:00) or regular (09:30-16:00) weekday session holds."""
    idx = pd.date_range(start_utc, end_utc, freq="1min", inclusive="left", tz="UTC")
    local = idx.tz_convert(NY)
    minutes = local.hour * 60 + local.minute
    lo, hi = (240, 1200) if outside_rth else (570, 960)
    return idx[(local.weekday < 5) & (minutes >= lo) & (minutes < hi)]


def _walk(monkeypatch, end_local, sessions_back, outside_rth=True):
    requests = []

    def history(*, period, start_time, **_):
        requests.append(start_time)
        end = pd.Timestamp(start_time)
        span = pd.Timedelta(minutes=int(period.removesuffix("min")))
        idx = _session_minutes(end - span, end, outside_rth)[-1000:]
        data = [{"t": int(ts.value // 10**6), "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 1} for ts in idx]
        return {"volumeFactor": 1, "_botspot_meta": {"provider": "ibkr", "backend": "rest"}, "data": data}

    monkeypatch.setattr(h, "_resolve_conid", lambda **_: 4242)
    monkeypatch.setattr(h, "_ibkr_history_request", history)
    end = pd.Timestamp(end_local, tz=NY)
    start = end - pd.Timedelta(days=sessions_back)
    frame = h._fetch_history_between_dates(
        asset=Asset("MIDC", asset_type="stock"), quote=Asset("USD", asset_type="forex"),
        timestep="minute", start_dt=start.to_pydatetime(), end_dt=end.to_pydatetime(),
        exchange="SMART", include_after_hours=outside_rth, source="Trades", source_was_explicit=True,
    )
    return frame, requests


def test_walk_ending_at_midnight_asks_one_page_per_session(monkeypatch):
    # Wed 2026-09-30 00:00 New York; Mon 09-28 and Tue 09-29 are the sessions in reach.
    frame, requests = _walk(monkeypatch, "2026-09-30 00:00", sessions_back=2)
    first = pd.Timestamp(requests[0]).tz_convert(NY)
    assert first == pd.Timestamp("2026-09-29 20:01", tz=NY)  # last close plus one bar
    assert len(requests) == 2
    expected = _session_minutes(pd.Timestamp("2026-09-28 04:00", tz=NY).tz_convert("UTC"),
                                pd.Timestamp("2026-09-30 00:00", tz=NY).tz_convert("UTC"), True)
    got = frame.index.tz_convert("UTC")
    assert got.min() == expected.min() and got.max() == expected.max()
    assert set(expected).issubset(set(got))


def test_walk_ending_inside_a_session_keeps_its_end(monkeypatch):
    _frame, requests = _walk(monkeypatch, "2026-09-29 12:00", sessions_back=1)
    assert pd.Timestamp(requests[0]).tz_convert(NY) == pd.Timestamp("2026-09-29 12:01", tz=NY)


def test_walk_ending_on_a_weekend_anchors_at_friday_close(monkeypatch):
    _frame, requests = _walk(monkeypatch, "2026-09-27 00:00", sessions_back=3)  # Sunday
    assert pd.Timestamp(requests[0]).tz_convert(NY) == pd.Timestamp("2026-09-25 20:01", tz=NY)
