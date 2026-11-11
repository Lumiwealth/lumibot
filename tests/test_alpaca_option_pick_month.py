"""A delta pick loads candidate contracts only from the month of (pick day - 7 days).

Each pick loaded every candidate from the start of the backtest, so in a long run each pick
read one shared-cache partition per elapsed month per contract: about 49 partitions a pick in
the first month, 97 by the third (local three-month iron condor). Prints before the pick
month are loaded only when a lookup needs them: a last price with no print in the loaded
part, or any history request. Every answer must stay identical to loading everything.
"""

from datetime import date, datetime

import pandas as pd
from alpaca.data.models import BarSet

from lumibot.entities import Asset
from tests.test_alpaca_backtesting_multitimeframe_unit import _NY_TZ, _alpaca_source

ROWS = {
    "SPY260821C00640000": [("2026-07-15T14:00:00Z", 2.00, 2.10, 1.95, 2.05, 4),
                           ("2026-08-04T14:00:00Z", 1.00, 1.15, 0.98, 1.10, 5),
                           ("2026-08-06T15:00:00Z", 1.40, 1.45, 1.35, 1.42, 2)],
    "SPY260821C00645000": [("2026-06-20T15:30:00Z", 0.70, 0.75, 0.65, 0.72, 2)],  # nothing since June
    "SPY260821C00650000": [],
}
STRIKES = [640, 645, 650]


class _Client:
    def __init__(self):
        self.windows = []

    def get_option_bars(self, request):
        symbols = request.symbol_or_symbols
        symbols = [symbols] if isinstance(symbols, str) else list(symbols)
        start, end = pd.Timestamp(request.start), pd.Timestamp(request.end)
        start = start.tz_localize("UTC") if start.tzinfo is None else start
        end = end.tz_localize("UTC") if end.tzinfo is None else end
        self.windows.append((tuple(symbols), start, end))
        rows = {s: [dict(t=t, o=o, h=h, l=l, c=c, v=v, n=1, vw=c) for (t, o, h, l, c, v) in ROWS[s]
                    if start <= pd.Timestamp(t) <= end] for s in symbols}
        return BarSet({s: r for s, r in rows.items() if r})


def _call(strike):
    return Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 8, 21), strike=strike, right="CALL")


CLOCKS = [datetime(2026, 8, 4, 10, 0), datetime(2026, 8, 4, 15, 0), datetime(2026, 8, 5, 10, 0),
          datetime(2026, 8, 6, 11, 30), datetime(2026, 8, 7, 10, 0)]


def _answers(source, probe=False):
    out = []
    for i, clock in enumerate(CLOCKS):
        source._datetime = _NY_TZ.localize(clock)
        if probe and i in (0, 2):  # a delta pick on two days
            source.prefetch_option_bars([_call(k) for k in STRIKES], until=source._datetime)
        out.append(tuple(source.get_last_price(_call(k)) for k in STRIKES))
    bars = source.get_historical_prices(_call(640), 3, "minute")
    out.append(None if bars is None else tuple(bars.df["close"].tolist()))
    return out


def _source(monkeypatch, path):
    source = _alpaca_source(monkeypatch, path, start=datetime(2026, 6, 1), end=datetime(2026, 8, 31))
    source._option_client = _Client()
    return source


def test_pick_loads_from_the_pick_month_and_answers_stay_identical(monkeypatch, tmp_path):
    expected = _answers(_source(monkeypatch, tmp_path / "full"))
    assert expected[2][1] == 0.72  # the June print, found only by reading before July

    picked = _source(monkeypatch, tmp_path / "pick")
    assert _answers(picked, probe=True) == expected
    first = picked._option_client.windows[0]
    # A pick on 08-04 loads from the start of the month a week earlier (July), not June.
    assert first[1] >= pd.Timestamp("2026-07-01T00:00:00Z")


def test_partial_frames_never_reach_the_window_csv_cache(monkeypatch, tmp_path):
    picked = _source(monkeypatch, tmp_path / "pick")
    picked._datetime = _NY_TZ.localize(CLOCKS[0])
    picked.prefetch_option_bars([_call(k) for k in STRIKES], until=picked._datetime)
    picked._datetime = _NY_TZ.localize(CLOCKS[2])
    picked.get_last_price(_call(640))  # extends past the pick day, still from August only
    later = _source(monkeypatch, tmp_path / "pick")  # same cache folder, new run
    expected = _answers(_source(monkeypatch, tmp_path / "full"))
    assert _answers(later) == expected


def test_short_history_requests_are_served_without_reading_earlier_months(monkeypatch, tmp_path):
    """A fill or a short history request needs only the newest bars; the loaded part has them."""
    full = _source(monkeypatch, tmp_path / "full")
    full._datetime = _NY_TZ.localize(CLOCKS[3])
    expected = full.get_historical_prices(_call(640), 2, "minute").df

    picked = _source(monkeypatch, tmp_path / "pick")
    picked._datetime = _NY_TZ.localize(CLOCKS[0])
    picked.prefetch_option_bars([_call(k) for k in STRIKES], until=picked._datetime)
    picked._datetime = _NY_TZ.localize(CLOCKS[3])
    got = picked.get_historical_prices(_call(640), 2, "minute").df
    pd.testing.assert_frame_equal(got, expected)
    starts = [w[1] for w in picked._option_client.windows]
    assert min(starts) >= pd.Timestamp("2026-07-01T00:00:00Z")  # no read before the loaded month
    # A longer request than the loaded part holds still reads the earlier months.
    assert tuple(picked.get_historical_prices(_call(640), 5, "minute").df["close"]) == tuple(
        full.get_historical_prices(_call(640), 5, "minute").df["close"])
