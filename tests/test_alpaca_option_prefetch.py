"""Option bars for many contracts come from one Alpaca request, with identical prices.

A delta strike search probes many strikes. Each probe used to download one contract's
whole backtest window in its own request, so a month of a daily iron condor made about
630 requests and a year hit Alpaca's request limit (2026-11-11). The option bars
endpoint accepts up to 100 symbols per request; prefetch_option_bars loads many
contracts at once and every later price lookup reads the same stored real prints.
"""

from datetime import date, datetime

import pandas as pd
from alpaca.data.models import BarSet

from lumibot.entities import Asset
from tests.test_alpaca_backtesting_multitimeframe_unit import _NY_TZ, _alpaca_source

_ROWS = {
    "SPY260821C00640000": [("2026-08-04T14:00:00Z", 1.00, 1.15, 0.98, 1.10, 5),
                           ("2026-08-04T14:05:00Z", 1.20, 1.30, 1.18, 1.25, 3),
                           ("2026-08-06T15:00:00Z", 1.40, 1.45, 1.35, 1.42, 2)],
    "SPY260821C00645000": [("2026-08-04T14:01:00Z", 0.80, 0.85, 0.75, 0.82, 2)],
    "SPY260821C00650000": [],  # never traded in the window
}


class _MultiSymbolClient:
    """Like GET /v1beta1/options/bars: answers only the requested symbols."""

    def __init__(self):
        self.requests = []
        self.windows = []

    def get_option_bars(self, request):
        symbols = request.symbol_or_symbols
        symbols = [symbols] if isinstance(symbols, str) else list(symbols)
        self.requests.append(symbols)
        self.windows.append((request.start, request.end))
        start, end = pd.Timestamp(request.start), pd.Timestamp(request.end)
        start = start.tz_localize("UTC") if start.tzinfo is None else start
        end = end.tz_localize("UTC") if end.tzinfo is None else end

        def inside(t):
            return start <= pd.Timestamp(t) <= end

        rows = {s: [dict(t=t, o=o, h=h, l=l, c=c, v=v, n=1, vw=c) for (t, o, h, l, c, v) in _ROWS[s] if inside(t)]
                for s in symbols}
        return BarSet({s: r for s, r in rows.items() if r})


def _call(strike):
    return Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 8, 21), strike=strike, right="CALL")


def _prices(source, clocks, strikes):
    out = []
    for clock in clocks:
        source._datetime = _NY_TZ.localize(clock)
        out.append(tuple(source.get_last_price(_call(k)) for k in strikes))
    return out


CLOCKS = [datetime(2026, 8, 4, 9, 45), datetime(2026, 8, 4, 10, 0), datetime(2026, 8, 4, 10, 1),
          datetime(2026, 8, 4, 10, 5), datetime(2026, 8, 4, 15, 0)]
STRIKES = [640, 645, 650]


def test_prefetch_loads_many_contracts_in_one_request_with_identical_prices(monkeypatch, tmp_path):
    one_by_one = _alpaca_source(monkeypatch, tmp_path / "a")
    one_by_one._option_client = _MultiSymbolClient()
    expected = _prices(one_by_one, CLOCKS, STRIKES)
    assert len(one_by_one._option_client.requests) == 3  # one request per contract today

    batched = _alpaca_source(monkeypatch, tmp_path / "b")
    batched._option_client = _MultiSymbolClient()
    batched._datetime = _NY_TZ.localize(CLOCKS[0])
    assert batched.prefetch_option_bars([_call(k) for k in STRIKES]) == 3
    assert batched._option_client.requests == [sorted(_ROWS)]
    assert _prices(batched, CLOCKS, STRIKES) == expected
    assert len(batched._option_client.requests) == 1  # nothing downloaded again
    assert expected[1] == (1.0, None, None) and expected[-1][2] is None


def test_prefetch_splits_requests_at_the_alpaca_symbol_limit(monkeypatch, tmp_path):
    source = _alpaca_source(monkeypatch, tmp_path)
    source._option_client = _MultiSymbolClient()
    source.ALPACA_OPTION_SYMBOLS_PER_REQUEST = 2
    source.prefetch_option_bars([_call(k) for k in STRIKES])
    assert [len(r) for r in source._option_client.requests] == [2, 1]


def test_prefetch_skips_contracts_already_loaded(monkeypatch, tmp_path):
    source = _alpaca_source(monkeypatch, tmp_path)
    source._option_client = _MultiSymbolClient()
    source._datetime = _NY_TZ.localize(CLOCKS[1])
    source.get_last_price(_call(640))
    assert source.prefetch_option_bars([_call(k) for k in STRIKES]) == 2
    assert source._option_client.requests[-1] == ["SPY260821C00645000", "SPY260821C00650000"]
    assert source.prefetch_option_bars([_call(k) for k in STRIKES]) == 0


def test_prefetch_until_loads_only_that_day_then_extends_once_when_needed(monkeypatch, tmp_path):
    """A probe at time T only needs prints up to T; later days load once, on first use."""
    clocks = CLOCKS + [datetime(2026, 8, 5, 10, 0), datetime(2026, 8, 6, 11, 30), datetime(2026, 8, 7, 10, 0)]
    one_by_one = _alpaca_source(monkeypatch, tmp_path / "a")
    one_by_one._option_client = _MultiSymbolClient()
    expected = _prices(one_by_one, clocks, STRIKES)

    batched = _alpaca_source(monkeypatch, tmp_path / "b")
    batched._option_client = _MultiSymbolClient()
    probe_time = _NY_TZ.localize(datetime(2026, 8, 4, 9, 45))
    batched._datetime = probe_time
    batched.prefetch_option_bars([_call(k) for k in STRIKES], until=probe_time)
    _start, first_end = batched._option_client.windows[0]
    first_end = pd.Timestamp(first_end)
    first_end = first_end.tz_localize("UTC") if first_end.tzinfo is None else first_end
    assert first_end < pd.Timestamp("2026-08-05T05:00:00Z")  # nothing after the probe day

    assert _prices(batched, clocks, STRIKES) == expected
    # One batched request, then one extension per contract read after the probe day.
    assert len(batched._option_client.requests) == 1 + len(STRIKES)
    assert expected[-2][0] == 1.42  # the 08-06 print (its close) appears only after extending
