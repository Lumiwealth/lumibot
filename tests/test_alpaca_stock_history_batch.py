"""Stock history for many symbols comes from a few Alpaca requests, with identical bars.

A scanner asks for the history of hundreds of stocks in one grouped call
(``get_historical_prices_for_assets``). AlpacaBacktesting used to download each symbol's
window in its own request, although the stock bars endpoint accepts many symbols per
request (the live source and the option path already send 100). The grouped call must
send about N/100 requests and return exactly the bars the one-by-one path returns.

The fake client answers like GET /v2/stocks/bars (recorded response shape: a BarSet keyed
by symbol, only for the symbols requested, only inside the requested window). No live calls.
"""

from datetime import datetime, timedelta

import pandas as pd
from alpaca.data.models import BarSet

from lumibot.entities import Asset
from tests.test_alpaca_backtesting_multitimeframe_unit import _NY_TZ, _alpaca_source

SYMBOLS = [f"SYM{i:03d}" for i in range(250)]
NO_BARS = "SYM249"  # a symbol Alpaca has no bars for


def _daily_rows(symbol):
    base = 10.0 + SYMBOLS.index(symbol)
    rows = []
    # 2026-06-01 .. 2026-08-14, weekdays, stamped at 04:00 UTC like Alpaca daily bars.
    for n, day in enumerate(pd.bdate_range("2026-06-01", "2026-08-14")):
        price = round(base + n * 0.25, 2)
        rows.append(dict(t=f"{day.date()}T04:00:00Z", o=price, h=price + 1, l=price - 1, c=price + 0.5,
                         v=1000 + n, n=10, vw=price))
    return rows


class _StockBarsClient:
    def __init__(self):
        self.requests = []

    def get_stock_bars(self, request):
        symbols = request.symbol_or_symbols
        symbols = [symbols] if isinstance(symbols, str) else list(symbols)
        self.requests.append(symbols)
        start, end = pd.Timestamp(request.start), pd.Timestamp(request.end)
        start = start.tz_localize("UTC") if start.tzinfo is None else start
        end = end.tz_localize("UTC") if end.tzinfo is None else end
        out = {}
        for symbol in symbols:
            if symbol == NO_BARS:
                continue
            rows = [r for r in _daily_rows(symbol) if start <= pd.Timestamp(r["t"]) <= end]
            if rows:
                out[symbol] = rows
        return BarSet(out)


def _source(monkeypatch, tmp_path, **kwargs):
    source = _alpaca_source(monkeypatch, tmp_path, timestep="day", **kwargs)
    source._stock_client = _StockBarsClient()
    return source


def _frames(result):
    return {asset.symbol: (None if bars is None else bars.df) for asset, bars in result.items()}


def _assert_same(got, expected):
    assert set(got) == set(expected)
    for symbol, frame in expected.items():
        if frame is None:
            assert got[symbol] is None, symbol
        else:
            pd.testing.assert_frame_equal(got[symbol], frame)


def test_grouped_history_batches_stock_symbols_and_matches_one_by_one(monkeypatch, tmp_path):
    assets = [Asset(symbol) for symbol in SYMBOLS]
    clock = _NY_TZ.localize(datetime(2026, 8, 10, 9, 30))

    one_by_one = _source(monkeypatch, tmp_path / "a")
    one_by_one._datetime = clock
    expected = {}
    for asset in assets:
        try:
            bars = one_by_one.get_historical_prices(asset, 5, timestep="day")
            expected[asset.symbol] = bars.df
        except Exception:
            expected[asset.symbol] = None
    assert len(one_by_one._stock_client.requests) == len(SYMBOLS)  # one request per symbol
    assert expected[NO_BARS] is None and len(expected["SYM000"]) == 5

    batched = _source(monkeypatch, tmp_path / "b")
    batched._datetime = clock
    got = _frames(batched.get_bars(assets, 5, timestep="day"))
    requests = batched._stock_client.requests
    # 250 symbols: three requests of at most 100, plus the one-by-one retry of the symbol
    # with no bars (it must fail exactly as before).
    assert [len(r) for r in requests[:3]] == [100, 100, 50]
    assert requests[3:] == [[NO_BARS]]
    _assert_same(got, expected)

    # Same per-symbol cache files as the one-by-one path, byte for byte.
    files_a = sorted(p.name for p in (tmp_path / "a").rglob("*.csv"))
    files_b = sorted(p.name for p in (tmp_path / "b").rglob("*.csv"))
    assert files_a == files_b and len(files_a) == len(SYMBOLS) - 1
    for path in (tmp_path / "a").rglob("*.csv"):
        other = next((tmp_path / "b").rglob(path.name))
        assert path.read_bytes() == other.read_bytes()

    # The next day's scan downloads nothing for symbols that have bars.
    batched._datetime = clock + timedelta(days=1)
    before = len(requests)
    batched.get_bars(assets, 5, timestep="day")
    assert requests[before:] == [[NO_BARS]]


def test_grouped_history_before_the_window_is_batched_too(monkeypatch, tmp_path):
    """With history_before_start, the bars before the window also come in grouped requests."""
    assets = [Asset(symbol) for symbol in SYMBOLS[:120]]
    clock = _NY_TZ.localize(datetime(2026, 8, 4, 9, 30))

    one_by_one = _source(monkeypatch, tmp_path / "a")
    one_by_one._history_before_start = True
    one_by_one._datetime = clock
    expected = {a.symbol: one_by_one.get_historical_prices(a, 20, timestep="day").df for a in assets}
    assert len(one_by_one._stock_client.requests) == 2 * len(assets)  # window + earlier bars, per symbol
    assert len(expected["SYM000"]) == 20

    batched = _source(monkeypatch, tmp_path / "b")
    batched._history_before_start = True
    batched._datetime = clock
    got = _frames(batched.get_bars(assets, 20, timestep="day"))
    assert [len(r) for r in batched._stock_client.requests] == [100, 20, 100, 20]
    _assert_same(got, expected)


def test_grouped_history_falls_back_to_one_by_one_when_the_grouped_request_fails(monkeypatch, tmp_path):
    assets = [Asset(symbol) for symbol in SYMBOLS[:3]]
    source = _source(monkeypatch, tmp_path)
    source._datetime = _NY_TZ.localize(datetime(2026, 8, 10, 9, 30))
    real = source._stock_client.get_stock_bars

    def refuse_groups(request):
        if not isinstance(request.symbol_or_symbols, str) and len(request.symbol_or_symbols) > 1:
            raise RuntimeError("grouped request refused")
        return real(request)

    source._stock_client.get_stock_bars = refuse_groups
    got = _frames(source.get_bars(assets, 5, timestep="day"))
    assert all(frame is not None and len(frame) == 5 for frame in got.values())
