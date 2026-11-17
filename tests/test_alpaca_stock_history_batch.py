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
import pytest
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


# -- as-traded closes (market cap needs the price traded that day) ---------------------------


class _AdjustedAndRawClient(_StockBarsClient):
    """Answers like Alpaca: ``adjustment=raw`` is the price traded, anything else is adjusted.

    Made-up history: every symbol has a 4-for-1 split on 2026-07-01 and a dividend adjustment
    of 10% before 2026-08-03, so before the split the traded price is 4 / 0.9 times the
    adjusted one.
    """

    SPLIT = pd.Timestamp("2026-07-01", tz="UTC")
    DIVIDEND = pd.Timestamp("2026-08-03", tz="UTC")

    def __init__(self):
        super().__init__()
        self.adjustments = []

    def get_stock_bars(self, request):
        adjustment = getattr(request.adjustment, "value", request.adjustment)
        self.adjustments.append(adjustment)
        bars = super().get_stock_bars(request)
        if adjustment != "raw":
            return bars
        out = {}
        for symbol, rows in bars.data.items():
            out[symbol] = []
            for bar in rows:
                stamp = pd.Timestamp(bar.timestamp)
                factor = (4.0 if stamp < self.SPLIT else 1.0) / (0.9 if stamp < self.DIVIDEND else 1.0)
                out[symbol].append(dict(
                    t=stamp.isoformat(), o=bar.open * factor, h=bar.high * factor, l=bar.low * factor,
                    c=bar.close * factor, v=bar.volume, n=10, vw=bar.close * factor,
                ))
        return BarSet(out)


def _as_traded_source(monkeypatch, tmp_path):
    source = _alpaca_source(
        monkeypatch, tmp_path, timestep="day", start=datetime(2026, 6, 1), end=datetime(2026, 8, 14)
    )
    source._stock_client = _AdjustedAndRawClient()
    return source


def test_as_traded_daily_closes_undo_split_and_dividend_adjustment(monkeypatch, tmp_path):
    source = _as_traded_source(monkeypatch, tmp_path)
    frame = source.get_backtest_as_traded_daily_closes(Asset("SYM000"))

    assert "raw" in source._stock_client.adjustments
    ratio = frame["as_traded_close"] / frame["close"]
    assert ratio[pd.Timestamp("2026-06-30")] == pytest.approx(4.0 / 0.9)   # before the split
    assert ratio[pd.Timestamp("2026-07-01")] == pytest.approx(1.0 / 0.9)   # split day: new share basis
    assert ratio[pd.Timestamp("2026-08-03")] == pytest.approx(1.0)         # nothing left to adjust
    # Only sessions with a real traded bar, each once.
    assert frame.index.is_unique and frame.index.is_monotonic_increasing
    assert len(frame) <= len(pd.bdate_range("2026-06-01", "2026-08-14"))

    # Asking again reads nothing more.
    before = len(source._stock_client.requests)
    assert source.get_backtest_as_traded_daily_closes(Asset("SYM000")) is frame
    assert len(source._stock_client.requests) == before


def test_as_traded_closes_for_a_universe_come_from_grouped_requests(monkeypatch, tmp_path):
    source = _as_traded_source(monkeypatch, tmp_path)
    assets = [Asset(symbol) for symbol in SYMBOLS]
    source.prefetch_backtest_as_traded_daily_closes(assets)

    requests = source._stock_client.requests
    # 250 symbols: three grouped requests for the backtest's own closes, three for as-traded.
    assert [len(r) for r in requests] == [100, 100, 50, 100, 100, 50]
    assert source._stock_client.adjustments[3:] == ["raw", "raw", "raw"]

    frame = source.get_backtest_as_traded_daily_closes(Asset("SYM007"))
    assert frame is not None and len(requests) == 6  # already loaded
    # A symbol Alpaca has no bars for has no as-traded close: None, never a made-up price.
    assert source.get_backtest_as_traded_daily_closes(Asset(NO_BARS)) is None


def test_only_stocks_have_as_traded_closes(monkeypatch, tmp_path):
    source = _as_traded_source(monkeypatch, tmp_path)

    assert source.get_backtest_as_traded_daily_closes(Asset("BTC", asset_type="crypto")) is None
    assert source._stock_client.requests == []



def test_daily_marks_find_their_session_without_rescanning_the_calendar(monkeypatch, tmp_path):
    """A daily price is asked for every symbol on every bar: the calendar is indexed by date once."""
    source = _source(monkeypatch, tmp_path, start=datetime(2026, 6, 1), end=datetime(2026, 8, 14))
    assets = [Asset(symbol) for symbol in SYMBOLS[:20]]
    moments = [
        datetime(2026, 6, 15, 9, 30),   # at the open: the open
        datetime(2026, 6, 15, 16, 0),   # at the close: the close
        datetime(2026, 7, 2, 12, 0),    # mid session: that day's open
    ]

    def marks():
        out = []
        for moment in moments:
            source._datetime = source.tzinfo.localize(moment)
            out.append([source.get_last_price(asset) for asset in assets])
        return out

    expected = marks()  # loads the bars
    assert expected[0] != expected[1] and all(price is not None for row in expected for price in row)

    calendar = source._trading_days["market_open"]
    accessor = type(calendar.dt)
    real_tz_convert = accessor.tz_convert
    scans = []

    def counting(self, *args, **kwargs):
        if len(self._parent) == len(calendar):
            scans.append(1)
        return real_tz_convert(self, *args, **kwargs)

    monkeypatch.setattr(accessor, "tz_convert", counting)
    assert marks() == expected
    assert len(scans) <= 1, f"the whole calendar was converted {len(scans)} times for {3 * len(assets)} prices"
