"""Regression guards that count operations, never elapsed time.

Each guard runs real LumiBot code against a local stand-in for the data provider and asserts how
many provider requests and cache reads it makes, read from the per-run telemetry file. A change
that brings back per-minute fetches, reads from the backtest start, or a chain fetch per lookup
fails here on any machine, at any speed. The stand-ins return synthetic bars only to count
requests; they are never market data.
"""

import json
from datetime import date, datetime, timedelta

import pandas as pd
from alpaca.data.models import BarSet

from lumibot.entities import Asset
from lumibot.tools import backtest_telemetry as bt

NY = "America/New_York"


# --------------------------------------------------------------------------- IBKR stock minute


def _ibkr_history(querystring):
    """1000-point Client Portal style page of extended-hours minute bars ending at startTime."""
    end = pd.Timestamp(datetime.strptime(querystring["startTime"], "%Y%m%d-%H:%M:%S"), tz="UTC")
    period = str(querystring["period"]).lower()
    bar = str(querystring.get("bar", "1min")).lower()
    if bar.endswith("d"):
        days = int(period.removesuffix("d").removesuffix("y")) * (365 if period.endswith("y") else 1)
        idx = pd.date_range(end - pd.Timedelta(days=days), end, freq="1D", tz="UTC", inclusive="left")
        idx = idx[idx.tz_convert(NY).weekday < 5]
    else:
        minutes = int(period.removesuffix("min"))
        idx = pd.date_range(end - pd.Timedelta(minutes=minutes), end, freq="1min", tz="UTC", inclusive="left")
        local = idx.tz_convert(NY)
        clock = local.hour * 60 + local.minute
        idx = idx[(local.weekday < 5) & (clock >= 240) & (clock < 1200)]
    idx = idx[-1000:]
    return {"volumeFactor": 1, "data": [
        {"t": int(ts.value // 10**6), "o": 100.0, "h": 100.5, "l": 99.5, "c": 100.1, "v": 10} for ts in idx]}


def _fake_downloader(url=None, querystring=None, headers=None, timeout=None, **_):
    path = url.split("/ibkr/")[-1]
    if path.endswith("secdef/search"):
        return [{"conid": 4242, "sections": [{"secType": "STK", "exchange": "NASDAQ"}]}]
    if path.endswith("/info"):
        return {"conid": 4242, "currency": "USD", "exchange": "NASDAQ", "instrument_type": "STK"}
    if path.endswith("marketdata/history"):
        return _ibkr_history(querystring)
    raise AssertionError(f"unexpected downloader path {path}")


def _run_ibkr_minute_backtest(monkeypatch, cache_dir, logs_dir):
    """One trading week of a stock strategy reading 20 minute bars every minute."""
    import lumibot.tools.ibkr_helper as ibkr_helper
    from lumibot.backtesting import InteractiveBrokersRESTBacktesting
    from lumibot.strategies import Strategy
    from lumibot.tools import data_downloader_queue_client as queue
    from lumibot.tools.ibkr_equity_basis import EquityBasisSnapshot

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", str(cache_dir))
    ibkr_helper._RUNTIME_CONID_CACHE.clear()
    basis = EquityBasisSnapshot.from_verified_actions(
        pd.DataFrame(columns=["Dividends", "Stock Splits"]), as_of=pd.Timestamp("2026-10-10T17:00Z"),
        coverage_through="2026-10-09")
    monkeypatch.setattr(ibkr_helper, "_get_current_equity_basis", lambda _symbol: basis)

    # The stand-in replaces the downloader transport, not queue_request, so queue_request's own
    # telemetry is what the guard reads.
    class _Client:
        def execute_request(self, method, path, query_params, headers=None, timeout=None, max_timeout_attempts=None):
            return _fake_downloader(url=f"http://standin/{path}", querystring=query_params), 200

    monkeypatch.setattr(queue, "get_queue_client", lambda *a, **k: _Client())
    monkeypatch.chdir(logs_dir)

    class Reader(Strategy):
        def initialize(self):
            self.sleeptime = "1M"

        def on_trading_iteration(self):
            self.get_historical_prices(Asset("MIDC", "stock"), 20, "minute")

    Reader.backtest(InteractiveBrokersRESTBacktesting, datetime(2026, 9, 14), datetime(2026, 9, 19),
                    benchmark_asset=None, show_plot=False, show_tearsheet=False, save_tearsheet=False,
                    show_indicators=False, quiet_logs=True, show_progress_bar=False)
    files = sorted((logs_dir / "logs").glob("*_telemetry.json"))
    assert files, "the backtest wrote no telemetry file"
    return json.loads(files[-1].read_text())


def _provider(telemetry, source, asset_class):
    for row in telemetry["providers"]:
        if row["source"] == source and row["asset_class"] == asset_class:
            return row
    return {"requests": 0, "rows": 0}


def test_ibkr_stock_minute_backtest_reads_each_session_once_and_nothing_when_warm(monkeypatch, tmp_path):
    cache = tmp_path / "cache"
    cold_dir, warm_dir = tmp_path / "cold", tmp_path / "warm"
    cold_dir.mkdir()
    warm_dir.mkdir()
    cold = _run_ibkr_minute_backtest(monkeypatch, cache, cold_dir)
    stock = _provider(cold, "ibkr", "stock")
    # Five simulated sessions plus the five-day lookback buffer: about one 1000-point page per
    # session (extended hours, 960 bars), one daily page, and the symbol lookup. A per-minute or
    # per-iteration fetch would be thousands.
    assert 1 <= stock["requests"] <= 16, stock
    assert cold["iterations"]["count"] > 1000  # it really ran every minute

    warm = _run_ibkr_minute_backtest(monkeypatch, cache, warm_dir)
    warm_stock = _provider(warm, "ibkr", "stock")
    history_free = warm_stock["requests"] <= 2  # symbol lookup and contract info at most
    assert history_free, warm_stock


# --------------------------------------------------------------------------- Alpaca option picks


def _option_symbol(expiration, right, strike):
    return f"SPY{expiration:%y%m%d}{right}{int(strike * 1000):08d}"


class _OptionBarsClient:
    """Two prints a weekday for every contract, inside the requested window."""

    _api_key = "standin-key"  # a credential scope, as production clients have: the shared cache applies

    def __init__(self):
        self.windows = []

    def get_option_bars(self, request):
        symbols = request.symbol_or_symbols
        symbols = [symbols] if isinstance(symbols, str) else list(symbols)
        start, end = pd.Timestamp(request.start), pd.Timestamp(request.end)
        start = start.tz_localize("UTC") if start.tzinfo is None else start.tz_convert("UTC")
        end = end.tz_localize("UTC") if end.tzinfo is None else end.tz_convert("UTC")
        self.windows.append((tuple(symbols), start, end))
        days = pd.date_range(start.normalize(), end, freq="1D")
        rows = {}
        for symbol in symbols:
            prints = [day + pd.Timedelta(hours=h) for day in days if day.weekday() < 5 for h in (14, 19)]
            prints = [t for t in prints if start <= t <= end]
            rows[symbol] = [dict(t=t.isoformat(), o=1.0, h=1.1, l=0.9, c=1.05, v=3, n=1, vw=1.0) for t in prints]
        return BarSet({s: r for s, r in rows.items() if r})


def _chain_pages(first_day):
    from tests.test_alpaca_backtesting_multitimeframe_unit import _contract

    contracts = []
    for week in range(8):
        expiration = first_day + timedelta(days=4 + 7 * week)  # Fridays
        for strike in range(600, 701, 5):
            for right in ("C", "P"):
                contracts.append(_contract(_option_symbol(expiration, right, strike), expiration, right, strike))
    return {"inactive": [contracts], "active": [[]]}


def _run_month_of_picks(monkeypatch, cache_dir):
    from lumibot import constants
    from lumibot.tools import shared_bar_cache
    from tests.test_alpaca_backtesting_multitimeframe_unit import _NY_TZ, _alpaca_source, _FakeTradingClient

    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(cache_dir))
    monkeypatch.setenv("LUMIBOT_SHARED_BARS", "true")  # tests/conftest.py turns it off by default
    shared_bar_cache._reset_process_memory()
    bt.reset()
    # The window starts in June so a read "from the backtest start" differs from the pick month.
    source = _alpaca_source(monkeypatch, cache_dir, start=datetime(2026, 6, 1), end=datetime(2026, 8, 29))
    trading = _FakeTradingClient(_chain_pages(date(2026, 8, 3)))
    bars = _OptionBarsClient()
    source._trading_client, source._option_client = trading, bars
    days = [d for d in pd.date_range("2026-08-03", "2026-08-28", freq="1D") if d.weekday() < 5]
    picks = []
    for day in days:
        source._datetime = _NY_TZ.localize(datetime(day.year, day.month, day.day, 10, 0))
        chains = source.get_chains(Asset("SPY"))
        source.get_chains(Asset("SPY"))  # a second lookup the same day must not fetch again
        expiration = min(e for e in chains["Chains"]["PUT"] if pd.Timestamp(e) >= day + pd.Timedelta(days=7))
        exp = pd.Timestamp(expiration).date()
        candidates = [Asset("SPY", Asset.AssetType.OPTION, expiration=exp, strike=float(k), right="PUT")
                      for k in range(600, 701, 5)]
        before = len(bars.windows)
        source.prefetch_option_bars(candidates, until=source._datetime)  # the delta search
        picks.append((day, bars.windows[before:]))
        for leg in candidates[8:10]:  # the two picked legs
            source.get_last_price(leg)
    shared_bar_cache.flush_shared_bar_writes(30)
    return source, trading, bars, picks


def test_month_of_option_picks_fetches_chains_once_a_day_and_reads_only_the_pick_day(monkeypatch, tmp_path):
    source, trading, bars, picks = _run_month_of_picks(monkeypatch, tmp_path / "cache")
    # The contract listing is fetched once for the run (one request per status) and each day's
    # chain is built from it, however often the strategy asks.
    assert len(trading.requests) <= 2
    # Each delta search makes at most one batched pick request, reading up to the end of its pick
    # day only, plus at most one batched extension of contracts an earlier pick loaded (from
    # their covered day to expiry). Nothing reads from the backtest start.
    for day, windows in picks:
        day_end = (day + pd.Timedelta(days=1)).tz_localize(NY)
        pick_requests = [w for w in windows if w[2] <= day_end]
        assert len(pick_requests) <= 1 and len(windows) <= 2, (day, windows)
        for _symbols, start, _end in windows:
            month_start = (day - pd.Timedelta(days=7)).replace(day=1).tz_localize("UTC")
            assert start >= month_start - pd.Timedelta(days=1), (day, start)
    assert len(bars.windows) <= 2 * len(picks)
    telemetry = bt.snapshot()
    assert _provider(telemetry, "alpaca", "option")["requests"] == len(trading.requests) + len(bars.windows)


def test_month_of_option_picks_makes_no_bar_requests_when_warm(monkeypatch, tmp_path):
    cache = tmp_path / "cache"
    _run_month_of_picks(monkeypatch, cache)
    _source, warm_trading, warm_bars, _picks = _run_month_of_picks(monkeypatch, cache)
    assert warm_bars.windows == [] and warm_trading.requests == []
    telemetry = bt.snapshot()
    assert _provider(telemetry, "alpaca", "option")["requests"] == 0
    layers = {row["layer"]: row for row in telemetry["caches"] if row["source"] == "alpaca"}
    assert layers["shared_bars"]["hits"] > 0 and layers["shared_bars"]["misses"] == 0
    assert layers["alpaca_option_chains"]["misses"] == 0

