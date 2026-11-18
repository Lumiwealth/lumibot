"""A full Alpaca option backtest gives identical results when contracts load from the lookup month.

Why: a warm two-year weekly call-spread backtest on cloud workers spent most of its simulation
reading thousands of tiny shared-cache partitions, because each contract's first read loaded
its whole window from the backtest start (one monthly partition per elapsed month, nearly all
from before the contract was listed). The first read now loads from the month a week before
the lookup and reads earlier prints only when a lookup needs them.

Invariant under test: the trade log and every account value per iteration are exactly the
same as the whole-window read, compared in the same process (estimated fills depend on the
installed numeric libraries). And no first read of a contract starts at the backtest start.
All prices are synthetic and offline.
"""

import hashlib
from datetime import date, datetime, timedelta

import pandas as pd
import pytz
from alpaca.data.models import BarSet

import lumibot.backtesting.alpaca_backtesting as ab
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from tests.test_alpaca_backtesting_multitimeframe_unit import (
    _NY_TZ,
    _FakeTradingClient,
    _spy_contract_pages,
)

EXPIRY = date(2026, 8, 21)
START = datetime(2026, 5, 4)
END = datetime(2026, 8, 20)


def _trading_days():
    days = pd.bdate_range(START, END)
    return [d.date() for d in days if d.date() not in (date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3))]


def _stamp(day, minute):
    ts = _NY_TZ.localize(datetime.combine(day, datetime.min.time()) + timedelta(hours=9, minutes=30 + minute))
    return ts.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _rows():
    rows = {"SPY": []}
    for i, day in enumerate(_trading_days()):
        for minute in range(0, 390, 5):
            px = round(640.0 + 3.0 * ((i % 7) - 3) + minute * 0.004, 2)
            rows["SPY"].append((_stamp(day, minute), px, px + 0.05, px - 0.05, px, 1000))
    # Sparse option prints: 640 trades every few days from June, 645 rarely (and once in May,
    # so only a read before the lookup month finds it), 650 never.
    rows["SPY260821C00640000"] = []
    rows["SPY260821C00645000"] = [(_stamp(date(2026, 5, 12), 31), 3.10, 3.15, 3.05, 3.12, 2)]
    for i, day in enumerate(_trading_days()):
        if day >= date(2026, 6, 1) and i % 3 == 0:
            px = round(9.0 - 0.05 * i + 0.01 * (i % 4), 2)
            rows["SPY260821C00640000"].append((_stamp(day, 61), px, px + 0.1, px - 0.1, px, 4))
        if day >= date(2026, 7, 20) and i % 9 == 0:
            px = round(5.0 - 0.03 * i, 2)
            rows["SPY260821C00645000"].append((_stamp(day, 121), px, px + 0.1, px - 0.1, px, 1))
    rows["SPY260821C00650000"] = []
    return rows


ROWS = _rows()


class _WindowClient:
    """Answers only the requested window, like Alpaca; records each request."""

    def __init__(self):
        self.windows = []

    def _answer(self, request):
        symbols = request.symbol_or_symbols
        symbols = [symbols] if isinstance(symbols, str) else list(symbols)
        start, end = pd.Timestamp(request.start), pd.Timestamp(request.end)
        start = start.tz_localize("UTC") if start.tzinfo is None else start
        end = end.tz_localize("UTC") if end.tzinfo is None else end
        self.windows.append((tuple(symbols), start, end))
        out = {s: [dict(t=t, o=o, h=h, l=l, c=c, v=v, n=1, vw=c) for (t, o, h, l, c, v) in ROWS.get(s, [])
                   if start <= pd.Timestamp(t) <= end] for s in symbols}
        return BarSet({s: r for s, r in out.items() if r})

    get_option_bars = _answer
    get_stock_bars = _answer


def _call(strike):
    return Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration=EXPIRY, strike=strike, right="CALL")


def _run(monkeypatch, tmp_path, month_first):
    monkeypatch.setattr(ab, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    option_client = _WindowClient()

    class Offline(ab.AlpacaBacktesting):
        OPTION_FIRST_LOAD_FROM_LOOKUP_MONTH = month_first

        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            self._stock_client = _WindowClient()
            self._option_client = option_client
            self._trading_client = _FakeTradingClient(_spy_contract_pages())
            self._sleep = lambda s: None

    class WeeklySpread(Strategy):
        def initialize(self):
            self.sleeptime = "15M"
            self.vars.legs = None
            self.vars.values = []

        def on_trading_iteration(self):
            now = self.get_datetime()
            # Options are first touched in July, two months into the backtest.
            reading = now.date() >= date(2026, 7, 6) and now.minute == 0
            prices = tuple(self.get_last_price(_call(k)) for k in (640, 645, 650)) if reading else None
            if self.vars.legs is None and now.weekday() == 0 and now.hour == 10 and now.date() >= date(2026, 7, 6):
                self.submit_orders(
                    [self.create_order(_call(640), 2, "buy"), self.create_order(_call(645), 1, "sell")],
                    is_multileg=True,
                )
                self.vars.legs = now.date()
            elif self.vars.legs is not None and now.weekday() == 4 and (now.hour, now.minute) == (15, 30):
                for asset in (_call(640), _call(645)):
                    position = self.get_position(asset)
                    if position is not None and float(position.quantity) != 0:
                        side = "sell" if float(position.quantity) > 0 else "buy"
                        self.submit_order(self.create_order(asset, abs(float(position.quantity)), side))
                self.vars.legs = None
            self.vars.values.append((str(now), repr(float(self.get_portfolio_value())),
                                     repr(float(self.get_cash())), repr(prices)))

    _results, strategy = WeeklySpread.run_backtest(
        Offline,
        backtesting_start=_NY_TZ.localize(START),
        backtesting_end=_NY_TZ.localize(END),
        benchmark_asset=None, analyze_backtest=False, show_plot=False, save_tearsheet=False,
        show_tearsheet=False, show_progress_bar=False, budget=100_000, timestep="minute", market="NYSE",
        config={"API_KEY": "test-key", "API_SECRET": "test-secret", "PAPER": True},
    )
    log = strategy.broker._trade_event_log_df
    columns = [c for c in ("time", "symbol", "strike", "type", "status", "side", "price", "filled_quantity",
                           "price_source") if c in log.columns]
    trades = hashlib.sha256(log[columns].astype(str).to_csv(index=False).encode()).hexdigest()
    values = hashlib.sha256("\n".join(map(",".join, strategy.vars.values)).encode()).hexdigest()
    fills = int((log["status"] == "fill").sum()) if "status" in log.columns else 0
    return (trades, values, fills), option_client.windows


def test_full_backtest_is_identical_and_first_reads_skip_the_months_before_the_lookup(
    monkeypatch, tmp_path, disable_datasource_override
):
    whole, whole_windows = _run(monkeypatch, tmp_path / "whole", month_first=False)
    month, month_windows = _run(monkeypatch, tmp_path / "month", month_first=True)

    assert whole[2] > 0, "the fixture must fill orders"
    assert month == whole

    def first_reads(windows):
        seen = {}
        for symbols, start, _end in windows:
            for symbol in symbols:
                seen.setdefault(symbol, start)
        return seen

    backtest_start = pd.Timestamp(_NY_TZ.localize(START)).tz_convert("UTC")
    whole_firsts = {s: v for s, v in first_reads(whole_windows).items() if s != "SPY"}
    assert min(whole_firsts.values()) <= backtest_start + pd.Timedelta(days=1)
    option_firsts = {s: v for s, v in first_reads(month_windows).items() if s != "SPY"}
    assert set(option_firsts) == {"SPY260821C00640000", "SPY260821C00645000", "SPY260821C00650000"}
    # First touched on 2026-07-06: loaded from the month a week earlier (June), not from May.
    assert all(start >= pd.Timestamp("2026-06-01T00:00:00Z") for start in option_firsts.values()), option_firsts
    # The May print of 645 is still found when a lookup needs it: the answers above are equal.
