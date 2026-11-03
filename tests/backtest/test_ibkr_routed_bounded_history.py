"""Provider fixtures enforce request bounds so extra fixture history cannot hide gaps."""
from datetime import datetime

import pandas as pd
import pandas_market_calendars as mcal
import pytest

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset
from tests.backtest.test_routed_backtesting_ibkr_prefetch import _make_router


def _bounded_router(monkeypatch, day, cadence):
    from lumibot.tools import ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime.fromisoformat(day + "T09:30:00"))
    end = start + pd.Timedelta(days=5)
    router = _make_router(start, end, {"default": "ibkr", "stock": "ibkr"})
    schedule = mcal.get_calendar("NYSE").schedule(start - pd.Timedelta(days=100), end)
    if cadence == "day":
        index = pd.DatetimeIndex(schedule.market_close).tz_convert(LUMIBOT_DEFAULT_PYTZ)
    else:
        index = pd.DatetimeIndex([], tz="UTC")
        for row in schedule.itertuples():
            index = index.append(pd.date_range(row.market_open, row.market_close, freq="min", inclusive="left"))
        index = index.tz_convert(LUMIBOT_DEFAULT_PYTZ)
    values = pd.Series(range(len(index)), index=index, dtype=float) + 100
    frame = pd.DataFrame({"open": values, "high": values + 1, "low": values - 1,
                          "close": values + .5, "volume": 1000}, index=index)
    calls = []

    def prices(**kwargs):
        calls.append(kwargs)
        assert kwargs["timestep"] == cadence
        return frame.loc[(frame.index >= kwargs["start_dt"]) & (frame.index <= kwargs["end_dt"])].copy()

    monkeypatch.setattr(ibkr_helper, "get_price_data", prices)
    router._datetime = start
    return router, frame, calls


@pytest.mark.parametrize("day", ["2026-09-14", "2026-09-08"])
@pytest.mark.parametrize("cadence,length", [("day", 3), ("minute", 10)])
def test_routed_history_has_completed_warmup_across_weekend_and_holiday(monkeypatch, day, cadence, length):
    router, frame, calls = _bounded_router(monkeypatch, day, cadence)
    asset, quote = Asset("SPX", "index") if cadence == "minute" else Asset("SPY"), Asset("USD", "forex")
    bars = router.get_historical_prices(asset, length, cadence, quote=quote)
    expected = frame.loc[frame.index < router.get_datetime()].tail(length)
    assert bars is not None
    assert len(bars.df) == length
    assert bars.df.close.tolist() == expected.close.tolist()
    again = router.get_historical_prices(asset, length, cadence, quote=quote)
    assert again.df.close.tolist() == expected.close.tolist()
    assert len(calls) == 1, "Identical historical lookback must reuse the prefetched frame"


@pytest.mark.parametrize("direct", [False, True], ids=["routed", "direct"])
def test_routed_daily_prefetch_expands_for_later_longer_lookback(monkeypatch, direct):
    router, frame, calls = _bounded_router(monkeypatch, "2026-09-14", "day")
    if direct:
        from lumibot.backtesting import InteractiveBrokersRESTBacktesting
        router = InteractiveBrokersRESTBacktesting(datetime_start=router.datetime_start, datetime_end=router.datetime_end)
        router._update_datetime(router.datetime_start)
    asset, quote = Asset("SPY"), Asset("USD", "forex")
    router.get_historical_prices(asset, 3, "day", quote=quote)
    bars = router.get_historical_prices(asset, 30, "day", quote=quote)
    assert bars is not None
    assert len(bars.df) == 30
    assert bars.df.close.tolist() == frame.loc[frame.index < router.get_datetime()].tail(30).close.tolist()
    router.get_historical_prices(asset, 30, "day", quote=quote)
    assert len(calls) == 2


@pytest.mark.usefixtures("disable_datasource_override")
def test_no_benchmark_backtest_still_writes_filled_trade_artifact(monkeypatch, tmp_path):
    from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
    from lumibot.strategies import Strategy
    from lumibot.tools import ibkr_helper

    monkeypatch.chdir(tmp_path)
    _bounded_router(monkeypatch, "2026-09-14", "day")
    monkeypatch.setattr(ibkr_helper, "_get_cached_equity_actions", lambda *a, **k: pd.DataFrame())

    class BuyOne(Strategy):
        def initialize(self):
            self.sleeptime = "1D"

        def on_trading_iteration(self):
            self.add_line("observed", float(self.get_historical_prices(Asset("SPY"), 3, "day").df.close.iloc[-1]))
            if not self.get_position("SPY"):
                self.submit_order(self.create_order("SPY", 1, "buy"))

    _, strategy = BuyOne.run_backtest(
        RoutedBacktestingPandas, datetime(2026, 9, 14), datetime(2026, 9, 16),
        config={"backtesting_data_routing": {"default": "ibkr", "stock": "ibkr"}},
        benchmark_asset=None, risk_free_rate=0, show_plot=True,
        show_tearsheet=False, show_indicators=False, save_tearsheet=False,
        show_progress_bar=False, quiet_logs=True,
    )
    assert strategy.get_position("SPY").quantity == 1
    files = list((tmp_path / "logs").glob("*_trades.csv"))
    assert len(files) == 1, "Fills must be exported even without a benchmark chart"
    trades = pd.read_csv(files[0])
    assert (trades["status"].isin(["fill", "filled"])).sum() == 1


def test_mgc_51_completed_daily_bars_are_identical_on_cold_and_warm_route(monkeypatch):
    from lumibot.tools import ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 10, 1))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 10, 9))
    router = _make_router(start, end, {"default": "ibkr"})
    schedule = mcal.get_calendar("us_futures").schedule("2026-07-01", "2026-10-09")
    index = pd.DatetimeIndex(schedule.market_close).tz_convert(LUMIBOT_DEFAULT_PYTZ)
    closes = pd.Series(range(len(index)), index=index, dtype=float) + 4000
    frame = pd.DataFrame({"open": closes, "high": closes + 1, "low": closes - 1,
                          "close": closes, "volume": 1000}, index=index)

    def prices(**kwargs):
        return frame.loc[(frame.index >= kwargs["start_dt"]) & (frame.index <= kwargs["end_dt"])].copy()

    monkeypatch.setattr(ibkr_helper, "get_price_data", prices)
    for day in [1, 2, 5, 6, 7, 8]:
        router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 10, day, 18))
        expected = frame.loc[frame.index <= router.get_datetime()].tail(51)
        for _ in range(2):
            bars = router.get_historical_prices(Asset("MGC", "cont_future"), 51, "day")
            assert bars is not None and len(bars.df) == 51
            assert bars.df.close.tolist() == expected.close.tolist()
            assert bars.df.index.tolist() == expected.index.tolist()


@pytest.mark.parametrize("when,expected", [("2026-09-20 17:59:59", 104.), ("2026-09-20 18:00:00", None)], ids=["before-reopening", "exact-reopening"])
def test_closed_futures_mark_expires_at_exact_reopening(when, expected):
    from lumibot.tools.ibkr_helper import closed_futures_mark

    frame = pd.DataFrame({"close": [104.]}, index=pd.DatetimeIndex(["2026-09-18 16:59"], tz="America/New_York"))
    assert closed_futures_mark(frame, timestep="minute", when=pd.Timestamp(when, tz="America/New_York")) == expected


@pytest.mark.parametrize("holiday", ["2026-09-07", "2025-11-27"], ids=["labor-day", "thanksgiving"])
@pytest.mark.parametrize("routed", [False, True], ids=["direct", "routed"])
@pytest.mark.parametrize("symbol,market,multiplier", [
    ("MES", "CME_Equity", 5),
    ("MGC", "CMEGlobex_EnergyAndMetals", 10),
])
def test_futures_holiday_valuation_uses_initialized_exchange_calendar(monkeypatch, routed, symbol, market, multiplier, holiday):
    """A genuine scheduled early close must not become missing required data."""
    from lumibot.backtesting import BacktestingBroker, InteractiveBrokersRESTBacktesting
    from lumibot.strategies import Strategy
    from lumibot.tools import ibkr_helper
    from lumibot.tools.ibkr_history_health import reset_ibkr_history_health_for_testing, ibkr_history_health_snapshot

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    reset_ibkr_history_health_for_testing()
    day = pd.Timestamp(holiday, tz="America/New_York")
    schedule = mcal.get_calendar(market).schedule((day - pd.Timedelta(days=1)).date(),
                                                (day + pd.Timedelta(days=2)).date())
    close = pd.Timestamp(schedule.loc[holiday, "market_close"]).tz_convert("America/New_York")
    start = close - pd.Timedelta(minutes=2)
    end = day + pd.Timedelta(days=1, hours=18, minutes=1)
    frame = pd.DataFrame({"open": [100., 100., 200.], "high": [101., 101., 202.],
                          "low": [99., 99., 199.], "close": [100., 101., 201.], "volume": 1000.},
                         index=pd.DatetimeIndex([start, start + pd.Timedelta(minutes=1),
                                                day + pd.Timedelta(hours=18)]))

    def prices(**kwargs):
        return frame.loc[(frame.index >= kwargs["start_dt"]) & (frame.index <= kwargs["end_dt"])].copy()

    monkeypatch.setattr(ibkr_helper, "get_price_data", prices)
    source = (_make_router(start, end, {"default": "ibkr", "future": "ibkr"}) if routed
              else InteractiveBrokersRESTBacktesting(start, end))
    broker = BacktestingBroker(data_source=source)
    broker.market = market
    broker.initialize_market_calendars(schedule)
    asset = Asset(symbol, "future", expiration=datetime(2026, 12, 29).date(), multiplier=multiplier)
    quote = Asset("USD", "forex")
    source._update_datetime((start + pd.Timedelta(minutes=1)).to_pydatetime())
    strategy = Strategy(broker=broker, budget=100000, benchmark_asset=None)
    entry = strategy.create_order(asset, 1, "buy")
    strategy.submit_order(entry)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert entry.is_filled(), "Exercise a real native position rather than an empty account."
    assert entry.get_fill_price() == 100.

    source._update_datetime((day + pd.Timedelta(hours=17)).to_pydatetime())
    assert source.get_last_price(asset, quote=quote) == 101.
    assert strategy._update_portfolio_value() == pytest.approx(100000 + multiplier)
    assert ibkr_history_health_snapshot()["required_complete"] is True

    # A missing bar after a reopening is unknown, not another holiday mark.
    source._update_datetime((day + pd.Timedelta(days=1, hours=12)).to_pydatetime())
    assert source.get_last_price(asset, quote=quote) is None
    reset_ibkr_history_health_for_testing()


def test_direct_futures_reopening_refreshes_friday_mark_before_valuation(monkeypatch):
    from lumibot.backtesting import InteractiveBrokersRESTBacktesting
    from lumibot.tools import ibkr_helper

    opened = pd.Timestamp("2026-09-20 18:00", tz="America/New_York")
    asset, quote = Asset("MGC", "future", expiration=datetime(2026, 10, 28).date()), Asset("USD", "forex")
    source = InteractiveBrokersRESTBacktesting(opened.to_pydatetime(), (opened + pd.Timedelta(hours=1)).to_pydatetime())
    source._update_datetime(opened.to_pydatetime())
    frame = pd.DataFrame({"open": [100., 110.], "high": [105., 112.], "low": [99., 109.],
                          "close": [104., 111.], "volume": 1.},
                         index=pd.DatetimeIndex([opened - pd.Timedelta(days=2, minutes=61), opened]))
    calls = []
    def prices(**kwargs):
        calls.append(kwargs)
        return frame.loc[(frame.index >= kwargs["start_dt"]) & (frame.index <= kwargs["end_dt"])].copy()
    monkeypatch.setattr(ibkr_helper, "get_price_data", prices)
    source._update_pandas_data(asset, quote, "minute", start_dt=frame.index[0], end_dt=frame.index[0], exchange=None, include_after_hours=True)
    assert source.get_last_price(asset, quote=quote) == 110.
    assert len(calls) == 2
    assert source.get_last_price(asset, quote=quote) == 110.
    assert len(calls) == 2


@pytest.mark.parametrize("when,expected", [
    ("2026-10-02 19:59:00", 100.0),
    ("2026-10-02 23:59:00", 101.0),
    ("2026-10-04 23:59:00", 101.0),
    ("2026-10-05 04:00:00", None),
], ids=["forming", "overnight", "weekend", "exact-premarket-reopen"])
def test_stock_intraday_mark_retains_close_only_through_verified_closure(when, expected):
    from types import SimpleNamespace
    from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting

    frame = pd.DataFrame({"open": [100.], "close": [101.]},
                         index=pd.DatetimeIndex(["2026-10-02 19:59"], tz="America/New_York"))
    mark = InteractiveBrokersRESTBacktesting._intraday_mark_from_series(
        SimpleNamespace(df=frame), pd.Timestamp(when, tz="America/New_York"), 1)
    assert (mark[1] if mark is not None else None) == expected


def test_stock_intraday_mark_does_not_treat_missing_session_tail_as_closed():
    from types import SimpleNamespace
    from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting

    frame = pd.DataFrame({"open": [100.], "close": [101.]},
                         index=pd.DatetimeIndex(["2026-10-02 19:45"], tz="America/New_York"))
    assert InteractiveBrokersRESTBacktesting._intraday_mark_from_series(
        SimpleNamespace(df=frame), pd.Timestamp("2026-10-02 23:59", tz="America/New_York"), 1) is None
