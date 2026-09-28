from datetime import datetime

import pandas as pd
import pytz

from lumibot.backtesting import AlpacaBacktesting
from lumibot.entities import Asset


def test_alpaca_option_history_uses_the_listed_contract():
    """Option backtests must request the OCC contract, not the underlying stock."""
    data_source = AlpacaBacktesting.__new__(AlpacaBacktesting)
    data_source.market = "NYSE"
    data_source.tzinfo = pytz.UTC
    data_source._auto_adjust = True
    data_source._timestep = "day"
    data_source._option_client = object()
    data_source._stock_client = object()
    start = datetime(2026, 1, 23, tzinfo=pytz.UTC)
    end = datetime(2026, 1, 27, tzinfo=pytz.UTC)
    quote = Asset("USD", asset_type="forex")
    call_100 = Asset(
        "AAPL",
        asset_type=Asset.AssetType.OPTION,
        expiration="2027-01-15",
        strike=100,
        right="CALL",
    )
    call_120 = Asset(
        "AAPL",
        asset_type=Asset.AssetType.OPTION,
        expiration="2027-01-15",
        strike=120,
        right="CALL",
    )
    key_100 = data_source._get_asset_key(
        base_asset=call_100,
        quote_asset=quote,
        timestep="day",
        data_datetime_start=start,
        data_datetime_end=end,
    )
    key_120 = data_source._get_asset_key(
        base_asset=call_120,
        quote_asset=quote,
        timestep="day",
        data_datetime_start=start,
        data_datetime_end=end,
    )
    assert key_100 != key_120
    assert "100" in key_100
    assert "120" in key_120

    client, request = data_source._history_request(
        base_asset=call_100,
        quote_asset=quote,
        timestep="day",
        data_datetime_start=start,
        data_datetime_end=end,
        auto_adjust=True,
    )
    assert client is data_source._option_client
    assert request.symbol_or_symbols == "AAPL270115C00100000"


def test_alpaca_backtesting_normalizes_common_multi_timeframe_aliases():
    assert AlpacaBacktesting._normalize_timestep_for_source("15min") == ("15minute", None)
    assert AlpacaBacktesting._normalize_timestep_for_source("13 minutes") == ("13minute", None)
    assert AlpacaBacktesting._normalize_timestep_for_source("20min") == ("20minute", None)
    assert AlpacaBacktesting._normalize_timestep_for_source("1h") == ("hour", None)
    assert AlpacaBacktesting._normalize_timestep_for_source("4 hours") == ("4hour", None)
    assert AlpacaBacktesting._normalize_timestep_for_source("2d") == ("day", "2D")


def test_alpaca_backtesting_uses_native_15min_request():
    tzinfo = pytz.timezone("America/New_York")
    data_source = AlpacaBacktesting.__new__(AlpacaBacktesting)
    data_source._remove_incomplete_current_bar = False
    data_source._timestep = "minute"
    data_source._datetime = tzinfo.localize(datetime(2026, 1, 2, 10, 45))
    data_source._data_datetime_start = tzinfo.localize(datetime(2026, 1, 2, 0, 0))
    data_source._data_datetime_end = tzinfo.localize(datetime(2026, 1, 2, 23, 59))
    data_source._auto_adjust = True
    data_source.tzinfo = tzinfo

    index = pd.date_range(
        tzinfo.localize(datetime(2026, 1, 2, 9, 30)),
        periods=6,
        freq="15min",
    )
    native_15min_df = pd.DataFrame(
        {
            "open": range(len(index)),
            "high": [value + 0.5 for value in range(len(index))],
            "low": [value - 0.5 for value in range(len(index))],
            "close": [value + 0.25 for value in range(len(index))],
            "volume": [1] * len(index),
        },
        index=index,
    )

    requested_timesteps = []

    def fake_get_historical_prices_between_dates(**kwargs):
        requested_timesteps.append(kwargs["timestep"])
        return native_15min_df

    data_source.get_historical_prices_between_dates = fake_get_historical_prices_between_dates

    bars = data_source.get_historical_prices(Asset("TSLA"), length=3, timestep="15min")
    df = bars.pandas_df

    assert requested_timesteps == ["15minute"]
    assert list(df.index) == list(index[-3:])
    assert list(df["open"]) == [3, 4, 5]
    assert list(df["close"]) == [3.25, 4.25, 5.25]
    assert list(df["volume"]) == [1, 1, 1]


def test_alpaca_backtesting_uses_latest_completed_native_intraday_bar():
    tzinfo = pytz.timezone("America/New_York")
    data_source = AlpacaBacktesting.__new__(AlpacaBacktesting)
    data_source._remove_incomplete_current_bar = False
    data_source._timestep = "minute"
    data_source._datetime = tzinfo.localize(datetime(2026, 1, 2, 10, 50))
    data_source._data_datetime_start = tzinfo.localize(datetime(2026, 1, 2, 0, 0))
    data_source._data_datetime_end = tzinfo.localize(datetime(2026, 1, 2, 23, 59))
    data_source._auto_adjust = True
    data_source.tzinfo = tzinfo

    index = pd.DatetimeIndex(
        [
            tzinfo.localize(datetime(2026, 1, 2, 10, 0)),
            tzinfo.localize(datetime(2026, 1, 2, 10, 20)),
            tzinfo.localize(datetime(2026, 1, 2, 10, 40)),
            tzinfo.localize(datetime(2026, 1, 2, 11, 0)),
        ]
    )
    native_20min_df = pd.DataFrame(
        {
            "open": range(len(index)),
            "high": [value + 0.5 for value in range(len(index))],
            "low": [value - 0.5 for value in range(len(index))],
            "close": [value + 0.25 for value in range(len(index))],
            "volume": [1] * len(index),
        },
        index=index,
    )

    requested_timesteps = []

    def fake_get_historical_prices_between_dates(**kwargs):
        requested_timesteps.append(kwargs["timestep"])
        return native_20min_df

    data_source.get_historical_prices_between_dates = fake_get_historical_prices_between_dates

    bars = data_source.get_historical_prices(Asset("TSLA"), length=2, timestep="20min")
    df = bars.pandas_df

    assert requested_timesteps == ["20minute"]
    assert list(df.index) == list(index[1:3])
    assert list(df["open"]) == [1, 2]


def test_alpaca_remove_incomplete_current_bar_drops_a_native_bar_that_is_still_forming():
    """The documented option must drop the forming bar even when now is inside the bar.

    It used to drop the current bar only when its label equaled now exactly, so at 10:50 the
    20-minute bar labeled 10:40 (closing at 11:00) was still returned.
    """
    tzinfo = pytz.timezone("America/New_York")
    data_source = AlpacaBacktesting.__new__(AlpacaBacktesting)
    data_source._remove_incomplete_current_bar = True
    data_source._timestep = "minute"
    data_source._datetime = tzinfo.localize(datetime(2026, 1, 2, 10, 50))
    data_source._data_datetime_start = tzinfo.localize(datetime(2026, 1, 2, 0, 0))
    data_source._data_datetime_end = tzinfo.localize(datetime(2026, 1, 2, 23, 59))
    data_source._auto_adjust = True
    data_source.tzinfo = tzinfo

    index = pd.DatetimeIndex(
        [tzinfo.localize(datetime(2026, 1, 2, hour, minute)) for hour, minute in ((10, 0), (10, 20), (10, 40), (11, 0))]
    )
    native_20min_df = pd.DataFrame(
        {
            "open": range(len(index)),
            "high": [value + 0.5 for value in range(len(index))],
            "low": [value - 0.5 for value in range(len(index))],
            "close": [value + 0.25 for value in range(len(index))],
            "volume": [1] * len(index),
        },
        index=index,
    )
    data_source.get_historical_prices_between_dates = lambda **kwargs: native_20min_df

    bars = data_source.get_historical_prices(Asset("TSLA"), length=2, timestep="20min")
    assert list(bars.pandas_df.index) == list(index[0:2])

    # At 11:00 the 10:40 bar has closed and is returned.
    data_source._datetime = tzinfo.localize(datetime(2026, 1, 2, 11, 0))
    bars = data_source.get_historical_prices(Asset("TSLA"), length=2, timestep="20min")
    assert list(bars.pandas_df.index) == list(index[1:3])


def test_alpaca_backtesting_uses_alpaca_sdk_timeframes_for_intraday_multiples():
    assert str(AlpacaBacktesting._get_alpaca_timeframe("13minute")) == "13Min"
    assert str(AlpacaBacktesting._get_alpaca_timeframe("15minute")) == "15Min"
    assert str(AlpacaBacktesting._get_alpaca_timeframe("20minute")) == "20Min"
    assert str(AlpacaBacktesting._get_alpaca_timeframe("hour")) == "1Hour"
    assert str(AlpacaBacktesting._get_alpaca_timeframe("4hour")) == "4Hour"


# ---------------------------------------------------------------------------
# 2026-09-23: Alpaca options backtesting (bring-your-own-key options data).
#
# get_chains() used to be a stub that returned {}, so strategies that discover
# strikes found nothing, and option bars were reindexed and forward-filled like
# stock bars, which invents prices between sparse option trades (RULE #1).
# ---------------------------------------------------------------------------

import json
from datetime import date, timedelta
from types import SimpleNamespace

import pytest
from alpaca.common.exceptions import APIError
from alpaca.data.models import BarSet
from alpaca.trading.enums import AssetStatus, ContractType

from lumibot.backtesting import BacktestingBroker
from lumibot.entities import Chains, Order

_NY_TZ = pytz.timezone("America/New_York")


def _alpaca_source(monkeypatch, tmp_path, *, timestep="minute", start=datetime(2026, 8, 3), end=datetime(2026, 8, 14)):
    import lumibot.backtesting.alpaca_backtesting as alpaca_backtesting

    monkeypatch.setattr(alpaca_backtesting, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    source = AlpacaBacktesting(
        datetime_start=_NY_TZ.localize(start),
        datetime_end=_NY_TZ.localize(end),
        config={"API_KEY": "test-key", "API_SECRET": "test-secret", "PAPER": True},
        timestep=timestep,
        market="NYSE",
    )
    source._sleep = lambda seconds: None
    return source


def _contract(symbol, expiration, right, strike, *, root="SPY", size="100", status=AssetStatus.INACTIVE):
    return SimpleNamespace(
        symbol=symbol,
        root_symbol=root,
        underlying_symbol="SPY",
        expiration_date=expiration,
        type=ContractType.CALL if right == "C" else ContractType.PUT,
        strike_price=float(strike),
        size=size,
        status=status,
    )


class _FakeTradingClient:
    """Pages of option contracts per status, like GET /v2/options/contracts."""

    def __init__(self, pages_by_status, *, failures=None):
        self.pages_by_status = pages_by_status
        self.failures = list(failures or [])
        self.requests = []

    def get_option_contracts(self, request):
        self.requests.append(request)
        if self.failures:
            raise self.failures.pop(0)
        pages = self.pages_by_status.get(request.status.value, [[]])
        idx = 0 if request.page_token is None else int(request.page_token)
        gte, lte = request.expiration_date_gte, request.expiration_date_lte
        rows = [c for c in pages[idx] if gte <= c.expiration_date <= lte]
        return SimpleNamespace(
            option_contracts=rows,
            next_page_token=str(idx + 1) if idx + 1 < len(pages) else None,
        )


def _spy_contract_pages():
    exp1, exp2, too_far = date(2026, 8, 7), date(2026, 8, 21), date(2027, 1, 15)
    inactive = [
        [
            _contract("SPY260807C00640000", exp1, "C", 640),
            _contract("SPY260807C00645000", exp1, "C", 645),
            _contract("SPY260807P00640000", exp1, "P", 640),
        ],
        [
            _contract("SPY260821C00650000", exp2, "C", 650),
            # Adjusted deliverable after a corporate action: not a standard 100-share SPY contract.
            _contract("SPY1260821C00650000", exp2, "C", 650, root="SPY1"),
            _contract("SPY260821P00650000", exp2, "P", 650, size="10"),
        ],
    ]
    active = [[_contract("SPY270115C00700000", too_far, "C", 700, status=AssetStatus.ACTIVE)]]
    return {"inactive": inactive, "active": active}


def _bars(symbol, rows):
    return BarSet({symbol: [dict(t=t, o=o, h=h, l=l, c=c, v=v, n=1, vw=c) for (t, o, h, l, c, v) in rows]})


def test_alpaca_backtesting_get_chains_lists_expired_and_active_contracts(monkeypatch, tmp_path):
    source = _alpaca_source(monkeypatch, tmp_path)
    source._trading_client = _FakeTradingClient(_spy_contract_pages())
    source._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 9, 30))

    chains = source.get_chains(Asset("SPY"))

    assert isinstance(chains, Chains)
    assert chains["Multiplier"] == 100
    assert chains["UnderlyingSymbol"] == "SPY"
    assert chains["Chains"]["CALL"] == {"2026-08-07": [640.0, 645.0], "2026-08-21": [650.0]}
    assert chains["Chains"]["PUT"] == {"2026-08-07": [640.0]}

    statuses = {r.status for r in source._trading_client.requests}
    assert statuses == {AssetStatus.INACTIVE, AssetStatus.ACTIVE}
    assert any(r.page_token == "1" for r in source._trading_client.requests), "pagination was not followed"
    for request in source._trading_client.requests:
        assert request.underlying_symbols == ["SPY"]
        assert request.expiration_date_gte == date(2026, 8, 3)
        assert request.expiration_date_lte >= date(2026, 8, 3) + timedelta(days=AlpacaBacktesting.OPTION_CHAIN_MAX_DAYS)
    # Nothing past the chain horizon, even though the listing is wider.
    assert "2027-01-15" not in chains["Chains"]["CALL"]


def test_alpaca_backtesting_get_chains_caches_per_day_in_memory_and_on_disk(monkeypatch, tmp_path):
    source = _alpaca_source(monkeypatch, tmp_path)
    source._trading_client = _FakeTradingClient(_spy_contract_pages())
    source._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 9, 30))

    first = source.get_chains(Asset("SPY"))
    calls_after_first = len(source._trading_client.requests)
    assert calls_after_first > 0

    # Every iteration of the same day is served from memory.
    source._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 15, 0))
    assert source.get_chains(Asset("SPY"))["Chains"] == first["Chains"]
    assert len(source._trading_client.requests) == calls_after_first

    # A later day inside the listing window reuses the listing: no new API calls.
    source._datetime = _NY_TZ.localize(datetime(2026, 8, 10, 9, 30))
    later = source.get_chains(Asset("SPY"))
    assert len(source._trading_client.requests) == calls_after_first
    assert "2026-08-07" not in later["Chains"]["CALL"]  # expired before this simulated day
    assert later["Chains"]["CALL"]["2026-08-21"] == [650.0]

    cached_files = list((tmp_path / "alpaca" / "option_chains").glob("SPY_2026-08-03*.json"))
    assert cached_files, "chain was not cached on disk"
    assert json.loads(cached_files[0].read_text())["Chains"]["CALL"]["2026-08-07"] == [640.0, 645.0]

    # A fresh process reads the disk cache instead of calling Alpaca.
    fresh = _alpaca_source(monkeypatch, tmp_path)
    fresh._trading_client = _FakeTradingClient(_spy_contract_pages())
    fresh._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 11, 0))
    assert fresh.get_chains(Asset("SPY"))["Chains"] == first["Chains"]
    assert fresh._trading_client.requests == []


def test_alpaca_backtesting_get_chains_waits_politely_on_rate_limit(monkeypatch, tmp_path):
    source = _alpaca_source(monkeypatch, tmp_path)
    waits = []
    source._sleep = waits.append
    rate_limited = APIError(
        '{"code": 42910000, "message": "rate limit exceeded"}',
        http_error=SimpleNamespace(response=SimpleNamespace(status_code=429, headers={"Retry-After": "7"}), request=None),
    )
    source._trading_client = _FakeTradingClient(_spy_contract_pages(), failures=[rate_limited])
    source._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 9, 30))

    chains = source.get_chains(Asset("SPY"))

    assert chains["Chains"]["CALL"]["2026-08-07"] == [640.0, 645.0]
    assert waits == [7.0]

    # A key that stays rate limited fails loudly after a bounded number of waits.
    stuck = _alpaca_source(monkeypatch, tmp_path / "stuck")
    stuck_waits = []
    stuck._sleep = stuck_waits.append
    stuck._trading_client = _FakeTradingClient(_spy_contract_pages(), failures=[rate_limited] * 50)
    stuck._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 9, 30))
    with pytest.raises(RuntimeError, match="rate limit"):
        stuck.get_chains(Asset("SPY"))
    assert 0 < len(stuck_waits) <= 6
    assert sum(stuck_waits) <= 120


def _sparse_call():
    return Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 8, 21), strike=640, right="CALL")


class _FakeOptionClient:
    def __init__(self, barset):
        self.barset = barset
        self.requests = []

    def get_option_bars(self, request):
        self.requests.append(request)
        return self.barset


def test_alpaca_option_bars_keep_only_real_trade_prints(monkeypatch, tmp_path):
    source = _alpaca_source(monkeypatch, tmp_path)
    symbol = "SPY260821C00640000"
    source._option_client = _FakeOptionClient(
        _bars(
            symbol,
            [
                ("2026-08-04T14:00:00Z", 1.00, 1.15, 0.98, 1.10, 5),
                ("2026-08-04T14:05:00Z", 1.20, 1.30, 1.18, 1.25, 3),
                ("2026-08-04T18:30:00Z", 0.90, 0.96, 0.88, 0.95, 1),
            ],
        )
    )
    source._datetime = _NY_TZ.localize(datetime(2026, 8, 4, 15, 0))

    bars = source.get_historical_prices(_sparse_call(), length=5, timestep="minute")

    assert bars is not None
    assert [ts.strftime("%H:%M") for ts in bars.df.index] == ["10:00", "10:05", "14:30"]
    assert bars.df["open"].tolist() == [1.00, 1.20, 0.90]
    assert source._option_client.requests[0].symbol_or_symbols == symbol


def test_alpaca_option_last_price_uses_real_bars_and_none_before_the_first_trade(monkeypatch, tmp_path):
    source = _alpaca_source(monkeypatch, tmp_path)
    source._option_client = _FakeOptionClient(
        _bars(
            "SPY260821C00640000",
            [
                ("2026-08-04T14:00:00Z", 1.00, 1.15, 0.98, 1.10, 5),
                ("2026-08-04T14:05:00Z", 1.20, 1.30, 1.18, 1.25, 3),
                ("2026-08-04T18:30:00Z", 0.90, 0.96, 0.88, 0.95, 1),
            ],
        )
    )
    option = _sparse_call()

    source._datetime = _NY_TZ.localize(datetime(2026, 8, 4, 9, 45))
    assert source.get_last_price(option) is None  # no trade yet: never a back-filled future price
    assert source.get_historical_prices(option, length=3, timestep="minute") is None

    source._datetime = _NY_TZ.localize(datetime(2026, 8, 4, 10, 2))
    assert float(source.get_last_price(option)) == 1.10  # last real trade, not an invented 10:02 bar

    source._datetime = _NY_TZ.localize(datetime(2026, 8, 4, 10, 5))
    assert float(source.get_last_price(option)) == 1.20  # a bar prints now: its open, like stock bars

    source._datetime = _NY_TZ.localize(datetime(2026, 8, 4, 15, 0))
    assert float(source.get_last_price(option)) == 0.95
    assert len(source._option_client.requests) == 1


def test_alpaca_option_without_any_bars_returns_none_with_a_clear_error(monkeypatch, tmp_path, caplog):
    source = _alpaca_source(monkeypatch, tmp_path, start=datetime(2023, 6, 1), end=datetime(2023, 6, 14))
    source._option_client = _FakeOptionClient(BarSet({}))
    option = Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration=date(2023, 6, 16), strike=420, right="CALL")
    source._datetime = _NY_TZ.localize(datetime(2023, 6, 5, 10, 0))

    with caplog.at_level("ERROR"):
        assert source.get_last_price(option) is None
        assert source.get_historical_prices(option, length=5, timestep="minute") is None

    message = " ".join(record.getMessage() for record in caplog.records)
    assert "SPY230616C00420000" in message
    assert "no option bars" in message.lower()
    assert "february 2024" in message.lower()
    # The empty answer is remembered for the run instead of re-asking every bar.
    assert len(source._option_client.requests) == 1


def test_backtesting_broker_fills_alpaca_options_only_on_a_bar_that_printed_now():
    broker = BacktestingBroker.__new__(BacktestingBroker)
    broker.data_source = AlpacaBacktesting.__new__(AlpacaBacktesting)
    option_order = Order("test", asset=_sparse_call(), quantity=1, side="buy", order_type=Order.OrderType.MARKET)
    stock_order = Order("test", asset=Asset("SPY"), quantity=1, side="buy", order_type=Order.OrderType.MARKET)

    assert broker._requires_current_execution_bar(option_order, "minute", "ALPACA") is True
    assert broker._requires_current_execution_bar(option_order, "day", "ALPACA") is True
    # Stock bars keep their existing Alpaca behavior.
    assert broker._requires_current_execution_bar(stock_order, "minute", "ALPACA") is False
    assert broker._requires_current_execution_bar(stock_order, "day", "ALPACA") is False


class _FakeStockClient:
    def __init__(self, barset):
        self.barset = barset

    def get_stock_bars(self, request):
        return self.barset


def _spy_minute_bars(days):
    rows = []
    for day in days:
        for minute in range(390):
            ts = _NY_TZ.localize(datetime.fromisoformat(f"{day} 09:30")) + timedelta(minutes=minute)
            px = 640.0 + minute * 0.01
            rows.append((ts.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"), px, px + 0.05, px - 0.05, px, 1000))
    return _bars("SPY", rows)


def test_alpaca_option_order_fills_on_the_next_real_print_in_a_full_backtest(monkeypatch, tmp_path, disable_datasource_override):
    """Offline end-to-end: get_chains -> market order -> fill only on a real option print."""
    import lumibot.backtesting.alpaca_backtesting as alpaca_backtesting
    from lumibot.strategies import Strategy

    monkeypatch.setattr(alpaca_backtesting, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    days = ["2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06", "2026-08-07"]
    option_bars = _bars(
        "SPY260821C00650000",
        [
            ("2026-08-03T13:31:00Z", 5.00, 5.10, 4.95, 5.05, 3),  # 09:31, before the order
            ("2026-08-03T13:37:00Z", 5.40, 5.50, 5.35, 5.45, 2),  # 09:37, first print after it
            ("2026-08-03T15:00:00Z", 5.80, 5.90, 5.75, 5.85, 1),
        ],
    )

    class OfflineAlpacaBacktesting(AlpacaBacktesting):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._stock_client = _FakeStockClient(_spy_minute_bars(days))
            self._option_client = _FakeOptionClient(option_bars)
            self._trading_client = _FakeTradingClient(_spy_contract_pages())
            self._sleep = lambda seconds: None

    class BuyOneCall(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.vars.submitted_at = None
            self.vars.fills = []

        def on_trading_iteration(self):
            now = self.get_datetime()
            if self.vars.submitted_at is None and (now.hour, now.minute) >= (9, 35):
                chains = self.get_chains(Asset("SPY"))
                strikes = chains["Chains"]["CALL"]["2026-08-21"]
                contract = Asset(
                    "SPY", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 8, 21), strike=strikes[0], right="CALL"
                )
                self.submit_order(self.create_order(contract, 1, "buy"))
                self.vars.submitted_at = now

        def on_filled_order(self, position, order, price, quantity, multiplier):
            self.vars.fills.append((self.get_datetime(), float(price)))

    _results, strategy = BuyOneCall.run_backtest(
        OfflineAlpacaBacktesting,
        backtesting_start=_NY_TZ.localize(datetime(2026, 8, 3)),
        backtesting_end=_NY_TZ.localize(datetime(2026, 8, 7)),
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_progress_bar=False,
        budget=10_000,
        timestep="minute",
        market="NYSE",
        config={"API_KEY": "test-key", "API_SECRET": "test-secret", "PAPER": True},
    )

    assert strategy.vars.submitted_at.strftime("%H:%M") == "09:35"
    assert len(strategy.vars.fills) == 1
    filled_at, fill_price = strategy.vars.fills[0]
    # No print at 09:35 or 09:36: the order waits for the 09:37 trade and fills at its open.
    assert filled_at.strftime("%Y-%m-%d %H:%M") == "2026-08-03 09:37"
    assert fill_price == 5.40


# ---------------------------------------------------------------------------
# 2026-09-23: AlpacaBacktesting selected ONLY through BACKTESTING_DATA_SOURCE=alpaca.
#
# This is how BotSpot runs an Alpaca backtest: the strategy calls
# backtest(datasource_class=None, ...) without a config or timestep, BotSpot Node sets
# BACKTESTING_DATA_SOURCE=alpaca and ALPACA_IS_PAPER=true, and the customer's
# credentials arrive as ALPACA_API_KEY/ALPACA_API_SECRET or ALPACA_OAUTH_TOKEN.
# Before this change that path could not even build the data source (config=None),
# defaulted to daily bars and ended the backtest three sessions early.
# ---------------------------------------------------------------------------

_ENV_WEEK = [date(2026, 8, 3), date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)]
# Sessions the fake Alpaca serves: five weeks before the backtest window plus the window, so a
# history request can reach before backtesting_start. 2026-07-03 is the Independence Day
# holiday (observed) and has no bars.
_FAKE_SESSIONS = [
    day
    for day in (date(2026, 6, 29) + timedelta(days=offset) for offset in range(40))
    if day.weekday() < 5 and day != date(2026, 7, 3)
]


def _minute_open(day: date, minutes_after_open: int) -> float:
    # Unique per bar so a test can tell exactly which bar a price came from.
    return round(600 + (day - _ENV_WEEK[0]).days + minutes_after_open * 0.001, 3)


def _as_new_york(value) -> pd.Timestamp:
    # alpaca-py stores request datetimes as naive UTC.
    stamp = pd.Timestamp(value)
    return (stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp).tz_convert(_NY_TZ)


class _FakeStockHistoricalClient:
    requests = []

    def __init__(self, *args, **kwargs):
        pass

    def get_stock_bars(self, request):
        type(self).requests.append(request)
        # alpaca-py stores request datetimes as naive UTC.
        start = pd.Timestamp(request.start)
        start = (start.tz_localize("UTC") if start.tzinfo is None else start).tz_convert(_NY_TZ)
        end = pd.Timestamp(request.end)
        end = (end.tz_localize("UTC") if end.tzinfo is None else end).tz_convert(_NY_TZ)
        amount = int(request.timeframe.amount_value)
        unit = str(request.timeframe.unit_value.value).lower()
        rows = []
        for day in _FAKE_SESSIONS:
            if unit.startswith("day"):
                ts = _NY_TZ.localize(datetime.combine(day, datetime.min.time()))
                if start <= ts <= end:
                    o = _minute_open(day, 0)
                    rows.append((ts, o, o + 1, o - 1, o + 0.5, 1_000_000))
                continue
            step = amount * (60 if unit.startswith("hour") else 1)
            for minute in range(0, 390, step):
                ts = _NY_TZ.localize(datetime.combine(day, datetime.min.time()).replace(hour=9, minute=30)) + timedelta(minutes=minute)
                if start <= ts <= end:
                    o = _minute_open(day, minute)
                    # A multi-minute bar closes at the price of its last minute, which is
                    # later than the bar's label. That makes a leaked close visible.
                    c = _minute_open(day, minute + step - 1)
                    rows.append((ts, o, max(o, c) + 0.01, min(o, c) - 0.01, c, 1000 * step))
        return _bars(
            request.symbol_or_symbols,
            [(ts.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"), o, h, l, c, v) for ts, o, h, l, c, v in rows],
        )


class _FakeOptionHistoricalClient:
    barsets = {}
    requests = []

    def __init__(self, *args, **kwargs):
        pass

    def get_option_bars(self, request):
        type(self).requests.append(request)
        return type(self).barsets.get(request.symbol_or_symbols, BarSet({}))


class _FakeCryptoHistoricalClient:
    def __init__(self, *args, **kwargs):
        pass


def _select_alpaca_through_environment(monkeypatch, tmp_path):
    import lumibot.backtesting.alpaca_backtesting as alpaca_backtesting

    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "alpaca")
    monkeypatch.setenv("ALPACA_API_KEY", "env-key")
    monkeypatch.setenv("ALPACA_API_SECRET", "env-secret")
    monkeypatch.setenv("ALPACA_IS_PAPER", "true")
    for name in ("ALPACA_OAUTH_TOKEN", "BACKTESTING_START", "BACKTESTING_END"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(alpaca_backtesting, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(alpaca_backtesting, "StockHistoricalDataClient", _FakeStockHistoricalClient)
    monkeypatch.setattr(alpaca_backtesting, "OptionHistoricalDataClient", _FakeOptionHistoricalClient)
    monkeypatch.setattr(alpaca_backtesting, "CryptoHistoricalDataClient", _FakeCryptoHistoricalClient)
    _FakeStockHistoricalClient.requests = []
    _FakeOptionHistoricalClient.requests = []
    _FakeOptionHistoricalClient.barsets = {}


_ENV_RUN_KWARGS = dict(
    datasource_class=None,  # the BotSpot template: the environment picks the source
    backtesting_start=_NY_TZ.localize(datetime(2026, 8, 3)),
    backtesting_end=_NY_TZ.localize(datetime(2026, 8, 8)),
    benchmark_asset=None,
    analyze_backtest=False,
    show_plot=False,
    save_tearsheet=False,
    show_tearsheet=False,
    show_progress_bar=False,
    budget=10_000,
)


def test_env_selected_alpaca_runs_intraday_minute_bars_over_the_full_window(monkeypatch, tmp_path):
    from lumibot.strategies import Strategy

    _select_alpaca_through_environment(monkeypatch, tmp_path)

    class FiveMinuteProbe(Strategy):
        def initialize(self):
            self.sleeptime = "5M"
            self.vars.seen = []
            self.vars.fills = []

        def on_trading_iteration(self):
            now = self.get_datetime()
            bars = self.get_historical_prices("SPY", 3, "5minute")
            self.vars.seen.append((now, bars.df.index[-1] if bars is not None else None, self.get_last_price("SPY")))
            if len(self.vars.seen) == 3:
                self.submit_order(self.create_order("SPY", 1, "buy"))

        def on_filled_order(self, position, order, price, quantity, multiplier):
            self.vars.fills.append((self.get_datetime(), float(price)))

    _results, strategy = FiveMinuteProbe.run_backtest(**_ENV_RUN_KWARGS)

    source = strategy.broker.data_source
    assert isinstance(source, AlpacaBacktesting)
    assert source._timestep == "minute"
    seen_days = sorted({now.date() for now, _bar, _price in strategy.vars.seen})
    # Every requested session, not the old stop at the open of the third-to-last one.
    assert seen_days == _ENV_WEEK
    by_time = {now.strftime("%Y-%m-%d %H:%M"): (bar, price) for now, bar, price in strategy.vars.seen}
    # Minute resolution: the last price at 10:00 is the 10:00 minute bar, not the day's open.
    assert float(by_time["2026-08-04 10:00"][1]) == _minute_open(date(2026, 8, 4), 30)
    # The newest finished 5-minute bar at 10:00 is 09:55. (This line asserted 10:00, the bar
    # still forming, until the 2026-09-23 lookahead fix; see the test below.)
    assert by_time["2026-08-04 10:00"][0] == _NY_TZ.localize(datetime(2026, 8, 4, 9, 55))
    # The market order submitted at 09:40 fills on the 09:40 minute bar.
    assert strategy.vars.fills == [(_NY_TZ.localize(datetime(2026, 8, 3, 9, 40)), _minute_open(date(2026, 8, 3), 10))]
    assert {str(r.timeframe) for r in _FakeStockHistoricalClient.requests} >= {"1Min", "5Min"}


def test_env_selected_alpaca_history_never_returns_a_bar_that_closes_after_now(monkeypatch, tmp_path):
    """BotSpot path: history holds completed bars only, the contract IBKR, ThetaData and Polygon use.

    Before 2026-09-23 the 5-minute bar labeled 10:00 came back at 10:00 carrying its 10:04
    close (and the day's bar came back at 10:00 carrying the session's close), so a signal built
    on the latest bar saw into the future. The price used for fills is unchanged: the open
    of the bar that starts now.
    """
    from lumibot.strategies import Strategy

    _select_alpaca_through_environment(monkeypatch, tmp_path)
    order_time = _NY_TZ.localize(datetime(2026, 8, 4, 10, 0))
    lengths = {"5minute": timedelta(minutes=5), "minute": timedelta(minutes=1)}

    class HistoryProbe(Strategy):
        def initialize(self):
            self.sleeptime = "5M"
            self.vars.rows = {}
            self.vars.fills = []

        def on_trading_iteration(self):
            now = self.get_datetime()
            row = {"last": float(self.get_last_price("SPY"))}
            for timestep, length in (("5minute", 3), ("minute", 3), ("day", 1)):
                bars = self.get_historical_prices("SPY", length, timestep)
                row[timestep] = None if bars is None else bars.df[["open", "close"]].copy()
            self.vars.rows[now] = row
            if now == order_time:
                self.submit_order(self.create_order("SPY", 1, "buy"))

        def on_filled_order(self, position, order, price, quantity, multiplier):
            self.vars.fills.append((self.get_datetime(), float(price)))

    _results, strategy = HistoryProbe.run_backtest(**_ENV_RUN_KWARGS)

    rows = strategy.vars.rows
    assert len(rows) == 5 * 78
    for now, row in rows.items():
        for timestep, bar_length in lengths.items():
            df = row[timestep]
            if df is not None:
                assert (df.index + bar_length <= now).all(), f"{timestep} bar still forming at {now}: {list(df.index)}"
        day_df = row["day"]
        if day_df is not None:
            # Daily bars are labeled at midnight and close at the end of the session.
            assert (day_df.index.date < now.date()).all(), f"today's daily bar returned at {now}"

    at_ten = rows[order_time]
    day = date(2026, 8, 4)
    # The 5-minute bar labeled 10:00 closes at 10:05; the newest one finished by 10:00 is 09:55,
    # whose close is the 09:59 price. Its 10:04 close must not be visible at 10:00.
    assert at_ten["5minute"].index[-1] == _NY_TZ.localize(datetime(2026, 8, 4, 9, 55))
    assert float(at_ten["5minute"]["close"].iloc[-1]) == _minute_open(day, 29)
    assert _minute_open(day, 34) not in set(at_ten["5minute"]["close"].astype(float))
    assert at_ten["minute"].index[-1] == _NY_TZ.localize(datetime(2026, 8, 4, 9, 59))
    assert at_ten["day"].index[-1].date() == date(2026, 8, 3)
    # get_last_price and the fill both use the open of the bar that starts at 10:00.
    assert at_ten["last"] == _minute_open(day, 30)
    assert strategy.vars.fills == [(order_time, _minute_open(day, 30))]
    # At the first bar nothing inside the window has finished yet, so history comes from the
    # session before backtesting_start (these were None until history could reach back).
    first = rows[_NY_TZ.localize(datetime(2026, 8, 3, 9, 30))]
    last_session = date(2026, 7, 31)
    assert list(first["5minute"].index) == [
        _NY_TZ.localize(datetime(2026, 7, 31, 15, minute)) for minute in (45, 50, 55)
    ]
    assert list(first["minute"].index) == [
        _NY_TZ.localize(datetime(2026, 7, 31, 15, minute)) for minute in (57, 58, 59)
    ]
    assert [ts.date() for ts in first["day"].index] == [last_session]


def test_env_selected_alpaca_history_reaches_back_before_backtesting_start(monkeypatch, tmp_path):
    """BotSpot path: history before backtesting_start comes from real earlier bars, like IBKR and ThetaData.

    The opening-range strategy that started this work asks at its first bars for 250
    five-minute bars and for 15 daily bars (an ATR(14) filter). The data window started at
    backtesting_start, so it got None for the five-minute bars at 09:30 and a "Not enough
    historical data" error for the daily ones.
    """
    from lumibot.strategies import Strategy

    _select_alpaca_through_environment(monkeypatch, tmp_path)
    start = _NY_TZ.localize(datetime(2026, 8, 3))
    first_bar = start.replace(hour=9, minute=30)

    class FirstBarsProbe(Strategy):
        def initialize(self):
            self.sleeptime = "5M"
            self.vars.rows = {}

        def on_trading_iteration(self):
            five = self.get_historical_prices("SPY", 250, "5minute")
            daily = self.get_historical_prices("SPY", 15, "day")
            self.vars.rows[self.get_datetime()] = (
                None if five is None else five.df[["open", "close"]].copy(),
                None if daily is None else daily.df[["open", "close"]].copy(),
            )

    _results, strategy = FirstBarsProbe.run_backtest(**_ENV_RUN_KWARGS)

    rows = strategy.vars.rows
    assert len(rows) == 5 * 78
    five, daily = rows[first_bar]
    assert five is not None and len(five) == 250, f"got {None if five is None else len(five)} of 250 five-minute bars"
    assert daily is not None and len(daily) == 15, f"got {None if daily is None else len(daily)} of 15 daily bars"
    # All from before backtesting_start, and all finished by 09:30.
    assert five.index[-1] == _NY_TZ.localize(datetime(2026, 7, 31, 15, 55))
    assert [ts.date() for ts in daily.index][-1] == date(2026, 7, 31)
    assert (five.index < start).all() and (daily.index < start).all()
    # Real bars only: every bar is one the fake Alpaca served, with that bar's own prices.
    for ts, row in five.iterrows():
        minute = ts.hour * 60 + ts.minute - (9 * 60 + 30)
        assert float(row["open"]) == _minute_open(ts.date(), minute)
        assert float(row["close"]) == _minute_open(ts.date(), minute + 4)
    for ts, row in daily.iterrows():
        assert ts.date() in _FAKE_SESSIONS
        assert float(row["open"]) == _minute_open(ts.date(), 0)
    # Every bar of the week gets the full history, and never a bar that is still forming.
    for now, (five_df, daily_df) in rows.items():
        assert len(five_df) == 250 and len(daily_df) == 15
        assert (five_df.index + timedelta(minutes=5) <= now).all()
        assert (daily_df.index.date < now.date()).all()
    # Bounded and cached: one request per series reaches before the window, not one per bar,
    # and it reaches back about as far as the request needs (7 sessions for 250 five-minute
    # bars, 21 sessions for 15 daily bars, with a margin).
    early = [r for r in _FakeStockHistoricalClient.requests if _as_new_york(r.start) < start]
    assert sorted(str(r.timeframe) for r in early) == ["1Day", "5Min"]
    first_day = {str(r.timeframe): _as_new_york(r.start).date() for r in early}
    assert date(2026, 7, 20) <= first_day["5Min"] <= date(2026, 7, 27)
    assert date(2026, 6, 29) <= first_day["1Day"] <= date(2026, 7, 10)


def test_explicit_config_alpaca_history_keeps_the_window_unless_asked(monkeypatch, tmp_path):
    """An explicit config keeps its data window (the existing behavior) unless history_before_start=True."""
    _select_alpaca_through_environment(monkeypatch, tmp_path)
    config = {"API_KEY": "test-key", "API_SECRET": "test-secret", "PAPER": True}
    window = dict(
        datetime_start=_NY_TZ.localize(datetime(2026, 8, 3)),
        datetime_end=_NY_TZ.localize(datetime(2026, 8, 7)),
        config=config,
        timestep="minute",
    )
    now = _NY_TZ.localize(datetime(2026, 8, 3, 9, 35))

    legacy = AlpacaBacktesting(**window)
    legacy._datetime = now
    bars = legacy.get_historical_prices(Asset("SPY"), 250, "5minute")
    # Only the window's bars that closed by 09:35; the 09:35 bar is still forming.
    assert list(bars.df.index) == [_NY_TZ.localize(datetime(2026, 8, 3, 9, 30))]
    with pytest.raises(ValueError, match="Not enough historical data"):
        legacy.get_historical_prices(Asset("SPY"), 15, "day")
    assert not [r for r in _FakeStockHistoricalClient.requests if _as_new_york(r.start) < window["datetime_start"]]

    reaching = AlpacaBacktesting(**window, history_before_start=True, remove_incomplete_current_bar=True)
    reaching._datetime = now
    bars = reaching.get_historical_prices(Asset("SPY"), 250, "5minute")
    assert len(bars.df) == 250
    assert bars.df.index[-1] == _NY_TZ.localize(datetime(2026, 8, 3, 9, 30))
    assert len(reaching.get_historical_prices(Asset("SPY"), 15, "day").df) == 15


def test_env_selected_alpaca_daily_strategy_keeps_daily_bars(monkeypatch, tmp_path):
    from lumibot.strategies import Strategy

    _select_alpaca_through_environment(monkeypatch, tmp_path)

    class DailyProbe(Strategy):
        def initialize(self):
            self.sleeptime = "1D"
            self.vars.days = []

        def on_trading_iteration(self):
            self.vars.days.append(self.get_datetime().date())
            self.get_last_price("SPY")

    _results, strategy = DailyProbe.run_backtest(**_ENV_RUN_KWARGS)

    assert strategy.broker.data_source._timestep == "day"
    assert strategy.vars.days == _ENV_WEEK
    # A daily strategy does not download minute history it never uses.
    assert {str(r.timeframe) for r in _FakeStockHistoricalClient.requests} == {"1Day"}


def test_env_selected_alpaca_options_fill_on_real_prints(monkeypatch, tmp_path):
    import alpaca.trading.client as trading_client_module

    from lumibot.strategies import Strategy

    _select_alpaca_through_environment(monkeypatch, tmp_path)
    monkeypatch.setattr(
        trading_client_module, "TradingClient", lambda *args, **kwargs: _FakeTradingClient(_spy_contract_pages())
    )
    _FakeOptionHistoricalClient.barsets = {
        "SPY260821C00650000": _bars(
            "SPY260821C00650000",
            [
                ("2026-08-03T13:31:00Z", 5.00, 5.10, 4.95, 5.05, 3),
                ("2026-08-03T13:37:00Z", 5.40, 5.50, 5.35, 5.45, 2),
            ],
        )
    }

    class BuyOneCall(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.vars.done = False
            self.vars.fills = []

        def on_trading_iteration(self):
            now = self.get_datetime()
            if not self.vars.done and (now.hour, now.minute) >= (9, 35):
                strikes = self.get_chains(Asset("SPY"))["Chains"]["CALL"]["2026-08-21"]
                contract = Asset(
                    "SPY", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 8, 21), strike=strikes[0], right="CALL"
                )
                self.submit_order(self.create_order(contract, 1, "buy"))
                self.vars.done = True

        def on_filled_order(self, position, order, price, quantity, multiplier):
            self.vars.fills.append((self.get_datetime().strftime("%Y-%m-%d %H:%M"), float(price)))

    _results, strategy = BuyOneCall.run_backtest(**_ENV_RUN_KWARGS)

    assert strategy.vars.fills == [("2026-08-03 09:37", 5.40)]


def test_env_selected_alpaca_option_history_excludes_the_print_that_is_still_forming(monkeypatch, tmp_path):
    """Option history on the BotSpot path also stops at the newest finished bar."""
    _select_alpaca_through_environment(monkeypatch, tmp_path)
    _FakeOptionHistoricalClient.barsets = {
        "SPY260821C00650000": _bars(
            "SPY260821C00650000",
            [
                ("2026-08-03T13:31:00Z", 5.00, 5.10, 4.95, 5.05, 3),
                ("2026-08-03T13:37:00Z", 5.40, 5.50, 5.35, 5.45, 2),
            ],
        )
    }
    source = AlpacaBacktesting(
        datetime_start=_NY_TZ.localize(datetime(2026, 8, 3)),
        datetime_end=_NY_TZ.localize(datetime(2026, 8, 8)),
        config=None,
    )
    source._sleep = lambda seconds: None
    contract = Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 8, 21), strike=650, right="CALL")

    source._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 9, 37))
    bars = source.get_historical_prices(contract, 2, "minute")
    # The 09:37 print closes at 09:38: at 09:37 only the 09:31 bar has finished.
    assert list(bars.df.index) == [_NY_TZ.localize(datetime(2026, 8, 3, 9, 31))]
    # The fill price is still the open of the print that starts now.
    assert float(source.get_last_price(contract)) == 5.40

    source._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 9, 38))
    bars = source.get_historical_prices(contract, 2, "minute")
    assert list(bars.df.index) == [
        _NY_TZ.localize(datetime(2026, 8, 3, 9, 31)),
        _NY_TZ.localize(datetime(2026, 8, 3, 9, 37)),
    ]

    source._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 9, 31))
    # Before the first print has finished there is nothing honest to return.
    assert source.get_historical_prices(contract, 2, "minute") is None


def test_env_selected_alpaca_option_history_reaches_back_to_real_prints_before_the_start(monkeypatch, tmp_path):
    """Option history on the BotSpot path also reaches before backtesting_start, real prints only."""
    _select_alpaca_through_environment(monkeypatch, tmp_path)
    prints = [
        ("2026-07-30T14:05:00Z", 4.10, 4.20, 4.05, 4.15, 7),
        ("2026-07-31T19:58:00Z", 4.60, 4.70, 4.55, 4.65, 4),
        ("2026-08-03T13:31:00Z", 5.00, 5.10, 4.95, 5.05, 3),
    ]
    _FakeOptionHistoricalClient.barsets = {"SPY260821C00650000": _bars("SPY260821C00650000", prints)}
    start = _NY_TZ.localize(datetime(2026, 8, 3))
    source = AlpacaBacktesting(datetime_start=start, datetime_end=_NY_TZ.localize(datetime(2026, 8, 8)), config=None)
    source._sleep = lambda seconds: None
    contract = Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 8, 21), strike=650, right="CALL")

    source._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 9, 35))
    bars = source.get_historical_prices(contract, 3, "minute")
    # Exactly the three real prints: two before the start and the 09:31 print. No bar in between.
    assert list(bars.df.index) == [_as_new_york(ts) for ts, *_rest in prints]
    assert list(bars.df["open"]) == [4.10, 4.60, 5.00]

    # Asking again (every bar of a backtest does) never asks Alpaca again for the same history.
    early = lambda: [r for r in _FakeOptionHistoricalClient.requests if _as_new_york(r.start) < start]  # noqa: E731
    assert len(early()) == 1
    source._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 9, 36))
    source.get_historical_prices(contract, 3, "minute")
    assert len(early()) == 1
    # A request that needs more sessions reaches further back once, for the missing days only.
    bars = source.get_historical_prices(contract, 1000, "minute")
    assert len(early()) == 2
    assert _as_new_york(early()[1].end) <= _as_new_york(early()[0].start)
    assert len(bars.df) == 3  # the contract simply has no other prints: nothing is invented
    source.get_historical_prices(contract, 1000, "minute")
    assert len(early()) == 2

    # A second run with the same requests reads the window and both history segments from the
    # disk cache, including the empty one (no prints on those days).
    requests_before = len(_FakeOptionHistoricalClient.requests)
    rerun = AlpacaBacktesting(datetime_start=start, datetime_end=_NY_TZ.localize(datetime(2026, 8, 8)), config=None)
    rerun._sleep = lambda seconds: None
    rerun._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 9, 35))
    rerun.get_historical_prices(contract, 3, "minute")
    rerun._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 9, 36))
    bars = rerun.get_historical_prices(contract, 1000, "minute")
    assert len(_FakeOptionHistoricalClient.requests) == requests_before
    assert list(bars.df.index) == [_as_new_york(ts) for ts, *_rest in prints]


def test_alpaca_contract_listing_falls_back_to_the_other_trading_endpoint_on_401(monkeypatch, tmp_path):
    """BotSpot always sends ALPACA_IS_PAPER=true; a live-account key is refused by paper-api."""
    import alpaca.trading.client as trading_client_module

    unauthorized = APIError(
        '{"message": "unauthorized."}',
        http_error=SimpleNamespace(response=SimpleNamespace(status_code=401, headers={}), request=None),
    )
    endpoints = []

    def trading_client_factory(*args, paper=True, **kwargs):
        endpoints.append(paper)
        return _FakeTradingClient(_spy_contract_pages(), failures=[unauthorized] if paper else [])

    monkeypatch.setattr(trading_client_module, "TradingClient", trading_client_factory)
    source = _alpaca_source(monkeypatch, tmp_path)
    source._trading_client = None
    source._datetime = _NY_TZ.localize(datetime(2026, 8, 3, 9, 30))

    chains = source.get_chains(Asset("SPY"))

    assert chains["Chains"]["CALL"]["2026-08-07"] == [640.0, 645.0]
    assert endpoints == [True, False]


def test_alpaca_history_request_never_asks_for_the_latest_15_minutes(monkeypatch, tmp_path):
    """Free keys refuse recent SIP data, and a window clamped to now used to ask for tomorrow."""
    now = datetime.now(pytz.UTC)
    source = _alpaca_source(
        monkeypatch,
        tmp_path,
        start=(now - timedelta(days=5)).astimezone(_NY_TZ).replace(tzinfo=None),
        end=now.astimezone(_NY_TZ).replace(tzinfo=None),
    )
    _client, request = source._history_request(
        base_asset=Asset("SPY"),
        quote_asset=Asset("USD", asset_type="forex"),
        timestep="minute",
        data_datetime_start=source._data_datetime_start,
        data_datetime_end=source._data_datetime_end,
        auto_adjust=True,
    )
    end = pd.Timestamp(request.end)
    end = end.tz_localize("UTC") if end.tzinfo is None else end  # the SDK stores naive UTC
    assert end <= pd.Timestamp(now) - pd.Timedelta(minutes=15)


def _ohlcv(rows, tzinfo):
    index = pd.DatetimeIndex([tzinfo.localize(ts) for ts, _ in rows], tz=tzinfo, name="timestamp")
    return pd.DataFrame(
        [{"open": o, "high": max(o, c), "low": min(o, c), "close": c, "volume": 10} for _, (o, c) in rows],
        index=index,
    )


def _offline_option_source(now, window_rows, history_rows):
    """Environment-mode Alpaca source at the first bar of the window, with stubbed bars."""
    ny = pytz.timezone("America/New_York")
    source = AlpacaBacktesting.__new__(AlpacaBacktesting)
    source.market = "NYSE"
    source.tzinfo = ny
    source._auto_adjust = True
    source._timestep = "minute"
    source._history_before_start = True
    source._data_datetime_start = ny.localize(datetime(2026, 1, 5))
    source._data_datetime_end = ny.localize(datetime(2026, 1, 7, 23, 59))
    source._datetime = ny.localize(now)
    window = _ohlcv(window_rows, ny)
    history = _ohlcv(history_rows, ny)
    source._get_option_bars_frame = lambda asset, quote, timestep: window
    source._history_segment = lambda asset, quote, source_timestep, start, end: history
    return source


_SPY_CALL = Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration="2026-01-16", strike=690, right="CALL")


def test_alpaca_option_last_price_at_first_bar_uses_latest_print_before_the_window():
    """At 09:30 on day one there is no in-window print yet; the last real trade is from before start."""
    source = _offline_option_source(
        datetime(2026, 1, 5, 9, 30),
        window_rows=[(datetime(2026, 1, 5, 9, 45), (2.00, 2.10))],
        history_rows=[(datetime(2026, 1, 2, 15, 58), (1.20, 1.25))],
    )
    price = source.get_last_price(_SPY_CALL)
    assert price is not None
    assert float(price) == 1.25  # never the 09:45 print from after the simulated time


def test_alpaca_get_quote_for_option_reports_last_trade_without_inventing_bid_ask():
    source = _offline_option_source(
        datetime(2026, 1, 5, 9, 30),
        window_rows=[(datetime(2026, 1, 5, 9, 45), (2.00, 2.10))],
        history_rows=[(datetime(2026, 1, 2, 15, 58), (1.20, 1.25))],
    )
    quote = source.get_quote(_SPY_CALL, snapshot_only=True)
    assert float(quote.price) == 1.25
    assert quote.bid is None and quote.ask is None  # Alpaca historical options have trades only


def test_alpaca_get_quote_for_stock_uses_the_current_bar_price(monkeypatch):
    source = _offline_option_source(datetime(2026, 1, 5, 10, 0), window_rows=[], history_rows=[])
    monkeypatch.setattr(source, "get_last_price", lambda asset, quote=None, exchange=None: 690.5)
    quote = source.get_quote(Asset("SPY"))
    assert quote.price == 690.5
    assert quote.bid is None and quote.ask is None


def test_options_helper_validates_alpaca_option_marks_from_real_trades():
    """Expiration and strike probes read marks through get_quote; Alpaca must not fail them all."""
    from types import SimpleNamespace

    from lumibot.components.options_helper import OptionsHelper

    source = _offline_option_source(
        datetime(2026, 1, 5, 9, 30),
        window_rows=[],
        history_rows=[(datetime(2026, 1, 2, 15, 58), (1.20, 1.25))],
    )
    broker = SimpleNamespace(IS_BACKTESTING_BROKER=True, data_source=source, option_source=None)
    strategy = SimpleNamespace(
        broker=broker,
        get_datetime=lambda: source._datetime,
        log_message=lambda *args, **kwargs: None,
    )
    helper = OptionsHelper(strategy)
    mark, bid, ask = helper._get_option_mark_from_quote(_SPY_CALL, snapshot=True)
    assert mark == 1.25
    assert bid is None and ask is None


@pytest.mark.parametrize("config", [None, {"API_KEY": "test-key", "API_SECRET": "test-secret", "PAPER": True}])
def test_alpaca_history_defaults_to_completed_bars_in_both_modes(monkeypatch, tmp_path, config):
    """A forming bar carries its final OHLCV, a lookahead, so it is opt-in in every mode."""
    _select_alpaca_through_environment(monkeypatch, tmp_path)
    window = dict(
        datetime_start=_NY_TZ.localize(datetime(2026, 8, 3)),
        datetime_end=_NY_TZ.localize(datetime(2026, 8, 7)),
        config=config,
        timestep="minute",
    )

    assert AlpacaBacktesting(**window)._remove_incomplete_current_bar is True
    assert AlpacaBacktesting(**window, remove_incomplete_current_bar=False)._remove_incomplete_current_bar is False
