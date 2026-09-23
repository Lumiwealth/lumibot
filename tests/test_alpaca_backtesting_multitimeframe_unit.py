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
        "SPY260821C00640000",
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
