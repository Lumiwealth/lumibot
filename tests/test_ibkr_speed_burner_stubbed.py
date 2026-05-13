from __future__ import annotations

from datetime import date
from decimal import Decimal
from time import perf_counter

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy


class _SpeedBurnerBase(Strategy):
    def initialize(self, parameters=None):
        # Minute-cadence backtest loop.
        self.sleeptime = "1M"
        self._i = 0
        self.include_cash_positions = True

    def _burn_one_asset(self, asset: Asset):
        # Hot path: these are the calls that dominate runtime in real strategies.
        _ = self.get_last_price(asset)
        _ = self.get_historical_prices(asset, length=100, timestep="minute").df
        _ = self.get_historical_prices(asset, length=20, timestep="day").df


class _FuturesSpeedBurnerStrategy(_SpeedBurnerBase):
    def initialize(self, parameters=None):
        super().initialize(parameters=parameters)
        self.futs = parameters["futs"]

    def on_trading_iteration(self):
        for fut in self.futs:
            self._burn_one_asset(fut)

        side = Order.OrderSide.BUY if (self._i % 2 == 0) else Order.OrderSide.SELL
        for fut in self.futs:
            order = self.create_order(fut, Decimal("1"), side, order_type=Order.OrderType.MARKET)
            self.submit_order(order)
        self._i += 1


class _CryptoSpeedBurnerStrategy(_SpeedBurnerBase):
    def initialize(self, parameters=None):
        super().initialize(parameters=parameters)
        self.coins = parameters["coins"]

    def on_trading_iteration(self):
        for coin in self.coins:
            self._burn_one_asset(coin)

        side = Order.OrderSide.BUY if (self._i % 2 == 0) else Order.OrderSide.SELL
        for coin in self.coins:
            order = self.create_order(coin, Decimal("0.01"), side, order_type=Order.OrderType.MARKET)
            self.submit_order(order)
        self._i += 1


def _minute_df(index: pd.DatetimeIndex, start_price: float) -> pd.DataFrame:
    prices = start_price + (pd.Series(range(len(index))) * 0.01).to_numpy()
    return pd.DataFrame(
        {
            "open": prices,
            "high": prices + 0.01,
            "low": prices - 0.01,
            "close": prices,
            "volume": 1000,
        },
        index=index,
    )


def _day_df(index: pd.DatetimeIndex, start_price: float) -> pd.DataFrame:
    prices = start_price + (pd.Series(range(len(index))) * 1.0).to_numpy()
    return pd.DataFrame(
        {
            "open": prices,
            "high": prices + 1.0,
            "low": prices - 1.0,
            "close": prices,
            "volume": 1_000_000,
        },
        index=index,
    )


def _multi_minute_df(source: pd.DataFrame, minutes: int) -> pd.DataFrame:
    return (
        source.resample(f"{minutes}min")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
    )


def test_ibkr_speed_burner_prefetches_once_and_slices_forever(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    # SAFETY: avoid printing private downloader hostnames/API keys from local `.env` in unit test logs.
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "true")
    monkeypatch.setenv("IS_BACKTESTING", "true")
    monkeypatch.setenv("BACKTESTING_QUIET_LOGS", "true")

    # 2–3 symbols each (user requirement); keep the dataset small enough for unit tests but
    # large enough to catch per-iteration refetching.
    tz = "America/New_York"
    minute_index = pd.date_range("2025-12-08 09:30", periods=600, freq="1min", tz=tz)  # 10 hours
    day_index = pd.date_range("2025-09-01 00:00", periods=200, freq="1D", tz=tz)

    fut_mes = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=5)
    fut_mnq = Asset("MNQ", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=2)

    btc = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    eth = Asset("ETH", asset_type=Asset.AssetType.CRYPTO)
    sol = Asset("SOL", asset_type=Asset.AssetType.CRYPTO)

    datasets: dict[tuple[str, str], pd.DataFrame] = {
        ("MES", "minute"): _minute_df(minute_index, 6400.0),
        ("MES", "day"): _day_df(day_index, 6300.0),
        ("MNQ", "minute"): _minute_df(minute_index, 17000.0),
        ("MNQ", "day"): _day_df(day_index, 16500.0),
        ("BTC", "minute"): _minute_df(minute_index, 40000.0),
        ("BTC", "day"): _day_df(day_index, 39000.0),
        ("ETH", "minute"): _minute_df(minute_index, 2000.0),
        ("ETH", "day"): _day_df(day_index, 1900.0),
        ("SOL", "minute"): _minute_df(minute_index, 100.0),
        ("SOL", "day"): _day_df(day_index, 95.0),
    }
    # Multi-minute dataset: the IBKR layer should fetch native multi-minute bars when requested,
    # and we should cache them separately from 1-minute bars.
    datasets[("MES", "15minute")] = _multi_minute_df(datasets[("MES", "minute")], 15)

    calls: dict[tuple[str, str], int] = {}

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        sym = getattr(asset, "symbol", "")
        key = (sym, str(timestep))
        calls[key] = calls.get(key, 0) + 1
        df = datasets.get(key)
        if df is None:
            raise AssertionError(f"Missing stub dataset for {key}")
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    def _make_broker() -> BacktestingBroker:
        data_source = InteractiveBrokersRESTBacktesting(
            datetime_start=minute_index[0].to_pydatetime(),
            datetime_end=minute_index[-1].to_pydatetime(),
            market="24/7",
            show_progress_bar=False,
            log_backtest_progress_to_file=False,
        )
        data_source.load_data()

        broker = BacktestingBroker(data_source=data_source)
        broker.initialize_market_calendars(data_source.get_trading_days_pandas())
        broker._first_iteration = False
        # Start after enough history exists for the lookbacks (minute=100, day=20).
        broker._update_datetime(minute_index[200].to_pydatetime())
        return broker

    futures_broker = _make_broker()

    futures = _FuturesSpeedBurnerStrategy(
        broker=futures_broker,
        budget=100_000.0,
        analyze_backtest=False,
        parameters={"futs": [fut_mes, fut_mnq]},
    )
    futures._first_iteration = False
    # Unit tests call `on_trading_iteration()` directly; ensure `initialize()` has run.
    futures.initialize(parameters={"futs": [fut_mes, fut_mnq]})

    # Multi-timeframe request should work in backtesting without strategy-layer resampling.
    bars_15m = futures.get_historical_prices(fut_mes, length=10, timestep="15min")
    assert bars_15m is not None
    assert len(bars_15m.df) == 10
    assert (bars_15m.df.index[1] - bars_15m.df.index[0]) == pd.Timedelta(minutes=15)

    # Run a few hundred iterations of each loop. This is a correctness/speed-structure test:
    # it should not refetch the same series per iteration.
    iterations = 200

    t0 = perf_counter()
    for _ in range(iterations):
        futures.on_trading_iteration()
        futures_broker.process_pending_orders(futures)
        futures._executor.process_queue()
        futures_broker._update_datetime(60)

    crypto_broker = _make_broker()
    crypto = _CryptoSpeedBurnerStrategy(
        broker=crypto_broker,
        budget=100_000.0,
        analyze_backtest=False,
        parameters={"coins": [btc, eth, sol]},
    )
    crypto._first_iteration = False
    crypto.initialize(parameters={"coins": [btc, eth, sol]})

    for _ in range(iterations):
        crypto.on_trading_iteration()
        crypto_broker.process_pending_orders(crypto)
        crypto._executor.process_queue()
        crypto_broker._update_datetime(60)
    t1 = perf_counter()

    # Sanity: this is not a strict perf gate (CI machines vary), but it should not be pathological.
    assert (t1 - t0) < 60.0

    # Prefetch once → slice forever: each (symbol, timestep) should be loaded once.
    # If this fails, backtests will be dominated by redundant pandas/disk work.
    for key, count in sorted(calls.items()):
        assert count == 1, f"Expected 1 load for {key}, got {count}"

    # Correctness gates (deterministic): if any perf change alters order fill semantics or
    # data-window alignment, this should fail loudly.
    assert futures_broker.get_active_tracked_orders(futures) == []
    assert crypto_broker.get_active_tracked_orders(crypto) == []
    futures_positions = [
        p for p in futures_broker.get_tracked_positions(futures.name) if p.asset != futures.quote_asset
    ]
    crypto_positions = [
        p for p in crypto_broker.get_tracked_positions(crypto.name) if p.asset != crypto.quote_asset
    ]
    assert futures_positions == []
    assert crypto_positions == []

    # Deterministic PnL snapshot: monotonic prices + buy/sell alternation yields fixed PnL.
    price_step = 0.01
    pairs = iterations // 2
    fut_profit = pairs * price_step * (fut_mes.multiplier + fut_mnq.multiplier)
    crypto_profit = pairs * price_step * 0.01 * 3  # 3 coins, qty=0.01 each
    assert futures.cash == pytest.approx(100_000.0 + float(fut_profit))
    assert crypto.cash == pytest.approx(100_000.0 + float(crypto_profit))

    def _assert_fill(row, *, time, symbol, side, price, qty, multiplier):
        assert row["time"] == time
        assert row["symbol"] == symbol
        assert row["side"] == side
        assert row["price"] == pytest.approx(price)
        assert row["filled_quantity"] == pytest.approx(qty)
        assert row["multiplier"] == multiplier

    # Deterministic fill snapshot (first 2 minutes of futures, first+last minute of crypto).
    trade_events_fut = futures_broker._trade_event_log_df
    orders_total_fut = iterations * len([fut_mes, fut_mnq])
    assert len(trade_events_fut) == 2 * orders_total_fut  # one "new" and one "fill" per order
    fills_fut = trade_events_fut[trade_events_fut["status"] == "fill"].reset_index(drop=True)
    assert len(fills_fut) == orders_total_fut

    # Futures: first two iterations (dt=minute_index[200], minute_index[201]).
    _assert_fill(
        fills_fut.iloc[0],
        time=minute_index[200],
        symbol="MES",
        side=Order.OrderSide.BUY,
        price=6400.0 + 200 * price_step,
        qty=1.0,
        multiplier=5,
    )
    _assert_fill(
        fills_fut.iloc[1],
        time=minute_index[200],
        symbol="MNQ",
        side=Order.OrderSide.BUY,
        price=17000.0 + 200 * price_step,
        qty=1.0,
        multiplier=2,
    )
    _assert_fill(
        fills_fut.iloc[2],
        time=minute_index[201],
        symbol="MES",
        side=Order.OrderSide.SELL,
        price=6400.0 + 201 * price_step,
        qty=1.0,
        multiplier=5,
    )
    _assert_fill(
        fills_fut.iloc[3],
        time=minute_index[201],
        symbol="MNQ",
        side=Order.OrderSide.SELL,
        price=17000.0 + 201 * price_step,
        qty=1.0,
        multiplier=2,
    )

    trade_events_crypto = crypto_broker._trade_event_log_df
    orders_total_crypto = iterations * len([btc, eth, sol])
    assert len(trade_events_crypto) == 2 * orders_total_crypto
    crypto_fills = trade_events_crypto[trade_events_crypto["status"] == "fill"].reset_index(drop=True)
    assert len(crypto_fills) == orders_total_crypto
    assert list(crypto_fills.iloc[:3]["symbol"]) == ["BTC", "ETH", "SOL"]
    assert len(getattr(crypto_broker, "_filled_order_identifiers", ())) == 0
    for order in crypto_broker._filled_orders.get_list()[:6]:
        assert not hasattr(order, "_fast_trade_event_pair_row")
        assert not hasattr(order, "_fast_trade_event_pair_row_index")
        assert not hasattr(order, "_fast_trade_event_static")
    assert isinstance(crypto_broker._trade_event_log_rows[0], tuple)

    # Crypto: first iteration (dt=minute_index[200]).
    _assert_fill(
        crypto_fills.iloc[0],
        time=minute_index[200],
        symbol="BTC",
        side=Order.OrderSide.BUY,
        price=40000.0 + 200 * price_step,
        qty=0.01,
        multiplier=1,
    )
    _assert_fill(
        crypto_fills.iloc[1],
        time=minute_index[200],
        symbol="ETH",
        side=Order.OrderSide.BUY,
        price=2000.0 + 200 * price_step,
        qty=0.01,
        multiplier=1,
    )
    _assert_fill(
        crypto_fills.iloc[2],
        time=minute_index[200],
        symbol="SOL",
        side=Order.OrderSide.BUY,
        price=100.0 + 200 * price_step,
        qty=0.01,
        multiplier=1,
    )

    # Crypto: last iteration is dt=minute_index[399] (SELL).
    _assert_fill(
        crypto_fills.iloc[-3],
        time=minute_index[399],
        symbol="BTC",
        side=Order.OrderSide.SELL,
        price=40000.0 + 399 * price_step,
        qty=0.01,
        multiplier=1,
    )
    _assert_fill(
        crypto_fills.iloc[-2],
        time=minute_index[399],
        symbol="ETH",
        side=Order.OrderSide.SELL,
        price=2000.0 + 399 * price_step,
        qty=0.01,
        multiplier=1,
    )
    _assert_fill(
        crypto_fills.iloc[-1],
        time=minute_index[399],
        symbol="SOL",
        side=Order.OrderSide.SELL,
        price=100.0 + 399 * price_step,
        qty=0.01,
        multiplier=1,
    )
