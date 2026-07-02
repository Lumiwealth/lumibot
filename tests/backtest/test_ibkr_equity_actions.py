import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset
from lumibot.entities import Data
from lumibot.strategies.strategy import Strategy
from lumibot.tools import ibkr_helper
from lumibot.tools.yahoo_helper import YahooHelper
from lumibot.traders import Trader


class _HoldTqqqThroughSplitStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.asset = self.parameters["asset"]
        self.observed_quantities = {}
        self.did_buy = False
        self.did_sell = False

    def on_trading_iteration(self):
        current_date = self.get_datetime().date()
        position = self.get_position(self.asset)
        self.observed_quantities[current_date] = 0 if position is None else float(position.quantity)

        if not self.did_buy and position is None:
            self.submit_order(self.create_order(self.asset, 10, "buy"))
            self.did_buy = True
        elif current_date >= pd.Timestamp("2025-11-21").date() and position is not None and not self.did_sell:
            self.submit_order(self.create_order(self.asset, abs(position.quantity), "sell"))
            self.did_sell = True


def test_append_equity_corporate_actions_daily_populates_columns(monkeypatch):
    idx = pd.DatetimeIndex(
        [
            "2024-01-02 16:00:00-05:00",
            "2024-01-03 16:00:00-05:00",
            "2024-01-04 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "volume": [1000, 1100, 1200],
        },
        index=idx,
    )

    actions = pd.DataFrame(
        {
            "Dividends": [0.0, 0.25, 0.0],
            "Stock Splits": [0.0, 0.0, 2.0],
        },
        index=pd.DatetimeIndex(
            [
                "2024-01-02 00:00:00-05:00",
                "2024-01-03 00:00:00-05:00",
                "2024-01-04 00:00:00-05:00",
            ]
        ),
    )

    ibkr_helper._IBKR_EQUITY_ACTIONS_CACHE.clear()
    monkeypatch.setattr(
        YahooHelper,
        "get_symbol_data",
        staticmethod(
            lambda symbol, interval="1d", caching=True, auto_adjust=False, last_needed_datetime=None: actions
        ),
    )

    enriched, changed = ibkr_helper._append_equity_corporate_actions_daily(frame, Asset("AAPL", asset_type="stock"))

    assert changed is True
    assert "dividend" in enriched.columns
    assert "stock_splits" in enriched.columns
    assert float(enriched.loc[idx[1], "dividend"]) == 0.25
    assert float(enriched.loc[idx[2], "stock_splits"]) == 2.0


def test_append_equity_corporate_actions_daily_reuses_cached_actions_for_same_needed_date(monkeypatch):
    idx = pd.DatetimeIndex(
        [
            "2024-01-02 16:00:00-05:00",
            "2024-01-03 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [1000, 1100],
        },
        index=idx,
    )

    actions = pd.DataFrame(
        {
            "Dividends": [0.0, 0.25],
            "Stock Splits": [0.0, 0.0],
        },
        index=pd.DatetimeIndex(
            [
                "2024-01-02 00:00:00-05:00",
                "2024-01-03 00:00:00-05:00",
            ]
        ),
    )

    calls = {"count": 0}

    def _fake_get_symbol_data(symbol, interval="1d", caching=True, auto_adjust=False, last_needed_datetime=None):
        calls["count"] += 1
        assert symbol == "AAPL"
        assert interval == "1d"
        assert caching is True
        assert auto_adjust is False
        assert last_needed_datetime is not None
        return actions

    ibkr_helper._IBKR_EQUITY_ACTIONS_CACHE.clear()
    monkeypatch.setattr(YahooHelper, "get_symbol_data", staticmethod(_fake_get_symbol_data))

    first, _ = ibkr_helper._append_equity_corporate_actions_daily(frame, Asset("AAPL", asset_type="stock"))
    second, _ = ibkr_helper._append_equity_corporate_actions_daily(frame, Asset("AAPL", asset_type="stock"))

    assert calls["count"] == 1
    assert float(first.loc[idx[1], "dividend"]) == 0.25
    assert float(second.loc[idx[1], "dividend"]) == 0.25


def test_normalize_equity_daily_prices_for_raw_forward_split():
    idx = pd.DatetimeIndex(
        [
            "2025-11-19 16:00:00-05:00",
            "2025-11-20 16:00:00-05:00",
            "2025-11-21 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [98.0, 52.0, 47.0],
            "high": [104.0, 54.0, 49.0],
            "low": [97.0, 46.0, 45.0],
            "close": [100.0, 46.5, 47.5],
            "bid": [100.0, 46.5, 47.5],
            "ask": [100.0, 46.5, 47.5],
            "volume": [1000.0, 2000.0, 2100.0],
            "dividend": [0.2, 0.0, 0.0],
            "stock_splits": [0.0, 2.0, 0.0],
        },
        index=idx,
    )

    normalized, changed = ibkr_helper._normalize_equity_daily_prices_for_splits(frame)

    assert changed is True
    assert normalized["_split_adjusted"].all()
    assert normalized.loc[idx[0], "close"] == pytest.approx(50.0)
    assert normalized.loc[idx[0], "open"] == pytest.approx(49.0)
    assert normalized.loc[idx[0], "volume"] == pytest.approx(2000.0)
    assert normalized.loc[idx[0], "dividend"] == pytest.approx(0.1)
    assert normalized.loc[idx[1], "close"] == pytest.approx(46.5)


def test_normalize_equity_daily_prices_for_raw_reverse_split():
    idx = pd.DatetimeIndex(
        [
            "2025-01-02 16:00:00-05:00",
            "2025-01-03 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [9.5, 95.0],
            "high": [10.5, 105.0],
            "low": [9.0, 90.0],
            "close": [10.0, 100.0],
            "volume": [10000.0, 1000.0],
            "dividend": [1.0, 0.0],
            "stock_splits": [0.0, 0.1],
        },
        index=idx,
    )

    normalized, changed = ibkr_helper._normalize_equity_daily_prices_for_splits(frame)

    assert changed is True
    assert normalized["_split_adjusted"].all()
    assert normalized.loc[idx[0], "close"] == pytest.approx(100.0)
    assert normalized.loc[idx[0], "volume"] == pytest.approx(1000.0)
    assert normalized.loc[idx[0], "dividend"] == pytest.approx(10.0)


def test_normalize_equity_daily_prices_marks_already_adjusted_split_without_double_adjusting():
    idx = pd.DatetimeIndex(
        [
            "2025-11-19 16:00:00-05:00",
            "2025-11-20 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [49.0, 52.0],
            "high": [52.0, 54.0],
            "low": [48.5, 46.0],
            "close": [50.0, 46.5],
            "volume": [2000.0, 2000.0],
            "dividend": [0.0, 0.0],
            "stock_splits": [0.0, 2.0],
        },
        index=idx,
    )

    normalized, changed = ibkr_helper._normalize_equity_daily_prices_for_splits(frame)
    normalized_again, changed_again = ibkr_helper._normalize_equity_daily_prices_for_splits(normalized)

    assert changed is True
    assert changed_again is False
    assert normalized["_split_adjusted"].all()
    assert normalized.loc[idx[0], "close"] == pytest.approx(50.0)
    assert normalized_again.loc[idx[0], "close"] == pytest.approx(50.0)


def test_normalize_equity_daily_prices_still_checks_marked_frame_after_late_split_enrichment():
    idx = pd.DatetimeIndex(
        [
            "2025-11-19 16:00:00-05:00",
            "2025-11-20 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [98.0, 52.0],
            "high": [104.0, 54.0],
            "low": [97.0, 46.0],
            "close": [100.0, 46.5],
            "volume": [1000.0, 2000.0],
            "dividend": [0.0, 0.0],
            "stock_splits": [0.0, 2.0],
            "_split_adjusted": [True, True],
        },
        index=idx,
    )

    normalized, changed = ibkr_helper._normalize_equity_daily_prices_for_splits(frame)

    assert changed is True
    assert normalized["_split_adjusted"].all()
    assert normalized.loc[idx[0], "close"] == pytest.approx(50.0)
    assert normalized.loc[idx[0], "volume"] == pytest.approx(2000.0)


def test_normalize_equity_daily_prices_repairs_mixed_tqqq_cache_shape():
    idx = pd.DatetimeIndex(
        [
            "2021-01-20 16:00:00-05:00",
            "2021-01-21 16:00:00-05:00",
            "2022-01-12 16:00:00-05:00",
            "2022-01-13 16:00:00-05:00",
            "2025-11-19 16:00:00-05:00",
            "2025-11-20 16:00:00-05:00",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [23.96, 25.08, 38.48, 38.57, 98.66, 52.92],
            "high": [24.97, 25.54, 38.98, 38.78, 103.15, 53.54],
            "low": [23.88, 24.75, 37.47, 35.01, 97.43, 46.23],
            "close": [24.75, 25.36, 38.17, 35.40, 100.05, 46.45],
            "volume": [1000.0, 1000.0, 1000.0, 1000.0, 768163.78, 1576329.68],
            "dividend": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "stock_splits": [0.0, 2.0, 0.0, 2.0, 0.0, 2.0],
        },
        index=idx,
    )

    normalized, changed = ibkr_helper._normalize_equity_daily_prices_for_splits(frame)

    assert changed is True
    assert normalized["_split_adjusted"].all()
    # 2021 and 2022 were already continuous in the cache and must not be halved again.
    assert normalized.loc[idx[0], "close"] == pytest.approx(24.75)
    assert normalized.loc[idx[2], "close"] == pytest.approx(38.17)
    # The 2025 tail was raw and must be brought into the same adjusted price space.
    assert normalized.loc[idx[4], "close"] == pytest.approx(50.025)
    assert normalized.loc[idx[4], "open"] == pytest.approx(49.33)
    assert normalized.loc[idx[5], "close"] == pytest.approx(46.45)


def test_normalize_equity_daily_prices_repairs_old_dev_tqqq_tail_only():
    idx = pd.DatetimeIndex(
        [
            "2022-10-31 16:00:00",
            "2022-11-01 16:00:00",
            "2022-11-02 16:00:00",
            "2024-01-30 16:00:00",
            "2024-01-31 16:00:00",
            "2024-02-01 16:00:00",
            "2024-02-02 16:00:00",
            "2024-02-05 16:00:00",
        ],
        tz="America/New_York",
    )
    frame = pd.DataFrame(
        {
            "open": [10.9, 20.9, 9.4, 28.4, 52.9, 52.8, 52.7, 26.7],
            "high": [11.0, 21.1, 9.5, 28.5, 53.2, 53.1, 53.0, 27.0],
            "low": [10.1, 19.8, 8.8, 27.2, 52.0, 52.2, 52.0, 26.0],
            "close": [10.48, 20.30, 9.10, 27.97, 52.64, 52.80, 52.90, 26.45],
            "volume": [1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 768163.78, 1576329.68],
            "dividend": [0.0] * 8,
            "stock_splits": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.0],
        },
        index=idx,
    )

    normalized, changed = ibkr_helper._normalize_equity_daily_prices_for_splits(frame)

    assert changed is True
    assert normalized["_split_adjusted"].all()
    # This known dev-cache jump was not on a split date. Keep it visible for cache-quality audits
    # instead of masking arbitrary data anomalies as corporate actions.
    assert normalized.loc[idx[1], "close"] == pytest.approx(20.30)
    assert normalized.loc[idx[2], "close"] == pytest.approx(9.10)
    # The later raw segment is exactly what created the false 2025 TQQQ split cliff.
    assert normalized.loc[idx[4], "close"] == pytest.approx(26.32)
    assert normalized.loc[idx[5], "close"] == pytest.approx(26.40)
    assert normalized.loc[idx[6], "close"] == pytest.approx(26.45)
    assert normalized.loc[idx[7], "close"] == pytest.approx(26.45)


def test_get_price_data_normalizes_cached_ibkr_daily_stock_split_tail(tmp_path, monkeypatch):
    class NoRemoteCache:
        def ensure_local_file(self, *args, **kwargs):
            return False

        def on_local_update(self, *args, **kwargs):
            return False

    asset = Asset("TQQQ", Asset.AssetType.STOCK)
    quote = Asset("USD", Asset.AssetType.FOREX)
    cache_file = tmp_path / "stock_TQQQ_USD_day_AUTO_TRADES_RTH.parquet"
    idx = pd.DatetimeIndex(
        [
            "2025-11-18 16:00:00-05:00",
            "2025-11-19 16:00:00-05:00",
            "2025-11-20 16:00:00-05:00",
            "2025-11-21 16:00:00-05:00",
        ]
    )
    raw_ibkr_frame = pd.DataFrame(
        {
            "open": [98.0, 98.66, 52.92, 47.05],
            "high": [101.0, 103.15, 53.54, 48.0],
            "low": [97.0, 97.43, 46.23, 46.2],
            "close": [99.0, 100.05, 46.45, 46.85],
            "volume": [720000.0, 768163.78, 1576329.68, 1500000.0],
            "dividend": [0.0, 0.0, 0.0, 0.0],
            "stock_splits": [0.0, 0.0, 2.0, 0.0],
        },
        index=idx,
    )
    raw_ibkr_frame.to_parquet(cache_file)

    monkeypatch.setattr(ibkr_helper, "get_backtest_cache", lambda: NoRemoteCache())
    monkeypatch.setattr(ibkr_helper, "_cache_file_for", lambda **kwargs: cache_file)
    monkeypatch.setattr(
        ibkr_helper,
        "_append_equity_corporate_actions_daily",
        lambda frame, stock_asset: (frame, False),
    )

    returned = ibkr_helper.get_price_data(
        asset=asset,
        quote=quote,
        timestep="day",
        start_dt=idx[0].to_pydatetime(),
        end_dt=idx[-1].to_pydatetime(),
        exchange="AUTO",
        include_after_hours=False,
        source="TRADES",
    )

    assert returned["_split_adjusted"].all()
    assert returned.loc[idx[1], "close"] == pytest.approx(50.025)
    assert returned.loc[idx[2], "close"] == pytest.approx(46.45)

    persisted = pd.read_parquet(cache_file)
    assert persisted["_split_adjusted"].all()
    assert persisted.loc[idx[1], "close"] == pytest.approx(50.025)


def test_tqqq_daily_backtest_uses_normalized_ibkr_split_data_without_false_cliff():
    asset = Asset("TQQQ", Asset.AssetType.STOCK)
    idx = pd.DatetimeIndex(
        [
            "2025-11-17 16:00:00-05:00",
            "2025-11-18 16:00:00-05:00",
            "2025-11-19 16:00:00-05:00",
            "2025-11-20 16:00:00-05:00",
            "2025-11-21 16:00:00-05:00",
            "2025-11-24 16:00:00-05:00",
        ]
    )
    raw_ibkr_frame = pd.DataFrame(
        {
            "open": [96.0, 98.0, 98.66, 52.92, 47.05, 47.4],
            "high": [100.0, 101.0, 103.15, 53.54, 48.0, 48.2],
            "low": [95.0, 97.0, 97.43, 46.23, 46.2, 46.9],
            "close": [97.0, 99.0, 100.05, 46.45, 46.85, 47.3],
            "volume": [700000.0, 720000.0, 768163.78, 1576329.68, 1500000.0, 1450000.0],
            "dividend": [0.0] * 6,
            "stock_splits": [0.0, 0.0, 0.0, 2.0, 0.0, 0.0],
        },
        index=idx,
    )

    normalized, changed = ibkr_helper._normalize_equity_daily_prices_for_splits(raw_ibkr_frame)
    assert changed is True
    assert normalized.loc[idx[2], "close"] == pytest.approx(50.025)
    assert normalized.loc[idx[3], "close"] == pytest.approx(46.45)

    data_source = PandasDataBacktesting(
        pandas_data={asset: Data(asset, normalized, timestep="day")},
        datetime_start=idx[0].to_pydatetime(),
        datetime_end=idx[-1].to_pydatetime(),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    broker = BacktestingBroker(data_source=data_source)
    strategy = _HoldTqqqThroughSplitStrategy(
        broker=broker,
        data_source=data_source,
        budget=1000.0,
        risk_free_rate=0,
        benchmark_asset=None,
        analyze_backtest=False,
        parameters={"asset": asset},
    )

    trader = Trader(backtest=True, logfile="")
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False)

    assert strategy.observed_quantities[pd.Timestamp("2025-11-20").date()] == pytest.approx(10)
    assert strategy.observed_quantities[pd.Timestamp("2025-11-21").date()] == pytest.approx(10)

    trade_log = broker._trade_event_log_df
    split_events = trade_log.loc[trade_log["event_kind"] == "corporate_action"]
    assert split_events.empty

    fills = trade_log.loc[trade_log["status"] == "fill"]
    sell_fills = fills.loc[fills["side"].astype(str).str.lower().str.startswith("sell")]
    assert sell_fills.iloc[-1]["filled_quantity"] == pytest.approx(10)

    stats = strategy.stats
    split_eve_value = stats.loc[pd.Timestamp("2025-11-19 16:00:00-05:00"), "portfolio_value"]
    split_day_value = stats.loc[pd.Timestamp("2025-11-20 16:00:00-05:00"), "portfolio_value"]
    one_step_return = split_day_value / split_eve_value - 1
    assert one_step_return == pytest.approx(-0.0358, abs=0.01)
    assert one_step_return > -0.10
