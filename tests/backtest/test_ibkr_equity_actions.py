import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset
from lumibot.entities import Data
from lumibot.strategies.strategy import Strategy
from lumibot.tools import ibkr_helper
from lumibot.tools.ibkr_equity_basis import EquityBasisSnapshot, decorate_adjusted_frame
from lumibot.tools.yahoo_helper import YahooHelper
from lumibot.traders import Trader


# 2026-10-10: adjusted prices now require provider provenance. Price continuity
# cannot establish a split basis; old shape-based repair expectations were unsafe.
def _certify_adjusted(frame):
    actions = pd.DataFrame({
        "Dividends": frame["dividend"].to_numpy() if "dividend" in frame else 0.0,
        "Stock Splits": frame["stock_splits"].to_numpy() if "stock_splits" in frame else 0.0,
    }, index=frame.index.normalize())
    last = frame.index.max().date().isoformat()
    snapshot = EquityBasisSnapshot.from_verified_actions(
        actions, as_of=pd.Timestamp(last, tz="America/New_York") + pd.Timedelta(hours=23),
        coverage_through=last)
    return decorate_adjusted_frame(frame, snapshot, provider_prices_verified=True), snapshot


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

    original = frame.copy(deep=True)
    with pytest.raises(ValueError, match="basis|provenance"):
        ibkr_helper._normalize_equity_daily_prices_for_splits(frame)
    pd.testing.assert_frame_equal(frame, original)


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

    original = frame.copy(deep=True)
    with pytest.raises(ValueError, match="basis|provenance"):
        ibkr_helper._normalize_equity_daily_prices_for_splits(frame)
    pd.testing.assert_frame_equal(frame, original)


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

    frame, _ = _certify_adjusted(frame)
    normalized, changed = ibkr_helper._normalize_equity_daily_prices_for_splits(frame)
    normalized_again, changed_again = ibkr_helper._normalize_equity_daily_prices_for_splits(normalized)
    assert changed is False
    assert changed_again is False
    pd.testing.assert_frame_equal(normalized, frame)
    pd.testing.assert_frame_equal(normalized_again, frame)
    assert normalized.loc[idx[0], "close"] == pytest.approx(50.0)


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

    original = frame.copy(deep=True)
    # An old boolean marker cannot identify which splits the price source applied.
    with pytest.raises(ValueError, match="basis|provenance"):
        ibkr_helper._normalize_equity_daily_prices_for_splits(frame)
    pd.testing.assert_frame_equal(frame, original)


def test_normalize_equity_daily_prices_rejects_mixed_tqqq_basis():
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

    frame, snapshot = _certify_adjusted(frame)
    different = EquityBasisSnapshot.from_verified_actions(
        pd.DataFrame(columns=["Dividends", "Stock Splits"], index=pd.DatetimeIndex([])),
        as_of=snapshot.as_of, coverage_through=snapshot.coverage_through)
    frame.loc[idx[-2:], "_ibkr_split_basis"] = different.split_fingerprint
    original = frame.copy(deep=True)
    with pytest.raises(ValueError, match="basis|bases|provenance"):
        ibkr_helper._normalize_equity_daily_prices_for_splits(frame)
    pd.testing.assert_frame_equal(frame, original)


def test_normalize_equity_daily_prices_preserves_certified_large_moves():
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

    frame, _ = _certify_adjusted(frame)
    normalized, changed = ibkr_helper._normalize_equity_daily_prices_for_splits(frame)
    assert changed is False
    # Even split-sized jumps at/around a split may be real. Preserve every
    # certified provider value; never infer a splice point from price shape.
    pd.testing.assert_frame_equal(normalized, frame)
    assert normalized.loc[idx[1], "close"] == pytest.approx(20.30)
    assert normalized.loc[idx[4], "close"] == pytest.approx(52.64)
    assert normalized.loc[idx[-1], "close"] == pytest.approx(26.45)


def test_get_price_data_preserves_certified_ibkr_daily_stock_split_tail(tmp_path, monkeypatch):
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
    adjusted_ibkr_frame = pd.DataFrame(
        {
            "open": [49.0, 49.33, 52.92, 47.05],
            "high": [50.5, 51.575, 53.54, 48.0],
            "low": [48.5, 48.715, 46.23, 46.2],
            "close": [49.5, 50.025, 46.45, 46.85],
            "volume": [1440000.0, 1536327.56, 1576329.68, 1500000.0],
            "dividend": [0.0, 0.0, 0.0, 0.0],
            "stock_splits": [0.0, 0.0, 2.0, 0.0],
        },
        index=idx,
    )
    adjusted_ibkr_frame, snapshot = _certify_adjusted(adjusted_ibkr_frame)
    adjusted_ibkr_frame.to_parquet(cache_file)
    monkeypatch.setattr(ibkr_helper, "_get_current_equity_basis", lambda symbol: snapshot, raising=False)

    monkeypatch.setattr(ibkr_helper, "get_backtest_cache", lambda: NoRemoteCache())
    monkeypatch.setattr(ibkr_helper, "_cache_file_for", lambda **kwargs: cache_file)
    monkeypatch.setattr(
        ibkr_helper,
        "_append_equity_corporate_actions_daily",
        lambda frame, stock_asset, **kwargs: (frame, False),
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

    price_columns = ["open", "high", "low", "close", "volume", "dividend", "stock_splits"]
    pd.testing.assert_frame_equal(returned[price_columns].reset_index(drop=True),
                                  adjusted_ibkr_frame[price_columns].reset_index(drop=True))
    persisted = pd.read_parquet(cache_file)
    pd.testing.assert_frame_equal(persisted[price_columns].reset_index(drop=True),
                                  adjusted_ibkr_frame[price_columns].reset_index(drop=True))
    assert persisted["_split_adjusted"].all()
    assert persisted.loc[idx[1], "close"] == pytest.approx(50.025)


def test_tqqq_daily_backtest_uses_certified_adjusted_prices_without_false_cliff():
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
    adjusted_ibkr_frame = pd.DataFrame(
        {
            "open": [48.0, 49.0, 49.33, 52.92, 47.05, 47.4],
            "high": [50.0, 50.5, 51.575, 53.54, 48.0, 48.2],
            "low": [47.5, 48.5, 48.715, 46.23, 46.2, 46.9],
            "close": [48.5, 49.5, 50.025, 46.45, 46.85, 47.3],
            "volume": [1400000.0, 1440000.0, 1536327.56, 1576329.68, 1500000.0, 1450000.0],
            "dividend": [0.0] * 6,
            "stock_splits": [0.0, 0.0, 0.0, 2.0, 0.0, 0.0],
        },
        index=idx,
    )

    adjusted_ibkr_frame, _ = _certify_adjusted(adjusted_ibkr_frame)
    normalized, changed = ibkr_helper._normalize_equity_daily_prices_for_splits(adjusted_ibkr_frame)
    assert changed is False
    pd.testing.assert_frame_equal(normalized, adjusted_ibkr_frame)
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
