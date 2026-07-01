import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.tools import ibkr_helper
from lumibot.tools.yahoo_helper import YahooHelper


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
