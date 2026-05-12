import pandas as pd

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
        staticmethod(lambda symbol, interval="1d", caching=True, auto_adjust=False, last_needed_datetime=None: actions),
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
