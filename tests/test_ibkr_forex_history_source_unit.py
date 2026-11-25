"""Forex and spot metals (for example XAU/USD) default to IBKR Midpoint history.

IBKR does not publish reliable TRADES bars for forex or spot metals: a Dev
backtest on Nov 24 2026 asked for 50 hourly XAU/USD bars and got 28 to 31 with
source=Trades, and 50 of 50 with source=Midpoint. Customer backtests then failed
with "Required historical data unavailable". Like indexes and listed options,
forex must default to Midpoint unless the caller or IBKR_HISTORY_SOURCE picks a
source explicitly.
"""

from datetime import datetime, timezone

import pytest

import lumibot.tools.ibkr_helper as ibkr_helper
from lumibot.entities import Asset


class _Stop(Exception):
    pass


def _source_used(monkeypatch, asset, quote, **kwargs):
    seen = {}

    def fake_cache_file_for(**cache_kwargs):
        seen["source"] = cache_kwargs["source"]
        raise _Stop()

    monkeypatch.setattr(ibkr_helper, "_cache_file_for", fake_cache_file_for)
    with pytest.raises(_Stop):
        ibkr_helper.get_price_data(
            asset=asset,
            quote=quote,
            timestep="hour",
            start_dt=datetime(2026, 10, 23, tzinfo=timezone.utc),
            end_dt=datetime(2026, 11, 20, tzinfo=timezone.utc),
            **kwargs,
        )
    return seen["source"]


def _gold():
    return Asset("XAU", asset_type=Asset.AssetType.FOREX), Asset("USD", asset_type=Asset.AssetType.FOREX)


def test_forex_history_defaults_to_midpoint(monkeypatch):
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    asset, quote = _gold()
    assert _source_used(monkeypatch, asset, quote) == "Midpoint"


def test_currency_pair_history_defaults_to_midpoint(monkeypatch):
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    asset = Asset("EUR", asset_type=Asset.AssetType.FOREX)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    assert _source_used(monkeypatch, asset, quote) == "Midpoint"


def test_forex_history_keeps_an_explicit_source(monkeypatch):
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    asset, quote = _gold()
    assert _source_used(monkeypatch, asset, quote, source="Bid_Ask") == "Bid_Ask"


def test_forex_history_keeps_an_explicit_env_source(monkeypatch):
    monkeypatch.setenv("IBKR_HISTORY_SOURCE", "Trades")
    asset, quote = _gold()
    assert _source_used(monkeypatch, asset, quote) == "Trades"


def test_stock_history_still_defaults_to_trades(monkeypatch):
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    monkeypatch.setattr(ibkr_helper, "_get_current_equity_basis", lambda symbol: None)
    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    assert _source_used(monkeypatch, asset, None) == "Trades"
