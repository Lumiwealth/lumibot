"""Option lookups on the routed (mixed) backtesting source must follow the routing map.

The routed source inherits from the ThetaData source. Before this fix, get_chains() and
snapshot quotes were inherited unchanged, so a map routing "option" to another provider still
asked ThetaData for chains and snapshot quotes.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from lumibot.backtesting.routed_backtesting import (
    RoutedBacktestingPandas,
    RoutingProviderError,
    _ProviderRegistry,
)
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset
import lumibot.tools.thetadata_helper as thetadata_helper


CHAIN = {
    "Multiplier": 100,
    "Exchange": "SMART",
    "Chains": {"CALL": {"2025-01-17": [100.0, 105.0]}, "PUT": {"2025-01-17": [95.0, 100.0]}},
}


def _router(routing):
    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = RoutedBacktestingPandas._normalize_routing(routing)
    routed._registry = _ProviderRegistry(routed)
    routed._datetime = datetime(2025, 1, 2, 10, 0)
    routed.datetime_start = datetime(2025, 1, 2)
    routed.datetime_end = datetime(2025, 1, 31)
    return routed


def _option():
    return Asset("SPY", asset_type="option", expiration=datetime(2025, 1, 17).date(), strike=100.0, right="CALL")


@pytest.fixture
def theta_forbidden(monkeypatch):
    calls = []

    def boom(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("ThetaData must not be used when options are routed elsewhere")

    monkeypatch.setattr(thetadata_helper, "get_chains_cached", boom)
    monkeypatch.setattr(thetadata_helper, "get_historical_data_snapshot_cached", boom)
    monkeypatch.setattr(ThetaDataBacktestingPandas, "get_chains", boom)
    return calls


class _FakeChainSource:
    def __init__(self):
        self.calls = []
        self._datetime = None
        self._chain_constraints = None

    def get_chains(self, asset, quote=None):
        self.calls.append((asset, self._datetime, self._chain_constraints))
        return dict(CHAIN)


def test_get_chains_uses_option_route_alpaca(theta_forbidden):
    routed = _router({"default": "ibkr", "stock": "ibkr", "option": "alpaca"})
    adapter = routed._registry.adapter_for_spec(routed._registry.resolve_provider_spec("alpaca"))
    fake = _FakeChainSource()
    adapter._source = fake
    routed._chain_constraints = {"max_expiration_date": datetime(2025, 2, 1).date()}

    chains = routed.get_chains(Asset("SPY"))

    assert chains["Chains"]["CALL"]["2025-01-17"] == [100.0, 105.0]
    assert len(fake.calls) == 1
    _asset, source_dt, constraints = fake.calls[0]
    assert source_dt == routed._datetime  # built as of the routed backtest's simulated time
    assert constraints == routed._chain_constraints
    assert theta_forbidden == []


def test_get_chains_uses_option_route_polygon(theta_forbidden, monkeypatch):
    import lumibot.tools.polygon_helper as polygon_helper

    seen = {}

    def fake_polygon_chains(*, api_key, asset, quote=None, exchange=None, current_date=None, polygon_client=None):
        seen["date"] = current_date
        seen["symbol"] = asset.symbol
        return dict(CHAIN)

    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(polygon_helper, "get_chains_cached", fake_polygon_chains)
    routed = _router({"default": "ibkr", "option": "polygon"})

    chains = routed.get_chains(Asset("SPY"))

    assert chains["Chains"]["PUT"]["2025-01-17"] == [95.0, 100.0]
    assert seen == {"date": datetime(2025, 1, 2).date(), "symbol": "SPY"}
    assert theta_forbidden == []


def test_get_chains_raises_clear_error_when_option_route_has_no_chains(theta_forbidden):
    routed = _router({"default": "thetadata", "option": "ibkr"})

    with pytest.raises(RoutingProviderError, match="ibkr"):
        routed.get_chains(Asset("SPY"))
    assert theta_forbidden == []


def test_get_chains_still_uses_thetadata_when_options_route_there(monkeypatch):
    called = {}

    def fake_theta_chains(self, asset):
        called["asset"] = asset
        return dict(CHAIN)

    monkeypatch.setattr(ThetaDataBacktestingPandas, "get_chains", fake_theta_chains)
    routed = _router({"default": "ibkr", "option": "thetadata"})

    routed.get_chains(Asset("SPY"))
    assert called["asset"].symbol == "SPY"


def _capture_super_quote(monkeypatch):
    seen = {}

    def fake_super_get_quote(self, asset, quote=None, exchange=None, timestep="minute", **kwargs):
        seen["kwargs"] = dict(kwargs)
        return None

    monkeypatch.setattr(ThetaDataBacktestingPandas, "get_quote", fake_super_get_quote)
    return seen


def test_snapshot_quote_for_option_routed_elsewhere_skips_thetadata_snapshot(theta_forbidden, monkeypatch):
    seen = _capture_super_quote(monkeypatch)
    routed = _router({"default": "ibkr", "option": "alpaca"})

    routed.get_quote(_option(), snapshot_only=True)

    assert "snapshot_only" not in seen["kwargs"]
    assert theta_forbidden == []


def test_snapshot_quote_kept_when_options_route_to_thetadata(monkeypatch):
    seen = _capture_super_quote(monkeypatch)
    routed = _router({"default": "ibkr", "option": "thetadata"})

    routed.get_quote(_option(), snapshot_only=True)

    assert seen["kwargs"].get("snapshot_only") is True
