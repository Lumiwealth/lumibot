"""Routed native CCXT terms and Phemex public response contracts; offline only."""

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset
from lumibot.tools.ccxt_data_store import CcxtCacheDB
from tests.test_ccxt_derivatives import derivatives
from tests.test_ccxt_unified_spot import make_broker
from tests.test_backtesting_multileg_unit import _StubStrategy

FIXTURE = json.loads((Path(__file__).parent / "fixtures/ccxt_public_perpetual_data.json").read_text())


def _router(monkeypatch, tmp_path, api):
    import lumibot.tools.ccxt_data_store as store
    import lumibot.tools.thetadata_helper as theta

    monkeypatch.setattr(store, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_a, **_k: None)
    monkeypatch.setattr(theta, "reset_theta_terminal_tracking", lambda *_a, **_k: None)
    router = RoutedBacktestingPandas(
        datetime_start=datetime(2026, 11, 13, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 11, 13, 0, 10, tzinfo=timezone.utc),
        config={"backtesting_data_routing": {"crypto_future": "phemex", "crypto": "coinbase"}},
        username="test", password="test", use_quote_data=False,
        show_progress_bar=False, log_backtest_progress_to_file=False,
    )
    cache = CcxtCacheDB.__new__(CcxtCacheDB)
    cache.api, cache.exchange_id = api, "phemex"
    cache.max_download_limit = 50000
    cache.logger = logging.getLogger("offline-ccxt")
    router._registry._adapters["ccxt"]._cache_by_exchange["phemex"] = cache
    return router, cache


@pytest.mark.parametrize("symbol,settle,inverse", [
    ("BTC/USD:BTC", "BTC", True), ("BTCUSD", "BTC", True),
    ("BTC/USDT:USDT", "USDT", False), ("BTC/USDC:USDC", "USDC", False),
])
def test_routed_ccxt_terms_preserve_native_settlement(monkeypatch, tmp_path, derivatives, symbol, settle, inverse):
    router, _ = _router(monkeypatch, tmp_path, derivatives.api)
    spec = router.get_contract_spec(Asset(symbol, "crypto_future"), Asset(settle, "crypto"))
    assert spec.settle == settle and spec.inverse is inverse
    assert spec.quantity_unit == "contracts" and spec.contract_size == 1


def test_routed_ccxt_history_resolves_native_contract_before_fetch(monkeypatch, tmp_path, derivatives):
    router, cache = _router(monkeypatch, tmp_path, derivatives.api)
    frame = pd.DataFrame({"open": [100], "high": [102], "low": [99], "close": [101], "volume": [2]},
                         index=pd.DatetimeIndex(["2026-11-13"], tz="UTC"))
    cache.download_ohlcv = Mock(return_value=frame)
    router._update_pandas_data(Asset("BTCUSD", "crypto_future"), Asset("BTC", "crypto"),
                               length=1, timestep="minute", start_dt=router.datetime_start)
    assert cache.download_ohlcv.call_args.args[0] == "BTC/USD:BTC"


def test_routed_ccxt_funding_and_marks_use_the_same_cache_owner(monkeypatch, tmp_path, derivatives):
    router, cache = _router(monkeypatch, tmp_path, derivatives.api)
    asset = Asset("BTC/USD:BTC", "crypto_future")
    expected = [{"timestamp": pd.Timestamp("2026-11-13", tz="UTC"), "fundingRate": "0.0001"}]
    cache.get_funding_events = Mock(return_value=expected)
    cache.get_perpetual_mark = Mock(return_value=(71312.7, True))
    assert router.get_funding_events(asset, router.datetime_start, router.datetime_end) == expected
    assert router.get_perpetual_mark(asset, router.datetime_start) == (71312.7, True)
    assert cache.get_funding_events.call_count == cache.get_perpetual_mark.call_count == 1


def test_mark_helper_reuses_metadata_symbol_bounds_and_native_parser(derivatives):
    from lumibot.tools.ccxt_phemex import fetch_mark_ohlcv

    api = derivatives.api
    api.markets["BTC/USD:BTC"]["info"].update(FIXTURE["products"]["BTCUSD"])
    api.publicGetMdV2KlineList = Mock(return_value=FIXTURE["mark_candles"])
    rows = fetch_mark_ohlcv(api, "BTC/USD:BTC", "1m", since=1794528000000, limit=10,
                           params={"until": 1794528600000})
    assert rows[0][0] == 1794528000000
    assert rows[0][1] == 71346.7  # Provider open; close is a later observation.
    request = api.publicGetMdV2KlineList.call_args.args[0]
    assert request["symbol"] == ".MBTC" and request["from"] == 1794528000 and request["to"] == 1794528600


def test_generic_mark_helper_uses_declared_capability_without_trade_substitution():
    from lumibot.tools.ccxt_phemex import fetch_mark_ohlcv

    api = SimpleNamespace(id="other", has={"fetchMarkOHLCV": False},
                          fetch_ohlcv=Mock(side_effect=AssertionError("trade substitution")))
    assert fetch_mark_ohlcv(api, "COIN/USD:COIN", since=1000, limit=2) is None
    api.has["fetchMarkOHLCV"] = True
    api.fetch_mark_ohlcv = Mock(return_value=[[1000, 10, 11, 9, 10, 0]])
    assert fetch_mark_ohlcv(api, "COIN/USD:COIN", since=1000, limit=2)[0][0] == 1000
    assert api.fetch_mark_ohlcv.call_args.kwargs["since"] == 1000


@pytest.mark.parametrize("symbol,wallet,budget,quantity", [
    ("BTC/USD:BTC", "BTC", .1, 1000),
    ("BTC/USDT:USDT", "USDT", 2000, .02),
    ("BTC/USDC:USDC", "USDC", 2000, .02),
])
def test_routed_native_fill_uses_contract_and_wallet_units(monkeypatch, tmp_path, derivatives,
                                                          symbol, wallet, budget, quantity):
    """Synthetic $50k fill plus declared metadata; not a live exchange fill."""
    from lumibot.backtesting import BacktestingBroker

    router, cache = _router(monkeypatch, tmp_path, derivatives.api)
    cache.get_funding_events = Mock(return_value=None)
    cache.get_perpetual_mark = Mock(return_value=(50000, True))
    broker = BacktestingBroker(router)
    try:
        strategy = _StubStrategy(broker=broker, quote_asset=Asset(wallet, "crypto"), budget=budget,
                                 analyze_backtest=False, buy_trading_fees=[], sell_trading_fees=[])
        asset = Asset(symbol, "crypto_future", leverage=3)
        order = strategy.create_order(asset, quantity, "buy")
        broker._process_new_order(order)
        broker._execute_filled_order(order, 50000, Decimal(str(quantity)), strategy)
        position = broker.get_tracked_position(strategy.name, asset)
        # Position quantities may be floats; compare their exact decimal text,
        # not Decimal-versus-binary-float object equality.
        assert position is not None and Decimal(str(position.quantity)) == Decimal(str(quantity))
        assert strategy.cash == pytest.approx(budget)  # Collateral is reserved, not spent.
        assert strategy.get_contract_exposure(asset, quantity, 50000)["quote"] == 1000
    finally:
        broker.cleanup_streams()


def test_routed_inverse_funding_uses_recorded_native_settlement_mark(monkeypatch, tmp_path, derivatives):
    """An explicitly seeded synthetic holding pays the recorded public funding."""
    from lumibot.backtesting import BacktestingBroker

    api = derivatives.api
    api.markets["BTC/USD:BTC"]["info"].update(FIXTURE["products"]["BTCUSD"])
    api.markets["BTC/USD:BTC"].setdefault("baseId", "BTC")
    api.has["fetchFundingRateHistory"] = True
    api.v2GetApiDataPublicDataFundingRateHistory = Mock(return_value=FIXTURE["funding"])
    api.publicGetMdV2KlineList = Mock(return_value=FIXTURE["mark_candles"])
    router, _ = _router(monkeypatch, tmp_path, api)
    before = datetime(2026, 11, 12, 23, 59, tzinfo=timezone.utc)
    router.datetime_start = before
    router._update_datetime(before)
    broker = BacktestingBroker(router)
    try:
        strategy = _StubStrategy(broker=broker, quote_asset=Asset("BTC", "crypto"), budget=.1,
                                 analyze_backtest=False, buy_trading_fees=[], sell_trading_fees=[])
        asset = Asset("BTC/USD:BTC", "crypto_future", leverage=3)
        order = strategy.create_order(asset, 1000, "buy")
        broker._process_new_order(order)
        broker._execute_filled_order(order, 71000, Decimal(1000), strategy)
        router._update_datetime(datetime(2026, 11, 13, tzinfo=timezone.utc))
        broker._perpetuals.process_events(strategy)
        expected = .1 - 1000 * .0001 / 71346.7
        assert strategy.cash == pytest.approx(expected, abs=1e-12)
        broker._perpetuals.process_events(strategy)
        assert strategy.cash == pytest.approx(expected, abs=1e-12)  # Never charge twice.
        assert api.publicGetMdV2KlineList.call_args.args[0]["symbol"] == ".MBTC"
    finally:
        broker.cleanup_streams()
