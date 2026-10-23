"""Opt-in public Phemex testnet qualification. No account or order writes."""

import pytest
from concurrent.futures import ThreadPoolExecutor

from lumibot.brokers import Ccxt
from lumibot.entities import Asset


@pytest.fixture(scope="module")
def phemex_testnet():
    broker = Ccxt({"exchange_id": "phemex", "sandbox": True, "timeout": 10000},
                  connect_stream=False, start_orders_thread=False)
    try:
        yield broker
    finally:
        broker.cleanup_streams()
        broker.api.session.close()


@pytest.mark.apitest
@pytest.mark.public_http
@pytest.mark.parametrize("symbol", ["BTC/USDT", "BTC/USDT:USDT", "BTC/USD:BTC", "BTC/USDC:USDC"])
def test_public_testnet_market_and_price(phemex_testnet, symbol):
    broker = phemex_testnet
    assert all("testnet" in endpoint for endpoint in broker.api.urls["api"].values())
    asset = broker.get_asset(symbol)
    market = broker.get_market(symbol)
    assert market["symbol"] == symbol
    assert asset.asset_type == ("crypto_future" if market["contract"] else "crypto")
    assert broker.data_source.get_last_price(asset, Asset(market["quote"], "crypto")) > 0


@pytest.mark.apitest
@pytest.mark.public_http
@pytest.mark.parametrize("sandbox,symbol", [
    pytest.param(True, "BTC/USDT", id="spot-testnet"),
    pytest.param(True, "BTC/USDT:USDT", id="usdt-testnet"),
    pytest.param(True, "BTC/USD:BTC", id="inverse-testnet"),
    pytest.param(True, "BTC/USDC:USDC", id="usdc-testnet"),
    pytest.param(False, "BTC/USDC:USDC", id="usdc-production-public"),
])
def test_public_testnet_stream_family(sandbox, symbol):
    broker = Ccxt({"exchange_id": "phemex", "sandbox": sandbox, "timeout": 10000},
                  start_orders_thread=False)
    try:
        broker._ensure_markets_loaded()
        methods = ["watch_ticker", "watch_order_book", "watch_trades", "watch_ohlcv"]
        def watch(method):
            kwargs = {"timeframe": "1m"} if method == "watch_ohlcv" else {}
            return broker.stream.snapshot(method, symbol=symbol, timeout=12, **kwargs)
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {method: pool.submit(watch, method) for method in methods}
            result, errors = {}, {}
            for method, future in futures.items():
                try:
                    result[method] = future.result()
                except Exception as exc:
                    errors[method] = type(exc).__name__
        assert not errors, errors
        assert result["watch_ticker"]["last"] > 0
        assert result["watch_order_book"]["bids"] and result["watch_order_book"]["asks"]
        assert result["watch_trades"]
        assert result["watch_ohlcv"]
    finally:
        broker.cleanup_streams()
        broker.api.session.close()
