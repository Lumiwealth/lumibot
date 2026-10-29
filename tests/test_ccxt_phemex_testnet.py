"""Opt-in public Phemex qualification. No account or exchange order writes."""

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
    pytest.param(False, "BTC/USDT", id="spot-production-public"),
    pytest.param(False, "BTC/USDT:USDT", id="usdt-production-public"),
    pytest.param(False, "BTC/USD:BTC", id="inverse-production-public"),
])
def test_public_testnet_stream_family(sandbox, symbol):
    broker = Ccxt({"exchange_id": "phemex", "sandbox": sandbox, "timeout": 10000},
                  start_orders_thread=False)
    try:
        broker._ensure_markets_loaded()
        methods = ["watch_ticker", "watch_order_book", "watch_trades", "watch_ohlcv"]
        def watch(method):
            kwargs = {"timeframe": "1m"} if method == "watch_ohlcv" else {}
            return broker.stream.snapshot(method, symbol=symbol, timeout=None, **kwargs)
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


@pytest.mark.apitest
@pytest.mark.public_http
@pytest.mark.parametrize("symbol", ["BTC/USDT", "BTC/USDT:USDT", "BTC/USDC:USDC", "BTC/USD:BTC"])
def test_public_phemex_historical_roundtrip(tmp_path, monkeypatch, symbol):
    """Download real candles/funding; matching is the local backtesting broker."""
    import shutil
    from decimal import Decimal
    import ccxt
    import pandas as pd
    import lumibot.tools.ccxt_data_store as store
    from lumibot.entities import ContractSpec
    from lumibot.data_sources.ccxt_backtesting_data import CcxtBacktestingData
    from lumibot.tools.ccxt_phemex import fetch_ohlcv, normalize_markets
    from tests.test_backtesting_perpetuals import RoundTrip

    cache_directory = tmp_path / "phemex-public-candles"
    cache_directory.mkdir()
    monkeypatch.setattr(store, "LUMIBOT_CACHE_FOLDER", str(cache_directory))
    api = ccxt.phemex({"enableRateLimit": True})
    strategy = None
    start = pd.Timestamp("2026-10-26 07:58", tz="UTC")
    end = start + pd.Timedelta(minutes=8)
    try:
        api.load_markets()
        normalize_markets(api)
        market = api.market(symbol)
        spec = ContractSpec.from_ccxt_market(market) if market["contract"] else None
        asset = Asset(symbol if spec else market["base"], "crypto_future" if spec else "crypto",
                      leverage=10, contract_spec=spec)
        first = fetch_ohlcv(api, symbol, "1m", since=int(start.timestamp()*1000), limit=5)[0][1]
        size = spec.quantity_for_notional(1000, first) if spec else Decimal(1000) / Decimal(str(first))
        quantity = float(api.amount_to_precision(symbol, float(size)))
        quote = Asset(market["settle"] if spec else market["quote"], "crypto")
        budget = .1 if spec and spec.inverse else 2000
        _, strategy = RoundTrip.run_backtest(
            CcxtBacktestingData, start, end, exchange_id="phemex", quote_asset=quote,
            budget=budget, parameters={"asset": asset, "quantity": quantity},
            benchmark_asset=None, analyze_backtest=False, show_plot=False,
            show_tearsheet=False, save_tearsheet=False, show_indicators=False,
            show_progress_bar=False, save_logfile=False, save_stats_file=False,
        )
        assert len(strategy.fills) == 2
        assert strategy.broker.get_tracked_position(strategy.name, asset) is None
        entry, exit = strategy.fills[0][2], strategy.fills[1][2]
        pnl = float(spec.pnl(quantity, entry, exit)) if spec else quantity * (exit - entry)
        funding = [row for row in strategy.broker.perpetual_events if row["type"] == "funding"]
        if spec:
            assert funding, "The historical interval crosses a real funding event"
        expected = budget + pnl + sum(float(row["amount"]) for row in funding)
        assert strategy.cash == pytest.approx(expected)
        assert strategy._update_portfolio_value() == pytest.approx(expected)
    finally:
        api.session.close()
        if strategy is not None:
            strategy.broker.data_source.cache_db.api.session.close()
            strategy.broker.cleanup_streams()
        shutil.rmtree(cache_directory)
