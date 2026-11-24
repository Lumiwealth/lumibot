import json


def test_build_backtest_data_provenance_prefers_observed_routes_and_redacts_secrets(monkeypatch):
    from lumibot.backtesting.data_provenance import build_backtest_data_provenance

    class RoutedSource:
        def get_data_provenance(self):
            return {
                "observedRoutes": [
                    {
                        "assetClass": "crypto",
                        "symbol": "BTC",
                        "adapter": "CcxtRoutingAdapter",
                        "vendor": "ccxt",
                        "exchange": "coinbase",
                        "feedType": "ohlc",
                        "resolution": "minute",
                        "apiKey": "must-not-leak",
                        "signedUrl": "https://example.invalid/private",
                    }
                ]
            }

    monkeypatch.setenv(
        "BOTSPOT_DATA_ROUTING_POLICY",
        json.dumps({"version": "botspot-auto-2026-08-28", "routes": {"crypto": "coinbase"}}),
    )

    provenance = build_backtest_data_provenance(RoutedSource())

    assert provenance == {
        "policyVersion": "botspot-auto-2026-08-28",
        "selection": "BotSpot Auto",
        "observedRoutes": [
            {
                "assetClass": "crypto",
                "symbol": "BTC",
                "adapter": "CcxtRoutingAdapter",
                "vendor": "ccxt",
                "exchange": "coinbase",
                "feedType": "ohlc",
                "resolution": "minute",
            }
        ],
    }
    assert "must-not-leak" not in json.dumps(provenance)
    assert "example.invalid" not in json.dumps(provenance)


def test_write_backtest_data_provenance_creates_a_stable_artifact(tmp_path):
    from lumibot.backtesting.data_provenance import write_backtest_data_provenance

    class YahooDataBacktesting:
        pass

    artifact = write_backtest_data_provenance(YahooDataBacktesting(), tmp_path)

    assert artifact == tmp_path / "data_provenance.json"
    assert json.loads(artifact.read_text()) == {
        "policyVersion": None,
        "selection": "Explicit data source",
        "observedRoutes": [
            {
                "adapter": "YahooDataBacktesting",
                "vendor": "yahoo",
            }
        ],
    }


def test_saved_provenance_keeps_price_funding_sources_and_notices(tmp_path, monkeypatch):
    """The saved artifact must name where prices and funding came from.

    Routed crypto adapters report priceSource, servedSymbol, fundingSource and
    markSource per route, plus notices such as unpublished funding. Before this
    fix the writer kept a fixed key list and silently dropped all of them, so a
    reader of data_provenance.json could not tell Coinbase USD from Binance USDT.
    """
    from lumibot.backtesting.data_provenance import write_backtest_data_provenance

    monkeypatch.delenv("BOTSPOT_DATA_ROUTING_POLICY", raising=False)

    class RoutedSource:
        def get_data_provenance(self):
            return {
                "observedRoutes": [
                    {"assetClass": "crypto_future", "symbol": "BTCUSDT", "vendor": "public",
                     "priceSource": "binance_usdm_bulk", "servedSymbol": "BTCUSDT",
                     "fundingSource": "binance_usdm_bulk", "markSource": "binance_usdm_bulk",
                     "apiKey": "must-not-leak"},
                ],
                "notices": [
                    {"kind": "funding_not_published", "source": "binance_usdm_bulk", "symbol": "BTCUSDT",
                     "through": None, "message": "funding not published", "secret": {"nested": "drop"}},
                ],
            }

    saved = json.loads(write_backtest_data_provenance(RoutedSource(), tmp_path).read_text())

    route = saved["observedRoutes"][0]
    assert route["priceSource"] == "binance_usdm_bulk"
    assert route["servedSymbol"] == "BTCUSDT"
    assert route["fundingSource"] == "binance_usdm_bulk"
    assert route["markSource"] == "binance_usdm_bulk"
    assert "apiKey" not in route
    assert saved["notices"] == [
        {"kind": "funding_not_published", "source": "binance_usdm_bulk", "symbol": "BTCUSDT",
         "message": "funding not published"},
    ]


def test_a_real_backtest_run_writes_its_data_provenance(tmp_path, monkeypatch):
    """run_backtest is a classmethod: the provenance writer must read the strategy instance's broker.

    Before the fix every hosted run logged "Backtest data provenance artifact could not be written:
    type object '<Strategy>' has no attribute 'broker'" and left no data_provenance.json, so no result
    screen could say where its prices came from.
    """
    from datetime import datetime

    import numpy as np
    import pandas as pd

    from lumibot.backtesting import PandasDataBacktesting
    from lumibot.entities import Asset, Data
    from lumibot.strategies import Strategy

    quote = Asset("USD", Asset.AssetType.FOREX)
    asset = Asset("SPY")
    index = pd.date_range("2026-08-03 09:30", "2026-08-04 16:00", freq="1min", tz="America/New_York")
    close = 100 + np.cumsum(np.random.default_rng(2).normal(0, 0.05, len(index)))
    frame = pd.DataFrame({"open": close, "high": close, "low": close, "close": close, "volume": 1.0}, index=index)

    class _Reads(Strategy):
        def initialize(self):
            self.sleeptime = "60M"

        def on_trading_iteration(self):
            self.get_last_price(asset)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "none")
    _Reads.run_backtest(
        PandasDataBacktesting, datetime(2026, 8, 3), datetime(2026, 8, 4),
        pandas_data={(asset, quote): Data(asset, frame, timestep="minute", quote=quote)},
        benchmark_asset=None, show_plot=False, show_tearsheet=False, save_tearsheet=False,
        show_indicators=False, quiet_logs=True, show_progress_bar=False, budget=10_000)
    artifact = tmp_path / "logs" / "data_provenance.json"
    assert artifact.is_file()
    routes = json.loads(artifact.read_text())["observedRoutes"]
    assert routes[0]["adapter"] == "PandasDataBacktesting"
