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
