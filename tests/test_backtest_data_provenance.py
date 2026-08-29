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
