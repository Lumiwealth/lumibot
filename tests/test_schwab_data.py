from types import SimpleNamespace

from lumibot.data_sources.schwab_data import SchwabData
from lumibot.entities.asset import Asset


class _Response:
    status_code = 200
    text = "ok"

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _QuoteClient:
    def get_quotes(self, symbols):
        symbol = symbols[0]
        return _Response(
            {
                symbol: {
                    "quote": {
                        "lastPrice": 123.45,
                        "bidPrice": 123.4,
                        "askPrice": 123.5,
                        "totalVolume": 1000,
                        "quoteTime": 1_700_000_000_000,
                    }
                }
            }
        )


class _ChainClient:
    Options = SimpleNamespace(
        StrikeRange=SimpleNamespace(ALL="ALL", NTM="NTM"),
        ContractType=SimpleNamespace(ALL="ALL"),
        Strategy=SimpleNamespace(SINGLE="SINGLE"),
    )

    def get_option_chain(self, **_params):
        return {
            "callExpDateMap": {
                "2025-01-17:100": {
                    "100.0": [{}],
                    "105.0": [{}],
                }
            },
            "putExpDateMap": {
                "2025-01-17:100": {
                    "95.0": [{}],
                    "100.0": [{}],
                }
            },
        }


def test_schwab_quote_parses_response_payload():
    data_source = SchwabData(client=_QuoteClient())
    quote = data_source.get_quote(Asset("AAPL"))

    assert quote is not None
    assert quote.price == 123.45
    assert quote.bid == 123.4
    assert quote.ask == 123.5
    assert quote.symbol_used == "AAPL"


def test_schwab_quote_rejects_malformed_option_asset():
    data_source = SchwabData(client=_QuoteClient())
    malformed = Asset("AAPL", asset_type=Asset.AssetType.OPTION)

    assert data_source.get_quote(malformed) is None


def test_schwab_chains_parse_call_and_put_maps():
    data_source = SchwabData(client=_ChainClient())
    chains = data_source.get_chains(Asset("AAPL"))

    assert chains["Chains"]["CALL"]["2025-01-17"] == [100.0, 105.0]
    assert chains["Chains"]["PUT"]["2025-01-17"] == [95.0, 100.0]
