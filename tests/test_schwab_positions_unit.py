from types import SimpleNamespace

from lumibot.brokers.schwab import Schwab
from lumibot.entities import Asset


class _Response:
    status_code = 200
    text = "OK"

    def json(self):
        return {
            "securitiesAccount": {
                "positions": [
                    {
                        "instrument": {
                            "assetType": "MUTUAL_FUND",
                            "symbol": "SWVXX",
                        },
                        "longQuantity": 10,
                        "shortQuantity": 0,
                        "averagePrice": 1.0,
                        "marketValue": 10.0,
                    },
                    {
                        "instrument": {
                            "assetType": "EQUITY",
                            "symbol": "SPY",
                        },
                        "longQuantity": 3,
                        "shortQuantity": 0,
                        "averagePrice": 500.0,
                        "marketValue": 1500.0,
                    },
                ],
            },
        }


class _Client:
    Account = SimpleNamespace(Fields=SimpleNamespace(POSITIONS="positions"))

    def get_account(self, hash_value, fields):
        return _Response()


def test_schwab_pull_positions_skips_unsupported_mutual_funds():
    broker = Schwab.__new__(Schwab)
    broker._broker_fully_ready = True
    broker.schwab_authorization_error = False
    broker.client = _Client()
    broker.hash_value = "account-hash"

    positions = broker._pull_positions(SimpleNamespace(name="unit-test"))

    assert len(positions) == 1
    assert positions[0].asset.symbol == "SPY"
    assert positions[0].asset.asset_type == Asset.AssetType.STOCK
    assert positions[0].quantity == 3
