import datetime
from types import SimpleNamespace

from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset


class _StubStrategy:
    def __init__(self, *, underlying_price=60.0):
        self.parameters = {}
        self.underlying_price = underlying_price
        self.last_greeks_call = None

    def log_message(self, *args, **kwargs):
        return None

    def get_last_price(self, asset):
        if asset.asset_type == Asset.AssetType.STOCK:
            return self.underlying_price
        return None

    def get_quote(self, option_asset):
        return SimpleNamespace(bid=1.0, ask=3.0)

    def get_greeks(
        self,
        asset,
        asset_price=None,
        underlying_price=None,
        risk_free_rate=None,
        query_greeks=False,
    ):
        self.last_greeks_call = {
            "asset": asset,
            "asset_price": asset_price,
            "underlying_price": underlying_price,
        }
        return {"delta": 0.5}


def test_get_delta_for_strike_uses_quote_mid_when_last_trade_missing():
    strategy = _StubStrategy()
    helper = OptionsHelper(strategy)

    underlying = Asset("UBER", asset_type=Asset.AssetType.STOCK)
    expiry = datetime.date(2027, 6, 17)

    delta = helper.get_delta_for_strike(
        underlying_asset=underlying,
        underlying_price=60.0,
        strike=60.0,
        expiry=expiry,
        right="call",
    )

    assert delta == 0.5
    assert strategy.last_greeks_call is not None
    assert strategy.last_greeks_call["asset_price"] == 2.0
    assert strategy.last_greeks_call["underlying_price"] == 60.0


def test_get_expiration_on_or_after_date_rejects_expiry_without_strikes_near_underlying():
    strategy = _StubStrategy(underlying_price=60.0)
    helper = OptionsHelper(strategy)

    chains = {
        "UnderlyingSymbol": "UBER",
        "Chains": {
            "CALL": {"2027-06-17": [120.0, 125.0, 130.0]},
            "PUT": {"2027-06-17": [120.0, 125.0, 130.0]},
        },
    }

    expiry = helper.get_expiration_on_or_after_date(
        dt=datetime.date(2027, 6, 17),
        chains=chains,
        call_or_put="call",
        underlying_asset=Asset("UBER", asset_type=Asset.AssetType.STOCK),
        allow_prior=False,
    )

    assert expiry is None


def test_get_expiration_on_or_after_date_accepts_expiry_with_nearby_strikes():
    strategy = _StubStrategy(underlying_price=60.0)
    helper = OptionsHelper(strategy)

    chains = {
        "UnderlyingSymbol": "UBER",
        "Chains": {
            "CALL": {"2027-06-17": [50.0, 60.0, 70.0]},
            "PUT": {"2027-06-17": [50.0, 60.0, 70.0]},
        },
    }

    expiry = helper.get_expiration_on_or_after_date(
        dt=datetime.date(2027, 6, 17),
        chains=chains,
        call_or_put="call",
        underlying_asset=Asset("UBER", asset_type=Asset.AssetType.STOCK),
        allow_prior=False,
    )

    assert expiry == datetime.date(2027, 6, 17)

