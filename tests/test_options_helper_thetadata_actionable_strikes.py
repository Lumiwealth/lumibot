import datetime
from types import SimpleNamespace

from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset


class _DummyThetaDataBacktestingPandas:
    """Named to match the real ThetaData backtesting data source class."""

    __name__ = "ThetaDataBacktestingPandas"


class _DummyBroker:
    def __init__(self):
        # Instance whose class name matches what OptionsHelper gates on.
        class ThetaDataBacktestingPandas:  # noqa: N801 - intentional for __name__ match
            pass

        self.data_source = ThetaDataBacktestingPandas()


class _StubChains:
    def __init__(self, strikes):
        self._strikes = strikes

    def strikes(self, expiry, right):
        return list(self._strikes)


class _StubStrategy:
    def __init__(self, strikes, *, max_spread_pct=None):
        self.is_backtesting = True
        self.broker = _DummyBroker()
        self.parameters = {}
        if max_spread_pct is not None:
            self.parameters["max_spread_pct"] = max_spread_pct
        self._chains = _StubChains(strikes)

    def log_message(self, *args, **kwargs):
        return None

    def get_chains(self, underlying_asset):
        return self._chains

    def get_quote(self, option_asset):
        if option_asset.strike == 39.0:
            return SimpleNamespace(bid=0.0, ask=0.5)
        if option_asset.strike == 38.0:
            return SimpleNamespace(bid=0.05, ask=0.25)
        if option_asset.strike == 37.0:
            return SimpleNamespace(bid=0.15, ask=0.25)
        return None

    def get_last_price(self, option_asset):
        return None


def test_find_next_valid_option_prefers_actionable_two_sided_quotes_in_thetadata_backtests():
    strategy = _StubStrategy(strikes=[38.0, 39.0])
    helper = OptionsHelper(strategy)

    underlying = Asset("MFC", asset_type=Asset.AssetType.STOCK)
    expiry = datetime.date(2026, 3, 20)

    option = helper.find_next_valid_option(
        underlying_asset=underlying,
        rounded_underlying_price=39.0,
        expiry=expiry,
        put_or_call="call",
    )

    assert option is not None
    assert option.strike == 38.0


def test_find_next_valid_option_respects_strategy_max_spread_pct_when_present():
    strategy = _StubStrategy(strikes=[37.0, 38.0, 39.0], max_spread_pct=0.6)
    helper = OptionsHelper(strategy)

    underlying = Asset("MFC", asset_type=Asset.AssetType.STOCK)
    expiry = datetime.date(2026, 3, 20)

    option = helper.find_next_valid_option(
        underlying_asset=underlying,
        rounded_underlying_price=39.0,
        expiry=expiry,
        put_or_call="call",
    )

    assert option is not None
    assert option.strike == 37.0
