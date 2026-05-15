from __future__ import annotations

from datetime import date, datetime

from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset


class _Strategy:
    def __init__(self):
        self.parameters = {}

    def log_message(self, *_args, **_kwargs):
        return None

    def get_datetime(self):
        return datetime(2026, 1, 15, 9, 30)

    def get_chains(self, _underlying_asset):
        return {
            "UnderlyingSymbol": "TEST",
            "Chains": {
                "CALL": {"2026-02-20": [90.0, 95.0, 100.0]},
                "PUT": {"2026-02-20": [90.0, 95.0, 100.0]},
            },
        }


def test_find_strike_for_delta_normalizes_positive_put_delta():
    strategy = _Strategy()
    helper = OptionsHelper(strategy)

    delta_map = {90.0: -0.2, 95.0: -0.3, 100.0: -0.5}

    def fake_get_delta(_underlying_asset, _underlying_price, strike, _expiry, _right):
        return delta_map[float(strike)]

    helper.get_delta_for_strike = fake_get_delta  # type: ignore[assignment]

    underlying = Asset("TEST", asset_type=Asset.AssetType.STOCK)
    expiry = date(2026, 2, 20)

    strike_positive_input = helper.find_strike_for_delta(
        underlying_asset=underlying,
        underlying_price=100.0,
        target_delta=0.30,
        expiry=expiry,
        right="put",
    )
    strike_negative_input = helper.find_strike_for_delta(
        underlying_asset=underlying,
        underlying_price=100.0,
        target_delta=-0.30,
        expiry=expiry,
        right="put",
    )

    assert strike_positive_input == 95.0
    assert strike_negative_input == 95.0


def test_find_strike_for_delta_uses_model_path_for_theta_daily_backtests():
    class ThetaDataBacktestingPandas:
        _timestep = "day"

    class _Broker:
        IS_BACKTESTING_BROKER = True

        def __init__(self):
            source = ThetaDataBacktestingPandas()
            self.option_source = source
            self.data_source = source

    strategy = _Strategy()
    strategy.sleeptime = "1D"
    strategy.broker = _Broker()
    helper = OptionsHelper(strategy)

    def should_not_call_delta(*_args, **_kwargs):
        raise AssertionError("daily theta model path should not call get_delta_for_strike")

    helper.get_delta_for_strike = should_not_call_delta  # type: ignore[assignment]

    strike = helper.find_strike_for_delta(
        underlying_asset=Asset("TEST", asset_type=Asset.AssetType.STOCK),
        underlying_price=100.0,
        target_delta=0.30,
        expiry=date(2026, 2, 20),
        right="put",
    )

    assert strike in {90.0, 95.0, 100.0}


def test_find_strike_for_delta_uses_model_path_for_routed_theta_options_daily_backtests():
    class RoutedBacktestingPandas:
        _timestep = "day"
        _routing = {"default": "ibkr", "option": "thetadata"}

    class _Broker:
        IS_BACKTESTING_BROKER = True

        def __init__(self):
            source = RoutedBacktestingPandas()
            self.option_source = source
            self.data_source = source

    strategy = _Strategy()
    strategy.sleeptime = "1D"
    strategy.broker = _Broker()
    helper = OptionsHelper(strategy)

    def should_not_call_delta(*_args, **_kwargs):
        raise AssertionError("daily routed-theta model path should not call get_delta_for_strike")

    helper.get_delta_for_strike = should_not_call_delta  # type: ignore[assignment]

    strike = helper.find_strike_for_delta(
        underlying_asset=Asset("TEST", asset_type=Asset.AssetType.STOCK),
        underlying_price=100.0,
        target_delta=0.30,
        expiry=date(2026, 2, 20),
        right="put",
    )

    assert strike in {90.0, 95.0, 100.0}


def test_find_strike_for_delta_uses_model_path_for_theta_intraday_backtests():
    class ThetaDataBacktestingPandas:
        _timestep = "minute"

    class _Broker:
        IS_BACKTESTING_BROKER = True

        def __init__(self):
            source = ThetaDataBacktestingPandas()
            self.option_source = source
            self.data_source = source

    strategy = _Strategy()
    strategy.sleeptime = "1H"
    strategy.broker = _Broker()
    helper = OptionsHelper(strategy)

    def should_not_call_delta(*_args, **_kwargs):
        raise AssertionError("theta backtest model path should not call get_delta_for_strike")

    helper.get_delta_for_strike = should_not_call_delta  # type: ignore[assignment]

    strike = helper.find_strike_for_delta(
        underlying_asset=Asset("TEST", asset_type=Asset.AssetType.STOCK),
        underlying_price=100.0,
        target_delta=0.30,
        expiry=date(2026, 2, 20),
        right="put",
    )

    assert strike in {90.0, 95.0, 100.0}
