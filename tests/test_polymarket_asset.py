import pytest

from lumibot.entities import Asset


def test_prediction_contract_asset_type_creation():
    asset = Asset("1234567890", asset_type=Asset.AssetType.PREDICTION_CONTRACT, precision="0.000001")

    assert asset.symbol == "1234567890"
    assert asset.asset_type == Asset.AssetType.PREDICTION_CONTRACT
    assert asset.precision == "0.000001"


def test_prediction_contract_asset_type_enum_value():
    assert Asset.AssetType.PREDICTION_CONTRACT == "prediction_contract"


def test_invalid_asset_type_still_rejected():
    with pytest.raises(Exception):
        Asset("ABC", asset_type="not_prediction_contract")
