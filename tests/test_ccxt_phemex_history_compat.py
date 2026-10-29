"""Phemex public-response shapes observed on October 26, 2026."""

from unittest.mock import Mock
from decimal import Decimal

import pytest

from lumibot.tools.ccxt_phemex import fetch_ohlcv
from tests.test_ccxt_derivatives import derivatives
from tests.test_ccxt_unified_spot import make_broker


def test_phemex_base_unit_metadata_repairs_missing_usdc_size(derivatives):
    market = derivatives.api.markets["BTC/USDC:USDC"]
    market["contractSize"] = None
    market["info"].update(type="PerpetualV2", contractUnderlyingAssets="BTC")
    asset = derivatives.get_asset(market["symbol"])
    assert asset.multiplier == 1
    assert asset.contract_spec.contract_size == 1
    assert derivatives.get_quantity_for_notional(asset, 1000, 50000) == Decimal(".02")


@pytest.mark.parametrize("symbol,scale,value_scale,row,expected", [
    ("BTC/USDT", 8, 8, [1793001480, 60, "8010756000000", "8011601000000", "8011601000000", "8007373000000", "8009034000000", "284683100", "22800375733001", "sBTCUSDT"], 80116.01),
    ("BTC/USD:BTC", 4, 8, [1793001480, 60, "799569000", "799613000", "799613000", "799249000", "799249000", "80226", "100374127", "BTCUSD"], 79961.3),
])
def test_bounded_candles_keep_since_and_native_price_scaling(derivatives, symbol, scale, value_scale, row, expected):
    api = derivatives.api
    market = api.market(symbol)
    market.update(priceScale=scale, valueScale=value_scale)
    api.publicGetMdV2KlineList = Mock(return_value={"code": 0, "msg": "OK", "data": {"rows": [row]}})
    result = fetch_ohlcv(api, symbol, "1m", since=1793001480000, limit=300)
    assert result[0][0] == 1793001480000
    assert result[0][1] == expected
    assert api.publicGetMdV2KlineList.call_args.args[0]["from"] == 1793001480
