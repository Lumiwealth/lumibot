"""Phemex public-response shapes observed on October 26, 2026."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from lumibot.backtesting.perpetuals import PerpetualAccount
from lumibot.entities import Asset
from lumibot.tools.ccxt_data_store import CcxtCacheDB
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


def _funding_cache(interval, *, exchange="phemex", timestamp_unit=None, product_type="Perpetual"):
    # Minimal normalized shape from public Phemex products. CCXT merges the
    # legacy product's hours over the newer product's seconds for inverse swaps.
    market = {"id": "BTCUSD", "symbol": "BTC/USD:BTC", "base": "BTC", "quote": "USD",
              "settle": "BTC", "type": "swap", "spot": False, "swap": True,
              "future": False, "option": False, "contract": True, "linear": False,
              "inverse": True, "contractSize": 1, "info": {"type": product_type}}
    if interval is not None:
        market["info"]["fundingInterval"] = interval
    cache = CcxtCacheDB.__new__(CcxtCacheDB)
    cache.exchange_id = exchange
    cache.api = SimpleNamespace(markets={market["symbol"]: market}, id=exchange)
    cache._funding_frames = {}
    if timestamp_unit is not None:
        # Controlled event timestamps only, not fabricated funding rates or a
        # fresh history response. Both existing timestamp scales must survive.
        scale = 1000 if timestamp_unit == "ms" else 1
        first = int(pd.Timestamp("2026-01-01", tz="UTC").timestamp()) * scale
        index = pd.to_datetime([first + i * 8 * 3600 * scale for i in range(3)],
                               unit=timestamp_unit, utc=True)
        cache._funding_frames[(market["symbol"], "2026-01")] = pd.DataFrame(
            {"fixture_marker": ["controlled timestamp"] * 3}, index=index)
    return cache


@pytest.mark.parametrize("raw,hours", [
    (8, 8), ("8", 8), (3600, 1), (7200, 2), (14400, 4), (28800, 8), ("28800", 8),
], ids=["legacy-hours", "legacy-hours-string", "new-seconds-one-hour",
        "new-seconds-two-hours", "new-seconds-four-hours", "new-seconds-eight-hours",
        "new-seconds-string"])
def test_funding_coverage_decodes_recorded_phemex_interval_units(raw, hours):
    # Legacy /v1/exchange/public/products reports 8 hours; /public/products
    # reports 3600/7200/14400/28800 seconds. These are the same field name.
    coverage = _funding_cache(raw).get_funding_coverage(Asset("BTC/USD:BTC", "crypto_future"))
    assert coverage == {"source": "ccxt:phemex", "first": None, "last": None,
                        "interval": pd.Timedelta(hours=hours)}


@pytest.mark.parametrize("exchange,raw,unit", [
    ("phemex", None, "ms"), ("phemex", None, "s"), ("other", 8, "ms"),
    ("phemex", None, None), ("other", 8, None),
], ids=["phemex-observed-milliseconds", "phemex-observed-seconds", "other-observed-milliseconds",
        "phemex-unknown", "other-unknown"])
def test_funding_coverage_preserves_observed_timestamp_units_and_unknown(exchange, raw, unit):
    coverage = _funding_cache(raw, exchange=exchange, timestamp_unit=unit).get_funding_coverage(
        Asset("BTC/USD:BTC", "crypto_future"))
    expected = pd.Timestamp("2026-01-01", tz="UTC") if unit is not None else None
    assert coverage["first"] == expected
    assert coverage["last"] == (expected + pd.Timedelta(hours=16) if expected is not None else None)
    assert coverage["interval"] == (pd.Timedelta(hours=8) if unit is not None else None)
    assert coverage["source"] == f"ccxt:{exchange}"


def test_newer_phemex_product_preserves_seconds_even_for_a_small_interval():
    # A synthetic short cadence checks units, not a claim that this product is
    # currently listed. The legacy hours decoder must not change V2 seconds.
    coverage = _funding_cache(8, product_type="PerpetualV2").get_funding_coverage(
        Asset("BTC/USD:BTC", "crypto_future"))
    assert coverage["interval"] == pd.Timedelta(seconds=8)


def test_legacy_phemex_funding_interval_does_not_invent_a_gap():
    source = _funding_cache(8, timestamp_unit="ms")
    broker = SimpleNamespace(data_source=source, logger=Mock())
    account = PerpetualAccount(broker)
    asset = Asset("BTC/USD:BTC", "crypto_future")
    start = pd.Timestamp("2026-01-01", tz="UTC")
    last = start + pd.Timedelta(hours=16)
    account.report_funding_gaps(asset, start, last + pd.Timedelta(hours=4))
    assert broker.perpetual_funding_unavailable == []
    assert broker.backtest_assumptions == []
    # A genuinely unobserved tail must still be disclosed, not suppressed by
    # fixing the units or filled with synthetic zero funding.
    end = last + pd.Timedelta(hours=12)
    account.report_funding_gaps(asset, start, end)
    assert broker.perpetual_funding_unavailable == [{
        "symbol": asset.symbol, "source": "ccxt:phemex",
        "covered_from": start.isoformat(), "covered_to": last.isoformat(),
        "not_covered": [[(last + pd.Timedelta(hours=8)).isoformat(), end.isoformat()]],
    }]
    assert len(broker.backtest_assumptions) == 1
