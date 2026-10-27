"""Provider history pagination uses observed timestamps, never fixed intervals."""

from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from lumibot.data_sources.ccxt_backtesting_data import CcxtBacktestingData
from tests.test_ccxt_derivatives import derivatives
from tests.test_ccxt_unified_spot import make_broker


@pytest.mark.parametrize("symbol,native", [("BTC/USDT:USDT", ".BTCUSDTFR8H"),
                                          ("BTC/USDC:USDC", ".BTCUSDCFR8H"), ("BTC/USD:BTC", ".BTCFR8H")])
def test_phemex_funding_uses_working_v2_route_for_every_family(derivatives, symbol, native):
    source = CcxtBacktestingData.__new__(CcxtBacktestingData)
    source.name = "phemex"
    source.datetime_start = pd.Timestamp("2026-10-26", tz="UTC")
    source.datetime_end = source.datetime_start + pd.Timedelta(days=1)
    # Shape copied from the real public V2 response observed October 26, 2026.
    row = {"symbol": native, "fundingRate": "0.0001", "fundingTime": 1793001600000, "intervalSeconds": 28800}
    api = derivatives.api
    for market in api.markets.values():
        market.setdefault("baseId", market.get("base"))
    api.markets[symbol]["info"].update(fundingRateSymbol=native.replace("FR8H", "FR"), fundingRate8hSymbol=native)
    api.has["fetchFundingRateHistory"] = True
    api.v2GetApiDataPublicDataFundingRateHistory = Mock(return_value={"code": 0, "msg": "OK", "data": {"rows": [row]}})
    source.cache_db = SimpleNamespace(api=api)
    asset = derivatives.get_asset(symbol)
    rows = source.get_funding_events(asset, source.datetime_start, source.datetime_end)
    assert rows[0]["fundingRate"] == "0.0001"
    assert api.v2GetApiDataPublicDataFundingRateHistory.call_args.args[0]["symbol"] == native
    assert rows[0]["timestamp"] == pd.Timestamp("2026-10-26 08:00", tz="UTC")
    source.get_funding_events(asset, source.datetime_start, source.datetime_end)
    assert api.v2GetApiDataPublicDataFundingRateHistory.call_count == 1


def test_ccxt_funding_pagination_uses_last_observed_timestamp(derivatives):
    source = CcxtBacktestingData.__new__(CcxtBacktestingData)
    source.name = "other"
    source.datetime_start = pd.Timestamp("2026-01-01", tz="UTC")
    source.datetime_end = source.datetime_start + pd.Timedelta(days=5)
    start = int(source.datetime_start.timestamp()*1000)
    pages = [[{"timestamp": start+i*3600000, "fundingRate": ".001"} for i in range(100)],
             [{"timestamp": start+100*3600000, "fundingRate": ".002"}]]
    api = derivatives.api
    api.has["fetchFundingRateHistory"] = True
    api.fetch_funding_rate_history = Mock(side_effect=pages)
    source.cache_db = SimpleNamespace(api=api)
    asset = derivatives.get_asset("BTC/USDT:USDT")
    rows = source.get_funding_events(asset, source.datetime_start, source.datetime_end)
    assert len(rows) == 101
    assert api.fetch_funding_rate_history.call_args_list[1].kwargs["since"] == start+99*3600000+1
    assert "paginate" not in api.fetch_funding_rate_history.call_args.kwargs["params"]
