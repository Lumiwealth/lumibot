"""Public provider history checks: no account credentials and no trading writes."""

import pandas as pd
import pytest

from lumibot.data_sources import KalshiData
from lumibot.entities import Asset
from lumibot.tools.kalshi_client import KalshiClient

pytestmark = [pytest.mark.apitest, pytest.mark.kalshi, pytest.mark.public_http]


@pytest.mark.parametrize("demo", [True, False], ids=["demo", "public-production"])
@pytest.mark.parametrize("archive", [False, True], ids=["current", "archive"])
def test_populated_public_candles_and_selected_outcomes(demo, archive):
    with KalshiClient(
        {
            "IS_DEMO": demo,
            "BASE_URL": KalshiClient.DEMO_URL if demo else KalshiClient.PRODUCTION_URL,
            "API_KEY_ID": None,
            "PRIVATE_KEY": None,
            "PRIVATE_KEY_PATH": None,
        }
    ) as client:
        params = {"series_ticker": "KXHIGHNY", "limit": 20}
        if not archive:
            params["status"] = "open"
        rows = client.request(
            "GET", "/historical/markets" if archive else "/markets", params=params, authenticated=False
        )["markets"]
        market = max(rows, key=lambda row: float(row.get("volume_fp") or 0))
        end = pd.Timestamp(market["close_time"]) if archive else pd.Timestamp.now(tz="UTC")
        source = KalshiData(client=client)
        try:
            frames = []
            for side in ("yes", "no"):
                asset = Asset(market["ticker"], "prediction_contract", outcome=side)
                frame = source.get_historical_prices(asset, None, "hour", start=end - pd.Timedelta(days=7), end=end).df
                assert not frame.empty and frame["close"].notna().any()
                assert frame.index.max() <= end
                assert frame.index.is_unique
                frames.append(frame)
            pd.testing.assert_series_equal(
                frames[1]["high"], 1 - frames[0]["low"], check_names=False, check_exact=False
            )
        finally:
            source.shutdown()
