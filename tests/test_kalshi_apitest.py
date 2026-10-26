"""Read-only provider checks using the normal adapters and dedicated demo keys."""

import os

import pytest

from lumibot.brokers import Kalshi
from lumibot.data_sources import KalshiData
from lumibot.entities import Asset
from lumibot.tools.kalshi_client import KalshiClient

pytestmark = [pytest.mark.apitest, pytest.mark.kalshi]


@pytest.mark.public_http
def test_kalshi_demo_public_connectivity_and_market_metadata():
    with KalshiClient(
        {"IS_DEMO": True, "API_KEY_ID": None, "PRIVATE_KEY": None, "PRIVATE_KEY_PATH": None, "BASE_URL": None}
    ) as client:
        status = client.request("GET", "/exchange/status", authenticated=False)
        assert isinstance(status["exchange_active"], bool)
        payload = client.request("GET", "/markets", params={"limit": 1}, authenticated=False)
        assert isinstance(payload["markets"], list)
        if payload["markets"]:
            ticker = payload["markets"][0]["ticker"]
            source = KalshiData(client=client)
            try:
                assert source.get_market_metadata(ticker)["ticker"] == ticker
            finally:
                source.shutdown()


def test_kalshi_demo_authenticated_balance_snapshot():
    key_id = os.environ.get("KALSHI_API_KEY_ID")
    key, path = os.environ.get("KALSHI_PRIVATE_KEY"), os.environ.get("KALSHI_PRIVATE_KEY_PATH")
    if not key_id or not (key or path):
        pytest.skip("Dedicated Kalshi demo API key ID and private key are not configured")
    if os.environ.get("KALSHI_IS_DEMO", "true").lower() not in {"true", "1"}:
        pytest.skip("This integration test reads the demo environment only")
    broker = Kalshi(
        {
            "API_KEY_ID": key_id,
            "PRIVATE_KEY": key,
            "PRIVATE_KEY_PATH": path,
            "IS_DEMO": True,
            "BASE_URL": None,
            "SUBACCOUNT": os.environ.get("KALSHI_SUBACCOUNT"),
            "EXCHANGE_INDEX": os.environ.get("KALSHI_EXCHANGE_INDEX"),
        }
    )
    try:
        cash, holdings, total = broker._get_balances_at_broker(Asset("USD", asset_type="forex"), None)
        assert all(isinstance(value, float) for value in (cash, holdings, total))
        assert total == pytest.approx(cash + holdings)
        assert isinstance(broker.balance_updated_ts, int)
        # Independent flat/open mark and collateral reconciliation remains a
        # separate account acceptance check; algebra alone cannot prove it.
    finally:
        broker.close()
