"""Opt-in connected testnet checks through the real Python broker and local service.

METAMASK_TEST_CONFIG_FILE points to a private JSON list of setup responses
(agent, token, endpoint, wallet, network). No credentials belong in fixtures.
These tests read provider state and send only requests denied before signing.
"""

import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

from lumibot.brokers import MetaMask
from lumibot.entities import Asset
from lumibot.tools.metamask_client import MetaMaskAPIError, MetaMaskClient

pytestmark = [pytest.mark.apitest, pytest.mark.metamask]


@pytest.fixture(scope="module")
def connected():
    path = os.environ.get("METAMASK_TEST_CONFIG_FILE")
    if not path:
        pytest.skip("Private MetaMask testnet configuration is not supplied")
    rows = json.loads(Path(path).read_text())
    assert len(rows) >= 2, "Two dedicated testnet agents are required for isolation checks"
    assert all(row["network"] == "testnet" for row in rows), "Mainnet test configuration is forbidden"
    assert len({row["wallet"] for row in rows}) == len(rows)
    brokers = [
        MetaMask(
            {
                "BROKER_URL": row["endpoint"],
                "BROKER_TOKEN": row["token"],
                "WALLET_ADDRESS": row["wallet"],
                "NETWORK": "testnet",
                "MODE": "perp",
            },
            connect_stream=False,
            start_orders_thread=False,
        )
        for row in rows
    ]
    yield rows, brokers
    for broker in brokers:
        broker.client.close()
        broker.data_source.client.close()


def test_connected_identity_and_wallet_isolation(connected):
    rows, brokers = connected
    for row, broker in zip(rows, brokers):
        identity = broker.client.verify()
        assert identity["agent"] == row["agent"]
        assert identity["wallet"] == row["wallet"]
        assert identity["network"] == "testnet"
        with pytest.raises(MetaMaskAPIError, match="UNAUTHORIZED"):
            broker.client.request("GET", "/admin/status")
    mismatched = MetaMaskClient(
        {
            "BROKER_URL": rows[0]["endpoint"],
            "BROKER_TOKEN": rows[0]["token"],
            "WALLET_ADDRESS": rows[1]["wallet"],
            "NETWORK": "testnet",
        }
    )
    try:
        with pytest.raises(MetaMaskAPIError, match="IDENTITY_MISMATCH"):
            mismatched.verify()
    finally:
        mismatched.close()


def test_connected_perpetual_and_spot_account_reads(connected):
    _, brokers = connected
    for broker in brokers:
        cash, holdings, equity = broker._get_balances_at_broker(Asset("USDC", asset_type="crypto"), None)
        assert equity == pytest.approx(cash + holdings)
        assert isinstance(broker._pull_positions("connected-test"), list)
        spot = broker.client.call("GET", "/v1/account", params={"kind": "spot"})
        assert spot["currency"] == "USD"
        assert Decimal(spot["equity"]) == Decimal(spot["cash"]) + Decimal(spot["positionsValue"])


def test_connected_market_data_and_closed_candles(connected):
    _, brokers = connected
    source = brokers[0].data_source
    asset = Asset("BTC", asset_type="crypto_future")
    assert source.get_last_price(asset) > 0
    bars = source.get_historical_prices(asset, 3, "minute")
    assert len(bars.df) == 3
    assert bars.df.index.is_monotonic_increasing
    assert (bars.df["high"] >= bars.df["low"]).all()
    assert (bars.df["close"] > 0).all()


def test_connected_scope_denials_do_not_create_operations(connected):
    _, brokers = connected
    for broker in brokers:
        before = broker.client.call("GET", "/v1/orders")
        request = {
            "kind": "perp",
            "id": "scope-denial-test",
            "symbol": "OUTSIDE_SCOPE",
            "side": "buy",
            "quantity": "1",
            "orderType": "market",
        }
        with pytest.raises(MetaMaskAPIError, match="MARKET_DENIED"):
            broker.client.call("POST", "/v1/orders", payload=request)
        with pytest.raises(MetaMaskAPIError, match="INVALID_INPUT"):
            broker.client.call("POST", "/v1/orders", payload={"kind": "rawTransaction"})
        assert broker.client.call("GET", "/v1/orders") == before
