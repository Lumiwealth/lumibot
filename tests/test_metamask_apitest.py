"""Opt-in native Python testnet reads. No signatures or transactions are sent.

METAMASK_TEST_CONFIG_FILE selects a private JSON list of native broker config
objects. Credentials, wallet identities and state paths never belong in fixtures.
"""

import json
import os
from pathlib import Path

import pytest

from lumibot.brokers import MetaMask
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.tools.metamask_client import MetaMaskAPIError
from lumibot.traders import Trader

pytestmark = [pytest.mark.apitest, pytest.mark.metamask]


@pytest.fixture(scope="module")
def connected():
    path = os.environ.get("METAMASK_TEST_CONFIG_FILE")
    if not path:
        pytest.skip("Private native MetaMask testnet configuration is not supplied")
    configs = json.loads(Path(path).read_text())
    assert len(configs) >= 2, "Two dedicated testnet agents are required for isolation checks"
    assert all(row["NETWORK"] == "testnet" for row in configs), "Mainnet test configuration is forbidden"
    assert len({row["WALLET_ADDRESS"].lower() for row in configs}) == len(configs)
    # One journal owner can serve multiple wallet engines inside one process.
    brokers = []
    try:
        for config in configs:
            broker = MetaMask(config, connect_stream=False, start_orders_thread=False)
            if brokers and config["STATE_FILE"] == configs[0]["STATE_FILE"]:
                broker.client._journal = brokers[0].client.journal
                broker.client._tokens = brokers[0].client._tokens
                broker.client._provider = brokers[0].client.provider
            broker.client.verify()
            brokers.append(broker)
        yield configs, brokers
    finally:
        for broker in reversed(brokers):
            broker._stop_event.set()
            broker.client.close()


def test_connected_identity_and_guard_policy(connected):
    configs, brokers = connected
    for config, broker in zip(configs, brokers):
        identity = broker.client.verify()
        assert identity["agent"] == config["AGENT_ID"]
        assert identity["wallet"] == config["WALLET_ADDRESS"].lower()
        assert identity["network"] == "testnet"
        policy = broker.client.provider.policy(identity["wallet"])
        assert len(policy.digest) == 64
        assert policy.digest == broker.client.journal.binding(config["AGENT_ID"])["policyDigest"]


def test_connected_perpetual_accounts(connected):
    _, brokers = connected
    for broker in brokers:
        cash, holdings, equity = broker._get_balances_at_broker(Asset("USDC", asset_type="crypto"), None)
        assert equity == pytest.approx(cash + holdings)
        assert isinstance(broker._pull_positions("connected-test"), list)


def test_connected_market_data_and_closed_candles(connected):
    _, brokers = connected
    source = brokers[0].data_source
    asset = Asset("BTC", asset_type="crypto_future")
    assert source.get_last_price(asset) > 0
    bars = source.get_historical_prices(asset, 3, "minute")
    assert len(bars.df) == 3 and bars.df.index.is_monotonic_increasing
    assert (bars.df["high"] >= bars.df["low"]).all()
    assert (bars.df["close"] > 0).all()


def test_connected_expired_authority_never_signs(connected):
    _, brokers = connected
    import time
    from unittest.mock import patch

    for broker in brokers:
        before = broker.client.journal.operations(broker.client.config["AGENT_ID"])
        binding = broker.client.journal.binding(broker.client.config["AGENT_ID"])
        if binding["expiresAt"] > time.time() * 1000:
            continue
        with patch.object(
            broker.client.provider, "submit_signature", side_effect=AssertionError("Expired binding signed")
        ):
            with pytest.raises(MetaMaskAPIError, match="REVOKED"):
                broker.client.submit(
                    {
                        "kind": "perp",
                        "id": "native-expiry-negative-control",
                        "symbol": "BTC",
                        "side": "buy",
                        "quantity": "0.001",
                        "orderType": "market",
                    }
                )
        assert broker.client.journal.operations(broker.client.config["AGENT_ID"]) == before


def test_connected_strategy_restores_history_without_replaying_fills(connected):
    _, brokers = connected
    broker = brokers[0]
    iterations, callbacks = [], []

    class ReadOnlyStrategy(Strategy):
        def initialize(self, parameters=None):
            self.sleeptime = "1S"

        def on_trading_iteration(self):
            broker.do_polling()
            broker.do_polling()
            iterations.append(True)

        def on_filled_order(self, position, order, price, quantity, multiplier):
            callbacks.append(str(order.identifier))

    strategy = ReadOnlyStrategy(
        broker=broker,
        name="connected-history-read",
        quote_asset=Asset("USDC", asset_type="crypto"),
        benchmark_asset=None,
        analyze_backtest=False,
    )
    trader = Trader(quiet_logs=True)
    trader.add_strategy(strategy)
    trader.run_all(run_once=True, show_plot=False, show_tearsheet=False, save_tearsheet=False, show_indicators=False)
    assert iterations == [True] and callbacks == []
    from decimal import Decimal

    for order in broker.get_all_orders():
        raw = order._raw
        if raw and raw.get("averagePrice"):
            quantity = sum((Decimal(str(t.quantity)) for t in order.transactions), Decimal(0))
            assert quantity == Decimal(raw["filledQuantity"])
