"""Native EVM exact-input, allowance and managed-transaction recovery contracts."""

import time
from decimal import Decimal
from unittest.mock import Mock

import pytest
import requests

from lumibot.tools.metamask.evm import _TRANSFER, ZERO, EvmWallet
from lumibot.tools.metamask.journal import Journal
from lumibot.tools.metamask.models import TokenAsset
from lumibot.tools.metamask.provider import Credentials, GuardPolicy, MetaMaskError, MetaMaskProvider

WALLET, TOKEN, TARGET = ["0x" + digit * 40 for digit in ("1", "2", "3")]
SOURCE = {"symbol": "ETH", "chainId": 1, "address": "native", "decimals": 18}
DEST = {"symbol": "TOKEN", "chainId": 1, "address": TOKEN, "decimals": 6}


def intent():
    return {
        "kind": "swap",
        "id": "swap-one",
        "input": SOURCE,
        "output": DEST,
        "inputAmount": "0.001",
        "slippageBps": 50,
    }


@pytest.fixture
def wallet(tmp_path):
    journal = Journal(tmp_path / "broker.sqlite")
    journal.bind(
        {
            "id": "agent",
            "projectId": "project",
            "wallet": WALLET,
            "network": "mainnet",
            "enabled": True,
            "expiresAt": int(time.time() * 1000) + 86400000,
            "policyDigest": "a" * 64,
            "markets": ["BTC"],
            "chains": [1],
            "assets": [SOURCE, DEST],
            "maxOrderUsd": "25",
            "maxDailyUsd": "100",
            "maxLeverage": 1,
            "maxSlippageBps": 50,
        }
    )
    provider = Mock()
    provider.credentials.return_value = Credentials("project", "access")
    provider.policy.return_value = GuardPolicy("policy", "a" * 64)
    instance = EvmWallet(journal, provider, {"AGENT_ID": "agent", "NETWORK": "mainnet"})
    yield instance
    instance.close()
    journal.close()


def quote():
    return [
        {
            "quoteId": "provider-quote",
            "quote": {
                "requestId": "quote-request",
                "srcChainId": 1,
                "destChainId": 1,
                "srcAsset": {**SOURCE, "address": ZERO},
                "destAsset": DEST,
                "srcTokenAmount": "990000000000000",
                "destTokenAmount": "1000000",
                "minDestTokenAmount": "995000",
                "walletAddress": WALLET,
                "destWalletAddress": WALLET,
                "feeData": {"metabridge": {"asset": {**SOURCE, "address": ZERO}, "amount": "10000000000000"}},
            },
            "trade": {
                "from": WALLET,
                "to": TARGET,
                "chainId": 1,
                "value": hex(10**15),
                "data": "0x1234",
                "gasLimit": 50000,
            },
        }
    ]


def rpc(chain, method, params):
    return {
        "eth_estimateGas": "0x5208",
        "eth_getBlockByNumber": {"baseFeePerGas": "0x1"},
        "eth_maxPriorityFeePerGas": "0x1",
        "eth_getTransactionCount": "0x0",
    }[method]


def test_quote_binds_fee_net_input_and_uses_percentage_slippage(wallet):
    wallet._proxy = Mock(return_value=quote())
    wallet._rpc = Mock(side_effect=rpc)
    from lumibot.tools.metamask.models import SwapIntent

    binding, _ = wallet._binding()
    plan = wallet._quote(binding, SwapIntent.parse(intent()))
    assert plan["quoteId"] == "provider-quote" and plan["minimumOutput"] == "995000"
    assert wallet._proxy.call_args.args[1]["slippage"] == 0.5
    assert plan["trade"]["value"] == hex(10**15)


@pytest.mark.parametrize("mutation", ["wallet", "amount", "asset", "slippage", "value"])
def test_quote_mismatch_never_reaches_managed_signing(wallet, mutation):
    rows = quote()
    if mutation == "wallet":
        rows[0]["quote"]["destWalletAddress"] = TARGET
    if mutation == "amount":
        rows[0]["quote"]["srcTokenAmount"] = "990000000000001"
    if mutation == "asset":
        rows[0]["quote"]["destAsset"] = {**DEST, "address": TARGET}
    if mutation == "slippage":
        rows[0]["quote"]["minDestTokenAmount"] = "900000"
    if mutation == "value":
        rows[0]["trade"]["value"] = hex(2 * 10**15)
    wallet._proxy = Mock(return_value=rows)
    wallet._rpc = Mock(side_effect=rpc)
    with pytest.raises(MetaMaskError, match="NO_VERIFIABLE_ROUTE"):
        wallet.submit(intent())
    wallet.provider.submit_transaction.assert_not_called()


def test_unknown_managed_submission_polls_original_request_after_restart(wallet):
    wallet._proxy = Mock(return_value=quote())
    wallet._rpc = Mock(side_effect=rpc)
    wallet.price = Mock(return_value=Decimal("2000"))
    wallet._balance = Mock(return_value=Decimal("1"))

    def lost(request_id, tx, **kwargs):
        saved = wallet.journal.effect("agent", "swap-one", "swap-0")
        assert saved["requestId"] == request_id and saved["transaction"] == tx
        assert kwargs["quote_id"] == "provider-quote"
        raise MetaMaskError("REQUEST_OUTCOME_UNKNOWN", "Lost response")

    wallet.provider.submit_transaction.side_effect = lost
    assert wallet.submit(intent())["state"] == "unknown"
    original = wallet.journal.effect("agent", "swap-one", "swap-0")
    wallet.epoch = "restarted-owner"
    wallet.provider.transaction.return_value = {
        "requestId": original["requestId"],
        "status": "AWAITING_MFA",
        "tx": original["transaction"],
    }
    assert wallet.order("swap-one")["state"] == "awaiting_approval"
    assert wallet.provider.submit_transaction.call_count == 1
    assert wallet.provider.transaction.call_args.args[0] == original["requestId"]


def test_completed_swap_uses_actual_mined_erc20_output(wallet):
    wallet._proxy = Mock(return_value=quote())
    wallet._rpc = Mock(side_effect=rpc)
    wallet.price = Mock(return_value=Decimal("2000"))
    wallet._balance = Mock(return_value=Decimal("1"))
    wallet.provider.submit_transaction.side_effect = lambda request_id, tx, **kw: {
        "requestId": request_id,
        "status": "CONFIRMED",
        "tx": tx,
        "txHash": "0x" + "a" * 64,
    }
    wallet._receipt = Mock(
        return_value={
            "status": "0x1",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x2",
            "logs": [
                {
                    "address": TOKEN,
                    "topics": [_TRANSFER, "0x" + TARGET[2:].rjust(64, "0"), "0x" + WALLET[2:].rjust(64, "0")],
                    "data": hex(999000),
                }
            ],
        }
    )
    result = wallet.submit(intent())
    assert result["state"] == "filled" and result["outputAmount"] == "0.999000"
    assert result["outputAmount"] != "1.000000"


def test_missing_token_price_is_never_assumed_one_dollar(wallet):
    wallet._proxy = Mock(return_value={})
    with pytest.raises(MetaMaskError):
        wallet.price(TokenAsset.parse(DEST))


def test_provider_transaction_rejects_identity_change_and_does_not_broadcast_locally():
    session = Mock(spec=requests.Session)
    provider = MetaMaskProvider(Credentials("project", "access"), session=session)
    tx = {"from": WALLET, "to": TARGET, "chainId": 1, "data": "0x1234", "value": "0x0", "nonce": 0}
    response = Mock(status_code=200)
    response.json.return_value = {
        "requestId": "request",
        "status": "BROADCASTED",
        "tx": {**tx, "to": TOKEN},
        "txHash": "0x" + "a" * 64,
    }
    session.request.return_value = response
    with pytest.raises(MetaMaskError, match="JOB_CONTEXT_MISMATCH"):
        provider.submit_transaction("request", tx, quote_id="quote")
    assert session.request.call_count == 1
    assert session.request.call_args.kwargs["json"]["bridgeQuoteId"] == "quote"


@pytest.mark.parametrize(
    "broken",
    [{"status": None, "logs": []}, {"status": "0x1", "logs": None}, {"status": "0x1", "logs": [], "gasUsed": None}],
)
def test_malformed_mined_evidence_stays_unknown_without_rebroadcast(wallet, broken):
    wallet._proxy = Mock(return_value=quote())
    wallet._rpc = Mock(side_effect=rpc)
    wallet.price = Mock(return_value=Decimal("2000"))
    wallet._balance = Mock(return_value=Decimal("1"))
    wallet.provider.submit_transaction.side_effect = lambda request_id, tx, **kw: {
        "requestId": request_id,
        "status": "CONFIRMED",
        "tx": tx,
        "txHash": "0x" + "a" * 64,
    }
    wallet._receipt = Mock(return_value=broken)
    result = wallet.submit(intent())
    assert result["state"] == "unknown" and result["hashes"] == ["0x" + "a" * 64]
    wallet.provider.transaction.side_effect = MetaMaskError("NOT_FOUND", "Provider stopped tracking this job")
    wallet.order("swap-one")
    assert wallet.provider.submit_transaction.call_count == 1
