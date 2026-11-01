"""Native EVM exact-input, allowance and managed-transaction recovery contracts."""

import time
from decimal import Decimal, Inexact, localcontext
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
        "eth_blockNumber": "0x10",
        "eth_getCode": "0x",
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


def test_account_and_quote_prices_ignore_strategy_decimal_context(wallet):
    native, token = TokenAsset.parse(SOURCE), TokenAsset.parse(DEST)
    wallet._proxy = Mock(return_value={native.caip: {"usd": "2000.25"}, token.caip: {"usd": "0.5"}})
    wallet._balance = Mock(return_value=Decimal("1.0000001"))
    with localcontext() as context:
        context.prec = 6
        context.Emax = 2
        context.traps[Inexact] = True
        assert wallet.price(native, token) == Decimal("4000.5")
        account = wallet.account()
    assert account["equity"] == "2000.750200075"
    assert account["positions"][0]["marketValue"] == "2000.250200025"


def test_managed_swap_cancel_cannot_hide_pending_provider_execution(wallet):
    from lumibot.tools.metamask_client import MetaMaskClient

    wallet._proxy = Mock(return_value=quote())
    wallet._rpc = Mock(side_effect=rpc)
    wallet.price = Mock(return_value=Decimal("2000"))
    wallet._balance = Mock(return_value=Decimal("1"))
    wallet.provider.submit_transaction.side_effect = lambda request_id, tx, **kw: {
        "requestId": request_id,
        "status": "AWAITING_MFA",
        "tx": tx,
    }
    assert wallet.submit(intent())["state"] == "awaiting_approval"
    client = MetaMaskClient({"NETWORK": "mainnet", "AGENT_ID": "agent"})
    client._journal, client._evm = wallet.journal, wallet
    client._engine = Mock()
    with pytest.raises(MetaMaskError, match="SWAP_CANCELLATION_UNSUPPORTED"):
        client.cancel("swap-one")
    client._engine.cancel.assert_not_called()
    effect = wallet.journal.effect("agent", "swap-one", "swap-0")
    wallet.provider.transaction.return_value = {
        "requestId": effect["requestId"],
        "status": "BROADCASTED",
        "tx": effect["transaction"],
        "txHash": "0x" + "a" * 64,
    }
    wallet._receipt = Mock(return_value=None)
    assert wallet.order("swap-one")["state"] == "submitted"
    assert wallet.provider.submit_transaction.call_count == 1


def test_reverted_swap_keeps_gas_and_daily_reservation(wallet):
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
    wallet._receipt = Mock(return_value={"status": "0x0", "gasUsed": "0x5208", "effectiveGasPrice": "0x2"})
    wallet.journal.bind({**wallet.journal.binding("agent"), "maxDailyUsd": "2.0000000003"})
    result = wallet.submit(intent())
    assert result["state"] == "rejected"
    assert result["fees"] == [{"asset": "native", "chainId": 1, "atomicAmount": "42000"}]
    with pytest.raises(MetaMaskError, match="DAILY_LIMIT"):
        wallet.submit({**intent(), "id": "swap-two"})
    assert wallet.provider.submit_transaction.call_count == 1


@pytest.mark.parametrize("method", ["eth_estimateGas", "eth_getTransactionCount"])
def test_revocation_during_rpc_preparation_prevents_managed_submission(wallet, method):
    wallet._proxy = Mock(return_value=quote())
    wallet.price = Mock(return_value=Decimal("2000"))
    wallet._balance = Mock(return_value=Decimal("1"))

    def revoke(chain, name, params):
        if name == method:
            wallet.journal.bind({**wallet.journal.binding("agent"), "enabled": False})
        return rpc(chain, name, params)

    wallet._rpc = Mock(side_effect=revoke)
    result = wallet.submit(intent())
    assert result["state"] != "filled"
    wallet.provider.submit_transaction.assert_not_called()


def test_input_precision_supports_registered_36_decimal_tokens():
    from lumibot.tools.metamask.models import SwapIntent

    value = {**intent(), "input": {**SOURCE, "decimals": 36}}
    assert SwapIntent.parse(value).amount == Decimal("0.001")
    assert SwapIntent.parse({**value, "inputAmount": "0.123456789012345678901234567890123456"}).amount == Decimal(
        "0.123456789012345678901234567890123456"
    )
    with pytest.raises(MetaMaskError, match="PRECISION"):
        SwapIntent.parse({**value, "inputAmount": "0.0000000000000000000000000000000000001"})


def test_atomic_roundtrip_ignores_strategy_decimal_context():
    asset = TokenAsset.parse({**DEST, "decimals": 36})
    atomic = 123456789012345678901234567890123456789
    with localcontext() as context:
        context.prec = 6
        amount = asset.from_atomic(atomic)
        assert amount == Decimal("123.456789012345678901234567890123456789")
        assert asset.to_atomic(amount) == atomic
        assert asset.to_atomic(asset.from_atomic(2**256 - 1)) == 2**256 - 1


def test_large_receipt_output_one_atomic_below_minimum_never_rounds_into_fill(wallet):
    destination = {**DEST, "decimals": 36}
    wallet.journal.bind({**wallet.journal.binding("agent"), "assets": [SOURCE, destination]})
    minimum = 123456789012345678901234567890123456
    rows = quote()
    rows[0]["quote"].update(destAsset=destination, minDestTokenAmount=str(minimum), destTokenAmount=str(minimum + 1))
    wallet._proxy = Mock(return_value=rows)
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
                    "data": hex(minimum - 1),
                }
            ],
        }
    )
    with localcontext() as context:
        context.prec = 6
        context.Emax = 2
        context.traps[Inexact] = True
        result = wallet.submit({**intent(), "output": destination})
    assert result["state"] == "unknown" and result["filledQuantity"] == "0"
    assert "minimum" in result["message"]


def test_rollup_fee_overrun_preserves_fill_and_requires_budget_review(wallet):
    from eth_utils import keccak

    l1_selector = "0x" + keccak(text="getL1FeeUpperBound(uint256)")[:4].hex()

    def op_rpc(chain, method, params):
        if method == "eth_getCode":
            return "0x6000"
        if method == "eth_call":
            return "0x" + format(10 if params[0]["data"].startswith(l1_selector) else 0, "064x")
        return rpc(chain, method, params)

    wallet._proxy = Mock(return_value=quote())
    wallet._rpc = Mock(side_effect=op_rpc)
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
            "blockNumber": "0xf",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x2",
            "l1Fee": hex(10**9),
            "daFootprintGasScalar": "0x190",
            "blobGasUsed": "0x9c40",
            "logs": [
                {
                    "address": TOKEN,
                    "topics": [_TRANSFER, "0x" + TARGET[2:].rjust(64, "0"), "0x" + WALLET[2:].rjust(64, "0")],
                    "data": hex(999000),
                }
            ],
        }
    )
    with localcontext() as context:
        context.prec = 6
        context.Emax = 2
        context.traps[Inexact] = True
        result = wallet.submit(intent())
    assert result["state"] == "filled" and result["outputAmount"] == "0.999000"
    assert result["budgetReviewRequired"] is True
    assert result["feeBudgetOverrun"] == {
        "chainId": 1,
        "reservedAtomic": "150010",
        "actualAtomic": "1000042000",
    }
    assert result["fees"][0]["atomicAmount"] == "1000042000"
    assert Decimal(result["feeAdjustedReservationUsd"]) == Decimal("2.000002000084")
    historical = [
        call for call in wallet._rpc.call_args_list if call.args[1] == "eth_call" and call.args[2][1] == "0xf"
    ]
    assert len(historical) == 1
    with pytest.raises(MetaMaskError, match="FEE_BUDGET_REVIEW_REQUIRED"):
        wallet.submit({**intent(), "id": "after-overrun"})
    assert wallet.provider.submit_transaction.call_count == 1


def test_rising_rollup_fee_estimate_prevents_submission(wallet):
    from eth_utils import keccak

    selector = "0x" + keccak(text="getL1FeeUpperBound(uint256)")[:4].hex()
    calls = 0

    def op_rpc(chain, method, params):
        nonlocal calls
        if method == "eth_getCode":
            return "0x6000"
        if method == "eth_call":
            if params[0]["data"].startswith(selector):
                calls += 1
                return "0x" + format(calls * 100, "064x")
            return "0x" + "0" * 64
        return rpc(chain, method, params)

    wallet._proxy = Mock(return_value=quote())
    wallet._rpc = Mock(side_effect=op_rpc)
    wallet.price = Mock(return_value=Decimal("2000"))
    wallet._balance = Mock(return_value=Decimal("1"))
    result = wallet.submit(intent())
    assert result["state"] == "rejected" and result["lastErrorCode"] == "REQUOTE_REQUIRED"
    wallet.provider.submit_transaction.assert_not_called()


@pytest.mark.parametrize("failure", ["missing_l1_fee", "historical_oracle_unavailable"])
def test_missing_rollup_fee_evidence_keeps_reconciliation_hold_without_replay(wallet, failure):
    def op_rpc(chain, method, params):
        if method == "eth_getCode":
            return "0x6000"
        if method == "eth_call":
            if params[1] == "0xf":
                raise MetaMaskError("RPC_UNAVAILABLE", "Historical oracle unavailable")
            return "0x" + "0" * 64
        return rpc(chain, method, params)

    wallet._proxy = Mock(return_value=quote())
    wallet._rpc = Mock(side_effect=op_rpc)
    wallet.price = Mock(return_value=Decimal("2000"))
    wallet._balance = Mock(return_value=Decimal("1"))

    def job(request_id, tx, **kw):
        return {"requestId": request_id, "status": "CONFIRMED", "tx": tx, "txHash": "0x" + "a" * 64}

    wallet.provider.submit_transaction.side_effect = job
    wallet.provider.transaction.side_effect = job
    receipt = {"status": "0x1", "blockNumber": "0xf", "gasUsed": "0x5208", "effectiveGasPrice": "0x2"}
    if failure == "historical_oracle_unavailable":
        receipt["l1Fee"] = "0x1"
    wallet._receipt = Mock(return_value=receipt)
    result = wallet.submit(intent())
    assert result["state"] == "unknown" and result["hashes"] == ["0x" + "a" * 64]
    assert result["fees"] == []
    with pytest.raises(MetaMaskError, match="RECONCILIATION_REQUIRED"):
        wallet.submit({**intent(), "id": "after-unknown-fees"})
    assert wallet.order("swap-one")["state"] == "unknown"
    assert wallet.provider.submit_transaction.call_count == 1


def test_historical_candles_reject_false_quote_currency_before_read():
    from lumibot.data_sources.metamask_data import MetaMaskData
    from lumibot.entities import Asset

    client = Mock()
    data = MetaMaskData(client=client)
    with pytest.raises(ValueError, match="denominated in USDC"):
        data.get_historical_prices(Asset("BTC", asset_type="crypto_future"), 2, quote=Asset("BTC", asset_type="crypto"))
    client.candles.assert_not_called()


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
