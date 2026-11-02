"""Rollup fee estimates and actual costs retain their block and exact atomic units."""

from unittest.mock import Mock

import pytest
import rlp
from eth_utils import keccak

from lumibot.tools.metamask.fees import ORACLE, estimate_fees, receipt_fees
from lumibot.tools.metamask.provider import MetaMaskError

TX = {
    "chainId": 10,
    "to": "0x" + "1" * 40,
    "data": "0x1234",
    "value": "0x0",
    "gasLimit": "0x186a0",
    "maxFeePerGas": "0x5",
    "maxPriorityFeePerGas": "0x1",
}


def word(value):
    return "0x" + format(value, "064x")


def test_op_estimate_uses_unsigned_size_once_and_pins_both_oracle_calls():
    def response(chain, method, params):
        if method == "eth_blockNumber":
            return "0x10"
        if method == "eth_getCode":
            assert params == [ORACLE, "0x10"]
            return "0x6000"
        assert method == "eth_call" and params[1] == "0x10"
        data = params[0]["data"]
        if data.startswith("0x" + keccak(text="getL1FeeUpperBound(uint256)")[:4].hex()):
            expected = b"\x02" + rlp.encode([10, 2**64 - 1, 1, 5, 100000, bytes.fromhex("1" * 40), 0, b"\x12\x34", []])
            assert int(data[10:], 16) == len(expected)
            return word(123)
        assert int(data[10:], 16) == 100000
        return word(456)

    result = estimate_fees(response, TX)
    assert result == {
        "model": "op-stack",
        "blockNumber": "0x10",
        "executionGasCeiling": "500000",
        "l1DataFeeEstimate": "123",
        "operatorFeeEstimate": "456",
        "totalEstimate": "500579",
    }


def test_non_op_chain_keeps_eip1559_fee_behavior():
    rpc = Mock(side_effect=["0x10", "0x"])
    result = estimate_fees(rpc, TX)
    assert result["model"] == "evm" and result["totalEstimate"] == "500000"
    assert rpc.call_count == 2


@pytest.mark.parametrize("result", ["0x", "0x1", "0x" + "0" * 65, None])
def test_present_oracle_with_malformed_result_never_becomes_zero_fees(result):
    rpc = Mock(side_effect=["0x10", "0x6000", result])
    with pytest.raises(MetaMaskError, match="FEE_ORACLE_UNAVAILABLE"):
        estimate_fees(rpc, TX)


def test_receipt_uses_historical_operator_cost_without_double_counting_jovian_da():
    rpc = Mock(return_value=word(12345))
    receipt = {
        "blockNumber": "0x5",
        "gasUsed": "0x64",
        "effectiveGasPrice": "0x2",
        "l1Fee": "0x3",
        "daFootprintGasScalar": "0x190",
        "blobGasUsed": "0xffffff",
        "blobGasPrice": "0xffffff",
    }
    result = receipt_fees(rpc, 10, receipt, "op-stack")
    assert result["totalFee"] == str(200 + 3 + 12345)
    assert result["operatorFee"] == "12345"
    assert rpc.call_args.args[2][1] == "0x5"
    assert int(rpc.call_args.args[2][0]["data"][10:], 16) == 100


def test_known_op_receipt_requires_l1_fee_even_when_operator_is_zero():
    rpc = Mock(return_value=word(0))
    with pytest.raises(MetaMaskError, match="INVALID_RESPONSE"):
        receipt_fees(rpc, 10, {"blockNumber": "0x5", "gasUsed": "0x64", "effectiveGasPrice": "0x2"}, "op-stack")
    rpc.assert_not_called()


def test_historical_oracle_failure_never_reports_incomplete_total_as_actual():
    rpc = Mock(side_effect=MetaMaskError("RPC_UNAVAILABLE", "Historical state unavailable"))
    with pytest.raises(MetaMaskError, match="RPC_UNAVAILABLE"):
        receipt_fees(
            rpc,
            10,
            {
                "blockNumber": "0x5",
                "gasUsed": "0x64",
                "effectiveGasPrice": "0x2",
                "l1Fee": "0x3",
            },
            "op-stack",
        )


def test_legacy_plan_receipt_retains_explicit_rollup_fee_evidence():
    rpc = Mock(return_value=word(7))
    result = receipt_fees(
        rpc,
        10,
        {
            "blockNumber": "0x5",
            "gasUsed": "0x64",
            "effectiveGasPrice": "0x2",
            "l1Fee": "0x3",
        },
    )
    assert result["totalFee"] == "210"
    assert rpc.call_count == 1 and rpc.call_args.args[1] == "eth_call"
