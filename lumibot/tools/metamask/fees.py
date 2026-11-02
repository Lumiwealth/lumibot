"""EVM execution fees and block-pinned OP Stack oracle estimates and receipts."""

import re

import rlp
from eth_utils import keccak

from .provider import MetaMaskError

ORACLE = "0x420000000000000000000000000000000000000f"
_HEX = re.compile(r"0x(?:[0-9a-fA-F]{2})*\Z")
_WORD = re.compile(r"0x[0-9a-fA-F]{64}\Z")


def integer(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise MetaMaskError("INVALID_RESPONSE", "Expected a nonnegative integer")
    try:
        result = int(value, 16) if isinstance(value, str) and value.startswith("0x") else int(value)
    except (ValueError, TypeError):
        raise MetaMaskError("INVALID_RESPONSE", "Expected an integer chain quantity") from None
    if result < 0:
        raise MetaMaskError("INVALID_RESPONSE", "Chain quantity must be an exact nonnegative integer")
    return result


def _op_stack(rpc, chain, block):
    code = rpc(chain, "eth_getCode", [ORACLE, block])
    if not isinstance(code, str) or not _HEX.fullmatch(code):
        raise MetaMaskError("INVALID_RESPONSE", "Fee oracle bytecode response is malformed")
    return code != "0x"


def _oracle(rpc, chain, method, value, block):
    data = "0x" + keccak(text=method)[:4].hex() + format(value, "064x")
    result = rpc(chain, "eth_call", [{"to": ORACLE, "data": data}, block])
    if not isinstance(result, str) or not _WORD.fullmatch(result):
        raise MetaMaskError("FEE_ORACLE_UNAVAILABLE", "Rollup fee oracle did not return a complete uint256")
    return int(result, 16)


def estimate_fees(rpc, tx):
    chain = tx["chainId"]
    block = hex(integer(rpc(chain, "eth_blockNumber", [])))
    execution = integer(tx["gasLimit"]) * integer(tx["maxFeePerGas"])
    is_op = _op_stack(rpc, chain, block)
    l1 = operator = 0
    if is_op:
        # Size the requested type-2 transaction; the oracle adds signature overhead.
        nonce = integer(tx.get("nonce", 2**64 - 1))
        if nonce >= 2**64:
            raise MetaMaskError("INVALID_RESPONSE", "Transaction nonce exceeds its uint64 limit")
        unsigned = b"\x02" + rlp.encode(
            [
                chain,
                nonce,
                integer(tx["maxPriorityFeePerGas"]),
                integer(tx["maxFeePerGas"]),
                integer(tx["gasLimit"]),
                bytes.fromhex(tx["to"][2:]),
                integer(tx["value"]),
                bytes.fromhex(tx["data"][2:]),
                [],
            ]
        )
        # This is the provider's current estimate, not a signer-enforceable L1 fee cap.
        l1 = _oracle(rpc, chain, "getL1FeeUpperBound(uint256)", len(unsigned), block)
        operator = _oracle(rpc, chain, "getOperatorFee(uint256)", integer(tx["gasLimit"]), block)
    return {
        "model": "op-stack" if is_op else "evm",
        "blockNumber": block,
        "executionGasCeiling": str(execution),
        "l1DataFeeEstimate": str(l1),
        "operatorFeeEstimate": str(operator),
        "totalEstimate": str(execution + l1 + operator),
    }


def receipt_fees(rpc, chain, receipt, model=None):
    execution = integer(receipt.get("gasUsed")) * integer(receipt.get("effectiveGasPrice"))
    block = hex(integer(receipt["blockNumber"])) if "blockNumber" in receipt else None
    is_op = model == "op-stack" or "l1Fee" in receipt
    if model is None:
        if block is None:
            raise MetaMaskError("INVALID_RESPONSE", "Fee reconciliation requires the receipt block")
        is_op = is_op or _op_stack(rpc, chain, block)
    l1 = operator = 0
    if is_op:
        if block is None:
            raise MetaMaskError("INVALID_RESPONSE", "Rollup fee reconciliation requires the receipt block")
        l1 = integer(receipt.get("l1Fee"))
        # Historical oracle state selects the correct Isthmus/Jovian formula.
        operator = _oracle(rpc, chain, "getOperatorFee(uint256)", integer(receipt["gasUsed"]), block)
    return {
        "model": "op-stack" if is_op else "evm",
        "blockNumber": block,
        "executionGas": str(execution),
        "l1DataFee": str(l1),
        "operatorFee": str(operator),
        "totalFee": str(execution + l1 + operator),
    }
