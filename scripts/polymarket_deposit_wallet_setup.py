#!/usr/bin/env python3
"""Polymarket deposit-wallet setup helper.

This script performs the account plumbing required before LumiBot can trade
through Polymarket's international CLOB deposit-wallet flow. It never prints raw
keys or private addresses beyond redacted summaries.
"""

from __future__ import annotations

import argparse
import json
import os
import stat
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
ENV_LOCAL = ROOT / ".env.local"
RELAYER_URL = "https://relayer-v2.polymarket.com"
PUSD_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_EXCHANGE_ADDRESS = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_CTF_EXCHANGE_ADDRESS = "0xe2222d279d744050d28e00520010520000310F59"
NEG_RISK_ADAPTER_ADDRESS = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"


def main() -> int:
    args = parse_args()
    load_dotenv(ENV_LOCAL, override=True)

    result: dict[str, Any] = {"env_file": str(ENV_LOCAL), "actions": []}

    if args.create_builder_key:
        result["actions"].append(create_builder_key())

    relayer = build_relayer(relay_tx_type=None)
    deposit_wallet = relayer.get_expected_deposit_wallet()
    result["deposit_wallet"] = redact_address(deposit_wallet)
    write_env({"POLYMARKET_DEPOSIT_WALLET_ADDRESS": deposit_wallet})

    if args.deploy:
        result["actions"].append(deploy_deposit_wallet(relayer, deposit_wallet))

    if args.fund_amount is not None:
        result["actions"].append(fund_deposit_wallet(deposit_wallet, args.fund_amount))

    if args.approve:
        result["actions"].append(approve_deposit_wallet(relayer, deposit_wallet))

    if args.approve_conditional:
        result["actions"].append(approve_conditional_tokens(relayer, deposit_wallet))

    if args.activate:
        write_env(
            {
                "POLYMARKET_PROXY_WALLET_ADDRESS": os.environ.get("POLYMARKET_WALLET_ADDRESS", ""),
                "POLYMARKET_WALLET_ADDRESS": deposit_wallet,
                "POLYMARKET_SIGNATURE_TYPE": "3",
                "POLYMARKET_PUSD_ADDRESS": os.environ.get("POLYMARKET_PUSD_ADDRESS", PUSD_ADDRESS),
            }
        )
        result["actions"].append({"activate": "updated POLYMARKET_WALLET_ADDRESS and POLYMARKET_SIGNATURE_TYPE"})

    result["verification"] = verify_deposit_wallet(deposit_wallet)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Set up a Polymarket CLOB deposit wallet.")
    parser.add_argument("--create-builder-key", action="store_true", help="Create/store builder HMAC credentials.")
    parser.add_argument("--deploy", action="store_true", help="Deploy the deterministic deposit wallet if needed.")
    parser.add_argument("--fund-amount", help="Transfer this many pUSD from the existing proxy wallet to deposit wallet.")
    parser.add_argument("--approve", action="store_true", help="Approve CLOB pUSD spenders from the deposit wallet.")
    parser.add_argument("--approve-conditional", action="store_true", help="Approve CTF outcome-token spenders for sells.")
    parser.add_argument("--activate", action="store_true", help="Switch .env.local trading funder to the deposit wallet.")
    parser.add_argument("--all", action="store_true", help="Run deploy, fund, approve, and activate.")
    args = parser.parse_args()
    if args.all:
        args.deploy = True
        args.approve = True
        args.approve_conditional = True
        args.activate = True
        args.fund_amount = args.fund_amount or os.environ.get("POLYMARKET_DEPOSIT_WALLET_FUND_AMOUNT", "5")
    return args


def create_builder_key() -> dict[str, Any]:
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import ApiCreds
    from py_clob_client_v2.constants import POLYGON

    client = ClobClient(
        os.environ.get("POLYMARKET_CLOB_URL", "https://clob.polymarket.com"),
        chain_id=int(os.environ.get("POLYMARKET_CHAIN_ID", POLYGON)),
        key=required_env("POLYMARKET_PRIVATE_KEY"),
        creds=ApiCreds(
            api_key=required_env("POLYMARKET_CLOB_API_KEY"),
            api_secret=required_env("POLYMARKET_CLOB_API_SECRET"),
            api_passphrase=required_env("POLYMARKET_CLOB_API_PASSPHRASE"),
        ),
        signature_type=int(os.environ.get("POLYMARKET_SIGNATURE_TYPE", "1")),
        funder=os.environ.get("POLYMARKET_WALLET_ADDRESS"),
        retry_on_error=True,
    )
    response = client.create_builder_api_key()
    payload = to_plain(response)
    key = payload.get("apiKey") or payload.get("key") or payload.get("api_key")
    secret = payload.get("secret") or payload.get("apiSecret") or payload.get("api_secret")
    passphrase = payload.get("passphrase") or payload.get("apiPassphrase") or payload.get("api_passphrase")
    if not (key and secret and passphrase):
        raise SystemExit(f"Builder key response missing expected fields: {sorted(payload)}")
    write_env(
        {
            "POLYMARKET_BUILDER_API_KEY": key,
            "POLYMARKET_BUILDER_SECRET": secret,
            "POLYMARKET_BUILDER_PASSPHRASE": passphrase,
            "BUILDER_API_KEY": key,
            "BUILDER_SECRET": secret,
            "BUILDER_PASS_PHRASE": passphrase,
            "POLYMARKET_RELAYER_URL": os.environ.get("POLYMARKET_RELAYER_URL", RELAYER_URL),
        }
    )
    return {"create_builder_key": "stored", "response_fields": sorted(payload)}


def build_relayer(relay_tx_type):
    from py_builder_relayer_client.client import RelayClient
    from py_builder_signing_sdk.config import BuilderApiKeyCreds, BuilderConfig

    builder_config = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(
            key=required_env("POLYMARKET_BUILDER_API_KEY"),
            secret=required_env("POLYMARKET_BUILDER_SECRET"),
            passphrase=required_env("POLYMARKET_BUILDER_PASSPHRASE"),
        )
    )
    return RelayClient(
        os.environ.get("POLYMARKET_RELAYER_URL", RELAYER_URL),
        int(os.environ.get("POLYMARKET_CHAIN_ID", "137")),
        required_env("POLYMARKET_PRIVATE_KEY"),
        builder_config,
        relay_tx_type=relay_tx_type,
    )


def deploy_deposit_wallet(relayer, deposit_wallet: str) -> dict[str, Any]:
    if relayer.get_deployed(deposit_wallet, "WALLET"):
        return {"deploy": "already_deployed", "deposit_wallet": redact_address(deposit_wallet)}
    response = relayer.deploy_deposit_wallet()
    return poll_transaction(relayer, response.transaction_id, {"deploy": "submitted"})


def fund_deposit_wallet(deposit_wallet: str, amount: str) -> dict[str, Any]:
    from py_builder_relayer_client.client import RelayerTxType
    from py_builder_relayer_client.models import Transaction

    amount_units = int(Decimal(str(amount)) * Decimal("1000000"))
    relayer = build_relayer(RelayerTxType.PROXY)
    response = relayer.execute(
        [
            Transaction(
                to=os.environ.get("POLYMARKET_PUSD_ADDRESS", PUSD_ADDRESS),
                data=erc20_transfer_calldata(deposit_wallet, amount_units),
                value="0",
            )
        ],
        metadata="lumibot deposit wallet funding",
    )
    return poll_transaction(
        relayer,
        response.transaction_id,
        {"fund": "submitted", "amount_usd": float(Decimal(str(amount)))},
    )


def approve_deposit_wallet(relayer, deposit_wallet: str) -> dict[str, Any]:
    from py_builder_relayer_client.models import DepositWalletCall, TransactionType
    from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
    from py_clob_client_v2.order_utils.model.signature_type_v2 import SignatureTypeV2

    client = clob_client(deposit_wallet)
    balance = to_plain(
        client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=SignatureTypeV2.POLY_1271)
        )
    )
    spenders = [address for address, value in (balance.get("allowances") or {}).items() if str(value) in {"0", "0.0"}]
    if not spenders:
        return {"approve": "already_approved", "spender_count": len(balance.get("allowances") or {})}

    nonce_payload = relayer.get_nonce(required_env("POLYMARKET_OWNER_ADDRESS"), TransactionType.WALLET.value)
    calls = [
        DepositWalletCall(
            target=os.environ.get("POLYMARKET_PUSD_ADDRESS", PUSD_ADDRESS),
            value="0",
            data=erc20_approve_calldata(spender),
        )
        for spender in spenders
    ]
    response = relayer.execute_deposit_wallet_batch(
        calls=calls,
        wallet_address=deposit_wallet,
        nonce=str(nonce_payload["nonce"]),
        deadline=str(int(time.time()) + 900),
    )
    result = poll_transaction(relayer, response.transaction_id, {"approve": "submitted", "approval_count": len(calls)})
    client.update_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=SignatureTypeV2.POLY_1271)
    )
    return result


def approve_conditional_tokens(relayer, deposit_wallet: str) -> dict[str, Any]:
    from py_builder_relayer_client.models import DepositWalletCall, TransactionType

    spenders = [
        os.environ.get("POLYMARKET_CTF_EXCHANGE_ADDRESS", CTF_EXCHANGE_ADDRESS),
        os.environ.get("POLYMARKET_NEG_RISK_CTF_EXCHANGE_ADDRESS", NEG_RISK_CTF_EXCHANGE_ADDRESS),
        os.environ.get("POLYMARKET_NEG_RISK_ADAPTER_ADDRESS", NEG_RISK_ADAPTER_ADDRESS),
    ]
    nonce_payload = relayer.get_nonce(required_env("POLYMARKET_OWNER_ADDRESS"), TransactionType.WALLET.value)
    calls = [
        DepositWalletCall(
            target=os.environ.get("POLYMARKET_CTF_ADDRESS", CTF_ADDRESS),
            value="0",
            data=erc1155_set_approval_for_all_calldata(spender, True),
        )
        for spender in spenders
        if spender
    ]
    response = relayer.execute_deposit_wallet_batch(
        calls=calls,
        wallet_address=deposit_wallet,
        nonce=str(nonce_payload["nonce"]),
        deadline=str(int(time.time()) + 900),
    )
    return poll_transaction(
        relayer,
        response.transaction_id,
        {
            "approve_conditional": "submitted",
            "ctf_contract": redact_address(os.environ.get("POLYMARKET_CTF_ADDRESS", CTF_ADDRESS)),
            "approval_count": len(calls),
        },
    )


def verify_deposit_wallet(deposit_wallet: str) -> dict[str, Any]:
    from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
    from py_clob_client_v2.order_utils.model.signature_type_v2 import SignatureTypeV2

    client = clob_client(deposit_wallet)
    balance = to_plain(
        client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=SignatureTypeV2.POLY_1271)
        )
    )
    return {
        "deposit_wallet": redact_address(deposit_wallet),
        "balance_raw_units": str(balance.get("balance")),
        "allowance_count": len(balance.get("allowances") or {}),
        "zero_allowance_count": sum(1 for value in (balance.get("allowances") or {}).values() if str(value) in {"0", "0.0"}),
    }


def clob_client(deposit_wallet: str):
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import ApiCreds
    from py_clob_client_v2.constants import POLYGON
    from py_clob_client_v2.order_utils.model.signature_type_v2 import SignatureTypeV2

    return ClobClient(
        os.environ.get("POLYMARKET_CLOB_URL", "https://clob.polymarket.com"),
        chain_id=int(os.environ.get("POLYMARKET_CHAIN_ID", POLYGON)),
        key=required_env("POLYMARKET_PRIVATE_KEY"),
        creds=ApiCreds(
            api_key=required_env("POLYMARKET_CLOB_API_KEY"),
            api_secret=required_env("POLYMARKET_CLOB_API_SECRET"),
            api_passphrase=required_env("POLYMARKET_CLOB_API_PASSPHRASE"),
        ),
        signature_type=SignatureTypeV2.POLY_1271,
        funder=deposit_wallet,
        retry_on_error=True,
    )


def poll_transaction(relayer, transaction_id: str, base: dict[str, Any]) -> dict[str, Any]:
    state = None
    tx_hash = None
    deadline = time.time() + 150
    while time.time() < deadline:
        try:
            payload = relayer.get_transaction(transaction_id)
            state = payload.get("state") if isinstance(payload, dict) else getattr(payload, "state", None)
            tx_hash = (
                payload.get("transactionHash") or payload.get("transaction_hash")
                if isinstance(payload, dict)
                else getattr(payload, "transaction_hash", None)
            )
            if state in {"STATE_CONFIRMED", "STATE_FAILED", "STATE_INVALID"}:
                break
        except Exception as exc:
            state = f"poll_error:{type(exc).__name__}"
        time.sleep(3)
    return {**base, "transaction_id": transaction_id, "final_state": state, "transaction_hash_present": bool(tx_hash)}


def erc20_transfer_calldata(to_address: str, amount_units: int) -> str:
    return "0xa9059cbb" + to_address.lower().removeprefix("0x").rjust(64, "0") + hex(amount_units)[2:].rjust(64, "0")


def erc20_approve_calldata(spender: str, amount: int = 2**256 - 1) -> str:
    return "0x095ea7b3" + spender.lower().removeprefix("0x").rjust(64, "0") + hex(amount)[2:].rjust(64, "0")


def erc1155_set_approval_for_all_calldata(operator: str, approved: bool = True) -> str:
    return (
        "0xa22cb465"
        + operator.lower().removeprefix("0x").rjust(64, "0")
        + ("1" if approved else "0").rjust(64, "0")
    )


def write_env(updates: dict[str, str]) -> None:
    existing = ENV_LOCAL.read_text() if ENV_LOCAL.exists() else ""
    lines = existing.splitlines()
    seen: set[str] = set()
    output: list[str] = []
    for line in lines:
        if line.strip() and not line.lstrip().startswith("#") and "=" in line:
            name = line.split("=", 1)[0].strip()
            if name in updates:
                if updates[name]:
                    output.append(f"{name}={updates[name]}")
                seen.add(name)
            else:
                output.append(line)
        else:
            output.append(line)
    if output and output[-1].strip():
        output.append("")
    for name, value in updates.items():
        if name not in seen and value:
            output.append(f"{name}={value}")
    ENV_LOCAL.write_text("\n".join(output) + "\n", encoding="utf-8")
    os.chmod(ENV_LOCAL, stat.S_IRUSR | stat.S_IWUSR)


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing {name} in {ENV_LOCAL}")
    return value


def to_plain(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "dict"):
        return value.dict()
    if isinstance(value, dict):
        return {key: to_plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_plain(item) for item in value]
    return value


def redact_address(value: str | None) -> str | None:
    return f"{value[:6]}...{value[-4:]}" if value and len(value) > 12 else value


if __name__ == "__main__":
    raise SystemExit(main())
