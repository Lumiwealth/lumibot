"""Explicit funded testnet qualification through ordinary native broker APIs.

Requires two dedicated, already-authorized and flat testnet wallets. Never
renews authority, funds wallets, changes policy, or retries an uncertain trade.
Run as python -m scripts.qualify_metamask_native from the repository root.
"""

import argparse
import json
import time
from decimal import ROUND_DOWN, Decimal
from pathlib import Path
from uuid import uuid4

from lumibot.brokers import MetaMask
from lumibot.entities import Asset, Order
from scripts.qualify_metamask_partial import (
    cleanup_budget_room,
    prepare_evidence_path,
    save_evidence,
    validate_authority,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--execute", action="store_true", required=True)
    args = parser.parse_args()
    configs = json.loads(args.config.read_text())
    if len(configs) != 2 or any(c["NETWORK"] != "testnet" for c in configs):
        raise ValueError("Qualification requires exactly two testnet configurations; mainnet is forbidden")
    if len({c["WALLET_ADDRESS"].lower() for c in configs}) != 2:
        raise ValueError("Qualification requires two distinct dedicated wallets")
    args.evidence = prepare_evidence_path(args.evidence, args.config, configs)
    prefix = "native-qualification-" + uuid4().hex[:12]
    evidence = {"run": prefix, "network": "testnet", "wallets": []}
    recovering, receipt_errors = False, []

    def save():
        try:
            save_evidence(args.evidence, evidence)
        except Exception as error:
            if not recovering:
                raise
            receipt_errors.append(str(error))

    save()

    for index, config in enumerate(configs):
        broker = MetaMask(config, connect_stream=False, start_orders_thread=False)
        created = []
        wallet = {"index": index, "operations": []}
        evidence["wallets"].append(wallet)
        save()

        def wait(item, states, broker=broker):
            deadline = time.monotonic() + 120
            while True:
                result = broker.client.get_order(str(item.identifier))
                broker._observe(item, result)
                if result["state"] in states:
                    return result
                if result["state"] in {"filled", "canceled", "rejected"} or time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"Operation {item.identifier} stopped in {result['state']}: {result.get('message')}"
                    )
                time.sleep(1)

        def submit(suffix, quantity, side, broker=broker, created=created, index=index, **kwargs):
            item = Order(
                "native-qualification",
                Asset("BTC", asset_type="crypto_future", leverage=1),
                quantity,
                side,
                quote=Asset("USDC", asset_type="crypto"),
                identifier=f"{prefix}-{index}-{suffix}",
                **kwargs,
            )
            created.append(item)
            broker.submit_order(item)
            return item

        def record(item, result, wallet=wallet):
            wallet["operations"].append(
                {
                    "id": str(item.identifier),
                    "state": result["state"],
                    "quantity": str(item.quantity),
                    "filledQuantity": result["filledQuantity"],
                    "averagePrice": result.get("averagePrice"),
                    "fees": result.get("fees"),
                    "providerOrderId": result.get("providerOrderId"),
                }
            )
            save()
            print(json.dumps(wallet["operations"][-1]), flush=True)

        try:
            identity = broker.client.verify()
            binding = broker.client.journal.binding(config["AGENT_ID"])
            now = int(time.time() * 1000)
            validate_authority(binding, index, now)
            wallet["remaining_budget_usd"] = str(
                cleanup_budget_room(broker.client.journal, config["AGENT_ID"], binding, now, required_usd=Decimal("75"))
            )
            account = broker.client.account("perp")
            if account["positions"] or broker.client.venue.info({"type": "openOrders", "user": identity["wallet"]}):
                raise ValueError("Qualification requires flat wallets without pre-existing open orders")
            wallet["before"] = account
            mark = broker.client.price("BTC")
            market = broker.client.venue.market("BTC")
            step = Decimal(1).scaleb(-market.size_decimals)
            quantity = (Decimal("12") / mark).quantize(step, rounding=ROUND_DOWN)
            if not Decimal("10") <= quantity * mark < Decimal("13"):
                raise ValueError("Venue lot size cannot satisfy the bounded test amount")
            resting = submit(
                "rest",
                quantity,
                "buy",
                order_type="limit",
                limit_price=(mark * Decimal("0.9")).quantize(Decimal("1")),
                time_in_force="alo",
            )
            result = wait(resting, {"open"})
            assert result["filledQuantity"] == "0"
            original_id = result["providerOrderId"]
            broker.client.submit(result["request"])
            assert broker.client.get_order(str(resting.identifier))["providerOrderId"] == original_id
            broker.cancel_order(resting)
            record(resting, wait(resting, {"canceled"}))
            opening = submit("open", quantity, "buy", order_type="market", time_in_force="ioc")
            result = wait(opening, {"filled"})
            assert Decimal(result["filledQuantity"]) == quantity
            assert opening.transactions and opening._closed_event.is_set()
            record(opening, result)
            closing = submit("close", quantity, "sell", order_type="market", time_in_force="ioc", reduce_only=True)
            record(closing, wait(closing, {"filled"}))
            account = broker.client.account("perp")
            assert account["positions"] == []
            assert broker.client.venue.info({"type": "openOrders", "user": identity["wallet"]}) == []
            wallet["after"] = account
            wallet["complete"] = True
            save()
        except BaseException as error:
            recovering = True
            wallet["error"] = str(error)
            wallet["complete"] = False
            wallet["receipt_errors"] = receipt_errors
            save()
            # Cancel only this run's tracked orders. Uncertain fills need review;
            # never infer that an HTTP failure means the wallet stayed flat.
            cleanup_errors = []
            for item in created:
                try:
                    broker.cancel_order(item)
                except Exception as cleanup_error:
                    cleanup_errors.append(str(cleanup_error))
            try:
                operations = [broker.client.get_order(str(item.identifier)) for item in created]
                known = sum(
                    (
                        Decimal(op["filledQuantity"]) * (1 if op["request"]["side"] == "buy" else -1)
                        for op in operations
                    ),
                    Decimal(0),
                )
                positions = broker.client.account("perp")["positions"]
                wallet["failure_positions"] = positions
                if any(op["state"] not in {"filled", "canceled", "rejected"} for op in operations):
                    raise RuntimeError("Unresolved run operations prevent safe residual cleanup")
                if broker.client.venue.info({"type": "openOrders", "user": identity["wallet"]}):
                    raise RuntimeError("Open venue orders prevent safe residual cleanup")
                if known:
                    if (
                        len(positions) != 1
                        or positions[0]["symbol"] != "BTC"
                        or Decimal(positions[0]["quantity"]) != known
                    ):
                        raise RuntimeError("Authoritative position does not match this run's exact known fills")
                    cleanup = submit(
                        "cleanup",
                        abs(known),
                        "sell" if known > 0 else "buy",
                        order_type="market",
                        time_in_force="ioc",
                        reduce_only=True,
                    )
                    record(cleanup, wait(cleanup, {"filled"}))
                if broker.client.account("perp")["positions"] or broker.client.venue.info(
                    {"type": "openOrders", "user": identity["wallet"]}
                ):
                    raise RuntimeError("Residual position or open orders remain after cleanup")
                wallet["failure_cleanup_flat"] = True
            except Exception as cleanup_error:
                cleanup_errors.append(str(cleanup_error))
            wallet["cleanup_errors"] = cleanup_errors
            save()
            raise
        finally:
            broker._stop_event.set()
            broker.client.close()


if __name__ == "__main__":
    main()
