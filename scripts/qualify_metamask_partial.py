"""Explicit two-wallet native testnet partial-fill/cancel qualification.

Signs real requests only. Signature holds are test orchestration, never a broker
feature. No authority renewal, funding, policy changes or uncertain trade retries.
"""

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import ROUND_DOWN, Context, Decimal, localcontext
from pathlib import Path
from threading import Event, Lock
from uuid import uuid4

import lumibot
from lumibot.brokers import MetaMask
from lumibot.entities import Asset, Order
from lumibot.tools.metamask.provider import MetaMaskError

TERMINAL = {"filled", "canceled", "rejected"}
STRATEGY = "native-partial-qualification"
TIMEOUT = 120


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def validate_configs(configs):
    require(isinstance(configs, list) and len(configs) == 2, "Exactly two wallet configurations are required")
    require(all(c.get("NETWORK") == "testnet" for c in configs), "Mainnet qualification is forbidden")
    require(all(c.get("MODE", "perp") == "perp" for c in configs), "Perpetual mode is required")
    require(len({c["WALLET_ADDRESS"].lower() for c in configs}) == 2, "Two distinct wallets are required")
    require(len({c["AGENT_ID"] for c in configs}) == 2, "Two distinct agent bindings are required")
    for key in ("STATE_FILE", "SESSION_FILE", "LEGACY_SESSION_FILE", "PROJECT_ID"):
        require(configs[0].get(key) == configs[1].get(key), f"Both wallets must share the same {key}")
    require(configs[0].get("STATE_FILE") and configs[0].get("SESSION_FILE"), "Durable state and session are required")


def validate_authority(binding, index, now):
    require(
        binding["network"] == "testnet"
        and binding["enabled"] is True
        and Decimal(binding["maxOrderUsd"]) == 25
        and Decimal(binding["maxDailyUsd"]) == (300 if index == 0 else 150)
        and binding["maxLeverage"] == 1
        and binding["maxSlippageBps"] == 50
        and set(binding["markets"]) == {"BTC", "ETH"}
        and now + 360_000 < binding["expiresAt"] <= now + 86_400_000,
        "Wallet authority differs from approved testnet limits or has insufficient remaining time",
    )


class Evidence:
    def __init__(self, path, run):
        self.path, self.lock = path, Lock()
        self.data = {"run": run, "network": "testnet", "stages": [], "complete": False}
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)

    def stage(self, name, **values):
        with self.lock:
            self.data["stages"].append({"stage": name, "time": int(time.time() * 1000), **values})
            descriptor, temporary = tempfile.mkstemp(dir=self.path.parent, prefix=".partial-")
            try:
                with os.fdopen(descriptor, "w") as stream:
                    json.dump(self.data, stream, indent=2)
                    stream.write("\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, self.path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)


def source_identity():
    root = Path(__file__).resolve().parents[1]
    require(Path(lumibot.__file__).resolve().parent == root / "lumibot", "Imported LumiBot differs from this checkout")
    files = [
        *sorted((root / "lumibot/tools/metamask").glob("*.py")),
        root / "lumibot/tools/metamask_client.py",
        root / "lumibot/brokers/metamask.py",
        root / "lumibot/brokers/broker.py",
        root / "lumibot/strategies/strategy_executor.py",
        Path(__file__).resolve(),
    ]
    return {
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True, timeout=5).strip(),
        "files": {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in files},
    }


class SignatureGate:
    def __init__(self, label, evidence, timeout=TIMEOUT):
        self.label, self.evidence, self.timeout = label, evidence, timeout
        self.ready, self.release, self.aborted = Event(), Event(), Event()

    def hold(self, job):
        if job.state != "SIGNED":
            raise MetaMaskError("QUALIFICATION_SIGNING_PENDING", "Qualification requires a completed real signature")
        self.evidence.stage("signature_ready", label=self.label, request_id=job.request_id)
        self.ready.set()
        if not self.release.wait(self.timeout) or self.aborted.is_set():
            raise MetaMaskError("QUALIFICATION_ABORTED", "Signature hold ended without execution release")
        return job

    def abort(self):
        self.aborted.set()
        self.release.set()


def await_gate(gate, future, timeout=TIMEOUT):
    deadline = time.monotonic() + timeout
    while not gate.ready.wait(min(0.1, max(0, deadline - time.monotonic()))):
        if future.done():
            future.result()
            raise RuntimeError(f"{gate.label} stopped before a signed request was ready")
        if time.monotonic() >= deadline:
            gate.abort()
            raise TimeoutError(f"{gate.label} signature preparation timed out")
    require(not future.done(), f"{gate.label} signature hold already ended")


class CallbackRecorder:
    name = STRATEGY
    NEW_ORDER, PARTIALLY_FILLED_ORDER, FILLED_ORDER, CANCELED_ORDER, ERROR_ORDER = (
        "new",
        "partial_fill",
        "fill",
        "canceled",
        "error",
    )

    def __init__(self):
        self.events = []

    def add_event(self, event, payload):
        self.events.append(
            {
                "event": event,
                "id": str(payload["order"].identifier),
                "quantity": str(payload.get("quantity")),
                "price": str(payload.get("price")),
            }
        )


def native_wait(broker, item, states, timeout=TIMEOUT):
    deadline = time.monotonic() + timeout
    while True:
        result = broker.client.get_order(str(item.identifier))
        broker._observe(item, result)
        if result["state"] in states:
            return result
        require(result["state"] not in TERMINAL, f"Unexpected terminal order state: {result['state']}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Native order reconciliation timed out: {item.identifier}")
        time.sleep(0.25)


def book(venue):
    value = venue.info({"type": "l2Book", "coin": "BTC"})
    require(
        value["coin"] == "BTC" and abs(int(time.time() * 1000) - value["time"]) < 10_000, "Book is stale or unrelated"
    )
    return Decimal(value["levels"][0][0]["px"]), Decimal(value["levels"][1][0]["px"])


def wait_for_eligible_book(venue, market, timeout=60):
    """Wait using public reads only; no order intent or signature exists yet."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        bid, ask = book(venue)
        require(bid.is_finite() and ask.is_finite() and 0 < bid < ask, "Invalid public book prices")
        with localcontext(Context(prec=160)):
            price = venue.normalize_price((bid + ask) / 2, market)
            tick = Decimal(1).scaleb(-min(6 - market.size_decimals, 4 - price.adjusted()))
            eligible = price - bid >= 5 * tick and ask - price >= 5 * tick
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        if eligible:
            return bid, ask, price, tick
        time.sleep(min(1, remaining))
    raise TimeoutError("No public book with five ticks of room on each side within the eligibility wait")


def owned_residual(operations, positions, open_orders, owned_ids):
    require(not open_orders, "Venue open orders prevent safe cleanup; foreign orders are never canceled")
    require(all(op["id"] in owned_ids for op in operations), "Cleanup received an unrelated operation")
    require(len({op["id"] for op in operations}) == len(operations), "Duplicate operations cannot prove ownership")
    require(all(op["state"] in TERMINAL for op in operations), "Unresolved own operations prevent cleanup")
    with localcontext(Context(prec=160)):
        known = Decimal(0)
        for operation in operations:
            request = operation["request"]
            require(request["symbol"] == "BTC" and request["side"] in {"buy", "sell"}, "Unexpected cleanup intent")
            filled = Decimal(operation["filledQuantity"])
            require(filled.is_finite() and 0 <= filled <= Decimal(request["quantity"]), "Invalid cumulative fill")
            known += filled if request["side"] == "buy" else -filled
    if known == 0:
        require(not positions, "Unowned position prevents cleanup")
    else:
        require(
            len(positions) == 1 and positions[0]["symbol"] == "BTC" and Decimal(positions[0]["quantity"]) == known,
            "Authoritative position differs from this run's exact known fills",
        )
    return known


def order(identifier, quantity, side, **values):
    return Order(
        STRATEGY,
        Asset("BTC", asset_type="crypto_future", leverage=1),
        quantity,
        side,
        quote=Asset("USDC", asset_type="crypto"),
        identifier=identifier,
        **values,
    )


def cleanup(broker, created, cleanup_id, evidence, index):
    for item in list(created):
        if broker.client.journal.get(broker.client.config["AGENT_ID"], str(item.identifier)) is not None:
            broker.cancel_order(item)
    operations = [
        broker.client.get_order(str(item.identifier))
        for item in created
        if broker.client.journal.get(broker.client.config["AGENT_ID"], str(item.identifier)) is not None
    ]
    wallet = broker.client.config["WALLET_ADDRESS"]
    positions = broker.client.account("perp")["positions"]
    open_orders = broker.client.venue.info({"type": "openOrders", "user": wallet})
    evidence.stage("cleanup_snapshot", index=index, operations=operations, positions=positions, open_orders=open_orders)
    remaining = owned_residual(operations, positions, open_orders, {str(item.identifier) for item in created})
    if remaining:
        closing = order(
            cleanup_id,
            abs(remaining),
            "sell" if remaining > 0 else "buy",
            order_type="market",
            time_in_force="ioc",
            reduce_only=True,
        )
        created.append(closing)
        evidence.stage("cleanup_prepared", index=index, id=cleanup_id, quantity=str(abs(remaining)), reduce_only=True)
        broker.submit_order(closing)
        evidence.stage("cleanup_receipt", index=index, receipt=native_wait(broker, closing, {"filled"}))
    account = broker.client.account("perp")
    open_orders = broker.client.venue.info({"type": "openOrders", "user": wallet})
    require(not account["positions"] and not open_orders, "Cleanup left positions or orders requiring owner review")
    evidence.stage("cleanup_flat", index=index, account=account, open_orders=open_orders)


def run(configs, evidence):
    validate_configs(configs)
    prefix = evidence.data["run"]
    ids = [f"{prefix}-maker", f"{prefix}-taker"]
    cleanup_ids = [f"{prefix}-cleanup-{i}" for i in range(2)]
    evidence.stage("prepared", source=source_identity(), ids=ids, cleanup_ids=cleanup_ids)
    brokers, gates, futures = [], {}, []
    created = [[], []]
    pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="metamask-partial")
    provider = original_sign = None
    success, failures = False, []
    try:
        for index, config in enumerate(configs):
            broker = MetaMask(config, connect_stream=False, start_orders_thread=False)
            brokers.append(broker)
            if index:
                broker.client._journal = brokers[0].client.journal
                broker.client._provider = brokers[0].client.provider
                broker.client._tokens = brokers[0].client._tokens
            identity = broker.client.verify()
            binding = broker.client.journal.binding(config["AGENT_ID"])
            validate_authority(binding, index, int(time.time() * 1000))
            require(
                all(op["state"] in TERMINAL for op in broker.client.journal.operations(config["AGENT_ID"])),
                "Pre-existing active or uncertain journal operations prevent qualification",
            )
            account = broker.client.account("perp")
            require(
                not account["positions"]
                and not broker.client.venue.info({"type": "openOrders", "user": identity["wallet"]}),
                "Both dedicated wallets must start flat without open orders",
            )
            evidence.stage(
                "preflight",
                index=index,
                wallet_suffix=identity["wallet"][-6:],
                account=account,
                expiry=binding["expiresAt"],
                policy_digest=binding["policyDigest"],
            )
        maker, taker = brokers
        provider, original_sign = maker.client.provider, maker.client.provider.submit_signature
        recorders = [CallbackRecorder(), CallbackRecorder()]
        for broker, recorder in zip(brokers, recorders):
            broker._add_subscriber(recorder)
        market = maker.client.venue.market("BTC")
        evidence.stage("book_wait", timeout_seconds=60, minimum_ticks_each_side=5)
        bid, ask, price, tick = wait_for_eligible_book(maker.client.venue, market)
        evidence.stage("book_eligible", bid=str(bid), ask=str(ask), price=str(price), tick=str(tick))
        mark = maker.client.price("BTC")
        with localcontext(Context(prec=160)):
            half = (Decimal(12) / mark).quantize(Decimal(1).scaleb(-market.size_decimals), rounding=ROUND_DOWN)
            require(bid < price < ask, "Book has no safe inside-spread maker price")
            require(Decimal(10) <= half * price <= Decimal(12), "Venue lots cannot fit the test amount")
            for index, broker in enumerate(brokers):
                amount = half * (2 if index == 0 else 1)
                reserved = (
                    amount
                    * max(price, mark * Decimal("1.005"))
                    * (1 + broker.client.venue.fee_rate(configs[index]["WALLET_ADDRESS"]))
                )
                require(
                    reserved < 25 and Decimal(broker.client.account("perp")["cash"]) >= reserved,
                    "Test amount exceeds approved order cap or available 1x collateral",
                )
        items = [
            order(ids[0], half * 2, "buy", order_type="limit", limit_price=price, time_in_force="alo"),
            order(ids[1], half, "sell", order_type="limit", limit_price=price, time_in_force="ioc"),
        ]
        for index, item in enumerate(items):
            created[index].append(item)
            gates[(configs[index]["WALLET_ADDRESS"].lower(), "order")] = SignatureGate(ids[index], evidence)
        evidence.stage("orders_prepared", quantity_half=str(half), maker_quantity=str(half * 2), price=str(price))

        def gated_sign(request_id, wallet, network, data, action):
            job = original_sign(request_id, wallet, network, data, action)
            gate = gates.get((wallet.lower(), action["type"]))
            return gate.hold(job) if gate is not None and job.state == "SIGNED" else job

        provider.submit_signature = gated_sign
        futures = [pool.submit(broker.submit_order, item) for broker, item in zip(brokers, items)]
        for index, future in enumerate(futures):
            await_gate(gates[(configs[index]["WALLET_ADDRESS"].lower(), "order")], future)
        bid, ask = book(maker.client.venue)
        require(bid < price < ask, "Book moved before maker release; frozen orders will be canceled")
        evidence.stage("maker_release")
        gates[(configs[0]["WALLET_ADDRESS"].lower(), "order")].release.set()
        futures[0].result(timeout=TIMEOUT)
        maker_open = native_wait(maker, items[0], {"open"})
        require(Decimal(maker_open["filledQuantity"]) == 0, "Maker acquired an unexpected early fill")
        evidence.stage("maker_open", receipt=maker_open)
        cancel_gate = SignatureGate(f"{prefix}-cancel", evidence)
        gates[(configs[0]["WALLET_ADDRESS"].lower(), "cancel")] = cancel_gate
        evidence.stage("cancel_prepared", id=ids[0], provider_order_id=maker_open["providerOrderId"])
        cancel_future = pool.submit(maker.cancel_order, items[0])
        futures.append(cancel_future)
        await_gate(cancel_gate, cancel_future)
        bid, _ = book(maker.client.venue)
        require(bid == price, "Maker is no longer the best bid; aborting controlled partial fill")
        evidence.stage("taker_release_cancel_held")
        gates[(configs[1]["WALLET_ADDRESS"].lower(), "order")].release.set()
        futures[1].result(timeout=TIMEOUT)
        taker_filled = native_wait(taker, items[1], {"filled"})
        require(Decimal(taker_filled["filledQuantity"]) == half, "Taker did not execute the exact planned half")
        evidence.stage("taker_filled", receipt=taker_filled)
        deadline = time.monotonic() + 30
        while True:
            status = maker.client.venue.order_status(configs[0]["WALLET_ADDRESS"], int(maker_open["providerOrderId"]))
            require(status.get("status") == "order", "Maker outcome is unknown")
            current = status["order"]
            require(current["status"] == "open", "Maker left the book before partial cancellation")
            venue_order = current["order"]
            require(
                venue_order["coin"] == "BTC"
                and venue_order["side"] == "B"
                and venue_order["oid"] == int(maker_open["providerOrderId"])
                and Decimal(venue_order["origSz"]) == half * 2,
                "Independent venue order differs from the maker intent",
            )
            fills = [
                row
                for row in maker.client.venue.fills(
                    configs[0]["WALLET_ADDRESS"], maker_open["createdAt"] - 5000, int(time.time() * 1000)
                )
                if row["oid"] == int(maker_open["providerOrderId"])
            ]
            require(all(row["coin"] == "BTC" and row["side"] == "B" for row in fills), "Unexpected maker fill identity")
            with localcontext(Context(prec=160)):
                quantity = sum((Decimal(row["sz"]) for row in fills), Decimal(0))
            require(quantity <= half, "Maker received excess fills; aborting qualification")
            if quantity == half and Decimal(venue_order["sz"]) == half:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("Independent maker partial-fill observation timed out")
            time.sleep(0.25)
        require(not cancel_future.done(), "Cancellation hold ended before partial-fill proof")
        taker_fills = [
            row
            for row in taker.client.venue.fills(
                configs[1]["WALLET_ADDRESS"], taker_filled["createdAt"] - 5000, int(time.time() * 1000)
            )
            if row["oid"] == int(taker_filled["providerOrderId"])
        ]
        require(
            {(row["tid"], Decimal(row["sz"]), Decimal(row["px"])) for row in fills}
            == {(row["tid"], Decimal(row["sz"]), Decimal(row["px"])) for row in taker_fills}
            and all(row["coin"] == "BTC" and row["side"] == "A" for row in taker_fills),
            "Independent fills do not prove the intended two-wallet match",
        )
        evidence.stage("independent_partial", status=status, fills=fills, taker_fills=taker_fills)
        cancel_gate.release.set()
        cancel_future.result(timeout=TIMEOUT)
        canceled = native_wait(maker, items[0], {"canceled"})
        require(Decimal(canceled["filledQuantity"]) == half, "Native canceled order lost its exact partial fill")
        fees = {}
        with localcontext(Context(prec=160)):
            for row in fills:
                fees[row["feeToken"]] = fees.get(row["feeToken"], Decimal(0)) + Decimal(row["fee"])
        require(
            {fee["asset"]: Decimal(fee["amount"]) for fee in canceled["fees"]} == fees,
            "Native cumulative fees differ from independent venue fills",
        )
        before = list(recorders[0].events)
        maker.do_polling()
        maker.sync_orders(STRATEGY)
        maker.do_polling()
        require(recorders[0].events == before, "Repeated reconciliation delivered duplicate callbacks")
        fill_events = [
            event for event in before if event["id"] == ids[0] and event["event"] in {"partial_fill", "fill"}
        ]
        require(
            len(fill_events) == 1 and Decimal(fill_events[0]["quantity"]) == half,
            "Broker callbacks did not report exactly one partial execution",
        )
        evidence.stage("native_partial_canceled", receipt=canceled, callbacks=before)
        success = True
    except BaseException as error:
        evidence.stage("failed", error=str(error))
        failures.append(error)
    finally:
        for gate in gates.values():
            gate.abort()
        # Every worker finishes before a shared session or journal can be closed.
        pool.shutdown(wait=True, cancel_futures=True)
        if provider is not None:
            provider.submit_signature = original_sign
        try:
            for index, future in enumerate(futures):
                try:
                    future.result()
                except BaseException as error:
                    evidence.stage("worker_failed", index=index, error=str(error))
                    failures.append(error)
            for index, broker in enumerate(brokers):
                if created[index]:
                    try:
                        cleanup(broker, created[index], cleanup_ids[index], evidence, index)
                    except BaseException as error:
                        evidence.stage("cleanup_failed", index=index, error=str(error))
                        failures.append(error)
        finally:
            closed = set()
            for broker in reversed(brokers):
                broker._stop_event.set()
                for component in (
                    broker.client._evm,
                    broker.client._venue,
                    broker.client._provider,
                    broker.client._tokens,
                    broker.client._journal,
                ):
                    if component is not None and id(component) not in closed:
                        try:
                            component.close()
                        except Exception as error:
                            failures.append(error)
                        closed.add(id(component))
    evidence.data["complete"] = success and not failures
    evidence.stage("finished", success=evidence.data["complete"])
    if failures:
        raise RuntimeError(
            "Qualification incomplete; inspect durable evidence and residual cleanup state"
        ) from failures[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--execute", action="store_true", required=True)
    args = parser.parse_args()
    configs = json.loads(args.config.read_text())
    validate_configs(configs)
    protected = {args.config.resolve()}
    protected.update(
        Path(config[key]).expanduser().resolve()
        for config in configs
        for key in ("STATE_FILE", "SESSION_FILE", "LEGACY_SESSION_FILE")
        if config.get(key)
    )
    require(args.evidence.resolve() not in protected, "Evidence must not overwrite configuration, session or journal")
    require(not args.evidence.exists(), "Choose a new evidence path to preserve earlier qualification receipts")
    evidence = Evidence(args.evidence, "native-partial-" + uuid4().hex[:12])
    run(configs, evidence)


if __name__ == "__main__":
    main()
