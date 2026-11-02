"""Exercise the shipped MetaMask example through normal scheduling and restart.

Run as python -m scripts.qualify_metamask_strategy. Provider-specific preflight,
evidence and emergency cleanup stay outside the unmodified example strategy.
"""

import argparse
import functools
import hashlib
import json
import time
from contextlib import ExitStack
from decimal import Decimal
from pathlib import Path
from threading import Event
from unittest.mock import patch

from lumibot.brokers import MetaMask
from lumibot.entities import Asset
from lumibot.example_strategies.metamask_perpetual import MetaMaskPerpetualRoundTrip
from lumibot.traders import Trader
from scripts.qualify_metamask_partial import Evidence, cleanup, cleanup_budget_room, validate_authority


def stop_strategy(trader, strategy, broker, iteration_idle, evidence):
    """Keep ownership until the actual user iteration and scheduler have stopped."""
    trader.stop_all()
    failure = None

    def record_wait(worker):
        nonlocal failure
        try:
            evidence.stage("shutdown_wait", worker=worker)
        except Exception as error:
            failure = error

    while not iteration_idle.wait(30):
        record_wait("user_iteration")
    if strategy is not None:
        while strategy._executor.is_alive():
            strategy._executor.join(timeout=30)
            if strategy._executor.is_alive():
                record_wait("strategy_executor")
    broker._orders_queue.join()
    return failure


def exercise(config, name, evidence, *, restart, daily_cap, timeout):
    broker = MetaMask(config)
    phase = "restart" if restart else "live"
    trader = Trader(quiet_logs=True, logfile=str(evidence.path.parent / f"{evidence.path.stem}-{phase}.log"))
    created, callbacks, errors, iterations = [], [], [], []
    strategy = None
    successful = False
    iteration_idle = Event()
    iteration_idle.set()
    observers = ExitStack()
    try:
        identity = broker.client.verify()
        binding = broker.client.journal.binding(config["AGENT_ID"])
        validate_authority(binding, 0, int(time.time() * 1000), (daily_cap, 150))
        if not restart:
            cleanup_budget_room(broker.client.journal, config["AGENT_ID"], binding, int(time.time() * 1000))
        before = broker.client.account("perp")
        if before["positions"] or broker.client.venue.info({"type": "openOrders", "user": identity["wallet"]}):
            raise RuntimeError("Qualification must start with a flat dedicated wallet and no venue open orders")
        starting_ids = {row["id"] for row in broker.client.journal.operations(config["AGENT_ID"])}
        evidence.stage("strategy_start", restart=restart, before=before, starting_operation_count=len(starting_ids))

        original_iteration = MetaMaskPerpetualRoundTrip.on_trading_iteration
        original_submit = MetaMaskPerpetualRoundTrip.submit_order

        @functools.wraps(original_iteration)
        def observe_iteration(instance):
            iteration_idle.clear()
            try:
                result = original_iteration(instance)
                iterations.append(time.time())
                return result
            finally:
                try:
                    evidence.stage(
                        "iteration",
                        restart=restart,
                        number=len(iterations),
                        completed=instance.vars.completed,
                        result=instance.vars.result,
                    )
                finally:
                    iteration_idle.set()

        @functools.wraps(original_submit)
        def observe_submission(instance, order, *args, **kwargs):
            created.append(order)
            evidence.stage(
                "submission",
                restart=restart,
                id=str(order.identifier),
                side=str(order.side),
                quantity=str(order.quantity),
                reduce_only=order.reduce_only,
            )
            return original_submit(instance, order, *args, **kwargs)

        # The executor captures its iteration callable during Strategy construction.
        observers.enter_context(patch.object(MetaMaskPerpetualRoundTrip, "on_trading_iteration", observe_iteration))
        observers.enter_context(patch.object(MetaMaskPerpetualRoundTrip, "submit_order", observe_submission))
        strategy = MetaMaskPerpetualRoundTrip(
            broker=broker,
            name=name,
            quote_asset=Asset("USDC", asset_type="crypto"),
            benchmark_asset=None,
            analyze_backtest=False,
            should_backup_variables_to_database=False,
            should_send_summary_to_discord=False,
        )
        market = broker.client.venue.market(strategy.parameters["symbol"])
        quantity = Decimal(str(strategy.parameters["quantity"]))
        if quantity != quantity.quantize(Decimal(1).scaleb(-market.size_decimals)):
            raise ValueError("The shipped example quantity does not match venue lot precision")
        for callback_name in ("on_partially_filled_order", "on_filled_order", "on_canceled_order", "on_bot_crash"):
            original = getattr(strategy, callback_name)

            def observe_callback(*args, callback_name=callback_name, original=original, **kwargs):
                if callback_name == "on_bot_crash":
                    errors.append(str(args[0]))
                    values = {"error": str(args[0])}
                elif callback_name == "on_canceled_order":
                    values = {"id": str(args[0].identifier)}
                else:
                    values = {"id": str(args[1].identifier), "price": str(args[2]), "quantity": str(args[3])}
                callbacks.append({"event": callback_name, **values})
                evidence.stage("callback", restart=restart, event=callback_name, **values)
                return original(*args, **kwargs)

            setattr(strategy, callback_name, functools.wraps(original)(observe_callback))

        trader.add_strategy(strategy)
        trader.run_all(
            async_=True,
            run_once=False,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
        )
        deadline = time.monotonic() + timeout
        minimum_iterations = 2 if restart else 3
        while not errors and time.monotonic() < deadline:
            if strategy.vars.get("completed", False) and len(iterations) >= minimum_iterations:
                break
            time.sleep(0.25)
        if errors or not strategy.vars.get("completed", False) or len(iterations) < minimum_iterations:
            raise RuntimeError("Ordinary scheduled strategy did not complete; inspect callbacks and iteration evidence")
        if strategy.vars.result != "completed":
            raise RuntimeError("Flat rejected entry is not a successful round trip")
        shutdown_failure = stop_strategy(trader, strategy, broker, iteration_idle, evidence)
        if shutdown_failure is not None:
            raise shutdown_failure
        if errors or not strategy.vars.completed or strategy.vars.result != "completed":
            raise RuntimeError("Final scheduled iteration did not retain a successful completed round trip")
        after = broker.client.account("perp")
        if after["positions"] or broker.client.venue.info({"type": "openOrders", "user": identity["wallet"]}):
            raise RuntimeError("Ordinary strategy left an open position or order")
        cash = strategy.get_cash()
        if cash is None or Decimal(str(cash)) != Decimal(after["cash"]):
            raise RuntimeError("Strategy cash differs from authoritative settled USDC cash")
        ending_ids = {row["id"] for row in broker.client.journal.operations(config["AGENT_ID"])}
        if restart:
            if created or callbacks or ending_ids != starting_ids:
                raise RuntimeError("Restart replayed a submission or historical callback")
        elif not created or not any(event["event"] == "on_filled_order" for event in callbacks):
            raise RuntimeError("No actual Strategy/Trader execution callbacks were observed")
        evidence.stage(
            "strategy_complete",
            restart=restart,
            iterations=len(iterations),
            submissions=len(created),
            callbacks=callbacks,
            after=after,
            strategy_cash=str(cash),
            new_operation_count=len(ending_ids - starting_ids),
        )
        successful = True
    finally:
        shutdown_failure = stop_strategy(trader, strategy, broker, iteration_idle, evidence)
        try:
            if not successful and created:
                recorded = [
                    order
                    for order in created
                    if broker.client.journal.get(config["AGENT_ID"], str(order.identifier)) is not None
                ]
                if recorded:
                    cleanup(broker, recorded, strategy.entry_id.removesuffix("-entry") + "-emergency", evidence, 0)
        finally:
            try:
                broker.close()
            finally:
                observers.close()
        if shutdown_failure is not None:
            raise shutdown_failure


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--name", default="metamask-perpetual-round-trip")
    parser.add_argument("--daily-cap", type=int, default=300)
    parser.add_argument("--timeout-seconds", type=int, default=240)
    parser.add_argument("--execute", action="store_true", required=True)
    args = parser.parse_args()
    if args.daily_cap <= 0 or args.timeout_seconds <= 0:
        parser.error("Daily cap and timeout must be positive")
    values = json.loads(args.config.read_text())
    if not isinstance(values, (dict, list)) or not values:
        raise ValueError("Configuration must be an object or nonempty list of objects")
    config = values[0] if isinstance(values, list) else values
    if not isinstance(config, dict):
        raise ValueError("Selected wallet configuration must be an object")
    if config.get("NETWORK") != "testnet" or config.get("MODE", "perp") != "perp":
        raise ValueError("Only explicit testnet perpetual configuration is allowed")
    protected = {args.config.resolve()}
    protected.update(
        Path(config[key]).expanduser().resolve()
        for key in ("STATE_FILE", "SESSION_FILE", "LEGACY_SESSION_FILE")
        if config.get(key)
    )
    if args.evidence.exists() or args.evidence.is_symlink() or args.evidence.resolve() in protected:
        raise ValueError("Choose a new evidence path; preserve configuration, sessions, journals and older receipts")
    example = Path(__file__).resolve().parents[1] / "lumibot/example_strategies/metamask_perpetual.py"
    evidence = Evidence(args.evidence, args.name)
    evidence.stage("prepared", example_sha256=hashlib.sha256(example.read_bytes()).hexdigest(), run_once=False)
    try:
        for restart in (False, True):
            exercise(
                config, args.name, evidence, restart=restart, daily_cap=args.daily_cap, timeout=args.timeout_seconds
            )
        evidence.data["complete"] = True
    finally:
        evidence.stage("finished", success=evidence.data["complete"])


if __name__ == "__main__":
    main()
