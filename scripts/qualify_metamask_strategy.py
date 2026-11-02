"""Explicit testnet ETH round trip through real Strategy and Trader callbacks."""

import argparse
import json
import time
from decimal import ROUND_DOWN, Decimal
from pathlib import Path

from lumibot.brokers import MetaMask
from lumibot.entities import Asset, Order
from lumibot.strategies import Strategy
from lumibot.traders import Trader


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--execute", action="store_true", required=True)
    args = parser.parse_args()
    if args.evidence.exists() or args.evidence.is_symlink():
        raise ValueError("Choose a new evidence path; existing files and symlinks must be preserved")
    config = json.loads(args.config.read_text())[0]
    if config["NETWORK"] != "testnet":
        raise ValueError("Mainnet qualification is forbidden")
    record = {"network": "testnet", "symbol": "ETH", "prepared": [], "callbacks": []}

    def save():
        args.evidence.parent.mkdir(parents=True, exist_ok=True)
        args.evidence.write_text(json.dumps(record, indent=2) + "\n")
        args.evidence.chmod(0o600)

    broker = MetaMask(config, connect_stream=False, start_orders_thread=False)
    try:
        identity = broker.client.verify()
        binding = broker.client.journal.binding(config["AGENT_ID"])
        if (
            binding["enabled"] is not True
            or binding["expiresAt"] <= int(time.time() * 1000)
            or binding["markets"] != ["BTC", "ETH"]
            or binding["maxOrderUsd"] != "25"
            or binding["maxDailyUsd"] != "300"
            or binding["maxLeverage"] != 1
            or binding["maxSlippageBps"] != 50
        ):
            raise ValueError("Authority differs from the approved testnet limits")
        before = broker.client.account("perp")
        if before["positions"] or broker.client.venue.info({"type": "openOrders", "user": identity["wallet"]}):
            raise ValueError("Qualification requires a flat dedicated wallet without existing orders")
        record["before"] = before
        mark = broker.client.price("ETH")
        step = Decimal(1).scaleb(-broker.client.venue.market("ETH").size_decimals)
        quantity = (Decimal("12") / mark).quantize(step, rounding=ROUND_DOWN)
        if not Decimal("10") <= mark * quantity < Decimal("13"):
            raise ValueError("Venue lot size cannot satisfy the bounded test amount")
        save()

        class RoundTrip(Strategy):
            def initialize(self, parameters=None):
                self.sleeptime = "1S"
                self.open_id = self.close_id = None

            def on_trading_iteration(self):
                opening = self.create_order(
                    Asset("ETH", asset_type="crypto_future", leverage=1),
                    quantity,
                    "buy",
                    order_type="market",
                    time_in_force="ioc",
                )
                self.open_id = str(opening.identifier)
                record["prepared"].append(self.open_id)
                save()
                self.submit_order(opening)

            def on_filled_order(self, position, order, price, quantity, multiplier):
                identifier = str(order.identifier)
                record["callbacks"].append(
                    {"id": identifier, "price": str(price), "quantity": str(quantity), "side": str(order.side)}
                )
                save()
                if identifier == self.open_id and self.close_id is None:
                    filled = sum((Decimal(str(t.quantity)) for t in order.transactions), Decimal(0))
                    closing = self.create_order(
                        order.asset, filled, "sell", order_type="market", time_in_force="ioc", reduce_only=True
                    )
                    self.close_id = str(closing.identifier)
                    record["prepared"].append(self.close_id)
                    save()
                    self.submit_order(closing)
                elif identifier != self.close_id:
                    raise RuntimeError("Unexpected or duplicate fill callback")

        strategy = RoundTrip(
            broker=broker,
            name="native-strategy-qualification",
            quote_asset=Asset("USDC", asset_type="crypto"),
            benchmark_asset=None,
            analyze_backtest=False,
        )
        trader = Trader(quiet_logs=True)
        trader.add_strategy(strategy)
        trader.run_all(
            run_once=True, show_plot=False, show_tearsheet=False, save_tearsheet=False, show_indicators=False
        )
        if len(record["callbacks"]) != 2 or len(set(x["id"] for x in record["callbacks"])) != 2:
            raise RuntimeError("The real strategy did not receive exactly one opening and closing fill callback")
        record["operations"] = [broker.client.get_order(identifier) for identifier in record["prepared"]]
        if any(op["state"] != "filled" for op in record["operations"]):
            raise RuntimeError("Strategy orders did not both reconcile as filled")
        record["strategy_cash"] = strategy.get_cash()
        record["after"] = broker.client.account("perp")
        if record["after"]["positions"] or broker.client.venue.info({"type": "openOrders", "user": identity["wallet"]}):
            raise RuntimeError("Strategy round trip left an open position or order")
        record["complete"] = True
        save()
        print(json.dumps({"complete": True, "callbacks": len(record["callbacks"]), "positions": 0}), flush=True)
    except BaseException as error:
        record["error"] = str(error)
        try:
            recorded_ids = [
                identifier
                for identifier in record["prepared"]
                if broker.client.journal.get(config["AGENT_ID"], identifier) is not None
            ]
            record["unsubmitted_ids"] = [
                identifier for identifier in record["prepared"] if identifier not in recorded_ids
            ]
            operations = [broker.client.get_order(identifier) for identifier in recorded_ids]
            for op in operations:
                if op["state"] not in {"filled", "canceled", "rejected"}:
                    broker.client.cancel(op["id"])
            operations = [broker.client.get_order(identifier) for identifier in recorded_ids]
            if any(op["state"] not in {"filled", "canceled", "rejected"} for op in operations):
                raise RuntimeError("Unknown strategy orders prevent residual cleanup")
            if broker.client.venue.info({"type": "openOrders", "user": config["WALLET_ADDRESS"]}):
                raise RuntimeError("Open venue orders prevent safe residual cleanup")
            known = sum(
                (Decimal(op["filledQuantity"]) * (1 if op["request"]["side"] == "buy" else -1) for op in operations),
                Decimal(0),
            )
            positions = broker.client.account("perp")["positions"]
            if known:
                if len(positions) != 1 or positions[0]["symbol"] != "ETH" or Decimal(positions[0]["quantity"]) != known:
                    raise RuntimeError("Residual position does not match the strategy's exact known fills")
                cleanup = Order(
                    "native-strategy-qualification",
                    Asset("ETH", asset_type="crypto_future", leverage=1),
                    abs(known),
                    "sell" if known > 0 else "buy",
                    quote=Asset("USDC", asset_type="crypto"),
                    order_type="market",
                    time_in_force="ioc",
                    reduce_only=True,
                )
                record["prepared"].append(str(cleanup.identifier))
                save()
                broker.submit_order(cleanup)
                if broker.client.get_order(str(cleanup.identifier))["state"] != "filled":
                    raise RuntimeError("Residual close requires reconciliation")
            if broker.client.account("perp")["positions"] or broker.client.venue.info(
                {"type": "openOrders", "user": config["WALLET_ADDRESS"]}
            ):
                raise RuntimeError("Qualification cleanup left positions or open orders")
            record["failure_cleanup_flat"] = True
        except Exception as cleanup_error:
            record["cleanup_error"] = str(cleanup_error)
        save()
        raise
    finally:
        broker._stop_event.set()
        broker.client.close()


if __name__ == "__main__":
    main()
