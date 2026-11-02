"""One restart-safe perpetual round trip through ordinary LumiBot strategy APIs."""

import hashlib
import os
from decimal import Decimal

from lumibot.entities import Asset, Order
from lumibot.strategies import Strategy


class MetaMaskPerpetualRoundTrip(Strategy):
    """Buy once, then close actual executed quantity on subsequent iterations.

    Reuse the same strategy name and parameters to resume. A different name
    intentionally starts a different round trip on the dedicated wallet.
    """

    parameters = {"symbol": "ETH", "quantity": "0.005", "sleeptime": "10S"}

    def initialize(self):
        if self.quote_asset != Asset("USDC", asset_type="crypto"):
            raise ValueError("Hyperliquid perpetuals require a USDC quote asset")
        self.asset = Asset(self.parameters["symbol"], asset_type="crypto_future", leverage=1)
        self.quantity = Decimal(str(self.parameters["quantity"]))
        if not self.quantity.is_finite() or self.quantity <= 0:
            raise ValueError("quantity must be finite, positive base units")
        self.set_market("24/7")
        self.sleeptime = self.parameters["sleeptime"]
        identity = "roundtrip-" + hashlib.sha256(self.name.encode()).hexdigest()
        self.entry_id, self.exit_prefix = identity + "-entry", identity + "-exit-"
        self.vars.entry_requested = False
        self.vars.pending_id = None
        self.vars.completed = False
        self.vars.result = None

    @staticmethod
    def _settled(order):
        outcome = order.submission_outcome or {}
        return (
            (order.is_filled() or order.is_canceled())
            and not outcome.get("reconciliation_required", True)
            and outcome.get("broker_state_certainty")
            in {"confirmed_submitted", "confirmed_rejected", "confirmed_not_submitted"}
        )

    @staticmethod
    def _filled_quantity(order):
        quantity = sum((Decimal(str(fill.quantity)) for fill in order.transactions), Decimal(0))
        if not 0 <= quantity <= Decimal(str(order.quantity)):
            raise RuntimeError("Order execution quantity conflicts with its requested quantity")
        if order.is_filled() and quantity != Decimal(str(order.quantity)):
            raise RuntimeError("Filled order is missing its complete execution history")
        return quantity

    def _validate_round_trip_order(self, order, side, reduce_only):
        if (
            order.asset != self.asset
            or order.asset.leverage != 1
            or order.quote != self.quote_asset
            or str(order.side) != side
            or order.reduce_only is not reduce_only
            or order.order_type != Order.OrderType.MARKET
            or order.time_in_force != "ioc"
        ):
            raise RuntimeError("Existing round-trip order conflicts with the strategy parameters")

    def on_trading_iteration(self):
        previously_completed = self.vars.completed
        self.vars.completed, self.vars.result = False, None
        orders = self.get_orders()
        by_id = {str(order.identifier): order for order in orders}
        if len(by_id) != len(orders):
            raise RuntimeError("Order inventory contains duplicate identities")
        pending = self.vars.pending_id
        if pending and (pending not in by_id or not self._settled(by_id[pending])):
            return
        self.vars.pending_id = None
        entry = by_id.get(self.entry_id)
        exits = [order for order in orders if str(order.identifier).startswith(self.exit_prefix)]
        if entry is None:
            if self.vars.entry_requested:
                return
            if exits or any(not self._settled(order) for order in orders):
                raise RuntimeError("Resolve existing orders before starting a new round trip")
            if any(position.asset != self.quote_asset and position.quantity for position in self.get_positions()):
                raise RuntimeError("A new round trip requires a flat dedicated wallet")
            price, cash = self.get_last_price(self.asset), self.get_cash()
            if price is None or cash is None:
                raise RuntimeError("Fresh market price and cash are required before entry")
            price, cash = Decimal(str(price)), Decimal(str(cash))
            if not price.is_finite() or price <= 0 or not cash.is_finite() or price * self.quantity > cash:
                raise RuntimeError("Entry exceeds available 1x collateral or market data is invalid")
            opening = self.create_order(self.asset, self.quantity, "buy", order_type="market", time_in_force="ioc")
            opening.identifier = self.entry_id
            self.vars.entry_requested, self.vars.pending_id = True, self.entry_id
            self.submit_order(opening)
            return

        self.vars.entry_requested = True
        self._validate_round_trip_order(entry, "buy", False)
        if Decimal(str(entry.quantity)) != self.quantity:
            raise RuntimeError("Existing entry conflicts with the strategy parameters")
        if not self._settled(entry):
            return
        owned_ids = {self.entry_id, *(str(order.identifier) for order in exits)}
        if any(str(order.identifier) not in owned_ids and not self._settled(order) for order in orders):
            raise RuntimeError("Resolve unrelated active orders before continuing this round trip")
        remaining = self._filled_quantity(entry)
        filled_entry = remaining
        try:
            exits.sort(key=lambda order: int(str(order.identifier).removeprefix(self.exit_prefix)))
        except ValueError as error:
            raise RuntimeError("Round-trip exit identity is malformed") from error
        for number, closing in enumerate(exits, 1):
            if str(closing.identifier) != f"{self.exit_prefix}{number}":
                raise RuntimeError("Round-trip exit history is missing or conflicting")
            self._validate_round_trip_order(closing, "sell", True)
            # Earlier closes may predate a later observed fill on the entry.
            if not 0 < Decimal(str(closing.quantity)) <= remaining:
                raise RuntimeError("Existing exit exceeds this round trip's available execution history")
            if not self._settled(closing):
                return
            filled = self._filled_quantity(closing)
            if not filled:
                raise RuntimeError("Exit did not execute; reconcile it before another closing intent")
            remaining -= filled

        positions = self.get_positions()
        if any(position.asset not in (self.asset, self.quote_asset) and position.quantity for position in positions):
            raise RuntimeError("Dedicated wallet contains an unrelated position")
        position = self.get_position(self.asset, broker_refresh=False)
        actual = Decimal(str(position.quantity)) if position else Decimal(0)
        if actual != remaining:
            raise RuntimeError("Fresh position does not match this round trip's exact residual fills")
        if not remaining:
            cash = self.get_cash()
            if cash is None or not Decimal(str(cash)).is_finite():
                raise RuntimeError("Fresh cash is required to confirm completion")
            self.vars.completed = True
            self.vars.result = "completed" if filled_entry else "entry_not_filled"
            if not previously_completed:
                self.log_message(f"Round trip {self.vars.result}; flat position, cash={cash}")
            return
        closing = self.create_order(
            self.asset, remaining, "sell", order_type="market", time_in_force="ioc", reduce_only=True
        )
        closing.identifier = f"{self.exit_prefix}{len(exits) + 1}"
        self.vars.pending_id = str(closing.identifier)
        self.submit_order(closing)

    def on_partially_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(f"Partial execution: {order.identifier}, quantity={quantity}, price={price}")

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(f"Execution: {order.identifier}, quantity={quantity}, price={price}")

    def on_canceled_order(self, order):
        self.log_message(f"Canceled remainder: {order.identifier}")


def main():
    if (
        os.environ.get("TRADING_BROKER", "").lower() != "metamask"
        or os.environ.get("METAMASK_NETWORK") != "testnet"
        or os.environ.get("METAMASK_MODE", "perp") != "perp"
    ):
        raise ValueError("Select TRADING_BROKER=metamask, METAMASK_NETWORK=testnet and perpetual mode explicitly")

    from lumibot.brokers import MetaMask
    from lumibot.credentials import get_default_broker
    from lumibot.traders import Trader

    broker = get_default_broker()
    if not isinstance(broker, MetaMask):
        raise ValueError("The selected broker is not MetaMask")
    trader = Trader()
    strategy = None
    try:
        strategy = MetaMaskPerpetualRoundTrip(
            broker=broker,
            name="metamask-perpetual-round-trip",
            quote_asset=Asset("USDC", asset_type="crypto"),
            benchmark_asset=None,
            analyze_backtest=False,
        )
        identity = broker.client.verify()
        # Journal order history cannot reveal foreign/manual venue orders.
        if broker.client.venue.info({"type": "openOrders", "user": identity["wallet"]}):
            raise RuntimeError("Resolve venue open orders before starting this dedicated-wallet example")
        market = broker.client.venue.market(strategy.parameters["symbol"])
        quantity = Decimal(str(strategy.parameters["quantity"]))
        step = Decimal(1).scaleb(-market.size_decimals)
        if not quantity.is_finite() or quantity <= 0 or quantity != quantity.quantize(step):
            raise ValueError("quantity must match the venue's base-unit lot precision")
        trader.add_strategy(strategy)
        trader.run_all(run_once=False)
    finally:
        trader.stop_all()
        if strategy is not None and strategy._executor.is_alive():
            strategy._executor.join()
        broker.close()


if __name__ == "__main__":
    main()
