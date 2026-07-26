"""
TickerAll hosted MT5 API broker for Lumibot.

Connects Lumibot to the hosted TickerAll MetaTrader 5 API
(https://tickerall.com), giving strategies access to the many global brokers
that run on MetaTrader 5 (Forex, metals, indices, CFDs, crypto). Because trading
goes through a hosted API, this broker runs on any OS with **no local
MetaTrader 5 terminal** installed - unlike the official MetaTrader5 Python
package, which is Windows-only and needs a running terminal.

Design notes
------------
- Modeled on the polling brokers in this codebase (``ccxt.py`` for the
  hosted-API shape, ``bitunix.py`` for the ``PollingStream`` fill loop).
- Order fills: market orders fill synchronously, so the fill event is dispatched
  from ``_submit_order``. Pending (limit/stop) orders are reported as NEW when
  placed; their eventual state is reconciled by the polling loop, and open
  positions are always kept in sync from the broker snapshot (the source of
  truth for what is actually open).
- Closing positions: ``close_position`` / ``sell_all`` use the hosted API's
  NATIVE position-close (by ticket) rather than the base broker's offsetting
  sell order. On a netting account this flattens cleanly and avoids leaving a
  transient phantom position in the tracker.
- Asset type: MT5 instruments are addressed by a single opaque symbol string
  (e.g. ``EURUSDm``, ``XAUUSDm``, ``BTCUSD``). This integration treats every MT5
  instrument under a single canonical asset type (``forex``) so that positions
  parsed from the broker match orders submitted by a strategy (Lumibot keys
  positions on symbol AND asset type). Construct assets as
  ``Asset("EURUSDm", asset_type="forex")``.
- Supported order types: ``market``, ``limit``, ``stop``. ``stop_limit`` and
  ``trailing_stop`` are not offered by the hosted API and are rejected with a
  clear message rather than silently mishandled.

License: MIT
"""

from __future__ import annotations

import os

from lumibot.tools.lumibot_logger import get_logger

from .broker import Broker

logger = get_logger(__name__)


def _colored(*args, **kwargs):
    try:
        from termcolor import colored
        return colored(*args, **kwargs)
    except Exception:
        return args[0]


class TickerAll(Broker):
    """Broker that trades MT5 accounts through the hosted TickerAll API."""

    NAME = "TickerAll"
    POLL_EVENT = "poll"
    DEFAULT_POLL_INTERVAL = 5  # seconds between polling cycles

    # Lumibot order types the hosted MT5 API can place.
    _SUPPORTED_ORDER_TYPES = {"market", "limit", "stop"}

    def __init__(
        self,
        config=None,
        data_source=None,
        connect_stream: bool = True,
        poll_interval: float | None = None,
        max_workers: int = 1,
        **kwargs,
    ):
        from lumibot.data_sources import TickerAllData

        if data_source is None:
            data_source = TickerAllData(config)
        if not isinstance(data_source, TickerAllData):
            raise ValueError(f"TickerAll broker's data source must be a TickerAllData, got {type(data_source)}")

        # Share the hosted-API client with the data source (single connection).
        self.api = data_source.api
        self.poll_interval = poll_interval or self.DEFAULT_POLL_INTERVAL
        # Tickets we cancelled ourselves, so the poll loop routes them to CANCELED
        # (rather than guessing they filled).
        self._cancelled_tickets: set[str] = set()

        # MT5 markets are ~24/5 (closed weekends). Default to always-tradeable
        # (the broker enforces real session hours and rejects out-of-session
        # orders); a user can pass config["MARKET"] to use a market calendar.
        cfg_market = config.get("MARKET") if isinstance(config, dict) else None
        desired_market = cfg_market or os.environ.get("MARKET") or "24/7"

        super().__init__(
            name=self.NAME,
            connect_stream=connect_stream,
            data_source=data_source,
            config=config,
            max_workers=max_workers,
            **kwargs,
        )
        # The base __init__ resets self.market (defaulting to NASDAQ for an
        # unknown data source); apply the MT5 market setting after it.
        self.market = desired_market

    # ── shared helpers ───────────────────────────────────────────────────────
    @property
    def account_id(self) -> str:
        return self.data_source.account_id

    def _resolve_symbol(self, asset) -> str:
        return self.data_source.resolve_symbol(asset)

    def _asset_from_symbol(self, symbol: str):
        from lumibot.data_sources.tickerall_data import CANONICAL_ASSET_TYPE
        from lumibot.entities import Asset

        return Asset(symbol=symbol, asset_type=CANONICAL_ASSET_TYPE)

    # ── balances ─────────────────────────────────────────────────────────────
    def _get_balances_at_broker(self, quote_asset, strategy) -> tuple:
        """Return (cash, positions_value, portfolio_value) as floats.

        cash            = account balance (booked)
        portfolio_value = account equity (balance + floating P&L) = net liquidation
        positions_value = portfolio_value - cash (value tied up in open positions)
        """
        detail = self.api.accounts.get(self.account_id)
        acc = detail.account
        if acc is None:
            logger.warning(
                f"TickerAll account snapshot unavailable ({detail.hint or 'offline'}); "
                "reporting zero balances."
            )
            return 0.0, 0.0, 0.0

        # NOTE: a balance of 0.0 is a valid account state - never treat 0 as "missing".
        cash = float(acc.balance)
        equity = float(acc.equity) if acc.equity is not None else cash
        positions_value = equity - cash
        return cash, positions_value, equity

    def get_historical_account_value(self) -> dict:
        # The hosted API does not expose a historical equity curve.
        return {"hourly": None, "daily": None}

    # ── positions ────────────────────────────────────────────────────────────
    def _net_position(self, plist, strategy):
        """Aggregate the broker positions for one symbol into a single net
        Lumibot position (BUY +, SELL -).

        A netting account has at most one position per symbol; a HEDGING account
        can hold several (long and short) at once. Lumibot keys positions on the
        asset (one per symbol), so we present the net exposure and remember every
        underlying ticket for closing.
        """
        from lumibot.entities import Position

        net = notional = total_vol = pnl = 0.0
        current = None
        tickets = []
        for p in plist:
            vol = float(p.volume)
            net += -vol if str(p.side).upper() == "SELL" else vol
            total_vol += vol
            if p.entry_price is not None:
                notional += vol * float(p.entry_price)
            if p.profit is not None:
                pnl += float(p.profit)
            if current is None and p.current_price is not None:
                current = float(p.current_price)
            tickets.append(int(p.ticket))

        asset = self._asset_from_symbol(plist[0].symbol)
        avg = (notional / total_vol) if total_vol else None
        pos = Position(strategy, asset, round(net, 8), avg_fill_price=avg)
        # Fields Lumibot attaches dynamically (not constructor args).
        if current is not None:
            pos.current_price = current
        pos.pnl = pnl
        pos.broker_tickets = tickets  # every underlying ticket (hedging may have >1)
        pos.broker_ticket = tickets[0] if tickets else None
        return pos

    def _pull_positions(self, strategy) -> list:
        detail = self.api.accounts.get(self.account_id)
        strategy_name = self._strategy_name_from_input(strategy) if strategy is not None else self._strategy_name
        by_symbol: dict = {}
        for p in detail.positions:
            by_symbol.setdefault(p.symbol, []).append(p)
        return [self._net_position(plist, strategy_name) for plist in by_symbol.values()]

    def _pull_position(self, strategy, asset):
        target = self._resolve_symbol(asset)
        detail = self.api.accounts.get(self.account_id)
        strategy_name = self._strategy_name_from_input(strategy) if strategy is not None else self._strategy_name
        plist = [p for p in detail.positions if p.symbol == target]
        return self._net_position(plist, strategy_name) if plist else None

    # ── orders (pull side) ───────────────────────────────────────────────────
    def _pull_broker_all_orders(self) -> list:
        """Open (pending) orders at the broker. Filled market orders are not
        listed here - they become positions immediately."""
        try:
            return list(self.api.orders.list_pending(self.account_id))
        except Exception as e:
            logger.warning(f"Could not pull pending orders from TickerAll: {e}")
            return []

    def _pull_broker_order(self, identifier: str):
        for o in self._pull_broker_all_orders():
            if str(o.ticket) == str(identifier):
                return o
        return None

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        from lumibot.entities import Order

        symbol = getattr(response, "symbol", None)
        asset = self._asset_from_symbol(symbol)
        side = "buy" if str(getattr(response, "side", "BUY")).upper() == "BUY" else "sell"
        volume = float(getattr(response, "volume", 0) or 0)

        # Map the MT5 pending-order type to a Lumibot order type.
        raw_type = str(getattr(response, "order_type", None) or getattr(response, "type", "")).lower()
        if "limit" in raw_type:
            order_type = Order.OrderType.LIMIT
        elif "stop" in raw_type:
            order_type = Order.OrderType.STOP
        else:
            order_type = Order.OrderType.LIMIT

        limit_price = getattr(response, "limit_price", None) or getattr(response, "price", None)
        stop_price = getattr(response, "price", None) if order_type == Order.OrderType.STOP else None
        sl = getattr(response, "stop_loss", None) or None
        tp = getattr(response, "take_profit", None) or None

        order = Order(
            strategy_name,
            asset,
            volume,
            side,
            order_type=order_type,
            limit_price=float(limit_price) if limit_price else None,
            stop_price=float(stop_price) if stop_price else None,
            secondary_stop_price=float(sl) if sl else None,
            secondary_limit_price=float(tp) if tp else None,
        )
        order.set_identifier(str(getattr(response, "ticket", "")))
        order.status = "open"
        order.update_raw(response)
        return order

    # ── order submission ─────────────────────────────────────────────────────
    @staticmethod
    def _extract_sl_tp(order):
        """Pull stop-loss / take-profit prices off a (possibly bracket) order."""
        sl = getattr(order, "secondary_stop_price", None) or getattr(order, "stop_loss_price", None)
        tp = getattr(order, "secondary_limit_price", None) or getattr(order, "take_profit_price", None)
        # Fall back to scanning child orders (bracket / OTO shape).
        for child in getattr(order, "child_orders", None) or []:
            ctype = str(getattr(child, "order_type", "")).lower()
            if tp is None and "limit" in ctype and getattr(child, "limit_price", None):
                tp = child.limit_price
            if sl is None and "stop" in ctype and getattr(child, "stop_price", None):
                sl = child.stop_price
        return (float(sl) if sl else None, float(tp) if tp else None)

    def _submit_order(self, order):

        # --- validate quantity ---
        if getattr(order, "quantity", None) is None or float(order.quantity) <= 0:
            logger.warning(f"Order {order} rejected: quantity must be > 0.")
            order.set_error("Order quantity must be greater than 0.")
            return order

        # --- map order type ---
        order_type = str(order.order_type).lower()
        if order_type not in self._SUPPORTED_ORDER_TYPES:
            msg = (
                f"Order type '{order_type}' is not supported by the TickerAll hosted MT5 API. "
                f"Supported types: market, limit, stop."
            )
            logger.error(_colored(msg, "red"))
            order.set_error(msg)
            return order

        side = "BUY" if str(order.side).lower().startswith("buy") else "SELL"
        symbol = self._resolve_symbol(order.asset)
        volume = float(order.quantity)
        sl, tp = self._extract_sl_tp(order)

        # Price for pending orders (limit -> limit_price, stop -> stop_price).
        price = None
        if order_type == "limit":
            price = float(order.limit_price) if order.limit_price is not None else None
        elif order_type == "stop":
            price = float(order.stop_price) if order.stop_price is not None else None

        try:
            result = self.api.orders.place(
                self.account_id,
                type=order_type,
                symbol=symbol,
                side=side,
                volume=volume,
                price=price,
                stop_loss=sl,
                take_profit=tp,
                comment=(order.tag or "lumibot"),
            )
        except Exception as e:
            msg = f"{order} did not go through. Error: {e}"
            logger.error(_colored(msg, "red"))
            order.set_error(e)
            return order

        order.set_identifier(str(result.ticket))
        order.update_raw(result)  # marks the order transmitted so the base tracks it

        if order_type == "market":
            # Market orders fill immediately; report the fill so on_filled_order fires.
            fill_price = self._fill_price(result, symbol)
            filled_qty = float(result.volume) if getattr(result, "volume", None) else volume
            self._report_market_fill(order, fill_price, filled_qty)
        else:
            # Pending order now resting at the broker.
            order.status = "new"
            self._dispatch(self.NEW_ORDER, order=order)

        return order

    def _fill_price(self, result, symbol: str) -> float:
        """Best available fill price for a just-filled market order."""
        if getattr(result, "price", None):
            return float(result.price)
        # Fall back to the resulting position's entry price, then last price.
        try:
            detail = self.api.accounts.get(self.account_id)
            for p in detail.positions:
                if int(p.ticket) == int(result.ticket) and p.entry_price is not None:
                    return float(p.entry_price)
        except Exception:
            pass
        last = self.data_source.get_last_price(self._asset_from_symbol(symbol))
        return float(last) if last else 0.0

    def _report_market_fill(self, order, price, quantity):
        """Dispatch NEW then FILLED for a synchronously-filled market order."""
        self._dispatch(self.NEW_ORDER, order=order)
        self._dispatch(self.FILLED_ORDER, order=order, price=price, filled_quantity=quantity)
        # Force an immediate position reconciliation from the broker snapshot so a
        # close-via-offset order does not leave a transient phantom position in the
        # tracker (on a netting account the offsetting fill nets to flat).
        stream = getattr(self, "stream", None)
        if stream is not None:
            stream.dispatch(self.POLL_EVENT)

    def _dispatch(self, event, **payload):
        """Queue an event on the polling stream, or process it inline if no stream."""
        stream = getattr(self, "stream", None)
        if stream is not None:
            stream.dispatch(event, **payload)
        else:
            order = payload.pop("order", None)
            self._process_trade_event(order, event, **payload)

    # ── cancel / modify ──────────────────────────────────────────────────────
    def cancel_order(self, order) -> None:
        if order is None or not order.identifier:
            return
        # Skip only if the order is already terminal. Do NOT skip on a local
        # "cancelling" status - the caller sets that right before calling this,
        # and is_canceled() treats "cancelling" as canceled. (Broker adapters
        # must not treat local CANCELLING as terminal before sending the cancel.)
        if order.is_filled() or str(order.status).lower() in ("canceled", "cancelled", "expired"):
            return
        try:
            self.api.orders.cancel_pending(self.account_id, int(order.identifier))
            self._cancelled_tickets.add(str(order.identifier))
            self._dispatch(self.CANCELED_ORDER, order=order)
        except Exception as e:
            logger.error(_colored(f"Could not cancel order {order}: {e}", "red"))

    def _modify_order(self, order, limit_price: float | None = None, stop_price: float | None = None):
        if order is None or not order.identifier:
            return
        new_price = limit_price if limit_price is not None else stop_price
        try:
            self.api.orders.modify_pending(
                self.account_id,
                int(order.identifier),
                price=float(new_price) if new_price is not None else None,
            )
            if limit_price is not None:
                order.limit_price = limit_price
            if stop_price is not None:
                order.stop_price = stop_price
        except Exception as e:
            logger.error(_colored(f"Could not modify order {order}: {e}", "red"))

    # ── closing positions ────────────────────────────────────────────────────
    def close_position(self, strategy_name: str, asset, fraction: float = 1.00):
        """Close a position using the hosted API's NATIVE position-close.

        MT5 positions are closed directly by ticket, not by an offsetting order.
        Using the native close (rather than the base broker's offsetting sell)
        flattens cleanly on a netting account and avoids the transient phantom
        position an offsetting-order fill would leave in the tracker.
        """
        symbol = self._resolve_symbol(asset)
        closed = 0
        for p in self.api.accounts.get(self.account_id).positions:
            if p.symbol != symbol:
                continue
            vol = round(float(p.volume) * float(fraction), 8) if fraction and float(fraction) < 1.0 else None
            try:
                self.api.positions.close(self.account_id, int(p.ticket), volume=vol)
                closed += 1
            except Exception as e:
                logger.error(_colored(f"Could not close position {p.ticket}: {e}", "red"))
        if closed:
            self._force_position_sync()
        return None

    def sell_all(self, strategy_name, cancel_open_orders=True, strategy=None, is_multileg=False):
        """Flatten all positions via the native position-close (see close_position)."""
        logger.warning(_colored(f"Closing all positions for {strategy_name}", "yellow"))
        if cancel_open_orders:
            self.cancel_open_orders(strategy_name)
        closed = 0
        for p in self.api.accounts.get(self.account_id).positions:
            try:
                self.api.positions.close(self.account_id, int(p.ticket))
                closed += 1
            except Exception as e:
                logger.error(_colored(f"Could not close position {p.ticket}: {e}", "red"))
        if closed:
            self._force_position_sync()

    def cancel_open_orders(self, strategy_name=None):
        """Cancel every pending order at the broker."""
        for o in self._pull_broker_all_orders():
            try:
                self.api.orders.cancel_pending(self.account_id, int(o.ticket))
                self._cancelled_tickets.add(str(o.ticket))
            except Exception as e:
                logger.error(_colored(f"Could not cancel pending order {o.ticket}: {e}", "red"))

    def _force_position_sync(self):
        """Reconcile the tracker from the broker snapshot right away."""
        stream = getattr(self, "stream", None)
        if stream is not None:
            stream.dispatch(self.POLL_EVENT)
        else:
            self.sync_positions(None)

    # ── polling stream ───────────────────────────────────────────────────────
    def _get_stream_object(self):
        from lumibot.trading_builtins import PollingStream

        return PollingStream(self.poll_interval)

    def _register_stream_events(self):
        broker = self

        @broker.stream.add_action(broker.POLL_EVENT)
        def on_poll():
            try:
                broker.do_polling()
            except Exception as e:
                logger.error(f"TickerAll polling error: {e}")

        @broker.stream.add_action(broker.NEW_ORDER)
        def on_new(order):
            try:
                broker._process_trade_event(order, broker.NEW_ORDER)
            except Exception as e:
                logger.error(f"TickerAll new-order event error: {e}")

        @broker.stream.add_action(broker.FILLED_ORDER)
        def on_fill(order, price, filled_quantity):
            try:
                broker._process_trade_event(
                    order, broker.FILLED_ORDER, price=price, filled_quantity=filled_quantity
                )
            except Exception as e:
                logger.error(f"TickerAll fill event error: {e}")

        @broker.stream.add_action(broker.CANCELED_ORDER)
        def on_cancel(order):
            try:
                broker._process_trade_event(order, broker.CANCELED_ORDER)
            except Exception as e:
                logger.error(f"TickerAll cancel event error: {e}")

    def _run_stream(self):
        self._stream_established()
        try:
            self.stream._run()
        except Exception as e:
            logger.error(f"Error running TickerAll polling stream: {e}")

    def do_polling(self):
        """Reconcile positions and pending orders once per poll cycle.

        Positions are the source of truth for what is open, so they are always
        synced from the broker snapshot. Pending (limit/stop) orders are
        reconciled against the tracked order set: newly-seen pending orders fire
        NEW; a tracked order that has left the pending list is reported FILLED
        when a matching position exists, otherwise CANCELED.
        """
        # 1) Positions: authoritative sync from the broker snapshot.
        self.sync_positions(None)

        # 2) Pending orders.
        raw_orders = self._pull_broker_all_orders()
        broker_ids = {str(o.ticket) for o in raw_orders}
        stored = {o.identifier: o for o in self.get_all_orders() if o.identifier}

        for o in raw_orders:
            parsed = self._parse_broker_order(o, strategy_name=self._strategy_name)
            if parsed is None:
                continue
            if parsed.identifier not in stored:
                # A pending order we have not tracked yet.
                self._process_trade_event(parsed, self.NEW_ORDER)

        # Tracked active orders that are no longer pending at the broker.
        open_positions = {p.asset.symbol for p in self._filled_positions.get_list()}
        for order in self.get_tracked_orders():
            if not order.identifier or not order.is_active():
                continue
            if order.identifier in broker_ids:
                continue  # still pending
            if str(order.identifier) in self._cancelled_tickets:
                # We cancelled it; the cancel event was already dispatched.
                self._cancelled_tickets.discard(str(order.identifier))
                continue
            # Left the pending list without our cancel: filled if a position
            # for the asset now exists, otherwise treat as canceled externally.
            if order.asset.symbol in open_positions:
                price = order.limit_price or order.stop_price or order.avg_fill_price or 0.0
                self._process_trade_event(
                    order, self.FILLED_ORDER, price=float(price), filled_quantity=float(order.quantity)
                )
            else:
                self._process_trade_event(order, self.CANCELED_ORDER)
