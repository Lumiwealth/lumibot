"""One managed CCXT Pro client for a broker's public and private subscriptions."""

import asyncio
from copy import deepcopy
import json
import threading
import time

from lumibot._lazy_imports import LazyLogger

logger = LazyLogger(__name__)


class CcxtStream:
    def __init__(self, api, config, on_orders=None, on_balance=None, client_factory=None, on_disconnect=None):
        self.api = api
        self.config = dict(config)
        self.on_orders = on_orders
        self.on_balance = on_balance
        self.client_factory = client_factory
        self.on_disconnect = on_disconnect
        self._private_request_scopes = {}
        self._private_acks = {}
        self._condition = threading.Condition()
        self._ready = threading.Event()
        self._thread = None
        self._loop = None
        self._client = None
        self._tasks = {}
        self._states = {}
        self._stopping = False

    def start(self):
        with self._condition:
            if self._stopping:
                raise RuntimeError("CCXT stream is closed")
            if self._thread is None:
                self._thread = threading.Thread(target=self._run, name="ccxt-stream", daemon=True)
                self._thread.start()
        if not self._ready.wait(5):
            raise TimeoutError("CCXT stream event loop did not start")

    def _run(self):
        asyncio.run(self._serve())

    async def _serve(self):
        self._loop = asyncio.get_running_loop()
        self._shutdown = asyncio.Event()
        self._ready.set()
        try:
            await self._shutdown.wait()
        finally:
            for task in self._tasks.values():
                task.cancel()
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
            if self._client is not None:
                await self._client.close()
            with self._condition:
                self._condition.notify_all()

    def stop(self):
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._shutdown.set)
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                raise TimeoutError("CCXT stream client did not close")

    def watch_params(self, method, symbol=None):
        market = (self.api.markets or {}).get(symbol) or {}
        if self.api.id == "phemex" and (market.get("info") or {}).get("type") == "PerpetualV2":
            channels = {"watch_ticker": "perp_market24h_pack_p.subscribe",
                        "watch_order_book": "orderbook_p.subscribe", "watch_trades": "trade_p.subscribe",
                        "watch_ohlcv": "kline_p.subscribe"}
            if method in channels:
                return {"method": channels[method]}
        return {}

    @staticmethod
    def _key(method, kwargs):
        return method, json.dumps(kwargs, sort_keys=True)

    def subscribe(self, method, **kwargs):
        params = {**self.watch_params(method, kwargs.get("symbol")), **(kwargs.pop("params", None) or {})}
        if params:
            kwargs["params"] = params
        key = self._key(method, kwargs)
        self.start()
        with self._condition:
            state = self._states.setdefault(key, {})
            if key not in self._tasks and not state.get("pending"):
                state["pending"] = True
                self._loop.call_soon_threadsafe(self._spawn, key, method, kwargs)
        return key

    def snapshot(self, method, *, timeout=10, max_age=5, require_snapshot=False, **kwargs):
        key = self.subscribe(method, **kwargs)
        deadline = time.monotonic() + timeout
        with self._condition:
            state = self._states[key]
            while not self._stopping:
                age = time.monotonic() - state.get("at", 0)
                if "value" in state and state.get("error") is None and (max_age is None or age <= max_age):
                    if not require_snapshot or state.get("snapshot_seen"):
                        return deepcopy(state["value"])
                if state.get("terminal_error"):
                    raise state["error"]
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"No usable CCXT {method} snapshot for {kwargs.get('symbol', 'account')}")
                self._condition.wait(remaining)
        raise RuntimeError("CCXT stream closed while waiting for data")

    def wait_subscribed(self, method, *, params, timeout=10):
        """Wait for a Phemex channel acknowledgment, not an account snapshot.

        The provider explicitly allows zero initial AOP snapshots. REST must
        establish account state; an acknowledgment only establishes transport.
        """
        key = self.subscribe(method, params=params)
        scope = self._key("private", params)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while not self._stopping:
                ack = self._private_acks.get(scope, {})
                if ack.get("error") is not None:
                    raise ack["error"]
                if ack.get("ready"):
                    return
                state = self._states.get(key, {})
                if state.get("terminal_error"):
                    raise state["error"]
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("Phemex private subscription was not acknowledged")
                self._condition.wait(remaining)
        raise RuntimeError("CCXT stream closed while subscribing")

    def _spawn(self, key, method, kwargs):
        self._tasks[key] = asyncio.create_task(self._watch(key, method, kwargs))

    def _get_client(self):
        if self._client is not None:
            return self._client
        if self.client_factory is not None:
            client = self.client_factory()
        else:
            import ccxt.pro

            exchange_class = getattr(ccxt.pro, self.api.id, None)
            if exchange_class is None:
                import ccxt

                raise ccxt.NotSupported(f"No CCXT Pro adapter for {self.api.id}")
            if self.api.id == "phemex":
                stream = self

                class PhemexClient(exchange_class):
                    def handle_message(self, socket, message):
                        super().handle_message(socket, message)
                        stream._phemex_message(self, message)

                exchange_class = PhemexClient
            client = exchange_class({**self.config, "newUpdates": True})
            from lumibot.tools.ccxt_partner_ids import apply_partner_config

            apply_partner_config(client)
        client.set_sandbox_mode(self.config.get("sandbox", True))
        client.set_markets(self.api.markets or {}, self.api.currencies or {})
        self._client = client
        return client

    async def _watch(self, key, method, kwargs):
        delay = 0.2
        while not self._stopping:
            try:
                client = self._get_client()
                watch = getattr(client, method, None)
                if watch is None:
                    import ccxt

                    raise ccxt.NotSupported(f"{self.api.id} has no {method} method")
                watch_kwargs = kwargs
                request_id = None
                if self.api.id == "phemex" and method in ("watch_orders", "watch_balance"):
                    # Upstream uses seconds as request IDs for all private
                    # channels. Distinct IDs let acknowledgments identify the
                    # subscribed family even when channels start concurrently.
                    request_id = client.request_id()
                    with self._condition:
                        self._private_request_scopes[request_id] = self._key("private", kwargs.get("params", {}))
                    watch_kwargs = {**kwargs, "params": {**kwargs.get("params", {}), "id": request_id}}
                try:
                    result = await watch(**watch_kwargs)
                finally:
                    if request_id is not None:
                        with self._condition:
                            self._private_request_scopes.pop(request_id, None)
                # Native frames also deliver empty account snapshots and fix
                # Phemex's V2 USDC generic-order channel resolution in CCXT.
                if not (self.api.id == "phemex" and method in ("watch_orders", "watch_balance")):
                    self._publish(key, result)
                delay = 0.2
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                import ccxt

                with self._condition:
                    state = self._states[key]
                    state["error"] = exc
                    state["snapshot_seen"] = False
                    state["terminal_error"] = isinstance(exc, (ccxt.NotSupported, ccxt.AuthenticationError,
                                                                ccxt.ArgumentsRequired, ccxt.BadSymbol))
                    self._condition.notify_all()
                    disconnected = isinstance(exc, (ccxt.NetworkError, ConnectionError))
                    if disconnected:
                        self._private_acks.clear()
                if disconnected and self.on_disconnect is not None:
                    self.on_disconnect()
                logger.warning("CCXT %s subscription failed: %s", method, type(exc).__name__)
                if state["terminal_error"]:
                    return
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10)

    def _publish(self, key, value, snapshot=False, scope=None):
        method, encoded = key
        if method == "watch_orders" and self.on_orders:
            self.on_orders(value, scope if scope is not None else json.loads(encoded).get("params", {}), snapshot)
        elif method == "watch_balance" and self.on_balance:
            self.on_balance(value)
        with self._condition:
            state = self._states.setdefault(key, {})
            state.update(value=deepcopy(value), at=time.monotonic(), error=None)
            state["snapshot_seen"] = state.get("snapshot_seen", False) or snapshot
            self._condition.notify_all()

    def _phemex_message(self, client, message):
        with self._condition:
            scope = self._private_request_scopes.pop(message.get("id"), None)
            if scope is not None:
                success = (message.get("result") or {}).get("status") == "success" and not message.get("error")
                if success:
                    self._private_acks[scope] = {"ready": True}
                else:
                    import ccxt

                    self._private_acks[scope] = {"error": ccxt.ExchangeError("Phemex rejected the private subscription")}
                self._condition.notify_all()
        for field, balance_scope in (("wallets", {"type": "spot"}),
                                     ("accounts", {"type": "swap"}),
                                     ("accounts_p", {"type": "swap", "settle": "USDT"})):
            if field not in message:
                continue
            key = self._key("watch_balance", {"params": balance_scope})
            snapshot = message.get("type") == "snapshot"
            with self._condition:
                # wo snapshots replace all spot wallets; AOP snapshots replace
                # one collateral account, not the entire perpetual family.
                previous = {} if snapshot and field == "wallets" else deepcopy(
                    self._states.get(key, {}).get("value", {}))
            rows = {code: value for code, value in previous.items()
                    if isinstance(value, dict) and "total" in value}
            # CCXT has already parsed the frame. Keep each account family's
            # values separate from its shared, cross-family balance cache.
            for raw_balance in message[field]:
                code = client.safe_currency_code(raw_balance["currency"])
                rows[code] = deepcopy(client.balance[code])
            balance = {**rows, **{name: {code: row.get(name) for code, row in rows.items()}
                                  for name in ("free", "used", "total")}}
            self._publish(key, balance, snapshot=snapshot)
        if "orders_p" in message:
            raw, scope = message["orders_p"], {"type": "swap", "settle": "USDT"}
        elif "orders" in message:
            raw = message["orders"]
            scope = {"type": "spot"} if isinstance(raw, dict) else {"type": "swap"}
        else:
            return
        if isinstance(raw, dict):
            orders = [client.parse_order(item) for item in raw.get("open", []) + raw.get("closed", [])]
        else:
            orders = [client.parse_ws_swap_order(item) for item in raw]
        # USDT here selects CCXT's V2 protocol, not a settlement-currency
        # filter: the aop_p account channel contains every V2 settlement.
        key = self._key("watch_orders", {"params": scope})
        if scope["type"] == "spot":
            self._publish(key, orders, snapshot=message.get("type") == "snapshot")
            return
        account_field = "accounts_p" if "orders_p" in message else "accounts"
        currencies = {client.safe_currency_code(row["currency"])
                      for row in message.get(account_field, [])}
        groups = {currency: [] for currency in currencies}
        for order in orders:
            market = (self.api.markets or {}).get(order.get("symbol")) or {}
            currency = market.get("settle")
            if currency is None and len(currencies) == 1:
                currency = next(iter(currencies))
            groups.setdefault(currency, []).append(order)
        if not groups:
            self._publish(key, orders, snapshot=message.get("type") == "snapshot")
        for currency, account_orders in groups.items():
            account_scope = {**scope, "currency": currency} if currency is not None else scope
            self._publish(key, account_orders, snapshot=message.get("type") == "snapshot",
                          scope=account_scope)
