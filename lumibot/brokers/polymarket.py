from __future__ import annotations

import json
import os
import queue
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from lumibot.brokers.broker import Broker, LumibotBrokerAPIError
from lumibot.data_sources.polymarket_data import PolymarketData
from lumibot.entities import Asset, Order, Position
from lumibot.trading_builtins import CustomStream, PollingStream


POLYMARKET_SECRET_KEYS = {
    "PRIVATE_KEY",
    "CLOB_API_KEY",
    "CLOB_API_SECRET",
    "CLOB_API_PASSPHRASE",
    "RELAYER_API_KEY",
    "RELAYER_API_KEY_ADDRESS",
    "API_CREDENTIALS_JSON",
}


@dataclass(frozen=True)
class PolymarketCredentials:
    private_key: str | None = None
    wallet_address: str | None = None
    clob_api_key: str | None = None
    clob_api_secret: str | None = None
    clob_api_passphrase: str | None = None
    relayer_api_key: str | None = None
    relayer_api_key_address: str | None = None
    api_credentials_json: str | None = None
    builder_code: str | None = None


class PolymarketCredentialStore:
    """Loads Polymarket CLOB credentials without logging raw secret values."""

    ENV_PREFIX = "POLYMARKET_"

    def __init__(self, config: dict | None = None):
        self.config = config or {}

    def load(self) -> PolymarketCredentials:
        return PolymarketCredentials(
            private_key=self._get("PRIVATE_KEY"),
            wallet_address=self._get("WALLET_ADDRESS"),
            clob_api_key=self._get("CLOB_API_KEY"),
            clob_api_secret=self._get("CLOB_API_SECRET"),
            clob_api_passphrase=self._get("CLOB_API_PASSPHRASE"),
            relayer_api_key=self._get("RELAYER_API_KEY"),
            relayer_api_key_address=self._get("RELAYER_API_KEY_ADDRESS"),
            api_credentials_json=self._get("API_CREDENTIALS_JSON"),
            builder_code=self._get("BUILDER_CODE"),
        )

    def redacted_config(self) -> dict:
        redacted = {}
        for key, value in self.config.items():
            if self._is_secret_key(key):
                redacted[key] = "<redacted>" if value else value
            else:
                redacted[key] = value
        return redacted

    def _get(self, suffix: str) -> str | None:
        return self.config.get(suffix) or self.config.get(f"{self.ENV_PREFIX}{suffix}") or os.environ.get(
            f"{self.ENV_PREFIX}{suffix}"
        )

    @staticmethod
    def _is_secret_key(key: str) -> bool:
        normalized = key.removeprefix("POLYMARKET_")
        return normalized in POLYMARKET_SECRET_KEYS


class PolymarketCLOBStream(CustomStream):
    """LumiBot stream bridge for Polymarket market/user events.

    The class exposes event handlers that are fully unit-testable with fake payloads. Live WebSocket connection support
    is intentionally conservative and optional because authenticated stream connection details depend on the beta SDK
    and generated CLOB credentials.
    """

    def __init__(
        self,
        broker: "Polymarket",
        *,
        polling_interval: float = 5.0,
        use_websocket: bool = True,
    ):
        super().__init__()
        self.broker = broker
        self.polling_interval = polling_interval
        self.use_websocket = use_websocket

    def handle_market_event(self, event: Any) -> None:
        self.broker.data_source.apply_market_event(event)

    def handle_user_event(self, event: Any) -> None:
        payload = PolymarketData._model_to_dict(event)
        parsed = self.broker._parse_broker_order(payload, self.broker._strategy_name or self.broker.name)
        status = parsed.status
        if status == Order.OrderStatus.FILLED:
            self.dispatch(self.broker.FILLED_ORDER, order=parsed, price=parsed.avg_fill_price, quantity=parsed.quantity)
        elif status == Order.OrderStatus.PARTIALLY_FILLED:
            self.dispatch(self.broker.PARTIALLY_FILLED_ORDER, order=parsed)
        elif status == Order.OrderStatus.CANCELED:
            self.dispatch(self.broker.CANCELED_ORDER, order=parsed)
        elif status in (Order.OrderStatus.ERROR, Order.OrderStatus.REJECTED):
            self.dispatch(self.broker.ERROR_ORDER, order=parsed)
        else:
            self.dispatch(self.broker.NEW_ORDER, order=parsed)

    def _run(self):
        # The SDK stream implementation is async-only. Until live credentials and SDK availability are verified, keep
        # the stream thread useful as a reconciliation loop and route event handling through tested handler methods.
        last_poll = 0.0
        while not self._stop_event.is_set():
            timeout = min(max(self.polling_interval, 0.1), 0.5)
            try:
                event, payload = self._queue.get(timeout=timeout)
                self._process_queue_event(event, payload)
                self._queue.task_done()
                continue
            except queue.Empty:
                pass
            except Exception:
                self.broker.logger.exception("Polymarket stream event processing failed")

            if time.monotonic() - last_poll < self.polling_interval:
                continue
            try:
                self.broker.do_polling()
                last_poll = time.monotonic()
            except Exception:
                self.broker.logger.exception("Polymarket stream reconciliation failed")


class Polymarket(Broker):
    NAME = "Polymarket"

    def __init__(
        self,
        config: dict | None = None,
        data_source: PolymarketData | None = None,
        *,
        polling_interval: float = 1.0,
        use_websocket: bool = True,
        connect_stream: bool = True,
        max_workers: int = 20,
        secure_client: Any | None = None,
        data_api_client: Any | None = None,
    ):
        self.config = config or {}
        self.credential_store = PolymarketCredentialStore(self.config)
        self.credentials = self.credential_store.load()
        self.polling_interval = polling_interval
        self.use_websocket = use_websocket
        self._secure_client = secure_client or self.config.get("secure_client")
        self._data_api_client = data_api_client or httpx.Client(timeout=float(self.config.get("TIMEOUT", 10.0)))
        self._owns_data_api_client = data_api_client is None
        self._clients_initialized = secure_client is not None
        self._trading_ready = False
        self._recent_order_cache: dict[str, Any] = {}
        self._recent_trade_cache: dict[str, Any] = {}

        if data_source is None:
            data_source = PolymarketData(config=self.config)

        broker_config = dict(self.config)
        broker_config.setdefault("MARKET", "24/7")
        super().__init__(
            name=self.NAME,
            data_source=data_source,
            config=broker_config,
            connect_stream=connect_stream,
            max_workers=max_workers,
        )

    def cleanup_streams(self):
        try:
            if self._owns_data_api_client and hasattr(self._data_api_client, "close"):
                self._data_api_client.close()
        finally:
            super().cleanup_streams()

    def _initialize_clients(self) -> Any:
        if self._clients_initialized and self._secure_client is not None:
            return self._secure_client

        if not self.credentials.private_key:
            raise LumibotBrokerAPIError(
                "Missing POLYMARKET_PRIVATE_KEY. Authenticated Polymarket CLOB reads/trading require a local wallet "
                "private key or session signer."
            )

        try:
            from py_clob_client_v2.client import ClobClient
            from py_clob_client_v2.clob_types import ApiCreds, BuilderConfig
            from py_clob_client_v2.constants import POLYGON
            from py_clob_client_v2.order_utils.model.signature_type_v2 import SignatureTypeV2
        except Exception as exc:
            raise LumibotBrokerAPIError(
                "The Polymarket CLOB SDK is required for authenticated operations. Install `py-clob-client-v2` and "
                "provide POLYMARKET_PRIVATE_KEY plus wallet/CLOB credentials."
            ) from exc

        creds = self._build_api_creds(ApiCreds)
        builder_config = None
        if self.credentials.builder_code:
            builder_config = BuilderConfig(builder_code=self.credentials.builder_code)

        signature_type = int(self.config.get("SIGNATURE_TYPE", SignatureTypeV2.POLY_1271))
        self._secure_client = ClobClient(
            self.config.get("CLOB_URL", PolymarketData.CLOB_URL),
            chain_id=int(self.config.get("CHAIN_ID", POLYGON)),
            key=self.credentials.private_key,
            creds=creds,
            signature_type=signature_type,
            funder=self.credentials.wallet_address,
            builder_config=builder_config,
            retry_on_error=True,
        )
        if creds is None:
            derived = self._secure_client.create_or_derive_api_key()
            self._secure_client.set_api_creds(derived)
            self._derived_api_creds = derived
        self._clients_initialized = True
        return self._secure_client

    def _build_api_creds(self, api_creds_cls):
        if self.credentials.api_credentials_json:
            try:
                raw = json.loads(self.credentials.api_credentials_json)
            except json.JSONDecodeError as exc:
                raise LumibotBrokerAPIError("POLYMARKET_API_CREDENTIALS_JSON is not valid JSON") from exc
            return api_creds_cls(
                api_key=raw.get("api_key") or raw.get("key"),
                api_secret=raw.get("api_secret") or raw.get("secret"),
                api_passphrase=raw.get("api_passphrase") or raw.get("passphrase"),
            )
        if (
            self.credentials.clob_api_key
            and self.credentials.clob_api_secret
            and self.credentials.clob_api_passphrase
        ):
            return api_creds_cls(
                api_key=self.credentials.clob_api_key,
                api_secret=self.credentials.clob_api_secret,
                api_passphrase=self.credentials.clob_api_passphrase,
            )
        return None

    def _ensure_trading_ready(self) -> None:
        if self._trading_ready:
            return
        secure_client = self._initialize_clients()
        if self._truthy(self.config.get("AUTO_APPROVE") or os.environ.get("POLYMARKET_AUTO_APPROVE")):
            try:
                from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

                secure_client.update_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
            except Exception as exc:
                raise LumibotBrokerAPIError(f"Polymarket trading approval setup failed: {exc}") from exc
        self._trading_ready = True

    def _get_balances_at_broker(self, quote_asset: Asset, strategy):
        positions_value = self._get_positions_value()
        cash = self._get_collateral_cash()
        portfolio_value = cash + positions_value

        secure_client = self._maybe_secure_client()
        if secure_client is not None:
            portfolio_values = self._call_if_exists(secure_client, "get_portfolio_values")
            total = self._first_present(portfolio_values, "total", "portfolio_value", "portfolioValue", "value")
            if total is not None:
                portfolio_value = self._to_float(total)
        return cash, positions_value, portfolio_value

    def get_historical_account_value(self) -> dict:
        secure_client = self._maybe_secure_client()
        if secure_client is None:
            return {}
        values = self._call_if_exists(secure_client, "get_portfolio_values")
        return PolymarketData._model_to_dict(values) if values is not None else {}

    def _get_stream_object(self):
        if self.use_websocket:
            return PolymarketCLOBStream(self, polling_interval=self.polling_interval, use_websocket=True)
        return PollingStream(self.polling_interval)

    def _register_stream_events(self):
        if isinstance(self.stream, PollingStream):
            @self.stream.add_action(self.stream.POLL_EVENT)
            def _poll_action():
                self.do_polling()

    def _run_stream(self):
        self.stream.run(self.name)

    def _pull_positions(self, strategy):
        rows = self._get_positions_payload()
        positions = []
        for row in rows:
            token_id = self._first_present(row, "asset", "token_id", "tokenId", "asset_id", "assetId")
            if not token_id:
                continue
            asset = Asset(str(token_id), asset_type=Asset.AssetType.PREDICTION_CONTRACT, precision="0.000001")
            quantity = self._to_float(self._first_present(row, "size", "quantity", "shares") or 0.0)
            avg_price = self._optional_float(self._first_present(row, "avgPrice", "avg_price", "averagePrice"))
            position = Position(strategy, asset, quantity, avg_fill_price=avg_price)
            position.current_price = self._optional_float(self._first_present(row, "curPrice", "currentPrice", "price"))
            position.market_value = self._optional_float(
                self._first_present(row, "currentValue", "marketValue", "value")
            )
            position.pnl = self._optional_float(self._first_present(row, "cashPnl", "pnl"))
            position.pnl_percent = self._optional_float(self._first_present(row, "percentPnl", "pnlPercent"))
            position.outcome = self._first_present(row, "outcome")
            position.condition_id = self._first_present(row, "conditionId", "condition_id")
            position.slug = self._first_present(row, "slug", "marketSlug")
            position._raw = row
            positions.append(position)
        return positions

    def _pull_position(self, strategy, asset):
        token_id = asset.symbol if isinstance(asset, Asset) else str(asset)
        for position in self._pull_positions(strategy):
            if str(position.asset.symbol) == str(token_id):
                return position
        return None

    def _pull_broker_all_orders(self):
        secure_client = self._initialize_clients()
        orders = self._call_if_exists(secure_client, "get_open_orders")
        return self._listify(orders)

    def _pull_broker_order(self, identifier):
        secure_client = self._initialize_clients()
        order = self._call_if_exists(secure_client, "get_order", identifier)
        if order is None:
            return self._recent_order_cache.get(str(identifier)) or self._recent_trade_cache.get(str(identifier))
        return order

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object=None):
        payload = PolymarketData._model_to_dict(response) or {}
        token_id = self._first_present(payload, "asset_id", "assetId", "token_id", "tokenId", "asset", "market")
        asset = Asset(str(token_id or "UNKNOWN"), asset_type=Asset.AssetType.PREDICTION_CONTRACT, precision="0.000001")
        side = self._map_side(self._first_present(payload, "side", "direction"))
        price = self._optional_float(self._first_present(payload, "price", "limitPrice", "average_price", "avgPrice"))
        quantity = self._optional_float(
            self._first_present(payload, "size", "original_size", "originalSize", "shares", "quantity")
        )
        if quantity is None:
            quantity = self._optional_float(self._first_present(payload, "matched_size", "matchedSize")) or 0.0

        order_type = self._map_order_type(self._first_present(payload, "order_type", "orderType", "type"))
        order = Order(
            strategy_name,
            asset=asset,
            quantity=quantity,
            side=side,
            limit_price=price if order_type == Order.OrderType.LIMIT else None,
            order_type=order_type,
            identifier=str(self._first_present(payload, "id", "order_id", "orderId") or ""),
            status=self._map_status(self._first_present(payload, "status", "state", "event_type", "eventType", "type")),
            avg_fill_price=self._optional_float(self._first_present(payload, "average_price", "avgPrice", "price")),
            time_in_force=str(self._first_present(payload, "time_in_force", "timeInForce", "tif") or "gtc").lower(),
        )
        order._raw = payload
        order.broker_create_date = self._first_present(payload, "created_at", "createdAt", "created")
        order.broker_update_date = self._first_present(payload, "updated_at", "updatedAt", "updated")
        return order

    def _submit_order(self, order):
        self._validate_order(order)
        if order.order_type == Order.OrderType.MARKET:
            return self._submit_market_order(order)
        if order.order_type == Order.OrderType.LIMIT:
            return self._submit_limit_order(order)
        raise ValueError(f"Unsupported Polymarket order type: {order.order_type}")

    def _submit_market_order(self, order):
        self._ensure_trading_ready()
        token_id = self._token_id(order.asset)
        side = str(order.side).upper()
        params = order.custom_params or {}
        explicit_tif = params.get("order_type") or params.get("time_in_force")
        order_tif = str(order.time_in_force or "").upper()
        tif = str(explicit_tif or (order_tif if order_tif in {"FAK", "FOK"} else "FAK")).upper()
        if tif not in {"FAK", "FOK"}:
            raise ValueError("Polymarket market orders require FAK or FOK time-in-force")

        kwargs = {"token_id": token_id, "side": side, "order_type": tif}
        if side == "BUY":
            amount = params.get("amount")
            if amount is None:
                raise ValueError('Polymarket BUY market orders require custom_params["amount"] in dollars')
            self._validate_notional_cap(amount)
            kwargs["amount"] = str(amount)
            max_spend = params.get("max_spend")
            if max_spend is not None:
                kwargs["max_spend"] = str(max_spend)
            max_price = params.get("max_price") or params.get("price")
            if max_price is not None:
                kwargs["price"] = str(max_price)
        else:
            shares = params.get("shares") or order.quantity
            if shares is None:
                raise ValueError("Polymarket SELL market orders require shares or order.quantity")
            kwargs["shares"] = str(shares)
            min_price = params.get("min_price") or params.get("price")
            if min_price is not None:
                kwargs["price"] = str(min_price)

        response = self._create_and_post_market_order(**kwargs)
        return self._order_from_submit_response(response, order)

    def _submit_limit_order(self, order):
        self._ensure_trading_ready()
        token_id = self._token_id(order.asset)
        if order.limit_price is None:
            raise ValueError("Polymarket limit orders require limit_price")
        self._validate_tick_size(token_id, order.limit_price)
        response = self._create_and_post_limit_order(
            token_id=token_id,
            side=str(order.side).upper(),
            price=str(order.limit_price),
            size=str(order.quantity),
            time_in_force=str(order.time_in_force or "GTC").upper(),
        )
        return self._order_from_submit_response(response, order)

    def cancel_order(self, order):
        identifier = getattr(order, "identifier", order)
        if not identifier:
            raise ValueError("Cannot cancel Polymarket order without an identifier")
        secure_client = self._initialize_clients()
        try:
            from py_clob_client_v2.clob_types import OrderPayload
        except Exception as exc:
            if hasattr(secure_client, "cancel_order"):
                response = secure_client.cancel_order(str(identifier))
                order.status = Order.OrderStatus.CANCELED
                return response
            raise LumibotBrokerAPIError("py-clob-client-v2 is required to cancel Polymarket orders") from exc
        response = secure_client.cancel_order(OrderPayload(orderID=str(identifier)))
        order.status = Order.OrderStatus.CANCELED
        return response

    def _modify_order(self, order, limit_price=None, stop_price=None):
        raise NotImplementedError("Polymarket does not support native order modification yet; use cancel-replace.")

    def do_polling(self):
        try:
            self._pull_positions(self._strategy_name or self.name)
        except Exception:
            self.logger.debug("Polymarket position polling failed", exc_info=True)
        try:
            for raw in self._pull_broker_all_orders():
                parsed = self._parse_broker_order(raw, self._strategy_name or self.name)
                if parsed.identifier:
                    self._recent_order_cache[parsed.identifier] = raw
        except Exception:
            self.logger.debug("Polymarket order polling failed", exc_info=True)

    def _get_positions_payload(self) -> list[dict]:
        secure_client = self._maybe_secure_client()
        if secure_client is not None:
            rows = self._call_if_exists(secure_client, "list_positions")
            if rows is not None:
                return self._listify(rows)

        wallet = self.credentials.wallet_address
        if not wallet:
            raise LumibotBrokerAPIError("Missing POLYMARKET_WALLET_ADDRESS for Polymarket position reads")
        payload = self._data_api_get("/positions", params={"user": wallet})
        return self._listify(payload)

    def _get_positions_value(self) -> float:
        secure_client = self._maybe_secure_client()
        if secure_client is not None:
            values = self._call_if_exists(secure_client, "get_portfolio_values")
            value = self._first_present(values, "value", "positions_value", "positionsValue", "current_value")
            if value is not None:
                return self._to_float(value)

        wallet = self.credentials.wallet_address
        if not wallet:
            return 0.0
        try:
            payload = self._data_api_get("/value", params={"user": wallet})
        except Exception:
            return 0.0
        rows = self._listify(payload)
        if not rows:
            return 0.0
        return self._to_float(self._first_present(rows[0], "value") or 0.0)

    def _get_collateral_cash(self) -> float:
        secure_client = self._maybe_secure_client()
        if secure_client is not None:
            for method_name in ("get_balance_allowance", "get_balance", "get_collateral_balance"):
                if method_name == "get_balance_allowance":
                    try:
                        from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams

                        value = secure_client.get_balance_allowance(
                            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                        )
                    except Exception:
                        value = self._call_if_exists(secure_client, "get_balance_allowance")
                else:
                    value = self._call_if_exists(secure_client, method_name)
                balance = self._first_present(value, "balance", "available", "cash", "collateral", "collateral_balance")
                if balance is not None:
                    return self._to_float(balance)
        override = self.config.get("CASH_BALANCE") or self.config.get("POLYMARKET_CASH_BALANCE")
        return self._to_float(override or 0.0)

    def _data_api_get(self, path: str, params: dict | None = None) -> Any:
        url = f"https://data-api.polymarket.com/{path.lstrip('/')}"
        response = self._data_api_client.get(url, params=params or {})
        if isinstance(response, (dict, list)):
            return response
        if hasattr(response, "raise_for_status"):
            response.raise_for_status()
        return response.json()

    def _maybe_secure_client(self):
        if self._secure_client is not None:
            return self._secure_client
        if self.credentials.private_key:
            return self._initialize_clients()
        return None

    def _create_and_post_market_order(self, **kwargs):
        secure_client = self._initialize_clients()
        try:
            from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType
        except Exception as exc:
            if hasattr(secure_client, "create_and_post_market_order"):
                return secure_client.create_and_post_market_order(kwargs)
            raise LumibotBrokerAPIError("py-clob-client-v2 is required for Polymarket market orders") from exc

        order_type = getattr(OrderType, str(kwargs.get("order_type") or "FOK").upper())
        amount = kwargs.get("amount") if kwargs.get("side") == "BUY" else kwargs.get("shares")
        order_kwargs = dict(
            token_id=kwargs["token_id"],
            amount=float(amount),
            side=kwargs["side"],
            price=float(kwargs.get("price") or 0),
            order_type=order_type,
        )
        if self.credentials.builder_code:
            order_kwargs["builder_code"] = self.credentials.builder_code
        order_args = MarketOrderArgs(**order_kwargs)
        return secure_client.create_and_post_market_order(order_args, order_type=order_type)

    def _create_and_post_limit_order(self, **kwargs):
        secure_client = self._initialize_clients()
        try:
            from py_clob_client_v2.clob_types import OrderArgs, OrderType
        except Exception as exc:
            if hasattr(secure_client, "create_and_post_order"):
                return secure_client.create_and_post_order(kwargs)
            raise LumibotBrokerAPIError("py-clob-client-v2 is required for Polymarket limit orders") from exc

        tif = str(kwargs.get("time_in_force") or "GTC").upper()
        if tif == "DAY":
            tif = "GTC"
        if tif not in {"GTC", "GTD"}:
            raise ValueError("Polymarket limit orders support GTC or GTD")
        order_kwargs = dict(
            token_id=kwargs["token_id"],
            price=float(kwargs["price"]),
            size=float(kwargs["size"]),
            side=kwargs["side"],
        )
        if self.credentials.builder_code:
            order_kwargs["builder_code"] = self.credentials.builder_code
        order_args = OrderArgs(**order_kwargs)
        return secure_client.create_and_post_order(order_args, order_type=getattr(OrderType, tif))

    @staticmethod
    def _call_if_exists(obj: Any, method_name: str, *args, **kwargs):
        if obj is None:
            return None
        method = getattr(obj, method_name, None)
        if method is None:
            return None
        return method(*args, **kwargs)

    def _order_from_submit_response(self, response: Any, original_order: Order) -> Order:
        parsed = self._parse_broker_order(response, original_order.strategy)
        if not parsed.identifier:
            parsed.identifier = original_order.identifier
        parsed.custom_params = original_order.custom_params
        parsed.quote = original_order.quote
        if parsed.asset.symbol == "UNKNOWN":
            parsed.asset = original_order.asset
        if parsed.quantity == 0:
            parsed.quantity = original_order.quantity
        if parsed.side == Order.OrderSide.UNKNOWN:
            parsed.side = original_order.side
        if parsed.order_type == Order.OrderType.UNKNOWN:
            parsed.order_type = original_order.order_type
        return parsed

    def _validate_order(self, order: Order):
        if order.asset is None or order.asset.asset_type != Asset.AssetType.PREDICTION_CONTRACT:
            raise ValueError("Polymarket orders require asset_type='prediction_contract'")
        if order.order_class not in (None, Order.OrderClass.SIMPLE, "simple"):
            raise ValueError("Polymarket broker only supports simple orders")
        if order.side not in (Order.OrderSide.BUY, Order.OrderSide.SELL, "buy", "sell"):
            raise ValueError(f"Unsupported Polymarket side: {order.side}")

    def _validate_notional_cap(self, amount: Any):
        amount = self._decimal(amount)
        cap = self._decimal(
            self.config.get("MAX_MARKET_ORDER_NOTIONAL")
            or self.config.get("POLYMARKET_MAX_MARKET_ORDER_NOTIONAL")
            or os.environ.get("POLYMARKET_MAX_MARKET_ORDER_NOTIONAL")
            or os.environ.get("POLYMARKET_TEST_MAX_NOTIONAL")
            or "5"
        )
        if amount > cap:
            raise ValueError(f"Polymarket market order amount {amount} exceeds configured cap {cap}")

    def _validate_tick_size(self, token_id: str, price: Any):
        tick_size = self.data_source.get_tick_size(token_id)
        if tick_size is None:
            return
        price = self._decimal(price)
        if price % tick_size != 0:
            raise ValueError(f"Polymarket limit price {price} is not aligned to tick size {tick_size}")

    @staticmethod
    def _token_id(asset: Asset) -> str:
        if asset.asset_type != Asset.AssetType.PREDICTION_CONTRACT:
            raise ValueError("Polymarket orders require prediction_contract assets")
        return str(asset.symbol)

    @staticmethod
    def _map_side(value: Any):
        side = str(value or "").lower()
        if side == "buy":
            return Order.OrderSide.BUY
        if side == "sell":
            return Order.OrderSide.SELL
        return Order.OrderSide.UNKNOWN

    @staticmethod
    def _map_order_type(value: Any):
        order_type = str(value or "").lower()
        if order_type in {"market", "fak", "fok"}:
            return Order.OrderType.MARKET
        if order_type in {"limit", "gtc", "gtd"}:
            return Order.OrderType.LIMIT
        return Order.OrderType.UNKNOWN

    @staticmethod
    def _map_status(value: Any):
        status = str(value or "").lower()
        if status in {"placement", "placed", "submitted", "live", "open", "matched"}:
            return Order.OrderStatus.OPEN
        if status in {"partially_filled", "partial_fill", "partially matched", "partial"}:
            return Order.OrderStatus.PARTIALLY_FILLED
        if status in {"filled", "fill", "mined", "confirmed"}:
            return Order.OrderStatus.FILLED
        if status in {"canceled", "cancelled", "cancellation"}:
            return Order.OrderStatus.CANCELED
        if status == "expired":
            return Order.OrderStatus.EXPIRED
        if status in {"failed", "error", "rejected"}:
            return Order.OrderStatus.ERROR
        return Order.OrderStatus.UNKNOWN

    @staticmethod
    def _first_present(mapping: Any, *keys: str) -> Any:
        mapping = PolymarketData._model_to_dict(mapping)
        if not isinstance(mapping, dict):
            return None
        for key in keys:
            if key in mapping and mapping[key] is not None:
                return mapping[key]
        return None

    @staticmethod
    def _listify(value: Any) -> list:
        value = PolymarketData._model_to_dict(value)
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        if isinstance(value, dict):
            for key in ("items", "data", "orders", "positions", "trades", "results"):
                if key in value and isinstance(value[key], (list, tuple)):
                    return list(value[key])
            return [value]
        return [value]

    @staticmethod
    def _decimal(value: Any) -> Decimal:
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError) as exc:
            raise ValueError(f"Invalid Polymarket numeric value: {value!r}") from exc

    @classmethod
    def _to_float(cls, value: Any) -> float:
        return float(cls._decimal(value))

    @classmethod
    def _optional_float(cls, value: Any) -> float | None:
        if value is None:
            return None
        return cls._to_float(value)

    @staticmethod
    def _truthy(value: Any) -> bool:
        return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}
