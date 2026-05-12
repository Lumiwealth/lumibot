import hashlib
import json
import os
import time
from collections.abc import Mapping
from typing import Any, cast

from lumibot.tools.lumibot_logger import get_logger


def _requests() -> Any:
    import requests

    return requests


def _json_dict(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(cast(Mapping[str, Any], value))
    return {"data": value}


class BitUnixClient:
    """
    Generic BitUnix futures REST client.
    """

    BASE_URL = "https://fapi.bitunix.com"
    # Base URL for spot (spot REST prefix is /api/spot/…)
    DEFAULT_HEADERS = {
        "language": "en-US",
        "Content-Type": "application/json",
    }

    def __init__(self, api_key: str, secret_key: str, timeout: float = 10.0) -> None:
        self.api_key = api_key
        self.secret_key = secret_key
        self.timeout = timeout

    def _nonce(self) -> str:
        return os.urandom(16).hex()

    def _timestamp(self) -> str:
        return str(int(time.time() * 1000))

    def _sign(self, params: dict[str, Any], body: dict[str, Any] | None, nonce: str, timestamp: str) -> str:
        """
        Generate signature per BitUnix API docs:
        digest = SHA256(nonce + timestamp + apiKey + queryParams + bodyJson)
        sign = SHA256(digest + secretKey)
        """
        # Prepare logger
        from lumibot.tools.lumibot_logger import get_logger

        logger = get_logger(__name__)
        # Prepare sorted query string: concatenated key and value, sorted by key
        qp = "".join(f"{k}{params[k]}" for k in sorted(params)) if params else ""
        # Prepare compact JSON body string without spaces
        body_str = json.dumps(body, separators=(",", ":"), ensure_ascii=False) if body else ""
        # Construct digest input and log it
        digest_input = nonce + timestamp + self.api_key + qp + body_str
        logger.debug("digest_input: %s", digest_input)
        # First SHA-256 hash
        digest = hashlib.sha256(digest_input.encode("utf-8")).hexdigest()
        logger.debug("digest: %s", digest)
        # Final signature input and log it
        sign_input = digest + self.secret_key
        logger.debug("sign_input: %s", sign_input)
        # Second SHA-256 hash and return
        signature = hashlib.sha256(sign_input.encode("utf-8")).hexdigest()
        logger.debug("signature: %s", signature)
        return signature

    def _headers(self, params: dict[str, Any], body: dict[str, Any] | None) -> dict[str, str]:
        nonce = self._nonce()
        timestamp = self._timestamp()
        sign = self._sign(params, body, nonce, timestamp)

        return {
            **self.DEFAULT_HEADERS,
            "api-key": self.api_key,
            "nonce": nonce,
            "timestamp": timestamp,
            "sign": sign,
        }

    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Generic request to BitUnix.

        Args:
            method:   "GET", "POST", etc.
            endpoint: e.g. "/api/v1/futures/account"
            params:   Query parameters
            json_body:     JSON body for POST/PUT
        """
        params = params or {}
        headers = self._headers(params, json_body)
        url = self.BASE_URL + endpoint
        # Use custom serialization for POST bodies so that the sent JSON matches the signature body exactly
        if method.upper() == "GET":
            resp = _requests().request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=params,
                timeout=self.timeout,
            )
        else:
            # Serialize body without spaces to match signature
            body_str = json.dumps(json_body, separators=(",", ":"), ensure_ascii=False) if json_body is not None else ""
            resp = _requests().request(
                method=method.upper(),
                url=url,
                headers=headers,
                data=body_str,
                timeout=self.timeout,
            )
        resp.raise_for_status()
        # If the business code is non‑zero, emit a debug log with details
        try:
            payload = _json_dict(resp.json())
            if payload.get("code") not in (0, None):
                get_logger(__name__).debug(
                    "BitUnix business error %s on %s %s: %s", payload.get("code"), method.upper(), endpoint, payload
                )
        except Exception:
            # Wasn't JSON or other error – ignore, will be caught by caller if needed
            pass
        return _json_dict(resp.json())

    # ——— Example wrappers ———

    def get_account(self, margin_coin: str = "USDT") -> dict[str, Any]:
        """
        Retrieve FUTURES account metrics for the specified quote asset (`margin_coin`).
        This might also include spot balances depending on the API implementation.

        Args:
            margin_coin: The symbol of the quote asset (e.g., "USDT").

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {... account fields ...}}``
        """
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/account",
            params={"marginCoin": margin_coin},
        )

    def get_positions(self) -> dict[str, Any]:
        """
        List all open positions under the given quote asset (`margin_coin`).

        Args:
            margin_coin: The symbol of the quote asset (e.g., "USDT").

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...position...}, ... ]}``
        """
        return self._request(method="GET", endpoint="/api/v1/futures/position/get_pending_positions")

    def place_order(
        self,
        symbol: str,
        side: str,
        orderType: str,
        qty: float,
        take_profit_price: float | None = None,
        stop_loss_price: float | None = None,
        price: float | None = None,
        clientId: str | None = None,
        tradeSide: str = "OPEN",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Place a single futures order.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"orderId": str, ... }}``
            - If **reduceOnly** is True, Bitunix will reject the order if it would increase position size.
        Optionally include take_profit_price and/or stop_loss_price to attach TP/SL triggers in the same request.
        """
        body: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "tradeSide": tradeSide,
            "orderType": orderType,
            "qty": qty,
            **({"price": price} if price is not None else {}),
            **({"clientId": clientId} if clientId is not None else {}),
            **({"tpPrice": take_profit_price} if take_profit_price is not None else {}),
            **({"slPrice": stop_loss_price} if stop_loss_price is not None else {}),
            **kwargs,
        }
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/place_order",
            json_body=body,
        )

    def cancel_order(
        self,
        order: Any | None = None,
        *,
        order_id: str | None = None,
        symbol: str | None = None,
    ) -> dict[str, Any]:
        """
        Cancel a single FUTURES order.
        """
        if order is not None:
            order_id = order_id or getattr(order, "identifier", None) or getattr(order, "id", None)
            symbol = symbol or getattr(order, "symbol", None)
            asset = getattr(order, "asset", None)
            if symbol is None and asset is not None:
                symbol = getattr(asset, "symbol", None)

        if not order_id:
            raise ValueError("BitUnixClient.cancel_order requires an order identifier")
        if not symbol:
            raise ValueError("BitUnixClient.cancel_order requires a symbol")

        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/cancel_orders",
            json_body={"symbol": symbol, "orderList": [str(order_id)]},
        )

    def adjust_position_margin(
        self,
        symbol: str,
        amount: str,
        margin_coin: str = "USDT",
        side: str | None = None,
        position_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Add or remove margin from an existing position.

        Args:
            symbol: The trading pair symbol.
            amount: The amount of margin to add/remove.
            margin_coin: The symbol of the quote asset (e.g., "USDT").
            side: Position side (optional).
            position_id: Position ID (optional).

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"positionId": str, "margin": str}}``
        """
        body = {
            "symbol": symbol,
            "amount": amount,
            "marginCoin": margin_coin,
            **({"side": side} if side is not None else {}),
            **({"positionId": position_id} if position_id is not None else {}),
        }
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/account/adjust_position_margin",
            json_body=body,
        )

    def change_leverage(
        self,
        symbol: str,
        leverage: int,
        margin_coin: str = "USDT",
    ) -> dict[str, Any]:
        """
        Change leverage for one symbol.

        Args:
            symbol: The trading pair symbol.
            leverage: The desired leverage multiplier.
            margin_coin: The symbol of the quote asset (e.g., "USDT").

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"symbol": str, "leverage": int}}``
        """
        body = {"symbol": symbol, "leverage": leverage, "marginCoin": margin_coin}
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/account/change_leverage",
            json_body=body,
        )

    def change_margin_mode(
        self,
        symbol: str,
        margin_mode: str,
        margin_coin: str = "USDT",
    ) -> dict[str, Any]:
        """
        Switch isolated / cross margin for a symbol.

        Args:
            symbol: The trading pair symbol.
            margin_mode: "ISOLATED" or "CROSSED".
            margin_coin: The symbol of the quote asset (e.g., "USDT").

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"symbol": str, "marginMode": str}}``
        """
        body = {"symbol": symbol, "marginMode": margin_mode, "marginCoin": margin_coin}
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/account/change_margin_mode",
            json_body=body,
        )

    def change_position_mode(self, position_mode: str) -> dict[str, Any]:
        """
        Switch hedge / one‑way mode for ALL symbols.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"positionMode": str}}``
        """
        body = {"positionMode": position_mode}
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/account/change_position_mode",
            json_body=body,
        )

    def get_leverage_and_margin_mode(
        self,
        symbol: str,
        margin_coin: str = "USDT",
    ) -> dict[str, Any]:
        """
        Query current leverage setting and margin mode.

        Args:
            symbol: The trading pair symbol.
            margin_coin: The symbol of the quote asset (e.g., "USDT").

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"leverage": int, "marginMode": str}}``
        """
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/account/get_leverage_margin_mode",
            params={"symbol": symbol, "marginCoin": margin_coin},
        )

    def get_depth(self, symbol: str, limit: str | None = None) -> dict[str, Any]:
        """
        Order‑book snapshot for `symbol`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"bids": [...], "asks": [...]}}``
        """
        params = {"symbol": symbol}
        if limit is not None:
            params["limit"] = limit
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/market/depth",
            params=params,
        )

    def get_funding_rate(self, symbol: str) -> dict[str, Any]:
        """
        Current funding rate for `symbol`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"fundingRate": str, "nextFundingTime": int}}``
        """
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/market/funding_rate",
            params={"symbol": symbol},
        )

    def get_kline(
        self,
        symbol: str,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int | None = None,
        kline_type: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Historical OHLCV candles.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...kline...}, ... ]}``
        """
        if "type" in kwargs:
            raw_kline_type = kwargs.pop("type")
            kline_type = None if raw_kline_type is None else str(raw_kline_type)
        if kwargs:
            unexpected = ", ".join(str(key) for key in kwargs)
            raise TypeError(f"Unexpected get_klines keyword argument(s): {unexpected}")

        params: dict[str, Any] = {"symbol": symbol, "interval": interval}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if limit is not None:
            params["limit"] = limit
        if kline_type is not None:
            params["type"] = kline_type
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/market/kline",
            params=params,
        )

    def get_tickers(self, symbols: str | None = None) -> dict[str, Any]:
        """
        24‑hour rolling statistics for one or many symbols.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...ticker...}, ... ]}``
        """
        params: dict[str, Any] = {}
        if symbols is not None:
            params["symbols"] = symbols
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/market/tickers",
            params=params,
        )

    def get_trading_pairs(self, symbols: str | None = None) -> dict[str, Any]:
        """
        Static information for trading pairs.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...pairInfo...}, ... ]}``
        """
        params: dict[str, Any] = {}
        if symbols is not None:
            params["symbols"] = symbols
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/market/trading_pairs",
            params=params,
        )

    def get_open_interest(self, symbol: str) -> dict[str, Any]:
        """
        Open interest for `symbol`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"openInterest": str}}``
        """
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/market/open_interest",
            params={"symbol": symbol},
        )

    def get_mark_price(self, symbol: str) -> dict[str, Any]:
        """
        Current mark price and funding details for `symbol`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"markPrice": str, "fundingRate": str, "nextFundingTime": int}}``
        """
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/market/mark_price",
            params={"symbol": symbol},
        )

    def get_index_price(self, symbol: str) -> dict[str, Any]:
        """
        Index price for `symbol`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"indexPrice": str}}``
        """
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/market/index_price",
            params={"symbol": symbol},
        )

    def get_history_positions(
        self,
        symbol: str | None = None,
        position_id: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        skip: int | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """
        Historical (closed) positions.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...position...}, ... ]}``
        """
        params: dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol
        if position_id is not None:
            params["positionId"] = position_id
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if skip is not None:
            params["skip"] = skip
        if limit is not None:
            params["limit"] = limit
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/position/get_history_positions",
            params=params,
        )

    def get_position_tiers(self, symbol: str) -> dict[str, Any]:
        """
        Risk tier ladder for a symbol.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...tier...}, ... ]}``
        """
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/position/get_position_tiers",
            params={"symbol": symbol},
        )

    def get_history_orders(
        self,
        symbol: str | None = None,
        order_id: str | None = None,
        client_id: str | None = None,
        status: str | None = None,
        order_type: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        skip: int | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """
        Query past orders.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...order...}, ... ]}``
        """
        params: dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol
        if order_id is not None:
            params["orderId"] = order_id
        if client_id is not None:
            params["clientId"] = client_id
        if status is not None:
            params["status"] = status
        if order_type is not None:
            params["type"] = order_type
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if skip is not None:
            params["skip"] = skip
        if limit is not None:
            params["limit"] = limit
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/trade/get_history_orders",
            params=params,
        )

    def get_history_trades(
        self,
        symbol: str | None = None,
        order_id: str | None = None,
        position_id: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        skip: int | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """
        Query trade fills.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...trade...}, ... ]}``
        """
        params: dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol
        if order_id is not None:
            params["orderId"] = order_id
        if position_id is not None:
            params["positionId"] = position_id
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if skip is not None:
            params["skip"] = skip
        if limit is not None:
            params["limit"] = limit
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/trade/get_history_trades",
            params=params,
        )

    def get_order_detail(
        self,
        order_id: str | None = None,
        client_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve one order by `order_id` or `client_id`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {...order detail...}}``
        """
        params: dict[str, Any] = {}
        if order_id is not None:
            params["orderId"] = order_id
        if client_id is not None:
            params["clientId"] = client_id
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/trade/get_order_detail",
            params=params,
        )

    def get_pending_orders(
        self,
        symbol: str | None = None,
        order_id: str | None = None,
        client_id: str | None = None,
        status: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        skip: int | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """
        Open (unfilled) orders.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...order...}, ... ]}``
        """
        params: dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol
        if order_id is not None:
            params["orderId"] = order_id
        if client_id is not None:
            params["clientId"] = client_id
        if status is not None:
            params["status"] = status
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if skip is not None:
            params["skip"] = skip
        if limit is not None:
            params["limit"] = limit
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/trade/get_pending_orders",
            params=params,
        )

    def get_pending_positions(
        self,
        symbol: str | None = None,
        position_id: str | None = None,
    ) -> dict[str, Any]:
        """
        List positions that are not yet fully opened/closed (i.e. in‑flight).

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...position...}, ... ]}``
        """
        params: dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol
        if position_id is not None:
            params["positionId"] = position_id
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/position/get_pending_positions",
            params=params,
        )

    def batch_order(self, orders: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Submit up to 20 orders in one request.

        Each item in `orders` must follow the same structure as `place_order`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"success": int, "failed": int, "orders": [...]}}``
        """
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/batch_order",
            json_body={"orders": orders},
        )

    def cancel_orders(self, order_ids: list[str]) -> dict[str, Any]:
        """
        Cancel multiple orders by id.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"cancelled": [...], "failed": [...]}}``
        """
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/cancel_orders",
            json_body={"orderIds": ",".join(order_ids)},
        )

    def cancel_all_orders(self, symbol: str | None = None) -> dict[str, Any]:
        """
        Cancel **all** open orders; optionally only those for `symbol`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"cancelled": int}}``
        """
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/cancel_all_orders",
            json_body=({"symbol": symbol} if symbol else {}),
        )

    def modify_order(
        self,
        order_id: str,
        symbol: str,
        **changes: Any,
    ) -> dict[str, Any]:
        """
        Amend a pending order (price / quantity / TP‑SL).

        `changes` are the fields allowed by the BitUnix docs.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"orderId": str, "status": str}}``
        """
        body = {"orderId": order_id, "symbol": symbol, **changes}
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/modify_order",
            json_body=body,
        )

    def close_all_position(self, symbol: str) -> dict[str, Any]:
        """
        Market‑close all size in `symbol`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"symbol": str, "closed": bool}}``
        """
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/close_all_position",
            json_body={"symbol": symbol},
        )

    def flash_close_position(
        self,
        position_id: str,
        side: str,
    ) -> dict[str, Any]:
        """
        Fast‑close one position by `position_id`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"positionId": str, "status": str}}``
        """
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/flash_close_position",
            json_body={"positionId": position_id, "side": side},
        )


"""
# ---- usage ----
if __name__ == "__main__":
    client = BitUnixClient(api_key="YOUR_API_KEY", secret_key="YOUR_SECRET_KEY")

    # 1) Get account
    print(client.get_account())

    # 2) List positions
    print(client.get_positions())

    # 3) Place a LIMIT order
    order = client.place_order(
        symbol="BTCUSDT",
        side="BUY",
        type="LIMIT",
        quantity=0.001,
        price=50000,
    )
    print("Order placed:", order)

    # 4) Cancel that order
    cancel = client.cancel_order(order_id=order["data"]["orderId"], symbol="BTCUSDT")
    print("Cancelled:", cancel)
"""
