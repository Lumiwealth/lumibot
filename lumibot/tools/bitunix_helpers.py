import os
import time
from typing import Dict, Any, Optional
import hashlib
import requests


class BitUnixClient:
    """
    Generic BitUnix futures REST client.
    """

    BASE_URL = "https://fapi.bitunix.com"
    # Base URL for spot (spot REST prefix is /api/spot/…)
    SPOT_BASE_URL = "https://openapi.bitunix.com"
    DEFAULT_HEADERS = {
        "language": "en-US",
        "Content-Type": "application/json",
    }

    def __init__(self, api_key: str, secret_key: str, timeout: float = 10.0):
        self.api_key = api_key
        self.secret_key = secret_key
        self.timeout = timeout

    def _nonce(self) -> str:
        return os.urandom(16).hex()

    def _timestamp(self) -> str:
        return str(int(time.time() * 1000))

    def _sign(self, params: Dict[str, Any], body: Optional[Dict[str, Any]], nonce: str, timestamp: str) -> str:
        # Merge params & JSON‑body dicts for signing
        merged = {**params, **(body or {})}
        qs = "".join(f"{k}{v}" for k, v in sorted(merged.items()))
        first = hashlib.sha256((nonce + timestamp + self.api_key + qs).encode()).hexdigest()
        return hashlib.sha256((first + self.secret_key).encode()).hexdigest()

    def _headers(self, params: Dict[str, Any], body: Optional[Dict[str, Any]]) -> Dict[str, str]:
        nonce     = self._nonce()
        timestamp = self._timestamp()
        sign      = self._sign(params, body, nonce, timestamp)

        return {
            **self.DEFAULT_HEADERS,
            "api-key":   self.api_key,
            "nonce":     nonce,
            "timestamp": timestamp,
            "sign":      sign,
        }

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json:   Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Generic request to BitUnix.

        Args:
            method:   "GET", "POST", etc.
            endpoint: e.g. "/api/v1/futures/account"
            params:   Query parameters
            json:     JSON body for POST/PUT
        """
        params = params or {}
        headers = self._headers(params, json)

        if endpoint.startswith("/api/spot/"):
            url = self.SPOT_BASE_URL + endpoint
        else:
            url = self.BASE_URL + endpoint
        resp = requests.request(
            method=method.upper(),
            url=url,
            headers=headers,
            params=params if method.upper() == "GET" else None,
            json=json   if method.upper() != "GET" else None,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        # If the business code is non‑zero, emit a debug log with details
        try:
            payload = resp.json()
            if isinstance(payload, dict) and payload.get("code") not in (0, None):
                import logging
                logging.getLogger(__name__).debug(
                    "BitUnix business error %s on %s %s: %s",
                    payload.get("code"), method.upper(), endpoint, payload
                )
        except Exception:
            # Wasn't JSON or other error – ignore, will be caught by caller if needed
            pass
        return resp.json()

    # ——— Example wrappers ———

    def get_account(self, margin_coin: str = "USDT") -> Dict[str, Any]:
        """
        Retrieve FUTURES account metrics for the specified `margin_coin`.
        This might also include spot balances depending on the API implementation.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {... account fields ...}}``
        """
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/account",
            params={"marginCoin": margin_coin},
        )

    def get_positions(self, margin_coin: str = "USDT") -> Dict[str, Any]:
        """
        List all open positions under the given `margin_coin`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...position...}, ... ]}``
        """
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/position/get_pending_positions",
            params={"marginCoin": margin_coin},
        )

    def place_order(
        self,
        symbol: str,
        side: str,
        orderType: str,
        qty: float,
        price: Optional[float] = None,
        marginCoin: str = "USDT",
        clientId: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Place a single futures order.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"orderId": str, ... }}``
        """
        body = {
            "symbol":      symbol,
            "side":        side,        # BUY / SELL
            "orderType":   orderType,   # LIMIT / MARKET / ...
            "qty":         qty,
            "marginCoin":  marginCoin,
            **({"price": price}      if price is not None else {}),
            **({"clientId": clientId} if clientId is not None else {}),
            **kwargs,
        }
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/place_order",
            json=body,
        )

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        Cancel a single FUTURES order.
        """
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/cancel_orders",
            json={"orderIds": order_id},
        )

    # ---------- Spot wrappers ----------

    def place_spot_order(
        self,
        symbol: str,
        side: int,
        type: int,
        volume: float,
        price: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Place a SPOT order.

        side: 1 = SELL, 2 = BUY
        type: 1 = LIMIT, 2 = MARKET
        volume: base‑currency quantity for LIMIT, quote‑currency amount for MARKET
        """
        body = {
            "symbol": symbol,
            "side": side,
            "type": type,
            "volume": volume,
            **({"price": price} if price is not None else {}),
        }
        return self._request(
            method="POST",
            endpoint="/api/spot/v1/order/place_order",
            json=body,
        )

    def cancel_spot_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """
        Cancel a single SPOT order.
        """
        return self._request(
            method="POST",
            endpoint="/api/spot/v1/order/cancel",
            json={"orderIdList": [{"orderId": order_id, "symbol": symbol}]},
        )

    def spot_last_price(self, symbol: str) -> Dict[str, Any]:
        """
        Get last traded price for a SPOT pair.
        """
        return self._request(
            method="GET",
            endpoint="/api/spot/v1/market/last_price",
            params={"symbol": symbol},
        )

    def spot_depth(self, symbol: str, precision: int = 5) -> Dict[str, Any]:
        """
        Order‑book snapshot for SPOT.
        """
        return self._request(
            method="GET",
            endpoint="/api/spot/v1/market/depth",
            params={"symbol": symbol, "precision": precision},
        )


    def adjust_position_margin(
        self,
        symbol: str,
        amount: str,
        margin_coin: str = "USDT",
        side: Optional[str] = None,
        position_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Add or remove margin from an existing position.

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
            json=body,
        )

    def change_leverage(
        self,
        symbol: str,
        leverage: int,
        margin_coin: str = "USDT",
    ) -> Dict[str, Any]:
        """
        Change leverage for one symbol.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"symbol": str, "leverage": int}}``
        """
        body = {"symbol": symbol, "leverage": leverage, "marginCoin": margin_coin}
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/account/change_leverage",
            json=body,
        )

    def change_margin_mode(
        self,
        symbol: str,
        margin_mode: str,
        margin_coin: str = "USDT",
    ) -> Dict[str, Any]:
        """
        Switch isolated / cross margin for a symbol.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"symbol": str, "marginMode": str}}``
        """
        body = {"symbol": symbol, "marginMode": margin_mode, "marginCoin": margin_coin}
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/account/change_margin_mode",
            json=body,
        )

    def change_position_mode(self, position_mode: str) -> Dict[str, Any]:
        """
        Switch hedge / one‑way mode for ALL symbols.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"positionMode": str}}``
        """
        body = {"positionMode": position_mode}
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/account/change_position_mode",
            json=body,
        )

    def get_leverage_and_margin_mode(
        self,
        symbol: str,
        margin_coin: str = "USDT",
    ) -> Dict[str, Any]:
        """
        Query current leverage setting and margin mode.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"leverage": int, "marginMode": str}}``
        """
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/account/get_leverage_margin_mode",
            params={"symbol": symbol, "marginCoin": margin_coin},
        )

    def get_depth(self, symbol: str, limit: Optional[str] = None) -> Dict[str, Any]:
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

    def get_funding_rate(self, symbol: str) -> Dict[str, Any]:
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
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
        type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Historical OHLCV candles.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...kline...}, ... ]}``
        """
        params = {"symbol": symbol, "interval": interval}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        if limit is not None:
            params["limit"] = limit
        if type is not None:
            params["type"] = type
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/market/kline",
            params=params,
        )

    def get_tickers(self, symbols: Optional[str] = None) -> Dict[str, Any]:
        """
        24‑hour rolling statistics for one or many symbols.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...ticker...}, ... ]}``
        """
        params = {}
        if symbols is not None:
            params["symbols"] = symbols
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/market/tickers",
            params=params,
        )

    def get_trading_pairs(self, symbols: Optional[str] = None) -> Dict[str, Any]:
        """
        Static information for trading pairs.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...pairInfo...}, ... ]}``
        """
        params = {}
        if symbols is not None:
            params["symbols"] = symbols
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/market/trading_pairs",
            params=params,
        )

    def get_open_interest(self, symbol: str) -> Dict[str, Any]:
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

    def get_mark_price(self, symbol: str) -> Dict[str, Any]:
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

    def get_index_price(self, symbol: str) -> Dict[str, Any]:
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
        symbol: Optional[str] = None,
        position_id: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Historical (closed) positions.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...position...}, ... ]}``
        """
        params = {}
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

    def get_position_tiers(self, symbol: str) -> Dict[str, Any]:
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
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        client_id: Optional[str] = None,
        status: Optional[str] = None,
        order_type: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Query past orders.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...order...}, ... ]}``
        """
        params = {}
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
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        position_id: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Query trade fills.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...trade...}, ... ]}``
        """
        params = {}
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
        order_id: Optional[str] = None,
        client_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Retrieve one order by `order_id` or `client_id`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {...order detail...}}``
        """
        params = {}
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
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        client_id: Optional[str] = None,
        status: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Open (unfilled) orders.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...order...}, ... ]}``
        """
        params = {}
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
        symbol: Optional[str] = None,
        position_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List positions that are not yet fully opened/closed (i.e. in‑flight).

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": [ {...position...}, ... ]}``
        """
        params: Dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol
        if position_id is not None:
            params["positionId"] = position_id
        return self._request(
            method="GET",
            endpoint="/api/v1/futures/position/get_pending_positions",
            params=params,
        )

    def batch_order(self, orders: list[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Submit up to 20 orders in one request.

        Each item in `orders` must follow the same structure as `place_order`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"success": int, "failed": int, "orders": [...]}}``
        """
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/batch_order",
            json={"orders": orders},
        )

    def cancel_orders(self, order_ids: list[str]) -> Dict[str, Any]:
        """
        Cancel multiple orders by id.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"cancelled": [...], "failed": [...]}}``
        """
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/cancel_orders",
            json={"orderIds": ",".join(order_ids)},
        )

    def cancel_all_orders(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        Cancel **all** open orders; optionally only those for `symbol`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"cancelled": int}}``
        """
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/cancel_all_orders",
            json=({"symbol": symbol} if symbol else {}),
        )

    def modify_order(
        self,
        order_id: str,
        symbol: str,
        **changes: Any,
    ) -> Dict[str, Any]:
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
            json=body,
        )

    def close_all_position(self, symbol: str) -> Dict[str, Any]:
        """
        Market‑close all size in `symbol`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"symbol": str, "closed": bool}}``
        """
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/close_all_position",
            json={"symbol": symbol},
        )

    def flash_close_position(
        self,
        position_id: str,
        side: str,
    ) -> Dict[str, Any]:
        """
        Fast‑close one position by `position_id`.

        Returns:
            Dict[str, Any]: ``{"code": int, "msg": str, "data": {"positionId": str, "status": str}}``
        """
        return self._request(
            method="POST",
            endpoint="/api/v1/futures/trade/flash_close_position",
            json={"positionId": position_id, "side": side},
        )

'''
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
'''