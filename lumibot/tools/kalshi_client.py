"""Internal Kalshi REST transport. Public trading behavior belongs to the broker."""

from __future__ import annotations

import base64
import os
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import quote, urlsplit

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from lumibot.brokers.broker import LumibotBrokerAPIError


class KalshiAPIError(LumibotBrokerAPIError):
    """Sanitized provider error; response bodies may contain account information."""

    def __init__(self, message, *, status_code=None):
        super().__init__(message)
        self.status_code = status_code


def decimal_value(value, name="value"):
    """Parse provider fixed-point numbers without accepting NaN or infinity."""
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ValueError(f"Kalshi {name} must be a finite number") from None
    if not result.is_finite():
        raise ValueError(f"Kalshi {name} must be a finite number")
    return result


def config_bool(value, name):
    if isinstance(value, bool):
        return value
    if str(value).lower() in ("true", "1"):
        return True
    if str(value).lower() in ("false", "0"):
        return False
    raise ValueError(f"{name} must be true or false")


class KalshiClient:
    """Small signed HTTP client shared by the broker and its data source.

    Only GET requests are retried automatically. An uncertain order response is
    surfaced to the caller; resubmitting with a new client ID could trade twice.
    """

    API_PATH = "/trade-api/v2"
    WS_PATH = "/trade-api/ws/v2"
    PRODUCTION_URL = "https://external-api.kalshi.com"
    DEMO_URL = "https://external-api.demo.kalshi.co"
    PRODUCTION_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    DEMO_WS = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"

    def __init__(self, config=None, *, http_client=None, require_auth=False, clock=time.time, sleep=time.sleep):
        config = config or {}

        def setting(key, default=None):
            return config.get(key, config.get(f"KALSHI_{key}", os.environ.get(f"KALSHI_{key}", default)))

        self.is_demo = config_bool(setting("IS_DEMO", True), "KALSHI_IS_DEMO")
        self.base_url = self.DEMO_URL if self.is_demo else self.PRODUCTION_URL
        self.ws_url = self.DEMO_WS if self.is_demo else self.PRODUCTION_WS
        self.api_key_id = setting("API_KEY_ID")
        self._private_key = None
        key = setting("PRIVATE_KEY")
        key_path = setting("PRIVATE_KEY_PATH")
        if key or key_path:
            try:
                pem = key.replace("\\n", "\n").encode() if key else Path(key_path).read_bytes()
                self._private_key = serialization.load_pem_private_key(pem, password=None)
                if not isinstance(self._private_key, rsa.RSAPrivateKey):
                    raise ValueError("RSA key required")
            except (OSError, ValueError, TypeError):
                raise ValueError(
                    "Invalid Kalshi RSA private key; check KALSHI_PRIVATE_KEY or KALSHI_PRIVATE_KEY_PATH"
                ) from None
        self._clock, self._sleep = clock, sleep
        self._owns_http = http_client is None
        self._http = http_client or httpx.Client(timeout=15, follow_redirects=False)
        if require_auth:
            self.require_auth()

    def require_auth(self):
        if not self.api_key_id or self._private_key is None:
            raise ValueError("Kalshi requires KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY (or KALSHI_PRIVATE_KEY_PATH)")

    def auth_headers(self, method, path):
        self.require_auth()
        timestamp = str(int(self._clock() * 1000))
        message = (timestamp + method.upper() + urlsplit(path).path).encode()
        signature = self._private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256().digest_size),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
        }

    def request(self, method, path, *, params=None, json=None, authenticated=True):
        method = method.upper()
        if not path.startswith("/") or "?" in path or "://" in path:
            raise ValueError("Kalshi request path must be relative; pass query parameters separately")
        full_path = self.API_PATH + path
        for attempt in range(3):
            headers = self.auth_headers(method, full_path) if authenticated else {}
            try:
                response = self._http.request(
                    method,
                    self.base_url + full_path,
                    params=params,
                    json=json,
                    headers=headers,
                )
            except httpx.TransportError:
                if method == "GET" and attempt < 2:
                    self._sleep(0.25 * 2**attempt)
                    continue
                raise KalshiAPIError("Kalshi request failed in transport; order outcome may be unknown") from None
            if response.status_code in (429, 500, 502, 503, 504) and method == "GET" and attempt < 2:
                self._sleep(0.25 * 2**attempt)
                continue
            if response.status_code >= 300:
                raise KalshiAPIError(
                    f"Kalshi {method} request failed (HTTP {response.status_code})",
                    status_code=response.status_code,
                )
            if response.status_code == 204:
                return {}
            try:
                result = response.json()
            except ValueError:
                raise KalshiAPIError("Kalshi returned invalid JSON") from None
            if not isinstance(result, dict):
                raise KalshiAPIError("Kalshi returned an invalid response object")
            return result

    def pages(self, path, key, *, params=None, authenticated=True):
        params = dict(params or {})
        seen = set()
        while True:
            payload = self.request("GET", path, params=params, authenticated=authenticated)
            rows = payload.get(key)
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise KalshiAPIError(f"Kalshi response is missing the {key} list")
            yield from rows
            cursor = payload.get("cursor")
            if not cursor:
                return
            if cursor in seen:
                raise KalshiAPIError("Kalshi pagination repeated a cursor")
            seen.add(cursor)
            params["cursor"] = cursor

    @staticmethod
    def path_id(value):
        return quote(str(value), safe="")

    def close(self):
        if self._owns_http:
            self._http.close()
