"""Native Hyperliquid protocol, using the official Python SDK's signing primitives."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal
from threading import RLock

import requests
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_keys.exceptions import BadSignature
from eth_utils import keccak
from hyperliquid.utils.signing import action_hash, construct_phantom_agent, l1_payload

from .models import decimal
from .provider import MetaMaskError, address, json_object

URLS = {"testnet": "https://api.hyperliquid-testnet.xyz", "mainnet": "https://api.hyperliquid.xyz"}
_CLIENT_ID = re.compile(r"0x[0-9a-fA-F]{32}\Z")
_INTERVALS = {"1m": 60_000, "1h": 3_600_000, "1d": 86_400_000}


def wire_decimal(value: Decimal) -> str:
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _json_bytes(value):
    if isinstance(value, bytes):
        return "0x" + value.hex()
    raise TypeError("Unsupported typed-data value")


def action_typed_data(action: dict[str, object], nonce: int, network: str, expires_after: int | None = None):
    if network not in URLS or isinstance(nonce, bool) or not isinstance(nonce, int) or nonce < 0:
        raise ValueError("Explicit network and nonnegative millisecond nonce are required")
    hashed = action_hash(action, None, nonce, expires_after)
    data = l1_payload(construct_phantom_agent(hashed, network == "mainnet"))
    return json_object(json.loads(json.dumps(data, default=_json_bytes)), "typed data")


def typed_data_hash(data: dict[str, object]) -> str:
    encoded = encode_typed_data(full_message=data)
    return "0x" + keccak(b"\x19" + encoded.version + encoded.header + encoded.body).hex()


def signature_parts(data: dict[str, object], signature: str, wallet: str) -> dict[str, object]:
    if not isinstance(signature, str) or not re.fullmatch(r"0x[0-9a-fA-F]{130}", signature):
        raise MetaMaskError("INVALID_SIGNATURE", "Managed signer returned an invalid EVM signature")
    try:
        signer = Account.recover_message(encode_typed_data(full_message=data), signature=signature)
    except (ValueError, TypeError, BadSignature):
        raise MetaMaskError("INVALID_SIGNATURE", "Managed signature cannot be verified") from None
    if address(signer) != address(wallet):
        raise MetaMaskError("SIGNER_MISMATCH", "Managed signature belongs to a different wallet")
    raw = bytes.fromhex(signature[2:])
    recovery = raw[-1] + 27 if raw[-1] in (0, 1) else raw[-1]
    if recovery not in (27, 28):
        raise MetaMaskError("INVALID_SIGNATURE", "Managed signature has an invalid recovery identifier")
    return {"r": "0x" + raw[:32].hex(), "s": "0x" + raw[32:64].hex(), "v": recovery}


@dataclass(frozen=True)
class Market:
    symbol: str
    index: int
    size_decimals: int
    max_leverage: int
    delisted: bool = False
    is_cross: bool = True


class HyperliquidVenue:
    def __init__(self, network: str, *, session: requests.Session | None = None):
        if network not in URLS:
            raise ValueError("NETWORK must be explicitly testnet or mainnet")
        self.network = network
        self.url = URLS[network]
        self.session = session if session is not None else requests.Session()
        self.session.trust_env = False
        self._owns_session = session is None
        self._markets: dict[str, Market] = {}
        self._market_times: dict[str, float] = {}
        self._lock = RLock()

    def _post(self, path: str, payload: dict[str, object], *, mutation=False) -> object:
        try:
            response = self.session.request(
                "POST",
                self.url + path,
                json=payload,
                timeout=(5, 30),
                allow_redirects=False,
            )
        except requests.RequestException:
            raise MetaMaskError(
                "VENUE_OUTCOME_UNKNOWN" if mutation else "VENUE_UNAVAILABLE",
                "Venue request lost its response; reconcile original client order ID"
                if mutation
                else "Venue read failed",
            ) from None
        if not 200 <= response.status_code < 300:
            raise MetaMaskError(
                "VENUE_OUTCOME_UNKNOWN" if mutation else "VENUE_UNAVAILABLE",
                f"Venue returned HTTP {response.status_code}; no automatic retransmission",
            )
        try:
            return response.json()
        except ValueError:
            raise MetaMaskError(
                "VENUE_OUTCOME_UNKNOWN" if mutation else "INVALID_RESPONSE",
                "Venue returned malformed JSON",
            ) from None

    def info(self, query: dict[str, object]) -> object:
        return self._post("/info", query)

    def market(self, symbol: str) -> Market:
        if not isinstance(symbol, str) or not symbol or len(symbol) > 60:
            raise ValueError("A venue market symbol is required")
        dex = symbol.split(":", 1)[0] if ":" in symbol else ""
        with self._lock:
            if symbol in self._markets and time.monotonic() - self._market_times.get(dex, 0) < 60:
                return self._markets[symbol]
            offset = 0
            if dex:
                dexs = self.info({"type": "perpDexs"})
                if not isinstance(dexs, list):
                    raise MetaMaskError("INVALID_RESPONSE", "Venue dex inventory is missing")
                matches = [i for i, item in enumerate(dexs[1:]) if isinstance(item, dict) and item.get("name") == dex]
                if len(matches) != 1:
                    raise MetaMaskError("UNKNOWN_MARKET", "Requested perpetual dex was not found")
                offset = 110_000 + matches[0] * 10_000
            meta = json_object(self.info({"type": "meta", **({"dex": dex} if dex else {})}), "market metadata")
            universe = meta.get("universe")
            if not isinstance(universe, list):
                raise MetaMaskError("INVALID_RESPONSE", "Venue market metadata is missing")
            refreshed = {}
            for index, raw in enumerate(universe):
                item = json_object(raw, "market")
                name, places, maximum = item.get("name"), item.get("szDecimals"), item.get("maxLeverage")
                if (
                    not isinstance(name, str)
                    or not name
                    or isinstance(places, bool)
                    or not isinstance(places, int)
                    or not 0 <= places <= 8
                    or isinstance(maximum, bool)
                    or not isinstance(maximum, int)
                    or maximum < 1
                ):
                    raise MetaMaskError("INVALID_RESPONSE", "Venue market definition is malformed")
                qualified = name if not dex or name.startswith(dex + ":") else dex + ":" + name
                if qualified in refreshed:
                    raise MetaMaskError("INVALID_RESPONSE", "Venue market identity is ambiguous")
                delisted, isolated = item.get("isDelisted", False), item.get("onlyIsolated", False)
                if not isinstance(delisted, bool) or not isinstance(isolated, bool):
                    raise MetaMaskError("INVALID_RESPONSE", "Venue trading restrictions are malformed")
                cross = not isolated and item.get("marginMode") not in {"strictIsolated", "noCross"}
                refreshed[qualified] = Market(qualified, offset + index, places, maximum, delisted, cross)
            for old in list(self._markets):
                old_dex = old.split(":", 1)[0] if ":" in old else ""
                if old_dex == dex:
                    del self._markets[old]
            self._markets.update(refreshed)
            self._market_times[dex] = time.monotonic()
            if symbol not in self._markets:
                raise MetaMaskError("UNKNOWN_MARKET", "Requested perpetual market was not found")
            return self._markets[symbol]

    def price(self, symbol: str) -> Decimal:
        query = {"type": "metaAndAssetCtxs"}
        if ":" in symbol:
            query["dex"] = symbol.split(":", 1)[0]
        result = self.info(query)
        if not isinstance(result, list) or len(result) != 2:
            raise MetaMaskError("INVALID_RESPONSE", "Venue mark-price contexts are missing")
        universe = json_object(result[0], "market metadata").get("universe")
        contexts = result[1]
        if not isinstance(universe, list) or not isinstance(contexts, list) or len(universe) != len(contexts):
            raise MetaMaskError("INVALID_RESPONSE", "Venue mark-price metadata is incomplete")
        names = [json_object(row).get("name") for row in universe]
        matches = [i for i, name in enumerate(names) if name == symbol]
        if len(matches) != 1:
            raise MetaMaskError("UNKNOWN_MARKET", "Requested mark-price market is absent or ambiguous")
        return decimal(json_object(contexts[matches[0]], "asset context").get("markPx"), positive=True)

    @staticmethod
    def normalize_price(price: Decimal, market: Market) -> Decimal:
        if price == price.to_integral_value():
            return price
        places = min(6 - market.size_decimals, 4 - price.adjusted())
        return price.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_EVEN)

    def order_action(
        self,
        symbol: str,
        side: str,
        quantity: Decimal,
        kind: str,
        limit: Decimal | None,
        tif: str,
        reduce_only: bool,
        client_id: str,
        *,
        slippage_bps: int = 50,
    ) -> dict[str, object]:
        market = self.market(symbol)
        if market.delisted:
            raise MetaMaskError("MARKET_DELISTED", "Delisted markets cannot accept new orders")
        quantity = decimal(quantity, positive=True)
        if side not in {"buy", "sell"} or kind not in {"market", "limit"} or tif not in {"gtc", "ioc", "alo"}:
            raise MetaMaskError("UNSUPPORTED_ORDER", "Use buy/sell, market/limit and explicit gtc/ioc/alo")
        if not isinstance(reduce_only, bool) or not _CLIENT_ID.fullmatch(client_id):
            raise ValueError("Canonical reduce-only flag and 128-bit client order ID are required")
        if quantity != quantity.quantize(Decimal(1).scaleb(-market.size_decimals)):
            raise MetaMaskError("SIZE_PRECISION", "Order size exceeds this market's exact precision")
        if kind == "market":
            if isinstance(slippage_bps, bool) or not 1 <= slippage_bps <= 500:
                raise ValueError("Slippage must be 1-500 basis points")
            base = self.price(symbol)
            intended = base * (1 + Decimal(slippage_bps if side == "buy" else -slippage_bps) / 10_000)
            price = self.normalize_price(intended, market)
            if (side == "buy" and price > intended) or (side == "sell" and price < intended):
                tick = Decimal(1).scaleb(-min(6 - market.size_decimals, 4 - intended.adjusted()))
                price += -tick if side == "buy" else tick
            tif = "ioc"
        else:
            intended = decimal(limit, positive=True)
            price = self.normalize_price(intended, market)
            if (side == "buy" and price > intended) or (side == "sell" and price < intended):
                raise MetaMaskError("PRICE_PRECISION", "Venue normalization would weaken the requested limit")
        if price <= 0:
            raise MetaMaskError("PRICE_PRECISION", "Price is below this market's supported precision")
        wire = {
            "a": market.index,
            "b": side == "buy",
            "p": wire_decimal(price),
            "s": wire_decimal(quantity),
            "r": reduce_only,
            "t": {"limit": {"tif": {"gtc": "Gtc", "ioc": "Ioc", "alo": "Alo"}[tif]}},
            "c": client_id,
        }
        return {"type": "order", "orders": [wire], "grouping": "na"}

    def submit_action(self, action: dict[str, object], nonce: int, signature: dict[str, object], expires_after=None):
        payload = {"action": action, "nonce": nonce, "signature": signature, "vaultAddress": None}
        if expires_after is not None:
            payload["expiresAfter"] = expires_after
        return json_object(self._post("/exchange", payload, mutation=True), "venue action response")

    def fee_rate(self, wallet: str) -> Decimal:
        fees = json_object(self.info({"type": "userFees", "user": address(wallet)}), "user fees")
        rate = max(Decimal(0), decimal(fees.get("userCrossRate")), decimal(fees.get("userAddRate")))
        if rate > Decimal("0.01"):
            raise MetaMaskError("FEE_RATE_UNSUPPORTED", "Venue fee rate exceeds the supported reservation envelope")
        return rate

    def account(self, wallet: str) -> dict[str, object]:
        wallet = address(wallet)
        mode = self.info({"type": "userAbstraction", "user": wallet})
        if mode not in ("default", "disabled", "unifiedAccount"):
            raise MetaMaskError(
                "ACCOUNT_MODE_UNSUPPORTED", "Portfolio margin and unknown account modes need separate valuation"
            )
        state = json_object(self.info({"type": "clearinghouseState", "user": wallet}), "perpetual account")
        margin = json_object(state.get("marginSummary"), "margin summary")
        if mode == "unifiedAccount":
            spot = json_object(self.info({"type": "spotClearinghouseState", "user": wallet}), "pooled collateral")
            balances = spot.get("balances")
            if not isinstance(balances, list):
                raise MetaMaskError("INVALID_RESPONSE", "Unified collateral ledger is missing")
            usdc = [json_object(row) for row in balances if isinstance(row, dict) and row.get("coin") == "USDC"]
            if len(usdc) > 1 or any(row.get("token") != 0 for row in usdc):
                raise MetaMaskError("ACCOUNT_BALANCE_MISMATCH", "HyperCore USDC identity is ambiguous")
            equity = decimal(usdc[0].get("total")) if usdc else Decimal(0)
            hold = decimal(usdc[0].get("hold")) if usdc else Decimal(0)
            if hold < 0:
                raise MetaMaskError("INVALID_RESPONSE", "Collateral hold cannot be negative")
            cash = max(Decimal(0), equity - hold)
        else:
            equity = decimal(margin.get("accountValue"))
            used = decimal(margin.get("totalMarginUsed"))
            if used < 0:
                raise MetaMaskError("INVALID_RESPONSE", "Used margin cannot be negative")
            cash = max(Decimal(0), min(decimal(state.get("withdrawable")), equity - used))
        raw_positions = state.get("assetPositions")
        if not isinstance(raw_positions, list):
            raise MetaMaskError("INVALID_RESPONSE", "Venue positions are missing")
        positions = []
        for raw in raw_positions:
            item = json_object(json_object(raw).get("position"), "position")
            quantity = decimal(item.get("szi"))
            if quantity == 0:
                continue
            coin = item.get("coin")
            if not isinstance(coin, str) or not coin:
                raise MetaMaskError("INVALID_RESPONSE", "Position market identity is missing")
            row = {
                "symbol": coin,
                "quantity": format(quantity, "f"),
                "marketValue": format(decimal(item.get("positionValue")), "f"),
                "unrealizedPnl": format(decimal(item.get("unrealizedPnl")), "f"),
            }
            if item.get("entryPx") is not None:
                row["entryPrice"] = format(decimal(item["entryPx"], positive=True), "f")
            positions.append(row)
        return {
            "cash": format(cash, "f"),
            "equity": format(equity, "f"),
            "positionsValue": format(equity - cash, "f"),
            "currency": "USDC",
            "positions": positions,
        }

    def candles(self, symbol: str, interval: str, start: int, end: int) -> list[list[object]]:
        if (
            interval not in _INTERVALS
            or isinstance(start, bool)
            or isinstance(end, bool)
            or not isinstance(start, int)
            or not isinstance(end, int)
            or not 0 <= start < end
        ):
            raise ValueError("Use supported interval and ordered Unix millisecond candle bounds")
        if end - start > 5000 * _INTERVALS[interval]:
            raise MetaMaskError(
                "CANDLE_HISTORY_INCOMPLETE", "Venue retains at most the latest 5000 candles per interval"
            )
        cutoff = min(end, int(time.time() * 1000))
        rows = self.info(
            {
                "type": "candleSnapshot",
                "req": {"coin": symbol, "interval": interval, "startTime": start, "endTime": end},
            }
        )
        if not isinstance(rows, list):
            raise MetaMaskError("INVALID_RESPONSE", "Candle response is not a list")
        candles = {}
        for raw in rows:
            row = json_object(raw, "candle")
            if row.get("s") != symbol or row.get("i") != interval:
                raise MetaMaskError("INVALID_RESPONSE", "Candle identity differs from the requested symbol or interval")
            stamp, closed = row.get("t"), row.get("T")
            if (
                isinstance(stamp, bool)
                or isinstance(closed, bool)
                or not isinstance(stamp, int)
                or not isinstance(closed, int)
                or closed != stamp + _INTERVALS[interval] - 1
            ):
                raise MetaMaskError("INVALID_RESPONSE", "Candle timestamps are invalid")
            if stamp < start or closed >= cutoff:
                continue
            opening, high, low, close = [decimal(row.get(key), positive=True) for key in ("o", "h", "l", "c")]
            volume = decimal(row.get("v"))
            if volume < 0 or high < max(opening, close, low) or low > min(opening, close, high):
                raise MetaMaskError("INVALID_RESPONSE", "Candle OHLC or volume values are inconsistent")
            values = [stamp, *[format(value, "f") for value in (opening, high, low, close, volume)]]
            if stamp in candles and candles[stamp] != values:
                raise MetaMaskError("INVALID_RESPONSE", "Candle history contains conflicting duplicate bars")
            candles[stamp] = values
        return [candles[stamp] for stamp in sorted(candles)]

    def order_status(self, wallet: str, venue_id: str | int) -> dict[str, object]:
        return json_object(self.info({"type": "orderStatus", "user": address(wallet), "oid": venue_id}), "order status")

    def fills(self, wallet: str, start: int, end: int) -> list[dict[str, object]]:
        if (
            isinstance(start, bool)
            or isinstance(end, bool)
            or not isinstance(start, int)
            or not isinstance(end, int)
            or not 0 <= start <= end
        ):
            raise ValueError("Fill history requires ordered Unix millisecond bounds")
        result = {}
        cursor = start
        while cursor <= end:
            rows = self.info(
                {
                    "type": "userFillsByTime",
                    "user": address(wallet),
                    "startTime": cursor,
                    "endTime": end,
                    "aggregateByTime": False,
                }
            )
            if not isinstance(rows, list):
                raise MetaMaskError("INVALID_RESPONSE", "Fill history is missing")
            newest = cursor
            for raw in rows:
                item = json_object(raw, "fill")
                stamp, trade_id = item.get("time"), item.get("tid")
                if (
                    isinstance(stamp, bool)
                    or isinstance(trade_id, bool)
                    or not isinstance(stamp, int)
                    or not isinstance(trade_id, int)
                    or not start <= stamp <= end
                ):
                    raise MetaMaskError("INVALID_RESPONSE", "Fill history identity or timestamp is invalid")
                newest = max(newest, stamp)
                if trade_id in result and result[trade_id] != item:
                    raise MetaMaskError("FILL_HISTORY_INCOMPLETE", "Venue returned conflicting records for one fill")
                result[trade_id] = item
            if len(rows) < 2000:
                break
            if newest <= cursor:
                raise MetaMaskError(
                    "FILL_HISTORY_INCOMPLETE", "Venue fill page cannot advance without dropping records"
                )
            cursor = newest
        return sorted(result.values(), key=lambda row: (row["time"], row["tid"]))

    def funding(self, wallet: str, start: int, end: int, symbol: str | None = None):
        rows = self.info({"type": "userFunding", "user": address(wallet), "startTime": start, "endTime": end})
        if not isinstance(rows, list):
            raise MetaMaskError("INVALID_RESPONSE", "Funding ledger is missing")
        if len(rows) >= 500:
            raise MetaMaskError("FUNDING_HISTORY_INCOMPLETE", "Request a smaller funding interval to avoid truncation")
        parsed = [json_object(row, "funding payment") for row in rows]
        return [row for row in parsed if symbol is None or json_object(row.get("delta")).get("coin") == symbol]

    def close(self):
        if self._owns_session:
            self.session.close()
