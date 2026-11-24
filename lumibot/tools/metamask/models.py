"""Validated wallet authority and execution intent for native MetaMask brokers."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from .provider import MetaMaskError, address, identifier, json_object

# Native CAIP identities follow ethereum-lists/chains; unknown chains must not inherit ETH.
_NATIVE_SLIP44 = {
    1: 60,
    10: 60,
    56: 714,
    97: 1,
    137: 966,
    324: 60,
    8453: 60,
    42161: 60,
    421614: 1,
    43114: 9005,
    43113: 1,
    59144: 60,
    80002: 1,
    84532: 1,
    11155111: 1,
    11155420: 1,
}


def decimal(value: object, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise MetaMaskError("INVALID_AMOUNT", "Monetary value must be a finite decimal")
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise MetaMaskError("INVALID_AMOUNT", "Monetary value is not a decimal") from None
    if not number.is_finite() or (positive and number <= 0):
        raise MetaMaskError("INVALID_AMOUNT", "Monetary value must be finite and positive")
    return number


def positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise MetaMaskError("INVALID_INTENT", f"{name} must be a positive integer")
    return value


@dataclass(frozen=True)
class TokenAsset:
    symbol: str
    chain_id: int
    address: str
    decimals: int

    @classmethod
    def parse(cls, value: object) -> TokenAsset:
        item = json_object(value, "token asset")
        symbol = item.get("symbol")
        if not isinstance(symbol, str) or not 1 <= len(symbol) <= 30:
            raise MetaMaskError("INVALID_ASSET", "Asset symbol is required")
        chain = positive_integer(item.get("chainId"), "asset chain")
        contract = item.get("address")
        contract = "native" if contract == "native" else address(contract)
        places = item.get("decimals")
        if isinstance(places, bool) or not isinstance(places, int) or not 0 <= places <= 36:
            raise MetaMaskError("INVALID_ASSET", "Token decimals must be 0-36")
        return cls(symbol, chain, contract, places)

    @property
    def identity(self) -> tuple[int, str, int]:
        return self.chain_id, self.address, self.decimals

    @property
    def caip(self) -> str:
        if self.address == "native":
            coin = _NATIVE_SLIP44.get(self.chain_id)
            if coin is None:
                raise MetaMaskError("NATIVE_ASSET_UNSUPPORTED", "Native price identity is not defined for this chain")
            token = f"slip44:{coin}"
        else:
            token = "erc20:" + self.address
        return f"eip155:{self.chain_id}/{token}"

    def record(self) -> dict[str, object]:
        return {"symbol": self.symbol, "chainId": self.chain_id, "address": self.address, "decimals": self.decimals}

    def to_atomic(self, amount: Decimal) -> int:
        parts = amount.as_tuple()
        scaled = Decimal((parts.sign, parts.digits, parts.exponent + self.decimals))
        if scaled != scaled.to_integral_value():
            raise MetaMaskError("PRECISION", "Input amount exceeds token precision")
        if scaled < 0 or scaled >= Decimal(2**256):
            raise MetaMaskError("INVALID_AMOUNT", "Token amount exceeds uint256 range")
        return int(scaled)

    def from_atomic(self, amount: int) -> Decimal:
        if not 0 <= amount < 2**256:
            raise MetaMaskError("INVALID_AMOUNT", "Token amount exceeds uint256 range")
        parts = Decimal(amount).as_tuple()
        return Decimal((parts.sign, parts.digits, -self.decimals))


@dataclass(frozen=True)
class Binding:
    agent_id: str
    wallet: str
    network: str
    enabled: bool
    expires_at: int
    policy_digest: str
    markets: tuple[str, ...]
    chains: tuple[int, ...]
    assets: tuple[TokenAsset, ...]
    max_order_usd: Decimal
    max_daily_usd: Decimal
    max_leverage: int
    max_slippage_bps: int

    @classmethod
    def parse(cls, value: dict[str, object], *, validate_registry: bool = True) -> Binding:
        agent = identifier(value.get("id"), "AGENT_ID")
        wallet = address(value.get("wallet"))
        network, enabled = value.get("network"), value.get("enabled")
        if network not in ("testnet", "mainnet") or not isinstance(enabled, bool):
            raise MetaMaskError("INVALID_BINDING", "Binding requires explicit network and enabled state")
        expiry = positive_integer(value.get("expiresAt"), "binding expiry")
        digest = value.get("policyDigest")
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise MetaMaskError("INVALID_BINDING", "Binding must pin its reviewed provider policy digest")
        markets, chains, assets = value.get("markets"), value.get("chains"), value.get("assets")
        if not isinstance(markets, list) or not all(isinstance(s, str) and 1 <= len(s) <= 60 for s in markets):
            raise MetaMaskError("INVALID_BINDING", "Binding must name its permitted markets")
        if not isinstance(chains, list) or not chains or not isinstance(assets, list):
            raise MetaMaskError("INVALID_BINDING", "Binding requires chain and token registries")
        parsed_chains = tuple(positive_integer(chain, "chain") for chain in chains)
        parsed_assets = tuple(TokenAsset.parse(asset) for asset in assets)
        identities = [(asset.chain_id, asset.address) for asset in parsed_assets]
        # Legacy registries must not prevent observing or canceling accepted perpetual orders.
        if validate_registry and len(set(identities)) != len(identities):
            raise MetaMaskError("INVALID_BINDING", "Asset registry contains duplicate chain and contract identities")
        if validate_registry and any(asset.chain_id not in parsed_chains for asset in parsed_assets):
            raise MetaMaskError("INVALID_BINDING", "Asset registry contains an unapproved chain")
        leverage = positive_integer(value.get("maxLeverage"), "maximum leverage")
        slippage = positive_integer(value.get("maxSlippageBps"), "maximum slippage")
        if leverage > 50 or slippage > 500:
            raise MetaMaskError("INVALID_BINDING", "Binding exceeds supported leverage or slippage limits")
        return cls(
            agent,
            wallet,
            network,
            enabled,
            expiry,
            digest,
            tuple(markets),
            parsed_chains,
            parsed_assets,
            decimal(value.get("maxOrderUsd"), positive=True),
            decimal(value.get("maxDailyUsd"), positive=True),
            leverage,
            slippage,
        )


@dataclass(frozen=True)
class PerpChildIntent:
    identifier: str
    side: str
    quantity: Decimal
    kind: str
    tpsl: str
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    reduce_only: bool = True

    @classmethod
    def parse(cls, value: object) -> PerpChildIntent:
        item = json_object(value, "child perpetual order")
        allowed = {"id", "side", "quantity", "orderType", "tpsl", "limitPrice", "stopPrice", "reduceOnly"}
        if set(item) - allowed:
            raise MetaMaskError("INVALID_INTENT", "Unknown child perpetual order parameters")
        operation_id = identifier(item.get("id"), "child order ID")
        side = item.get("side")
        kind = item.get("orderType")
        tpsl = item.get("tpsl")
        reduce_only = item.get("reduceOnly", True)
        if side not in ("buy", "sell") or kind not in ("limit", "stop", "stop_limit") or tpsl not in ("tp", "sl"):
            raise MetaMaskError("INVALID_INTENT", "Unsupported child side, order type or trigger role")
        if reduce_only is not True:
            raise MetaMaskError("INVALID_INTENT", "Grouped exit legs must be reduce-only")
        if kind == "limit":
            if tpsl != "tp":
                raise MetaMaskError("INVALID_INTENT", "Limit exit child orders must be take-profit legs")
            if item.get("stopPrice") is not None:
                raise MetaMaskError("INVALID_INTENT", "Take-profit limit leg does not take a separate stopPrice")
            limit_price = decimal(item.get("limitPrice"), positive=True)
            stop_price = None
        elif kind == "stop":
            if item.get("limitPrice") is not None:
                raise MetaMaskError("INVALID_INTENT", "Stop market leg cannot specify limitPrice")
            limit_price = None
            stop_price = decimal(item.get("stopPrice"), positive=True)
        else:
            limit_price = decimal(item.get("limitPrice"), positive=True)
            stop_price = decimal(item.get("stopPrice"), positive=True)
        return cls(
            operation_id,
            side,
            decimal(item.get("quantity"), positive=True),
            kind,
            tpsl,
            limit_price,
            stop_price,
            True,
        )

    def record(self) -> dict[str, object]:
        result: dict[str, object] = {
            "id": self.identifier,
            "side": self.side,
            "quantity": format(self.quantity, "f"),
            "orderType": self.kind,
            "tpsl": self.tpsl,
            "reduceOnly": True,
        }
        if self.limit_price is not None:
            result["limitPrice"] = format(self.limit_price, "f")
        if self.stop_price is not None:
            result["stopPrice"] = format(self.stop_price, "f")
        return result


@dataclass(frozen=True)
class PerpIntent:
    identifier: str
    symbol: str
    side: str
    quantity: Decimal
    kind: str
    limit_price: Decimal | None
    leverage: int
    reduce_only: bool
    tif: str
    stop_price: Decimal | None = None
    tpsl: str | None = None
    order_class: str = "simple"
    children: tuple[PerpChildIntent, ...] = ()

    @classmethod
    def parse(cls, value: dict[str, object]) -> PerpIntent:
        if value.get("kind") != "perp":
            raise MetaMaskError("INVALID_INTENT", "Expected a perpetual order")
        allowed = {
            "kind",
            "id",
            "symbol",
            "side",
            "quantity",
            "orderType",
            "limitPrice",
            "stopPrice",
            "tpsl",
            "orderClass",
            "children",
            "leverage",
            "reduceOnly",
            "tif",
        }
        if set(value) - allowed:
            raise MetaMaskError("INVALID_INTENT", "Unknown perpetual order parameters")
        operation_id = identifier(value.get("id"), "order ID")
        symbol = value.get("symbol")
        side, kind, tif = value.get("side"), value.get("orderType"), value.get("tif", "gtc")
        reduce_only = value.get("reduceOnly", False)
        order_class = value.get("orderClass", "simple")
        if not isinstance(symbol, str) or not 1 <= len(symbol) <= 60:
            raise MetaMaskError("INVALID_INTENT", "Perpetual market symbol is required")
        if order_class not in ("simple", "bracket", "oto", "oco"):
            raise MetaMaskError("INVALID_INTENT", "Unsupported perpetual order class")
        if (
            side not in ("buy", "sell")
            or kind not in ("market", "limit", "stop", "stop_limit")
            or tif not in ("gtc", "ioc", "alo")
        ):
            raise MetaMaskError("INVALID_INTENT", "Unsupported side, order type or time in force")
        if not isinstance(reduce_only, bool):
            raise MetaMaskError("INVALID_INTENT", "reduceOnly must be a canonical boolean")
        quantity = decimal(value.get("quantity"), positive=True)
        raw_children = value.get("children")
        if raw_children is None:
            parsed_children: tuple[PerpChildIntent, ...] = ()
        elif isinstance(raw_children, list):
            parsed_children = tuple(PerpChildIntent.parse(child) for child in raw_children)
        else:
            raise MetaMaskError("INVALID_INTENT", "Order children must be a list")

        if order_class == "simple":
            if parsed_children:
                raise MetaMaskError("INVALID_INTENT", "Simple orders cannot include child legs")
            if kind in ("stop", "stop_limit"):
                if tif != "gtc":
                    raise MetaMaskError("INVALID_INTENT", "Trigger orders support time in force 'gtc' only")
                stop_price = decimal(value.get("stopPrice"), positive=True)
                limit = decimal(value.get("limitPrice"), positive=True) if kind == "stop_limit" else None
                if kind == "stop" and value.get("limitPrice") is not None:
                    raise MetaMaskError("INVALID_INTENT", "Stop market orders cannot specify limitPrice")
                tpsl = value.get("tpsl", "sl")
                if tpsl not in ("tp", "sl"):
                    raise MetaMaskError("INVALID_INTENT", "Trigger role tpsl must be 'tp' or 'sl'")
            else:
                if value.get("stopPrice") is not None or value.get("tpsl") is not None:
                    raise MetaMaskError("INVALID_INTENT", "Market and limit orders cannot specify trigger fields")
                stop_price = None
                tpsl = None
                limit = decimal(value.get("limitPrice"), positive=True) if kind == "limit" else None
        else:
            if not 1 <= len(parsed_children) <= 2:
                raise MetaMaskError("INVALID_INTENT", "Grouped orders require one or two exit child legs")
            all_ids = [operation_id, *(child.identifier for child in parsed_children)]
            if len(set(all_ids)) != len(all_ids):
                raise MetaMaskError("INVALID_INTENT", "Grouped order and child identifiers must be distinct")
            if any(child.quantity != quantity for child in parsed_children):
                raise MetaMaskError("INVALID_INTENT", "Grouped exit leg quantities must match the parent quantity")
            roles = [child.tpsl for child in parsed_children]
            if len(set(roles)) != len(roles):
                raise MetaMaskError(
                    "INVALID_INTENT", "Grouped orders may include at most one take-profit and one stop-loss leg"
                )
            if order_class in ("bracket", "oto"):
                if kind not in ("market", "limit"):
                    raise MetaMaskError("INVALID_INTENT", "Bracket and OTO entries must be market or limit orders")
                if value.get("stopPrice") is not None or value.get("tpsl") is not None:
                    raise MetaMaskError("INVALID_INTENT", "Bracket and OTO entry orders cannot specify trigger fields")
                if order_class == "oto" and len(parsed_children) != 1:
                    raise MetaMaskError("INVALID_INTENT", "OTO orders require exactly one child leg")
                expected_child_side = "sell" if side == "buy" else "buy"
                if any(child.side != expected_child_side for child in parsed_children):
                    raise MetaMaskError("INVALID_INTENT", "Bracket and OTO exit legs must oppose the entry side")
                stop_price = None
                tpsl = None
                limit = decimal(value.get("limitPrice"), positive=True) if kind == "limit" else None
            else:  # oco
                if len(parsed_children) != 2 or set(roles) != {"tp", "sl"}:
                    raise MetaMaskError(
                        "INVALID_INTENT", "OCO orders require one take-profit and one stop-loss child leg"
                    )
                if (
                    value.get("limitPrice") is not None
                    or value.get("stopPrice") is not None
                    or value.get("tpsl") is not None
                ):
                    raise MetaMaskError("INVALID_INTENT", "OCO prices must be specified on the child legs")
                if any(child.side != side for child in parsed_children):
                    raise MetaMaskError("INVALID_INTENT", "OCO child legs must share the order exit side")
                if not reduce_only:
                    raise MetaMaskError("INVALID_INTENT", "OCO exit orders must be reduce-only")
                limit = None
                stop_price = None
                tpsl = None
        return cls(
            operation_id,
            symbol,
            side,
            quantity,
            kind,
            limit,
            positive_integer(value.get("leverage", 1), "leverage"),
            reduce_only,
            tif,
            stop_price=stop_price,
            tpsl=tpsl,
            order_class=order_class,
            children=parsed_children,
        )

    def record(self) -> dict[str, object]:
        result: dict[str, object] = {
            "kind": "perp",
            "id": self.identifier,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": format(self.quantity, "f"),
            "orderType": self.kind,
            "leverage": self.leverage,
            "reduceOnly": self.reduce_only,
            "tif": self.tif,
        }
        if self.limit_price is not None:
            result["limitPrice"] = format(self.limit_price, "f")
        if self.stop_price is not None:
            result["stopPrice"] = format(self.stop_price, "f")
        if self.tpsl is not None:
            result["tpsl"] = self.tpsl
        if self.order_class != "simple":
            result["orderClass"] = self.order_class
        if self.children:
            result["children"] = [child.record() for child in self.children]
        return result


@dataclass(frozen=True)
class SwapIntent:
    identifier: str
    source: TokenAsset
    destination: TokenAsset
    amount: Decimal
    slippage_bps: int

    @classmethod
    def parse(cls, value: dict[str, object]) -> SwapIntent:
        if value.get("kind") != "swap" or set(value) - {"kind", "id", "input", "output", "inputAmount", "slippageBps"}:
            raise MetaMaskError("INVALID_INTENT", "Expected a same-chain exact-input swap")
        source, destination = TokenAsset.parse(value.get("input")), TokenAsset.parse(value.get("output"))
        if source.chain_id != destination.chain_id or source.identity == destination.identity:
            raise MetaMaskError("INVALID_PAIR", "Swap must use distinct tokens on the same chain")
        amount = decimal(value.get("inputAmount"), positive=True)
        source.to_atomic(amount)
        return cls(
            identifier(value.get("id"), "swap ID"),
            source,
            destination,
            amount,
            positive_integer(value.get("slippageBps"), "slippage"),
        )

    def record(self) -> dict[str, object]:
        return {
            "kind": "swap",
            "id": self.identifier,
            "input": self.source.record(),
            "output": self.destination.record(),
            "inputAmount": format(self.amount, "f"),
            "slippageBps": self.slippage_bps,
        }
