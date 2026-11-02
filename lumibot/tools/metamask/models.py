"""Validated wallet authority and execution intent for native MetaMask brokers."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from .provider import MetaMaskError, address, identifier, json_object


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
        token = "slip44:60" if self.address == "native" else "erc20:" + self.address
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
    def parse(cls, value: dict[str, object]) -> Binding:
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
            tuple(positive_integer(chain, "chain") for chain in chains),
            tuple(TokenAsset.parse(asset) for asset in assets),
            decimal(value.get("maxOrderUsd"), positive=True),
            decimal(value.get("maxDailyUsd"), positive=True),
            leverage,
            slippage,
        )


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
        if not isinstance(symbol, str) or not 1 <= len(symbol) <= 60:
            raise MetaMaskError("INVALID_INTENT", "Perpetual market symbol is required")
        if side not in ("buy", "sell") or kind not in ("market", "limit") or tif not in ("gtc", "ioc", "alo"):
            raise MetaMaskError("INVALID_INTENT", "Unsupported side, order type or time in force")
        if not isinstance(reduce_only, bool):
            raise MetaMaskError("INVALID_INTENT", "reduceOnly must be a canonical boolean")
        limit = decimal(value.get("limitPrice"), positive=True) if kind == "limit" else None
        return cls(
            operation_id,
            symbol,
            side,
            decimal(value.get("quantity"), positive=True),
            kind,
            limit,
            positive_integer(value.get("leverage", 1), "leverage"),
            reduce_only,
            tif,
        )

    def record(self) -> dict[str, object]:
        result = {
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
