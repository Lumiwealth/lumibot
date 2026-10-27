"""Derivative instrument terms shared by provider adapters and simulation."""

from dataclasses import asdict, dataclass
from decimal import Decimal


def _decimal(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("Contract values must be finite")
    return result


@dataclass(frozen=True)
class ContractSpec:
    """Contract economics; quantity_unit preserves the adapter's native units.

    Linear P&L is denominated in quote currency, inverse P&L in base currency.
    A different settlement currency requires conversion at the accounting layer.
    Account position/margin modes are deliberately separate from instrument terms.
    """

    base: str
    quote: str
    settle: str
    contract_size: Decimal = Decimal("1")
    inverse: bool = False
    quantity_unit: str = "contracts"

    def __post_init__(self):
        for name in ("base", "quote", "settle"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Contract {name} currency is required")
            object.__setattr__(self, name, value.upper())
        size = _decimal(self.contract_size)
        if size <= 0:
            raise ValueError("Contract size must be positive")
        if self.quantity_unit not in ("contracts", "base"):
            raise ValueError("Contract quantity_unit must be contracts or base")
        object.__setattr__(self, "contract_size", size)

    @staticmethod
    def _price(price):
        price = _decimal(price)
        if price <= 0:
            raise ValueError("Contract price must be positive")
        return price

    @property
    def pnl_currency(self):
        return self.base if self.inverse else self.quote

    def exposure(self, quantity, price):
        quantity, price = _decimal(quantity), self._price(price)
        if self.quantity_unit == "base":
            base, quote = quantity, quantity * price
        elif self.inverse:
            quote = quantity * self.contract_size
            base = quote / price
        else:
            base = quantity * self.contract_size
            quote = base * price
        return {"base": base, "quote": quote, "settle": self.settle, "contract_size": self.contract_size}

    def quantity_for_notional(self, notional, price):
        """Native order quantity for a notional in this instrument's quote currency.

        Provider precision and minimums still apply at order submission.
        """
        notional, price = _decimal(notional), self._price(price)
        if self.quantity_unit == "base":
            return notional / price
        return notional / self.contract_size if self.inverse else notional / (self.contract_size * price)

    def pnl(self, quantity, entry_price, exit_price):
        """Signed P&L in pnl_currency, before costs and settlement conversion."""
        quantity = _decimal(quantity)
        entry, exit = self._price(entry_price), self._price(exit_price)
        if self.inverse:
            contracts = quantity * entry / self.contract_size if self.quantity_unit == "base" else quantity
            return contracts * self.contract_size * (1 / entry - 1 / exit)
        base = quantity if self.quantity_unit == "base" else quantity * self.contract_size
        return base * (exit - entry)

    def to_dict(self):
        return {**asdict(self), "contract_size": str(self.contract_size)}

    @classmethod
    def from_dict(cls, value):
        return cls(**value)

    @classmethod
    def from_ccxt_market(cls, market):
        return cls(market["base"], market["quote"], market["settle"], market["contractSize"],
                   bool(market.get("inverse")))
