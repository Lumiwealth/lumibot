from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

DecimalLike = Decimal | float | str | int


class TradingSlippage:
    """TradingSlippage class. Defines a per-order slippage amount for backtesting fills."""

    def __init__(self, amount: DecimalLike = 0.0) -> None:
        """
        Parameters
        ----------
        amount : Decimal, float, or None
            Absolute slippage amount applied to the fill price (quote currency).

        Example
        --------
        >>> from lumibot.entities import TradingSlippage
        >>> slippage = TradingSlippage(amount=0.05)  # $0.05 slippage
        """
        self.amount = Decimal(amount)

    def to_dict(self) -> dict[str, float]:
        return {"amount": float(self.amount)}

    @classmethod
    def from_dict(cls, data: Mapping[str, DecimalLike] | None) -> TradingSlippage | None:
        if data is None:
            return None
        return cls(amount=data.get("amount", 0.0))
