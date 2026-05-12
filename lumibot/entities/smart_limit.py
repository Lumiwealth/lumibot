from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast

from .trading_slippage import TradingSlippage


class SmartLimitPreset(StrEnum):
    FAST = "fast"
    NORMAL = "normal"
    PATIENT = "patient"


_SMART_LIMIT_PRESET_CONFIG: dict[SmartLimitPreset, dict[str, int]] = {
    SmartLimitPreset.FAST: {"steps": 3, "step_seconds": 5},
    SmartLimitPreset.NORMAL: {"steps": 4, "step_seconds": 10},
    SmartLimitPreset.PATIENT: {"steps": 5, "step_seconds": 20},
}


@dataclass
class SmartLimitConfig:
    """Configuration for SMART_LIMIT orders.

    Parameters
    ----------
    preset : SmartLimitPreset
        Execution pace (FAST, NORMAL, PATIENT).
    final_price_pct : float
        Percent of bid/ask spread allowed for the final price (1.0 = full spread).
    slippage : TradingSlippage | float | None
        Absolute slippage applied in backtests (mid ± slippage).
    step_seconds : int | None
        Optional override for seconds per step.
    final_hold_seconds : int | None
        Optional override for final hold time.
    """

    preset: SmartLimitPreset | str = SmartLimitPreset.NORMAL
    final_price_pct: float = 1.0
    slippage: TradingSlippage | float | None = None
    step_seconds: int | None = None
    final_hold_seconds: int | None = None

    def __post_init__(self) -> None:
        self.preset = SmartLimitPreset(self.preset)
        if self.slippage is not None and not isinstance(self.slippage, TradingSlippage):
            self.slippage = TradingSlippage(amount=self.slippage)

    def get_step_count(self) -> int:
        return _SMART_LIMIT_PRESET_CONFIG[SmartLimitPreset(self.preset)]["steps"]

    def get_step_seconds(self) -> int:
        if self.step_seconds is not None:
            return int(self.step_seconds)
        return _SMART_LIMIT_PRESET_CONFIG[SmartLimitPreset(self.preset)]["step_seconds"]

    def get_final_hold_seconds(self) -> int:
        return int(self.final_hold_seconds) if self.final_hold_seconds is not None else 120

    def get_slippage_amount(self) -> float:
        slippage = self.slippage
        if slippage is None:
            return 0.0
        if not isinstance(slippage, TradingSlippage):
            slippage = TradingSlippage(amount=slippage)
        return float(slippage.amount)

    def to_dict(self) -> dict[str, Any]:
        slippage = self.slippage if isinstance(self.slippage, TradingSlippage) else None
        return {
            "preset": SmartLimitPreset(self.preset).value,
            "final_price_pct": float(self.final_price_pct),
            "slippage": slippage.to_dict() if slippage else None,
            "step_seconds": self.step_seconds,
            "final_hold_seconds": self.final_hold_seconds,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any] | None) -> SmartLimitConfig | None:
        if data is None:
            return None
        slippage_data = data.get("slippage")
        slippage = (
            TradingSlippage.from_dict(cast(Mapping[str, Any], slippage_data))
            if isinstance(slippage_data, Mapping)
            else None
        )
        return cls(
            preset=str(data.get("preset", SmartLimitPreset.NORMAL)),
            final_price_pct=float(data.get("final_price_pct", 1.0)),
            slippage=slippage,
            step_seconds=int(data["step_seconds"]) if data.get("step_seconds") is not None else None,
            final_hold_seconds=int(data["final_hold_seconds"]) if data.get("final_hold_seconds") is not None else None,
        )
