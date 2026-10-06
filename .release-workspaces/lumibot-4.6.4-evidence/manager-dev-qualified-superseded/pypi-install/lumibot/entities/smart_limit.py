from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union

from .trading_slippage import TradingSlippage


class SmartLimitPreset(str, Enum):
    FAST = "fast"
    NORMAL = "normal"
    PATIENT = "patient"


_SMART_LIMIT_PRESET_CONFIG = {
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

    preset: SmartLimitPreset = SmartLimitPreset.NORMAL
    final_price_pct: float = 1.0
    slippage: Optional[Union[TradingSlippage, float]] = None
    step_seconds: Optional[int] = None
    final_hold_seconds: Optional[int] = None

    def __post_init__(self):
        if isinstance(self.preset, str):
            self.preset = SmartLimitPreset(self.preset)
        if self.slippage is not None and not isinstance(self.slippage, TradingSlippage):
            self.slippage = TradingSlippage(amount=self.slippage)

    def get_step_count(self) -> int:
        return _SMART_LIMIT_PRESET_CONFIG[self.preset]["steps"]

    def get_step_seconds(self) -> int:
        if self.step_seconds is not None:
            return int(self.step_seconds)
        return _SMART_LIMIT_PRESET_CONFIG[self.preset]["step_seconds"]

    def get_final_hold_seconds(self) -> int:
        return int(self.final_hold_seconds) if self.final_hold_seconds is not None else 120

    def get_slippage_amount(self) -> float:
        if self.slippage is None:
            return 0.0
        return float(self.slippage.amount)

    def to_dict(self) -> dict:
        return {
            "preset": self.preset.value,
            "final_price_pct": float(self.final_price_pct),
            "slippage": self.slippage.to_dict() if self.slippage else None,
            "step_seconds": self.step_seconds,
            "final_hold_seconds": self.final_hold_seconds,
        }

    @classmethod
    def from_dict(cls, data: Optional[dict]):
        if data is None:
            return None
        slippage_data = data.get("slippage")
        slippage = TradingSlippage.from_dict(slippage_data) if slippage_data else None
        return cls(
            preset=data.get("preset", SmartLimitPreset.NORMAL),
            final_price_pct=data.get("final_price_pct", 1.0),
            slippage=slippage,
            step_seconds=data.get("step_seconds"),
            final_hold_seconds=data.get("final_hold_seconds"),
        )
