"""Compatibility exports for entity classes without importing every entity."""
# pyright: reportUnsupportedDunderAll=false

from importlib import import_module as _import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .asset import Asset as Asset
    from .asset import AssetsMapping as AssetsMapping
    from .bar import Bar as Bar
    from .bars import Bars as Bars
    from .cash_event import CashEvent as CashEvent
    from .chains import Chains as Chains
    from .data import Data as Data
    from .data_polars import DataPolars as DataPolars
    from .dataline import Dataline as Dataline
    from .order import Order as Order
    from .position import Position as Position
    from .quote import Quote as Quote
    from .smart_limit import SmartLimitConfig as SmartLimitConfig
    from .smart_limit import SmartLimitPreset as SmartLimitPreset
    from .trading_fee import TradingFee as TradingFee
    from .trading_slippage import TradingSlippage as TradingSlippage

_NAME_TO_MODULE = {
    "Asset": "asset",
    "AssetsMapping": "asset",
    "Bar": "bar",
    "Bars": "bars",
    "CashEvent": "cash_event",
    "Chains": "chains",
    "Data": "data",
    "DataPolars": "data_polars",
    "Dataline": "dataline",
    "Order": "order",
    "Position": "position",
    "Quote": "quote",
    "TradingFee": "trading_fee",
    "TradingSlippage": "trading_slippage",
    "SmartLimitConfig": "smart_limit",
    "SmartLimitPreset": "smart_limit",
}

__all__: list[str] = sorted(_NAME_TO_MODULE)


def __getattr__(name: str) -> Any:
    module_name = _NAME_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = _import_module(f"{__name__}.{module_name}")
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
