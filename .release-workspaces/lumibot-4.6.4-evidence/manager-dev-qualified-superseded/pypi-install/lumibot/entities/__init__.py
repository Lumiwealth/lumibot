"""Compatibility exports for entity classes without importing every entity."""

from importlib import import_module as _import_module

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

__all__ = sorted(_NAME_TO_MODULE)


def __getattr__(name):
    module_name = _NAME_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = _import_module(f"{__name__}.{module_name}")
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
