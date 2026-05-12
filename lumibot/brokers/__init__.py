"""Broker package exports without importing every broker backend."""
# pyright: reportUnsupportedDunderAll=false

from importlib import import_module as _import_module
from typing import Any

_NAME_TO_MODULE = {
    "Alpaca": "alpaca",
    "Bitunix": "bitunix",
    "Broker": "broker",
    "LumibotBrokerAPIError": "broker",
    "Ccxt": "ccxt",
    "ExampleBroker": "example_broker",
    "InteractiveBrokers": "interactive_brokers",
    "InteractiveBrokersREST": "interactive_brokers_rest",
    "ProjectX": "projectx",
    "Schwab": "schwab",
    "Tradier": "tradier",
    "Tradovate": "tradovate",
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
