"""Backtesting package exports without importing every backend."""
# pyright: reportUnsupportedDunderAll=false

from importlib import import_module as _import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .alpaca_backtesting import AlpacaBacktesting as AlpacaBacktesting
    from .alpha_vantage_backtesting import AlphaVantageBacktesting as AlphaVantageBacktesting
    from .backtesting_broker import BacktestingBroker as BacktestingBroker
    from .ccxt_backtesting import CcxtBacktesting as CcxtBacktesting
    from .databento_backtesting import DataBentoDataBacktesting as DataBentoDataBacktesting
    from .databento_backtesting_pandas import DataBentoDataBacktestingPandas as DataBentoDataBacktestingPandas
    from .databento_backtesting_polars import DataBentoDataBacktestingPolars as DataBentoDataBacktestingPolars
    from .interactive_brokers_rest_backtesting import (
        InteractiveBrokersRESTBacktesting as InteractiveBrokersRESTBacktesting,
    )
    from .pandas_backtesting import PandasDataBacktesting as PandasDataBacktesting
    from .polygon_backtesting import PolygonDataBacktesting as PolygonDataBacktesting
    from .routed_backtesting import RoutedBacktestingPandas as RoutedBacktestingPandas
    from .thetadata_backtesting import ThetaDataBacktesting as ThetaDataBacktesting
    from .thetadata_backtesting_pandas import ThetaDataBacktestingPandas as ThetaDataBacktestingPandas
    from .yahoo_backtesting import YahooDataBacktesting as YahooDataBacktesting

_NAME_TO_MODULE = {
    "AlpacaBacktesting": "alpaca_backtesting",
    "AlphaVantageBacktesting": "alpha_vantage_backtesting",
    "BacktestingBroker": "backtesting_broker",
    "CcxtBacktesting": "ccxt_backtesting",
    "DataBentoDataBacktesting": "databento_backtesting",
    "DataBentoDataBacktestingPandas": "databento_backtesting_pandas",
    "DataBentoDataBacktestingPolars": "databento_backtesting_polars",
    "InteractiveBrokersRESTBacktesting": "interactive_brokers_rest_backtesting",
    "PandasDataBacktesting": "pandas_backtesting",
    "PolygonDataBacktesting": "polygon_backtesting",
    "RoutedBacktestingPandas": "routed_backtesting",
    "ThetaDataBacktesting": "thetadata_backtesting",
    "ThetaDataBacktestingPandas": "thetadata_backtesting_pandas",
    "YahooDataBacktesting": "yahoo_backtesting",
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
