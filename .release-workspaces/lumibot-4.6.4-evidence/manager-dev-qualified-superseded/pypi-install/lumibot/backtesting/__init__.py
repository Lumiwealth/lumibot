"""Backtesting package exports without importing every backend."""

from importlib import import_module as _import_module

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
    "PolymarketBacktesting": "polymarket_backtesting",
    "PolygonDataBacktesting": "polygon_backtesting",
    "RoutedBacktestingPandas": "routed_backtesting",
    "ThetaDataBacktesting": "thetadata_backtesting",
    "ThetaDataBacktestingPandas": "thetadata_backtesting_pandas",
    "YahooDataBacktesting": "yahoo_backtesting",
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
