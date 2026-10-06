"""Data source package exports without importing every provider backend."""

from importlib import import_module as _import_module

_NAME_TO_MODULE = {
    "AlpacaData": "alpaca_data",
    "AlphaVantageData": "alpha_vantage_data",
    "BitunixData": "bitunix_data",
    "CcxtBacktestingData": "ccxt_backtesting_data",
    "CcxtData": "ccxt_data",
    "DataBentoData": "databento_data",
    "DataBentoDataPandas": "databento_data",
    "DataBentoDataPolars": "databento_data",
    "DataSource": "data_source",
    "DataSourceBacktesting": "data_source_backtesting",
    "ExampleBrokerData": "example_broker_data",
    "InteractiveBrokersData": "interactive_brokers_data",
    "InteractiveBrokersRESTData": "interactive_brokers_rest_data",
    "NoDataFound": "exceptions",
    "PandasData": "pandas_data",
    "PolarsData": "polars_data",
    "PolymarketData": "polymarket_data",
    "PolygonDataBacktesting": "polygon_backtesting",
    "ProjectXData": "projectx_data",
    "SchwabData": "schwab_data",
    "TradierData": "tradier_data",
    "TradovateData": "tradovate_data",
    "UnavailabeTimestep": "exceptions",
    "YahooData": "yahoo_data",
}

_EXTERNAL_MODULES = {
    "polygon_backtesting": "lumibot.backtesting.polygon_backtesting",
}

__all__ = sorted(_NAME_TO_MODULE)


def __getattr__(name):
    module_name = _NAME_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    import_name = _EXTERNAL_MODULES.get(module_name, f"{__name__}.{module_name}")
    module = _import_module(import_name)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
