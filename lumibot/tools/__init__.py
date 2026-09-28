"""Compatibility exports for :mod:`lumibot.tools`.

The old package initializer imported every helper module at once. That made
basic imports pay for plotting, broker caches, Yahoo, CCXT, and analytics even
when callers only needed one small helper.
"""

from importlib import import_module as _import_module

_SUBMODULES = {
    "backtest_cache",
    "black_scholes",
    "ccxt_data_store",
    "data_downloader_queue_client",
    "databento_helper",
    "databento_helper_polars",
    "debugers",
    "decorators",
    "futures_roll",
    "helpers",
    "ibkr_helper",
    "ibkr_secdef",
    "indicators",
    "lumibot_logger",
    "pandas",
    "parquet_series_cache",
    "parquet_utils",
    "polars_utils",
    "polygon_helper",
    "polygon_helper_async",
    "polygon_helper_polars_optimized",
    "projectx_helpers",
    "runtime_telemetry",
    "schwab_helper",
    "smart_limit_utils",
    "symbol_parser",
    "symbol_normalization",
    "thetadata_helper",
    "types",
    "yahoo_helper",
    "alpaca_helpers",
    "bitunix_helpers",
}

_NAME_TO_MODULE = {
    "BS": "black_scholes",
    "CcxtCacheDB": "ccxt_data_store",
    "SchwabHelper": "schwab_helper",
    "YahooHelper": "yahoo_helper",
    "append_locals": "decorators",
    "execute_after": "decorators",
    "snatch_locals": "decorators",
    "staticdecorator": "decorators",
    "add_file_handler": "lumibot_logger",
    "get_logger": "lumibot_logger",
    "get_strategy_logger": "lumibot_logger",
    "set_log_level": "lumibot_logger",
    "cagr": "indicators",
    "calculate_returns": "indicators",
    "cash_flow_adjusted_returns": "indicators",
    "create_tearsheet": "indicators",
    "cumulative_to_period_flows": "indicators",
    "get_risk_free_rate": "indicators",
    "get_symbol_returns": "indicators",
    "max_drawdown": "indicators",
    "performance": "indicators",
    "plot_indicators": "indicators",
    "plot_returns": "indicators",
    "romad": "indicators",
    "sharpe": "indicators",
    "stats_summary": "indicators",
    "total_return": "indicators",
    "volatility": "indicators",
    "Decimal": "types",
    "check_numeric": "types",
    "check_positive": "types",
    "check_price": "types",
    "check_quantity": "types",
}

_HELPER_EXPORTS = {
    "LUMIBOT_DEFAULT_PYTZ",
    "LUMIBOT_DEFAULT_TIMEZONE",
    "MarketCalendar",
    "TwentyFourSevenCalendar",
    "ComparaisonMixin",
    "colored",
    "create_options_symbol",
    "date_n_trading_days_from_date",
    "day_deduplicate",
    "deduplicate_sequence",
    "dt",
    "fill_void",
    "get_chunks",
    "get_decimals",
    "get_lumibot_datetime",
    "get_timezone_from_datetime",
    "get_trading_days",
    "get_trading_times",
    "has_more_than_n_decimal_places",
    "hashlib",
    "is_daily_data",
    "is_market_open",
    "is_regular_trading_session",
    "lru_cache",
    "mcal",
    "os",
    "parse_symbol",
    "canonicalize_timestep",
    "parse_canonical_timestep",
    "parse_timestep_qty_and_unit",
    "pd",
    "prettify_dataframe_with_decimals",
    "print_full_pandas_dataframes",
    "print_progress_bar",
    "pytz",
    "quantize_to_num_decimals",
    "re",
    "set_pandas_float_display_precision",
    "sys",
    "time",
    "to_datetime_aware",
    "weakref",
}

for _name in _HELPER_EXPORTS:
    _NAME_TO_MODULE[_name] = "helpers"

_NAME_TO_MODULE["parse_symbol"] = "symbol_parser"

for _name in (
    "day_deduplicate",
    "fill_void",
    "is_daily_data",
    "np",
    "pd",
    "prettify_dataframe_with_decimals",
    "print_full_pandas_dataframes",
    "set_pandas_float_display_precision",
):
    _NAME_TO_MODULE[_name] = "pandas"

for _name in ("PerfCounters", "perf_counter", "perf_counters"):
    _NAME_TO_MODULE[_name] = "debugers"

_NAME_TO_MODULE["ROUND_HALF_EVEN"] = "helpers"

__all__ = sorted(_SUBMODULES | set(_NAME_TO_MODULE))


def __getattr__(name):
    if name in _SUBMODULES:
        module = _import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module

    module_name = _NAME_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = _import_module(f"{__name__}.{module_name}")
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
