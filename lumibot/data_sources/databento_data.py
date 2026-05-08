"""Canonical DataBento data source aliasing the Polars implementation."""

from importlib import import_module

__all__ = ["DataBentoData", "DataBentoDataPandas", "DataBentoDataPolars"]

_NAME_TO_IMPORT = {
    "DataBentoData": ("lumibot.data_sources.databento_data_polars", "DataBentoDataPolars"),
    "DataBentoDataPolars": ("lumibot.data_sources.databento_data_polars", "DataBentoDataPolars"),
    "DataBentoDataPandas": ("lumibot.data_sources.databento_data_pandas", "DataBentoDataPandas"),
}


def __getattr__(name):
    try:
        module_name, attr_name = _NAME_TO_IMPORT[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc

    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
