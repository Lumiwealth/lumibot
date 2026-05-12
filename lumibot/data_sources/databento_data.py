"""Canonical DataBento data source aliasing the Polars implementation."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lumibot.data_sources.databento_data_pandas import DataBentoDataPandas
    from lumibot.data_sources.databento_data_polars import DataBentoDataPolars

    DataBentoData = DataBentoDataPolars

__all__ = ["DataBentoData", "DataBentoDataPandas", "DataBentoDataPolars"]

_NAME_TO_IMPORT = {
    "DataBentoData": ("lumibot.data_sources.databento_data_polars", "DataBentoDataPolars"),
    "DataBentoDataPolars": ("lumibot.data_sources.databento_data_polars", "DataBentoDataPolars"),
    "DataBentoDataPandas": ("lumibot.data_sources.databento_data_pandas", "DataBentoDataPandas"),
}


def __getattr__(name: str) -> Any:
    try:
        module_name, attr_name = _NAME_TO_IMPORT[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc

    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
