"""Canonical DataBento data source aliasing the Polars implementation."""

from .databento_data_polars import DataBentoDataPolars as DataBentoData
from .databento_data_pandas import DataBentoDataPandas
from .databento_data_polars import DataBentoDataPolars

__all__ = ["DataBentoData", "DataBentoDataPandas", "DataBentoDataPolars"]
