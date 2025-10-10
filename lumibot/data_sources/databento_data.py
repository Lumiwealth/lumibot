"""Canonical DataBento data source aliasing the polars implementation."""

from .databento_data_polars_live import DataBentoDataPolarsLive as DataBentoData

__all__ = ["DataBentoData"]
