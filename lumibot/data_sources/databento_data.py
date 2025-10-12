"""Canonical DataBento data source aliasing the polars implementation."""

from .databento_data_polars import DataBentoDataPolars as DataBentoData

__all__ = ["DataBentoData"]
