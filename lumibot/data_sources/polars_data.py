"""Base class for Polars-backed backtesting data sources."""

from typing import Any

from .data_source_backtesting import DataSourceBacktesting
from .polars_mixin import PolarsMixin


class PolarsData(PolarsMixin, DataSourceBacktesting):
    """Shared base for Polars-based backtesting data sources.

    Mirrors :class:`PandasData` so concrete backtesters accept the standard
    ``datetime_start`` / ``datetime_end`` arguments while leveraging the polars
    mixin for storage and parsing.
    """

    SOURCE = "POLARS"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1d", "day"]},
        {"timestep": "minute", "representations": ["1m", "minute"]},
    ]

    def __init__(self, *args: Any, auto_adjust: bool = True, allow_option_quote_fallback: bool = False, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.option_quote_fallback_allowed = allow_option_quote_fallback
        self.auto_adjust = auto_adjust
        self.name = "polars"
        self._init_polars_storage()
