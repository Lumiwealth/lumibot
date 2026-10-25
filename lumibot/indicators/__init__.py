"""Technical indicators.

* :class:`Indicators` is the per-strategy accessor behind ``self.indicators``
  (pandas-ta-classic passthrough, custom functions, and ``get()`` for the
  Indicator v2 registry). See :mod:`lumibot.indicators.indicators`.
* Indicator v2 (:class:`Indicator`) processes one bar at a time with
  forming-bar rollback, runs inside strategies or standalone through
  :class:`IndicatorRunner`, and is discoverable through :data:`registry`.
  See :mod:`lumibot.indicators.base`.
* :mod:`lumibot.indicators.execution` is the hook for *how* indicators run
  (in-process by default; a host may run them elsewhere).
"""
from lumibot.indicators.base import (
    Bar,
    Indicator,
    InputSpec,
    InvalidIndicatorInputError,
    Level,
    OutputSpec,
)
from lumibot.indicators.execution import (
    IndicatorExecutor,
    InProcessExecutor,
    get_indicator_executor,
    set_indicator_executor,
    use_indicator_executor,
)
from lumibot.indicators.indicator_registry import (
    IndicatorRegistry,
    UnknownIndicatorError,
    UnknownIndicatorVersionError,
    register,
    registry,
)
from lumibot.indicators.indicators import IndicatorRow, Indicators
from lumibot.indicators.runner import IndicatorRunner

__all__ = [
    "Bar",
    "Indicator",
    "IndicatorExecutor",
    "IndicatorRegistry",
    "IndicatorRow",
    "IndicatorRunner",
    "Indicators",
    "InProcessExecutor",
    "InputSpec",
    "InvalidIndicatorInputError",
    "Level",
    "OutputSpec",
    "UnknownIndicatorError",
    "UnknownIndicatorVersionError",
    "get_indicator_executor",
    "register",
    "registry",
    "set_indicator_executor",
    "use_indicator_executor",
]
