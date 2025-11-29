"""ThetaData backtesting entry point (pandas-only)."""

from .thetadata_backtesting_pandas import START_BUFFER, ThetaDataBacktestingPandas

# Maintain legacy import name for backwards compatibility
ThetaDataBacktesting = ThetaDataBacktestingPandas

__all__ = [
    "ThetaDataBacktesting",
    "ThetaDataBacktestingPandas",
    "START_BUFFER",
]
