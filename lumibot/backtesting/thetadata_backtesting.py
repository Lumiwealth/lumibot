"""ThetaData backtesting entry point (pandas-only)."""

from .thetadata_backtesting_pandas import ThetaDataBacktestingPandas, START_BUFFER

# Maintain legacy import name for backwards compatibility
ThetaDataBacktesting = ThetaDataBacktestingPandas

__all__ = [
    "ThetaDataBacktesting",
    "ThetaDataBacktestingPandas",
    "START_BUFFER",
]
