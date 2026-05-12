from __future__ import annotations

from typing import Any

from lumibot.data_sources.pandas_data import PandasData


class PandasDataBacktesting(PandasData):
    """
    Backtesting implementation of the PandasData class.  This class is just kept around for legacy purposes.
    Please just use PandasData directly instead.
    """

    def __init__(self, *args: Any, pandas_data: Any | None = None, **kwargs: Any) -> None:
        super().__init__(*args, pandas_data=pandas_data, **kwargs)
