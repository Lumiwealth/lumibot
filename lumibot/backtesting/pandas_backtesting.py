from lumibot.data_sources import PandasData


class PandasDataBacktesting(PandasData):
    """
    Backtesting implementation of the PandasData class.  This class is just kept around for legacy purposes.
    Please just use PandasData directly instead.
    """

    def __init__(self, *args, pandas_data=None, **kwargs):
        super().__init__(*args, pandas_data=pandas_data, **kwargs)
