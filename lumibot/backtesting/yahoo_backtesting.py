from lumibot.data_sources import YahooData


class YahooDataBacktesting(YahooData):
    """
    YahooDataBacktesting is a DataSourceBacktesting that uses YahooData as a
    backtesting data source.
    """

    def __init__(self, datetime_start, datetime_end, **kwargs):
        # Call super().__init__ to ensure the MRO is followed correctly
        # Fix: Don't pass self as an argument to super().__init__
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, **kwargs)
