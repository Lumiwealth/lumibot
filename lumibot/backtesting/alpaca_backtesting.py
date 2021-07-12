from lumibot.data_sources import AlpacaData

from .data_source_backtesting import DataSourceBacktesting


class AlpacaBacktesting(DataSourceBacktesting, AlpacaData):
    def __init__(self, datetime_start, datetime_end, **kwargs):
        raise Exception("AlpacaBacktesting is not currently operational")

        self.LIVE_DATA_SOURCE = AlpacaData
        AlpacaData.__init__(self, **kwargs)
        DataSourceBacktesting.__init__(self, datetime_start, datetime_end)
