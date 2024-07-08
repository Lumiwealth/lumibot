from lumibot.data_sources import CcxtBacktestingData

class CcxtBacktesting(CcxtBacktestingData):
    def __init__(self, datetime_start, datetime_end, **kwargs):
        CcxtBacktestingData.__init__(self, datetime_start, datetime_end, **kwargs)