from lumibot.data_sources import CcxtBactestingData

class CcxtBacktesting(CcxtBactestingData):
    def __init__(self, datetime_start, datetime_end, **kwargs):
        CcxtBactestingData.__init__(self, datetime_start, datetime_end, **kwargs)