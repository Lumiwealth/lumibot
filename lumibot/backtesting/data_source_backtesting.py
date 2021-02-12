from lumibot.tools import print_progress_bar


class DataSourceBacktesting:
    def __init__(self, datetime_start, datetime_end):
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self._datetime = datetime_start

    def _update_datetime(self, new_datetime):
        self._datetime = new_datetime
        print_progress_bar(new_datetime, self.datetime_start, self.datetime_end)
