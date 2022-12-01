import datetime

from dateutil import tz
from lumibot.data_sources import TradovateData

from .broker import Broker


class Tradovate(TradovateData, Broker):
    NAME = "tradovate"

    def __init__(self, config, max_workers=20, chunk_size=100, connect_stream=False):
        # Calling init methods
        TradovateData.__init__(
            self, config, max_workers=max_workers, chunk_size=chunk_size
        )
        Broker.__init__(self, name=self.NAME, connect_stream=connect_stream)
        self.market = "us_futures"

    # =========Clock functions=====================

    def get_timestamp(self):
        """return current timestamp"""
        clock = self.ib.get_timestamp()
        return clock

    def market_close_time(self):
        return self.utc_to_local(self.market_hours(close=True))

    def is_market_open(self):
        """Return True if market is open else False"""
        open_time = self.utc_to_local(self.market_hours(close=False))
        close_time = self.utc_to_local(self.market_hours(close=True))

        current_time = datetime.datetime.now().astimezone(tz=tz.tzlocal())
        if self.market == "24/7":
            return True
        return (current_time >= open_time) and (close_time >= current_time)

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        open_time_this_day = self.utc_to_local(
            self.market_hours(close=False, next=False)
        )
        open_time_next_day = self.utc_to_local(
            self.market_hours(close=False, next=True)
        )
        now = self.utc_to_local(datetime.datetime.now())
        open_time = (
            open_time_this_day if open_time_this_day > now else open_time_next_day
        )
        current_time = datetime.datetime.now().astimezone(tz=tz.tzlocal())
        if self.is_market_open():
            return 0
        else:
            result = open_time.timestamp() - current_time.timestamp()
            return result

    def get_time_to_close(self):
        """Return the remaining time for the market to close in seconds"""
        close_time = self.utc_to_local(self.market_hours(close=True))
        current_time = datetime.datetime.now().astimezone(tz=tz.tzlocal())
        if self.is_market_open():
            result = close_time.timestamp() - current_time.timestamp()
            return result
        else:
            return 0

    # =========Positions functions==================

    def _get_balances_at_broker(self, quote_asset):
        """Get's the current actual cash, positions value, and total
        liquidation value from Alpaca.

        This method will get the current actual values from Alpaca
        for the actual cash, positions value, and total liquidation.

        Returns
        -------
        tuple of float
            (cash, positions_value, total_liquidation_value)
        """

        total_cash_value = 0
        gross_positions_value = 0
        net_liquidation_value = 0

        return (total_cash_value, gross_positions_value, net_liquidation_value)

    def _pull_positions(self, strategy):
        return []

    def get_historical_account_value(self):
        """Get the historical account value of the account."""

        return {
            "minute": None,
            "hour": None,
            "day": None,
        }