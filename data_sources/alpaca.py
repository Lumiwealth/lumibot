import logging
import pytz
from datetime import datetime
import pandas as pd

class Alpaca:
    @staticmethod
    def get_intraday_returns_for_asset(api, symbol, momentum_length=1):
        start=(pd.Timestamp.now() - pd.DateOffset(days=1)).isoformat()
        end=pd.Timestamp.now().isoformat()
        df = api.get_barset([symbol], 'minute', start=start, end=end).df[symbol]

        df['price_change'] = df['close'].pct_change()
        df['momentum'] = df['close'].pct_change(periods=momentum_length)
        return df
