import logging
import pytz
from datetime import datetime
import pandas as pd

class Alpaca:
    @staticmethod
    def get_recent_minute_momentum_for_asset(api, symbol, momentum_length=1):
        start=(pd.Timestamp.now() - pd.DateOffset(hours=2)).isoformat()
        end=pd.Timestamp.now().isoformat()
        df = api.get_minute_barset_df_for_symbol(symbol, start, end)

        df['price_change'] = df['close'].pct_change()
        df['momentum'] = df['close'].pct_change(periods=momentum_length)
        return df
