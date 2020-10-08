import logging
import pytz
from datetime import datetime
import pandas as pd

class Alpaca:
    @staticmethod
    def get_recent_minute_momentum_for_asset(api, symbol, momentum_length=1):
        if momentum_length > 100:
            logging.error(f"You cannot get more than 100 timestamps from Alpaca, but you set a momentum_length of {momentum_length}")
        
        start=(pd.Timestamp.now() - pd.DateOffset(minutes=momentum_length)).isoformat()
        end=pd.Timestamp.now().isoformat()
        df = api.get_minute_barset_df_for_symbol(symbol, start, end)

        df['price_change'] = df['close'].pct_change()
        df['momentum'] = df['close'].pct_change(periods=momentum_length)
        return df
