import logging

import yfinance as yf
import pandas as pd


class Yahoo:
    @staticmethod
    def get_interday_returns_for_asset(symbol, momentum_length=1, period=None):
        if period:
            period_str = "%dd" % period
        else:
            period_str = "max"

        ticker = yf.Ticker(symbol)
        daily = ticker.history(period=period_str)
        df = daily[["Close", "Dividends"]].rename(
            columns={"Close": "price", "Dividends": "dividend"}
        )
        # Added shift to prevent "seeing into the future"
        df["dividend_yield"] = df["dividend"] / df["price"]
        df["price_change"] = df["price"].pct_change()
        df["return"] = df["dividend_yield"] + df["price_change"]
        df["momentum"] = df["price"].pct_change(periods=momentum_length)
        return df

    @staticmethod
    def get_average_trading_volume(symbol, length):
        ticker = yf.Ticker(symbol)
        history = ticker.history(period=f"{length}d")
        average_trading_volume = history["Volume"].mean()
        if pd.isna(average_trading_volume):
            logging.info(
                "Yahoo finance: Average trading volume over %d days not available for symbol %s"
                % (length, symbol)
            )
            return 0
        return average_trading_volume
