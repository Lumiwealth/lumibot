from .dataline import Dataline
from lumibot import LUMIBOT_DEFAULT_PYTZ as DEFAULT_PYTZ
import pandas as pd


class Data:
    """Input and manage Pandas dataframes for backtesting.

    Parameters
    ----------

    Attributes
    ----------

    Methods
    -------

    """

    def __init__(
        self,
        strategy,
        asset,
        df,
        start_date=None,
        end_date=None,
        timestep="day",
        columns=None,
    ):
        self.strategy = strategy
        self.asset = asset
        self.symbol = self.asset.symbol

        self.start_date = start_date
        self.end_date = end_date

        self.df = self.columns(df)
        self.df = self.set_date_format(self.df)

        self.datalines = dict()
        self.to_datalines()
        x=1


    def columns(self, df):
        # Select columns to use, change to lower case, rename `date` if necessary.
        df.columns = [
            col.lower()
            if col.lower() in ["open", "high", "low", "close", "volume"]
            else col
            for col in df.columns
        ]

        return df

    def set_date_format(self, df):
        df.index.name = "datetime"
        df.index = pd.to_datetime(df.index)
        df.index = df.index.tz_localize(DEFAULT_PYTZ)
        return df

    def to_datalines(self):
        self.datalines.update(
            {
                "datetime": Dataline(
                    self.asset,
                    "datetime",
                    self.df.index.to_numpy(),
                    self.df.index.dtype,
                )
            }
        )
        setattr(self, "datetime", self.datalines["datetime"].dataline)

        for column in self.df.columns:
            self.datalines.update(
                {
                    column: Dataline(
                        self.asset,
                        column,
                        self.df[column].to_numpy(),
                        self.df[column].dtype,
                    )
                }
            )
            setattr(self, column, self.datalines[column].dataline)

    def get_last_price(self, iter_count):
        return self.datalines["close"].dataline[iter_count + 1]

    def get_bars(self, iter_count, length, timestep, timeshift):
        end_row = iter_count + 1 - timeshift
        start_row = end_row - length
        if start_row < 0:
            start_row = 0
        df_dict = {}

        for dl_name, dl in self.datalines.items():
            df_dict[dl_name] = dl.dataline[start_row: end_row]

        return df_dict

        # df = pd.DataFrame(df_dict).set_index('datetime')
        # return df

