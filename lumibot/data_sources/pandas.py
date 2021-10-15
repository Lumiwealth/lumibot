import pandas as pd

from lumibot.entities import Bars, AssetsMapping
from .data_source import DataSource


class PandasData(DataSource):
    IS_BACKTESTING_DATA_SOURCE = True
    SOURCE = "PANDAS"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1D", "day"]},
        {"timestep": "minute", "representations": ["1M", "minute"]},
    ]

    def __init__(self, pandas_data, config=None, auto_adjust=True, **kwargs):
        self.name = "pandas"
        self.pandas_data = pandas_data
        self.auto_adjust = auto_adjust
        self._data_store = {}
        self._date_index = None
        self._date_supply = None
        self._timestep = "day"
        self._expiries_exist = False

    def load_data(self, pandas_data):
        self._data_store = pandas_data
        self._expiries_exist = (
            len(
                [
                    v.asset.expiration
                    for v in self._data_store.values()
                    if v.asset.expiration is not None
                ]
            )
            > 0
        )
        self._date_index = self.update_date_index()
        self._timestep = list(self._data_store.values())[0].timestep
        pcal = self.get_trading_days_pandas()
        self._date_index = self.clean_trading_times(self._date_index, pcal)
        for asset, data in self._data_store.items():
            data.repair_times_and_fill(self._date_index)
        return pcal

    def clean_trading_times(self, dt_index, pcal):
        # Used to fill in blanks in the data, on trading days, within market trading hours.
        df = pd.DataFrame(range(len(dt_index)), index=dt_index)
        df = df.sort_index()
        df["dates"] = df.index.date
        df = df.merge(
            pcal[["market_open", "market_close"]], left_on="dates", right_index=True
        )
        if self._timestep == "minute":
            df = df.asfreq("1T", method="pad")
            result_index = df.loc[
                (df.index >= df["market_open"]) & (df.index <= df["market_close"]), :
            ].index
        else:
            result_index = df.index
        return result_index

    def get_trading_days_pandas(self):
        pcal = pd.DataFrame(self._date_index)
        pcal.columns = ["datetime"]
        pcal["date"] = pcal["datetime"].dt.date
        return pcal.groupby("date").agg(
            market_open=(
                "datetime",
                "first",
            ),
            market_close=(
                "datetime",
                "last",
            ),
        )

    def get_assets(self):
        return list(self._data_store.keys())

    def get_asset_by_name(self, name):
        return [asset for asset in self.get_assets() if asset.name == name]

    def get_asset_by_symbol(self, symbol):
        return [asset for asset in self.get_assets() if asset.symbol == symbol]

    def is_tradable(self, asset, dt, length=1, timestep="minute", timeshift=0):
        # Determines is an asset has data over dt, length, timestep, and timeshift.
        if self._data_store[asset].is_tradable(
            dt, length=length, timestep=timestep, timeshift=timeshift
        ):
            return True
        else:
            return False

    def get_tradable_assets(self, dt, length=1, timestep="minute", timeshift=0):
        # Returns list of assets that can be traded. Empty list if None.
        tradable = list()
        for asset, data in self._data_store.items():
            if data.is_tradable(
                dt, length=length, timestep=timestep, timeshift=timeshift
            ):
                tradable.append(asset)
        return tradable

    def update_date_index(self):
        dt_index = None
        for asset, data in self._data_store.items():
            if dt_index is None:
                dt_index = data.df.index
            else:
                dt_index = dt_index.join(data.df.index, how="outer")
        if self.datetime_end < dt_index[0]:
            raise ValueError(
                f"The ending date for the backtest was set for {self.datetime_end}. "
                f"The earliest data entered is {dt_index[0]}. \nNo backtest can "
                f"be run since there is no data before the backtest end date."
            )
        elif self.datetime_start > dt_index[-1]:
            raise ValueError(
                f"The starting date for the backtest was set for {self.datetime_start}. "
                f"The latest data entered is {dt_index[-1]}. \nNo backtest can "
                f"be run since there is no data after the backtest start date."
            )
        return dt_index

    def get_last_price(self, asset, timestep=None):
        # Takes an asset and returns the last known price
        return self._data_store[asset].get_last_price(self.get_datetime())

    def get_last_prices(self, assets, timestep=None):
        # Takes a list of assets and returns dictionary of last known prices for each.
        if timestep is None:
            timestep = self.MIN_TIMESTEP
        result = {}
        for asset in assets:
            result[asset] = self.get_last_price(asset, timestep=timestep)

        return AssetsMapping(result)

    def _pull_source_symbol_bars(
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=0
    ):
        if not timeshift:
            timeshift = 0

        if asset in self._data_store:
            data = self._data_store[asset]
        else:
            raise ValueError(
                f"The asset: `{asset}` does not exist or does not have data."
            )

        # result = data.tail(length)

        res = data.get_bars(
            self.get_datetime(), length=length, timestep=timestep, timeshift=timeshift
        )
        return res

    def _pull_source_bars(self, assets, length, timestep=MIN_TIMESTEP, timeshift=None):
        """pull broker bars for a list assets"""
        self._parse_source_timestep(timestep, reverse=True)

        result = {}
        for asset in assets:
            result[asset] = self._pull_source_symbol_bars(
                asset, length, timestep=timestep, timeshift=timeshift
            )
        return result

    def _parse_source_symbol_bars(self, response, asset):
        bars = Bars(response, self.SOURCE, asset, raw=response)
        return bars

    def get_yesterday_dividend(self, asset):
        pass

    def get_yesterday_dividends(self, assets):
        pass

    # =======Options methods.=================
    def get_chains(self, asset):
        """Returns option chains.

        Obtains option chain information for the asset (stock) from each
        of the exchanges the options trade on and returns a dictionary
        for each exchange.

        Parameters
        ----------
        asset : Asset object
            The stock whose option chain is being fetched. Represented
            as an asset object.

        Returns
        -------
        dictionary of dictionary for 'SMART' exchange only in
        backtesting. Each exchange has:
            - `Underlying conId` (int)
            - `TradingClass` (str) eg: `FB`
            - `Multiplier` (str) eg: `100`
            - `Expirations` (set of str) eg: {`20230616`, ...}
            - `Strikes` (set of floats)
        """
        SMART = dict(
            TradingClass=asset.symbol,
            Multiplier=100,
        )

        expirations = []
        strikes = []
        for store_asset, data in self._data_store.items():
            if store_asset.asset_type != "option":
                continue
            if store_asset.symbol != asset.symbol:
                continue
            expirations.append(store_asset.expiration)
            strikes.append(store_asset.strike)

        SMART["Expirations"] = sorted(list(set(expirations)))
        SMART["Strikes"] = sorted(list(set(strikes)))

        return {"SMART": SMART}

    def get_chain(self, chains, exchange="SMART"):
        """Returns option chain for a particular exchange.

        Takes in a full set of chains for all the exchanges and returns
        on chain for a given exchange. The the full chains are returned
        from `get_chains` method.

        Parameters
        ----------
        chains : dictionary of dictionaries
            The chains dictionary created by `get_chains` method.

        exchange : str optional
            The exchange such as `SMART`, `CBOE`. Default is `SMART`

        Returns
        -------
        dictionary
            A dictionary of option chain information for one stock and
            for one exchange. It will contain:
                - `Underlying conId` (int)
                - `TradingClass` (str) eg: `FB`
                - `Multiplier` (str) eg: `100`
                - `Expirations` (set of str) eg: {`20230616`, ...}
                - `Strikes` (set of floats)
        """

        for x, p in chains.items():
            if x == exchange:
                return p

        return None

    def get_expiration(self, chains, exchange="SMART"):
        """Returns expiration dates for an option chain for a particular
        exchange.

        Using the `chains` dictionary obtained from `get_chains` finds
        all of the expiry dates for the option chains on a given
        exchange. The return list is sorted.

        Parameters
        ---------
        chains : dictionary of dictionaries
            The chains dictionary created by `get_chains` method.

        exchange : str optional
            The exchange such as `SMART`, `CBOE`. Default is `SMART`.

        Returns
        -------
        list of str
            Sorted list of dates in the form of `20221013`.
        """
        if exchange != "SMART":
            raise ValueError(
                "When getting option expirations in backtesting, only the `SMART`"
                "exchange may be used. It is the default value. Please delete "
                "the `exchange` parameter or change the value to `SMART`."
            )
        return sorted(list(self.get_chain(chains, exchange=exchange)["Expirations"]))

    def get_multiplier(self, chains, exchange="SMART"):
        """Returns option chain for a particular exchange.

        Using the `chains` dictionary obtained from `get_chains` finds
        all of the multiplier for the option chains on a given
        exchange.

        Parameters
        ----------
        chains : dictionary of dictionaries
            The chains dictionary created by `get_chains` method.

        exchange : str optional
            The exchange such as `SMART`, `CBOE`. Default is `SMART`

        Returns
        -------
        list of str
            Sorted list of dates in the form of `20221013`.
        """

        return self.get_chain(chains, exchange)["Multiplier"]

    def get_strikes(self, asset):
        """Returns a list of strikes for a give underlying asset.

        Using the `chains` dictionary obtained from `get_chains` finds
        all of the multiplier for the option chains on a given
        exchange.

        Parameters
        ----------
        asset : Asset object
            Asset object as normally used for an option but without
            the strike information.

            Example:
            asset = self.create_asset(
                "FB",
                asset_type="option",
                expiration=self.options_expiry_to_datetime_date("20210924"),
                right="CALL",
                multiplier=100,
            )

            `expiration` can also be expressed as
            `datetime.datetime.date()`

        Returns
        -------
        list of floats
            Sorted list of strikes as floats.
        """
        strikes = []
        for store_asset, data in self._data_store.items():
            if (
                store_asset.asset_type == "option"
                and store_asset.symbol == asset.symbol
                and store_asset.expiration == asset.expiration
                and store_asset.right == asset.right
            ):
                strikes.append(store_asset.strike)

        return sorted(list(set(strikes)))
