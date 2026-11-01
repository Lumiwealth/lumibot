"""ProjectX trade-bar history through the standard Pandas/BacktestingBroker engine."""

import pandas as pd

from lumibot.data_sources import PandasData
from lumibot.data_sources.projectx_data import ProjectXData
from lumibot.entities import Asset, Data


class ProjectXBacktesting(PandasData):
    """Backtest with the user's own history entitlement; no historical bid/ask is invented.

    Contract selection is provider metadata as retrieved today, not historical roll
    membership. Dated futures select their requested expiration; continuous futures
    use the provider's current active contract and its available history.
    """

    def __init__(self, datetime_start, datetime_end, *, config=None, projectx_data=None,
                 assets=None, pandas_data=None, **kwargs):
        self.projectx_data = projectx_data
        self._projectx_config = config
        self._loaded_ranges = {}
        kwargs.setdefault("market", "us_futures")
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end,
                         pandas_data=pandas_data, config=config, **kwargs)
        for asset in assets or []:
            self._ensure_asset(asset)
        self.load_data()

    def load_data(self):
        # Keep each asset's actual printed timestamps. The standard engine owns
        # price availability, fill simulation and futures accounting.
        self._data_store = self.pandas_data
        index = pd.DatetimeIndex([self.datetime_start, self.datetime_end])
        for data in self._data_store.values():
            index = index.union(data.df.index)
        self._date_index = index.sort_values().unique()
        self._find_asset_in_data_store_cache.clear()
        return self.get_trading_days_pandas()

    def _ensure_asset(self, asset, length=1, timestep="minute", quote=None):
        if isinstance(asset, str):
            asset = Asset(asset)
        if isinstance(asset, tuple):
            asset, quote = asset
        # Fetch minute prints once; Data performs standard hour/day aggregation.
        length = int(length or 1)
        lookback = pd.Timedelta(days=max(7, length * (3 if timestep == "day" else 3 / 1440)))
        start = pd.Timestamp(self.datetime_start) - lookback
        key = (asset, quote or Asset("USD", "forex"))
        loaded = self._loaded_ranges.get(key)
        if (key in self.pandas_data and loaded is None) or (loaded is not None and loaded <= start):
            return
        if self.projectx_data is None:
            self.projectx_data = ProjectXData(self._projectx_config)
        bars = self.projectx_data.get_bars_from_datetime(asset, start.to_pydatetime(),
            self.datetime_end, "minute")
        self._loaded_ranges[key] = start
        if bars is None or bars.df.empty:
            return
        data = Data(asset, bars.df, timestep="minute", quote=key[1])
        # Initializing arrays on the original index does not create missing bars.
        data.repair_times_and_fill(data.df.index)
        self.pandas_data[key] = data
        self.load_data()

    def _pull_source_symbol_bars(self, asset, length, timestep="", timeshift=0, quote=None,
                                 exchange=None, include_after_hours=True):
        self._ensure_asset(asset, length, timestep or self._timestep, quote)
        return super()._pull_source_symbol_bars(asset, length, timestep, timeshift, quote,
                                               exchange, include_after_hours)

    def get_last_price(self, asset, quote=None, exchange=None):
        self._ensure_asset(asset, quote=quote)
        return super().get_last_price(asset, quote, exchange)

    def get_quote(self, asset, quote=None, exchange=None):
        self._ensure_asset(asset, quote=quote)
        return super().get_quote(asset, quote, exchange)

    def close(self):
        if self.projectx_data is not None:
            self.projectx_data.client.close()
