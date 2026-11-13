"""Backtest on the price history of the user's own broker connection (Schwab, Tradier).

``BrokerHistoryBacktesting`` is one generic source; a broker plugs in as a small adapter in
``lumibot.tools.broker_history``. It downloads each stock's daily bars for the whole backtest
window once, through the shared bar cache of that broker connection, then serves point-in-time
slices from memory with ``PandasData`` (the same slicing, and the same no-lookahead rules, as
the other pandas-backed sources).

Limits are loud: stocks and ETFs only, daily bars only for now, and each broker's history
depth and rate limit. A request outside them raises ``BrokerHistoryUnavailable`` naming the
limit. The source never switches to another provider and never fills a missing bar.

Select it with ``BACKTESTING_DATA_SOURCE=schwab`` or ``tradier``, or pass
``SchwabHistoryBacktesting`` / ``TradierHistoryBacktesting`` to ``backtest()``.
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd

from lumibot.data_sources import PandasData
from lumibot.entities import Asset, Data
from lumibot.tools import broker_history
from lumibot.tools.broker_history import BrokerHistoryUnavailable
from lumibot.tools.helpers import parse_timestep_qty_and_unit

START_BUFFER = timedelta(days=7)  # calendar days of extra daily bars before a lookback (weekends, holidays)


class BrokerHistoryBacktesting(PandasData):
    """Generic broker price-history backtests. Subclasses set ``BROKER``."""

    BROKER: str | None = None
    # The backtesting broker fills from pandas bars when SOURCE is "PANDAS" (as Polygon does).
    SOURCE = "PANDAS"

    def __init__(self, datetime_start, datetime_end, pandas_data=None, config=None, adapter=None, **kwargs):
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, pandas_data=pandas_data,
                         config=config, **kwargs)
        if adapter is None:
            if not self.BROKER:
                raise ValueError("BrokerHistoryBacktesting needs a broker: use SchwabHistoryBacktesting or "
                                 "TradierHistoryBacktesting, or pass adapter=")
            adapter = broker_history.make_adapter(self.BROKER, config if isinstance(config, dict) else None)
        self.adapter = adapter
        self.history_broker = adapter.name
        # Daily bars only: fills and valuations use the day's bar (the backtesting broker reads this).
        self._timestep = "day"
        self._loaded_from: dict[tuple, pd.Timestamp] = {}
        self._loaded_frames: dict[tuple, pd.DataFrame] = {}

    # ------------------------------------------------------------------ loading

    def _update_pandas_data(self, asset, quote, length, timestep, start_dt=None):
        asset_only, quote_asset = (asset if isinstance(asset, tuple) else (asset, quote))
        quote_asset = quote_asset if quote_asset is not None else Asset("USD", "forex")
        self.adapter.check_asset(asset_only)

        qty, unit = parse_timestep_qty_and_unit(timestep or "day")
        if unit != "day":
            minutes = int(qty) * (60 if unit == "hour" else 1)
            raise self.adapter.intraday_unavailable(minutes)

        start_datetime, _unit = self.get_start_datetime_and_ts_unit(length, "day", start_dt, start_buffer=START_BUFFER)
        key = (asset_only, quote_asset)
        loaded = self._loaded_from.get(key)
        wanted = pd.Timestamp(min(start_datetime, self.datetime_start - START_BUFFER))
        if loaded is not None and loaded <= wanted:
            return
        self.adapter.check_daily_window(wanted.to_pydatetime())
        existing = self._loaded_frames.get(key)
        # A longer lookback later in the run reads only the earlier stretch it is missing.
        read_end = (loaded - pd.Timedelta(days=1)) if existing is not None else pd.Timestamp(self.datetime_end)
        frame = self._read_daily(asset_only, wanted, read_end)
        if existing is not None:
            frame = existing if frame is None or frame.empty else pd.concat([frame, existing])
            frame = frame[~frame.index.duplicated(keep="last")].sort_index()
        if frame is None or frame.empty:
            raise BrokerHistoryUnavailable(
                f"{self.adapter.title} returned no daily bars for {asset_only.symbol} between "
                f"{wanted.date().isoformat()} and {pd.Timestamp(self.datetime_end).date().isoformat()}. "
                "Check the symbol and the backtest dates."
            )
        self._loaded_frames[key] = frame
        data = Data(asset_only, frame, timestep="day", quote=quote_asset)
        self.pandas_data.update(self._set_pandas_data_keys([data]))
        self._find_asset_in_data_store_cache.clear()
        self._loaded_from[key] = wanted

    def _read_daily(self, asset, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame | None:
        """Real daily bars through the shared bar cache of this broker connection."""
        from lumibot.tools import shared_bar_cache

        symbol = str(asset.symbol)
        series = shared_bar_cache.BarSeries.make(
            self.adapter.name, symbol, "1day", scope=self.adapter.scope, adjusted=True,
            assetType="stock", history="daily", session="regular",
        )

        def fetch(range_start, range_end):
            return self.adapter.daily_bars(symbol, range_start, range_end)

        tz = "America/New_York"
        start_utc = (start.tz_convert(tz) if start.tzinfo else start.tz_localize(tz)).normalize().tz_convert("UTC")
        end_utc = ((end.tz_convert(tz) if end.tzinfo else end.tz_localize(tz)).normalize()
                   + pd.Timedelta(days=1)).tz_convert("UTC")
        frame = shared_bar_cache.read_bars(series, start_utc, end_utc, fetch)
        if frame is None or frame.empty:
            return None
        frame = shared_bar_cache.to_timezone(frame, tz)
        # A daily bar is complete only after its session: a bar whose close is still ahead is dropped.
        frame = frame[frame.index <= pd.Timestamp.now(tz=tz)]
        frame.index.name = "datetime"
        return frame

    # ------------------------------------------------------------------ data source API

    def _pull_source_symbol_bars(self, asset, length, timestep="day", timeshift=None, quote=None,
                                 exchange=None, include_after_hours=True):
        self._update_pandas_data(asset, quote, length, timestep, self.get_datetime())
        return super()._pull_source_symbol_bars(asset, length, timestep, timeshift, quote, exchange,
                                                include_after_hours)

    def get_historical_prices_between_dates(self, asset, timestep="day", quote=None, exchange=None,
                                            include_after_hours=True, start_date=None, end_date=None):
        self._update_pandas_data(asset, quote, 1, timestep, start_date or self.get_datetime())
        response = super()._pull_source_symbol_bars_between_dates(
            asset, timestep, quote, exchange, include_after_hours, start_date, end_date
        )
        if response is None:
            return None
        return self._parse_source_symbol_bars(response, asset, quote=quote)

    def get_last_price(self, asset, timestep="day", quote=None, exchange=None, **kwargs):
        # Daily bars only: a fill or valuation reads the current day's bar.
        self._update_pandas_data(asset, quote, 1, "day", self.get_datetime())
        return super().get_last_price(asset=asset, quote=quote, exchange=exchange)

    def get_chains(self, asset, quote=None, exchange=None):
        raise BrokerHistoryUnavailable(
            f"{self.adapter.title} history backtests support stocks and ETFs only; option chains are not "
            "available here. Choose a data source with option history."
        )


class SchwabHistoryBacktesting(BrokerHistoryBacktesting):
    """Backtest on the daily history of your Schwab connection (stocks and ETFs)."""

    BROKER = "schwab"


class TradierHistoryBacktesting(BrokerHistoryBacktesting):
    """Backtest on the daily history of your Tradier connection (stocks and ETFs)."""

    BROKER = "tradier"


__all__ = ["BrokerHistoryBacktesting", "SchwabHistoryBacktesting", "TradierHistoryBacktesting"]
