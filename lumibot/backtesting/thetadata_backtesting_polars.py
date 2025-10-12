"""ThetaData backtesting data source implemented with polars."""

from __future__ import annotations

import logging
import subprocess
from datetime import timedelta
from decimal import Decimal
from typing import Optional, Union

import polars as pl

from lumibot.data_sources.polars_data import PolarsData
from lumibot.entities import Asset, Bars, Quote
from lumibot.credentials import THETADATA_CONFIG
from lumibot.tools import thetadata_helper

logger = logging.getLogger(__name__)


START_BUFFER = timedelta(days=5)


class ThetaDataBacktestingPolars(PolarsData):
    """Backtesting implementation of ThetaData using polars storage."""

    SOURCE = "THETADATA"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1d", "day"]},
        {"timestep": "hour", "representations": ["1h", "hour"]},
        {"timestep": "minute", "representations": ["1m", "minute"]},
    ]

    option_quote_fallback_allowed = True

    def __init__(
        self,
        datetime_start,
        datetime_end,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_quote_data: bool = True,
        **kwargs,
    ):
        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            allow_option_quote_fallback=True,
            **kwargs,
        )

        if username is None:
            username = THETADATA_CONFIG.get("THETADATA_USERNAME")
        if password is None:
            password = THETADATA_CONFIG.get("THETADATA_PASSWORD")
        if username is None or password is None:
            logger.warning("ThetaData credentials are not configured; ThetaTerminal may fail to authenticate.")

        self._username = username
        self._password = password
        self._use_quote_data = use_quote_data

        self.kill_processes_by_name("ThetaTerminal.jar")
        thetadata_helper.reset_theta_terminal_tracking()

    # ------------------------------------------------------------------
    # Utilities and storage helpers
    # ------------------------------------------------------------------
    def kill_processes_by_name(self, keyword: str) -> None:
        """Mirrors pandas implementation: ensure Theta terminal is reset."""
        try:
            result = subprocess.run(["pgrep", "-f", keyword], capture_output=True, text=True)
            pids = [pid for pid in result.stdout.strip().split("\n") if pid]

            for pid in pids:
                logger.info("Killing ThetaTerminal process %s", pid)
                subprocess.run(["kill", "-9", pid], check=False)

            if not pids:
                logger.info("No processes found related to '%s'", keyword)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("Failed to kill ThetaTerminal processes: %s", exc)

    def get_start_datetime_and_ts_unit(self, length, timestep, start_dt=None, start_buffer=START_BUFFER):
        td, ts_unit = self.convert_timestep_str_to_timedelta(timestep)
        if ts_unit == "day":
            td = timedelta(days=length + 3)
        else:
            td *= length

        if start_dt is not None:
            start_datetime = start_dt - td
        else:
            start_datetime = self.datetime_start - td
        start_datetime = start_datetime - start_buffer
        return start_datetime, ts_unit

    def _store_data(self, key, data: pl.DataFrame) -> None:
        lazy_frame = self._store_data_polars(key, data)
        if lazy_frame is None:
            return

        derived_columns = [pl.col("close").pct_change().alias("price_change")]
        if "dividend" in data.columns:
            derived_columns.extend([
                (pl.col("dividend") / pl.col("close")).alias("dividend_yield"),
                ((pl.col("dividend") / pl.col("close")) + pl.col("close").pct_change()).alias("return"),
            ])
        else:
            derived_columns.extend([
                pl.lit(0.0).alias("dividend_yield"),
                pl.col("close").pct_change().alias("return"),
            ])

        self._data_store[key] = lazy_frame.with_columns(derived_columns)

    # ------------------------------------------------------------------
    # Data retrieval
    # ------------------------------------------------------------------
    def _update_data(self, asset: Asset, quote: Optional[Asset], length: int, timestep: str, start_dt=None) -> None:
        search_asset = asset
        asset_separated = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(search_asset, tuple):
            asset_separated, quote_asset = search_asset
        else:
            search_asset = (search_asset, quote_asset)

        start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(length, timestep, start_dt)

        if search_asset in self._data_store:
            return

        try:
            current_dt = self.get_datetime()
            df_ohlc = thetadata_helper.get_price_data(
                self._username,
                self._password,
                asset_separated,
                start_datetime,
                self.datetime_end,
                timespan=ts_unit,
                quote_asset=quote_asset,
                dt=current_dt,
                datastyle="ohlc",
                include_after_hours=True,
            )
            if df_ohlc is None or df_ohlc.empty:
                return

            datetime_col = df_ohlc.index.name or "index"
            ohlc_frame = pl.from_pandas(df_ohlc.reset_index()).rename({datetime_col: "datetime"})
            ohlc_frame = ohlc_frame.sort("datetime")
            combined_frame = ohlc_frame

            if self._use_quote_data and ts_unit in {"minute", "hour", "second"}:
                df_quote = thetadata_helper.get_price_data(
                    self._username,
                    self._password,
                    asset_separated,
                    start_datetime,
                    self.datetime_end,
                    timespan=ts_unit,
                    quote_asset=quote_asset,
                    dt=current_dt,
                    datastyle="quote",
                    include_after_hours=True,
                )
                if df_quote is not None and not df_quote.empty:
                    quote_datetime_col = df_quote.index.name or "index"
                    quote_frame = pl.from_pandas(df_quote.reset_index()).rename({quote_datetime_col: "datetime"})
                    quote_frame = quote_frame.sort("datetime")
                    combined_frame = ohlc_frame.join(quote_frame, on="datetime", how="left")
                    quote_cols = [
                        "bid",
                        "ask",
                        "bid_size",
                        "ask_size",
                        "bid_condition",
                        "ask_condition",
                        "bid_exchange",
                        "ask_exchange",
                    ]
                    forward_fill_exprs = [
                        pl.col(col).fill_null(strategy="forward")
                        for col in quote_cols
                        if col in combined_frame.columns
                    ]
                    if forward_fill_exprs:
                        combined_frame = combined_frame.with_columns(forward_fill_exprs)
        except Exception as exc:  # pragma: no cover - logged upstream
            logger.error("Error getting data from ThetaData: %s", exc)
            return

        if combined_frame.is_empty():
            return

        self._store_data(search_asset, combined_frame)

    # ------------------------------------------------------------------
    # DataSourceBacktesting overrides
    # ------------------------------------------------------------------
    def _pull_source_symbol_bars(
        self,
        asset: Asset,
        length: int,
        timestep: str = "minute",
        timeshift: Optional[timedelta] = None,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        include_after_hours: bool = True,
    ) -> Optional[pl.DataFrame]:
        current_dt = self.get_datetime()
        self._update_data(asset, quote, length, timestep, current_dt)

        search_asset = asset if isinstance(asset, tuple) else (asset, quote or Asset("USD", "forex"))
        lazy_data = self._get_data_lazy(search_asset)
        if lazy_data is None:
            return None

        data = lazy_data.collect()
        if data.is_empty():
            return None

        end_filter = self.to_default_timezone(current_dt)
        if timestep == "day":
            end_filter = end_filter.replace(hour=23, minute=59, second=59, microsecond=999999) - timedelta(days=1)
        if timeshift:
            if isinstance(timeshift, int):
                timeshift = timedelta(days=timeshift)
            end_filter = end_filter - timeshift

        filtered = self._filter_data_polars(search_asset, data.lazy(), end_filter, length, timestep)
        if filtered is None or filtered.is_empty():
            return None

        if len(filtered) > length:
            filtered = filtered.tail(length)
        return filtered

    def _parse_source_symbol_bars(
        self,
        response: pl.DataFrame,
        asset: Asset,
        quote: Optional[Asset] = None,
        length: Optional[int] = None,
        return_polars: bool = False,
    ) -> Bars:
        return self._parse_source_symbol_bars_polars(
            response,
            asset,
            self.SOURCE,
            quote,
            length,
            return_polars=return_polars,
        )

    def get_historical_prices(
        self,
        asset: Asset | str,
        length: int,
        timestep: str = "minute",
        timeshift: Optional[timedelta] = None,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        include_after_hours: bool = True,
        return_polars: bool = False,
    ) -> Optional[Bars]:
        if isinstance(asset, str):
            asset = Asset(asset, asset_type=Asset.AssetType.STOCK)

        bars_df = self._pull_source_symbol_bars(asset, length, timestep, timeshift, quote, exchange, include_after_hours)
        if bars_df is None:
            return None
        return self._parse_source_symbol_bars(bars_df, asset, quote=quote, length=length, return_polars=return_polars)

    def get_historical_prices_between_dates(
        self,
        asset: Asset,
        timestep: str = "minute",
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        include_after_hours: bool = True,
        start_date=None,
        end_date=None,
        return_polars: bool = False,
    ) -> Optional[Bars]:
        self._update_data(asset, quote, 1, timestep)
        bars_df = self._pull_source_symbol_bars(asset, length=10_000, timestep=timestep, quote=quote)
        if bars_df is None:
            return None

        if start_date is not None:
            bars_df = bars_df.filter(pl.col("datetime") >= start_date)
        if end_date is not None:
            bars_df = bars_df.filter(pl.col("datetime") <= end_date)

        return self._parse_source_symbol_bars(bars_df, asset, quote=quote, return_polars=return_polars)

    def get_last_price(
        self,
        asset: Asset,
        timestep: str = "minute",
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        **kwargs,
    ) -> Union[float, Decimal, None]:
        bars_df = self._pull_source_symbol_bars(asset, length=1, timestep=timestep, quote=quote)
        if bars_df is None or bars_df.is_empty():
            return None
        value = bars_df["close"][0]
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, Decimal):
            return float(value)
        return float(value)

    def get_quote(
        self,
        asset: Asset,
        timestep: str = "minute",
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        **kwargs,
    ):
        try:
            return super().get_quote(asset=asset, quote=quote, exchange=exchange)
        except Exception as exc:
            logger.debug("ThetaData Polars get_quote fallback due to %s", exc)
            bars_df = self._pull_source_symbol_bars(asset, length=1, timestep=timestep, quote=quote)
            if bars_df is None or bars_df.is_empty():
                return Quote(asset=asset)

            last_row = bars_df.tail(1)
            row = {col: last_row.get_column(col)[0] for col in last_row.columns}

            return Quote(
                asset=asset,
                price=row.get("close"),
                bid=row.get("bid"),
                ask=row.get("ask"),
                volume=row.get("volume"),
                timestamp=self.get_datetime(),
                bid_size=row.get("bid_size"),
                ask_size=row.get("ask_size"),
                raw_data=row,
            )

    def get_chains(self, asset: Asset):
        from lumibot.entities import Chains

        chains_dict = thetadata_helper.get_chains_cached(
            username=self._username,
            password=self._password,
            asset=asset,
            current_date=self.get_datetime().date(),
        )
        return Chains(chains_dict)

__all__ = [
    "ThetaDataBacktestingPolars",
]
