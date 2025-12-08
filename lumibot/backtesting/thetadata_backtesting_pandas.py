from decimal import Decimal
from typing import Dict, Optional, Union, List

import logging
import pandas as pd
import pytz
import subprocess
from datetime import date, datetime, timedelta

from lumibot.data_sources import PandasData
from lumibot.entities import Asset, AssetsMapping, Data
from lumibot.credentials import THETADATA_CONFIG
from lumibot.tools import thetadata_helper

logger = logging.getLogger(__name__)


def _parity_log(message: str, *args) -> None:
    """Emit parity diagnostics only when debug logging is enabled."""
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(message, *args)


START_BUFFER = timedelta(days=5)


class ThetaDataBacktestingPandas(PandasData):
    """
    Backtesting implementation of ThetaData
    """

    # Allow both minute and day; broker decides cadence based on strategy sleeptime.
    MIN_TIMESTEP = "minute"
    # Allow the broker to switch to day-level fills for daily-cadence strategies
    ALLOW_DAILY_TIMESTEP = True

    IS_BACKTESTING_BROKER = True

    # Do not fall back to last_price when bid/ask quotes are unavailable for options
    option_quote_fallback_allowed = False

    def __init__(
        self,
        datetime_start,
        datetime_end,
        pandas_data=None,
        username=None,
        password=None,
        use_quote_data=True,
        **kwargs,
    ):
        # Pass allow_option_quote_fallback to parent to enable fallback mechanism
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, pandas_data=pandas_data,
                         allow_option_quote_fallback=True, **kwargs)

        # Default to minute; broker can flip to day for daily strategies.
        self._timestep = self.MIN_TIMESTEP

        if username is None:
            username = THETADATA_CONFIG.get("THETADATA_USERNAME")
        if password is None:
            password = THETADATA_CONFIG.get("THETADATA_PASSWORD")
        if username is None or password is None:
            logger.warning("ThetaData credentials are not configured; ThetaTerminal may fail to authenticate.")

        self._username       = username
        self._password       = password
        self._use_quote_data = use_quote_data

        self._dataset_metadata: Dict[tuple, Dict[str, object]] = {}
        self._chain_constraints = None

        # Set data_source to self since this class acts as both broker and data source
        self.data_source = self

        # CRITICAL FIX (2025-12-07): Set a unique client_id for queue fairness.
        # This ensures each backtest instance gets fair treatment in the queue,
        # even when multiple backtests are running concurrently.
        import uuid
        from lumibot.tools.thetadata_queue_client import set_queue_client_id
        unique_id = uuid.uuid4().hex[:8]
        strategy_name = kwargs.get('name', 'Backtest')
        client_id = f"{strategy_name}_{unique_id}"
        set_queue_client_id(client_id)
        logger.info(f"[THETA][QUEUE] Set unique client_id for queue fairness: {client_id}")

        self.kill_processes_by_name("ThetaTerminal.jar")
        thetadata_helper.reset_theta_terminal_tracking()

    def is_weekend(self, date):
        """
        Check if the given date is a weekend.

        :param date: datetime.date object
        :return: Boolean, True if weekend, False otherwise
        """
        return date.weekday() >= 5  # 5 = Saturday, 6 = Sunday

    def kill_processes_by_name(self, keyword):
        try:
            # Find all processes related to the keyword
            result = subprocess.run(['pgrep', '-f', keyword], capture_output=True, text=True)
            pids = result.stdout.strip().split('\n')

            if pids:
                for pid in pids:
                    if pid:  # Ensure the PID is not empty
                        logger.info(f"Killing process with PID: {pid}")
                        subprocess.run(['kill', '-9', pid])
                logger.info(f"All processes related to '{keyword}' have been killed.")
            else:
                logger.info(f"No processes found related to '{keyword}'.")

        except Exception as e:
            print(f"An error occurred during kill process: {e}")

    def _normalize_default_timezone(self, dt_value: Optional[datetime]) -> Optional[datetime]:
        """Normalize datetimes to the strategy timezone for consistent comparisons."""
        if dt_value is None:
            return None
        if isinstance(dt_value, pd.Timestamp):
            dt_value = dt_value.to_pydatetime()
        if dt_value.tzinfo is None:
            try:
                dt_value = self.tzinfo.localize(dt_value)
            except AttributeError:
                dt_value = dt_value.replace(tzinfo=self.tzinfo)
        return self.to_default_timezone(dt_value)

    def _build_dataset_keys(self, asset: Asset, quote: Optional[Asset], ts_unit: str) -> tuple[tuple, tuple]:
        """Return canonical (asset, quote, timestep) and legacy (asset, quote) cache keys."""
        quote_asset = quote if quote is not None else Asset("USD", "forex")
        canonical_key = (asset, quote_asset, ts_unit)
        legacy_key = (asset, quote_asset)
        return canonical_key, legacy_key

    def _option_expiration_end(self, asset: Asset) -> Optional[datetime]:
        """Return expiration datetime localized to default timezone, if applicable."""
        if getattr(asset, "asset_type", None) != Asset.AssetType.OPTION or asset.expiration is None:
            return None
        expiration_dt = datetime.combine(asset.expiration, datetime.max.time())
        try:
            expiration_dt = self.tzinfo.localize(expiration_dt)
        except AttributeError:
            expiration_dt = expiration_dt.replace(tzinfo=self.tzinfo)
        return self.to_default_timezone(expiration_dt)

    def _record_metadata(
        self,
        key,
        frame: pd.DataFrame,
        ts_unit: str,
        asset: Asset,
        has_quotes: bool = False,
        start_override: Optional[datetime] = None,
        end_override: Optional[datetime] = None,
        rows_override: Optional[int] = None,
        data_start_override: Optional[datetime] = None,
        data_end_override: Optional[datetime] = None,
        data_rows_override: Optional[int] = None,
    ) -> None:
        """Persist dataset coverage details for reuse checks."""
        previous_meta = self._dataset_metadata.get(key, {})

        if frame is None or frame.empty:
            start = end = None
            rows = 0
        else:
            if isinstance(frame.index, pd.DatetimeIndex):
                dt_source = frame.index
            elif "datetime" in frame.columns:
                dt_source = frame["datetime"]
            elif "index" in frame.columns:
                dt_source = frame["index"]
            else:
                dt_source = frame.index
            dt_index = pd.to_datetime(dt_source)
            if len(dt_index):
                if ts_unit == "day":
                    start_date = dt_index.min().date()
                    end_date = dt_index.max().date()
                    base_tz = getattr(dt_index, "tz", None)
                    start_dt = datetime.combine(start_date, datetime.min.time())
                    end_dt = datetime.combine(end_date, datetime.max.time())
                    if base_tz is not None:
                        start_dt = start_dt.replace(tzinfo=base_tz)
                        end_dt = end_dt.replace(tzinfo=base_tz)
                    else:
                        start_dt = start_dt.replace(tzinfo=pytz.UTC)
                        end_dt = end_dt.replace(tzinfo=pytz.UTC)
                    start = start_dt
                    end = end_dt
                else:
                    start = dt_index.min().to_pydatetime()
                    end = dt_index.max().to_pydatetime()
            else:
                start = end = None
            rows = len(frame)

        normalized_start = self._normalize_default_timezone(start)
        normalized_end = self._normalize_default_timezone(end)
        override_start = self._normalize_default_timezone(start_override)
        override_end = self._normalize_default_timezone(end_override)
        effective_rows = rows_override if rows_override is not None else rows
        normalized_data_start = self._normalize_default_timezone(data_start_override) or normalized_start
        normalized_data_end = self._normalize_default_timezone(data_end_override) or normalized_end
        effective_data_rows = data_rows_override if data_rows_override is not None else rows

        metadata: Dict[str, object] = {
            "timestep": ts_unit,
            "data_start": normalized_data_start,
            "data_end": normalized_data_end,
            "data_rows": effective_data_rows,
            "start": override_start or normalized_start,
            "end": override_end or normalized_end,
            "rows": effective_rows,
        }
        metadata["empty_fetch"] = frame is None or frame.empty
        metadata["has_quotes"] = bool(has_quotes)

        if frame is not None and not frame.empty and "missing" in frame.columns:
            placeholder_flags = frame["missing"].fillna(False).astype(bool)
            metadata["placeholders"] = int(placeholder_flags.sum())
            metadata["tail_placeholder"] = bool(placeholder_flags.iloc[-1])
            if placeholder_flags.shape[0] and bool(placeholder_flags.all()):
                metadata["empty_fetch"] = True
        else:
            metadata["placeholders"] = 0
            metadata["tail_placeholder"] = False
            if not metadata["empty_fetch"]:
                metadata["empty_fetch"] = False

        if getattr(asset, "asset_type", None) == Asset.AssetType.OPTION:
            metadata["expiration"] = asset.expiration

        if metadata.get("expiration") != previous_meta.get("expiration"):
            metadata["expiration_notice"] = False
        else:
            metadata["expiration_notice"] = previous_meta.get("expiration_notice", False)

        self._dataset_metadata[key] = metadata
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "[THETA][DEBUG][METADATA][WRITE] key=%s ts=%s start=%s end=%s data_start=%s data_end=%s rows=%s placeholders=%s has_quotes=%s",
                key,
                ts_unit,
                metadata.get("start"),
                metadata.get("end"),
                metadata.get("data_start"),
                metadata.get("data_end"),
                metadata.get("rows"),
                metadata.get("placeholders"),
                metadata.get("has_quotes"),
            )

    def _frame_has_quote_columns(self, frame: Optional[pd.DataFrame]) -> bool:
        if frame is None or frame.empty:
            return False
        quote_markers = {"bid", "ask", "bid_size", "ask_size", "last_trade_time", "last_bid_time", "last_ask_time"}
        return any(col in frame.columns for col in quote_markers)

    def _finalize_day_frame(
        self,
        pandas_df: Optional[pd.DataFrame],
        current_dt: datetime,
        requested_length: int,
        timeshift: Optional[timedelta],
        asset: Optional[Asset] = None,  # DEBUG-LOG: Added for logging
    ) -> Optional[pd.DataFrame]:
        # DEBUG-LOG: Method entry with full parameter context
        logger.debug(
            "[THETA][DEBUG][PANDAS][FINALIZE][ENTRY] asset=%s current_dt=%s requested_length=%s timeshift=%s input_shape=%s input_columns=%s input_index_type=%s input_has_tz=%s input_index_sample=%s",
            getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
            current_dt.isoformat() if hasattr(current_dt, 'isoformat') else current_dt,
            requested_length,
            timeshift,
            pandas_df.shape if pandas_df is not None else 'NONE',
            list(pandas_df.columns) if pandas_df is not None else 'NONE',
            type(pandas_df.index).__name__ if pandas_df is not None else 'NONE',
            getattr(pandas_df.index, 'tz', None) if pandas_df is not None else 'NONE',
            list(pandas_df.index[:5]) if pandas_df is not None and len(pandas_df) > 0 else 'EMPTY'
        )

        if pandas_df is None or pandas_df.empty:
            # DEBUG-LOG: Early return for empty input
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][EMPTY_INPUT] asset=%s returning_none_or_empty=True",
                getattr(asset, 'symbol', asset) if asset else 'UNKNOWN'
            )
            return pandas_df

        frame = pandas_df.copy()
        if "datetime" in frame.columns:
            frame = frame.set_index("datetime")

        frame.index = pd.to_datetime(frame.index)

        # DEBUG-LOG: Timezone state before localization
        logger.debug(
            "[THETA][DEBUG][PANDAS][FINALIZE][TZ_CHECK] asset=%s frame_index_tz=%s target_tz=%s needs_localization=%s frame_shape=%s",
            getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
            frame.index.tz,
            self.tzinfo,
            frame.index.tz is None,
            frame.shape
        )

        if frame.index.tz is None:
            frame.index = frame.index.tz_localize(pytz.UTC)
        localized_index = frame.index.tz_convert(self.tzinfo)
        normalized_for_cutoff = localized_index.normalize()

        # DEBUG-LOG: After localization
        logger.debug(
            "[THETA][DEBUG][PANDAS][FINALIZE][LOCALIZED] asset=%s localized_index_tz=%s localized_sample=%s",
            getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
            localized_index.tz,
            list(localized_index[:3]) if len(localized_index) > 0 else 'EMPTY'
        )

        cutoff = self.to_default_timezone(current_dt).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        cutoff_mask = normalized_for_cutoff <= cutoff

        # DEBUG-LOG: Cutoff filtering state
        logger.debug(
            "[THETA][DEBUG][PANDAS][FINALIZE][CUTOFF] asset=%s cutoff=%s cutoff_mask_true=%s cutoff_mask_false=%s",
            getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
            cutoff,
            int(cutoff_mask.sum()) if hasattr(cutoff_mask, 'sum') else 'N/A',
            int((~cutoff_mask).sum()) if hasattr(cutoff_mask, 'sum') else 'N/A'
        )

        if timeshift and not isinstance(timeshift, int):
            cutoff_mask &= normalized_for_cutoff <= (cutoff - timeshift)
            # DEBUG-LOG: After timeshift adjustment
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][TIMESHIFT_ADJUSTED] asset=%s timeshift=%s new_cutoff=%s cutoff_mask_true=%s",
                getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
                timeshift,
                cutoff - timeshift,
                int(cutoff_mask.sum()) if hasattr(cutoff_mask, 'sum') else 'N/A'
            )

        frame = frame.loc[cutoff_mask]
        localized_index = localized_index[cutoff_mask]
        normalized_for_cutoff = normalized_for_cutoff[cutoff_mask]

        # DEBUG-LOG: After cutoff filtering
        logger.debug(
            "[THETA][DEBUG][PANDAS][FINALIZE][AFTER_CUTOFF] asset=%s shape=%s index_range=%s",
            getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
            frame.shape,
            (localized_index[0], localized_index[-1]) if len(localized_index) > 0 else ('EMPTY', 'EMPTY')
        )

        if timeshift and isinstance(timeshift, int):
            if timeshift > 0:
                frame = frame.iloc[:-timeshift] if len(frame) > timeshift else frame.iloc[0:0]
                localized_index = localized_index[: len(frame)]

        normalized_index = localized_index.normalize()
        frame = frame.copy()
        frame.index = normalized_index
        raw_frame = frame.copy()

        # DEBUG-LOG: After normalization
        logger.debug(
            "[THETA][DEBUG][PANDAS][FINALIZE][NORMALIZED_INDEX] asset=%s shape=%s index_sample=%s",
            getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
            frame.shape,
            list(normalized_index[:3]) if len(normalized_index) > 0 else 'EMPTY'
        )

        expected_last_dt = self.to_default_timezone(current_dt).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        expected_last_dt_utc = expected_last_dt.astimezone(pytz.UTC)
        target_index = pd.date_range(end=expected_last_dt_utc, periods=requested_length, freq="D", tz=pytz.UTC).tz_convert(self.tzinfo)

        # DEBUG-LOG: Target index details
        logger.debug(
            "[THETA][DEBUG][PANDAS][FINALIZE][TARGET_INDEX] asset=%s target_length=%s target_range=%s",
            getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
            len(target_index),
            (target_index[0], target_index[-1]) if len(target_index) > 0 else ('EMPTY', 'EMPTY')
        )

        if "missing" not in frame.columns:
            frame["missing"] = False

        frame = frame.reindex(target_index)

        # DEBUG-LOG: After reindex
        logger.debug(
            "[THETA][DEBUG][PANDAS][FINALIZE][AFTER_REINDEX] asset=%s shape=%s columns=%s",
            getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
            frame.shape,
            list(frame.columns)
        )

        value_columns = [col for col in ["open", "high", "low", "close", "volume"] if col in frame.columns]
        if value_columns:
            placeholder_mask = frame[value_columns].isna().all(axis=1)
        else:
            placeholder_mask = frame.isna().all(axis=1)

        # DEBUG-LOG: Placeholder mask computation
        logger.debug(
            "[THETA][DEBUG][PANDAS][FINALIZE][PLACEHOLDER_MASK] asset=%s placeholder_true=%s placeholder_false=%s value_columns=%s",
            getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
            int(placeholder_mask.sum()) if hasattr(placeholder_mask, 'sum') else 'N/A',
            int((~placeholder_mask).sum()) if hasattr(placeholder_mask, 'sum') else 'N/A',
            value_columns
        )

        frame.loc[placeholder_mask, "missing"] = True
        frame["missing"] = frame["missing"].fillna(False)
        frame = frame.sort_index()
        frame.index.name = "datetime"

        if "missing" in frame.columns:
            # Drop placeholder rows (weekends/holidays) to avoid NaNs in returned results.
            missing_flags = frame["missing"].astype(bool)
            real_rows = frame.loc[~missing_flags]
            if len(real_rows) < requested_length:
                deficit = requested_length - len(real_rows)
                raw_missing_flags = raw_frame.get("missing")
                if raw_missing_flags is not None:
                    raw_real_rows = raw_frame.loc[~raw_missing_flags.astype(bool)]
                else:
                    raw_real_rows = raw_frame
                supplemental = raw_real_rows.tail(requested_length + deficit)
                combined = pd.concat([supplemental, real_rows]).sort_index()
                combined = combined[~combined.index.duplicated(keep="last")]
                frame = combined.tail(requested_length).copy()
            else:
                frame = real_rows.tail(requested_length).copy()
        else:
            frame = frame.tail(requested_length).copy()

        if value_columns:
            frame["missing"] = frame[value_columns].isna().all(axis=1)
        else:
            frame["missing"] = False

        # DEBUG-LOG: Final missing flag state
        try:
            missing_count = int(frame["missing"].sum())
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][MISSING_FINAL] asset=%s missing_true=%s missing_false=%s total_rows=%s",
                getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
                missing_count,
                len(frame) - missing_count,
                len(frame)
            )
        except Exception as e:
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][MISSING_FINAL] asset=%s error=%s",
                getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
                str(e)
            )

        # DEBUG-LOG: Return value
        logger.debug(
            "[THETA][DEBUG][PANDAS][FINALIZE][RETURN] asset=%s shape=%s columns=%s index_range=%s",
            getattr(asset, 'symbol', asset) if asset else 'UNKNOWN',
            frame.shape,
            list(frame.columns),
            (frame.index[0], frame.index[-1]) if len(frame) > 0 else ('EMPTY', 'EMPTY')
        )

        return frame

    def _load_sidecar_metadata(self, key, asset: Asset, ts_unit: str) -> Optional[Dict[str, object]]:
        """Hydrate in-memory metadata from an on-disk ThetaData cache sidecar."""
        cache_file = thetadata_helper.build_cache_filename(asset, ts_unit, "ohlc")
        sidecar = thetadata_helper._load_cache_sidecar(cache_file)
        if not sidecar:
            return None

        min_raw = sidecar.get("min")
        max_raw = sidecar.get("max")
        rows = sidecar.get("rows", 0)
        placeholders = sidecar.get("placeholders", 0)
        if ts_unit == "day":
            min_dt = pd.to_datetime(min_raw) if min_raw else None
            max_dt = pd.to_datetime(max_raw) if max_raw else None
            min_date = min_dt.date() if min_dt is not None else None
            max_date = max_dt.date() if max_dt is not None else None
            base_tz = getattr(min_dt, "tz", None) or getattr(max_dt, "tz", None) or pytz.UTC
            try:
                normalized_min = datetime.combine(min_date, datetime.min.time()).replace(tzinfo=base_tz) if min_date else None
                normalized_max = datetime.combine(max_date, datetime.max.time()).replace(tzinfo=base_tz) if max_date else None
                normalized_min = self.to_default_timezone(normalized_min) if normalized_min else None
                normalized_max = self.to_default_timezone(normalized_max) if normalized_max else None
            except Exception:
                normalized_min = datetime.combine(min_date, datetime.min.time()) if min_date else None
                normalized_max = datetime.combine(max_date, datetime.max.time()) if max_date else None
        else:
            normalized_min = self._normalize_default_timezone(pd.to_datetime(min_raw).to_pydatetime()) if min_raw else None
            normalized_max = self._normalize_default_timezone(pd.to_datetime(max_raw).to_pydatetime()) if max_raw else None

        meta = {
            "timestep": ts_unit,
            "start": normalized_min,
            "end": normalized_max,
            "data_start": normalized_min,
            "data_end": normalized_max,
            "rows": int(rows) if rows is not None else 0,
            "placeholders": int(placeholders) if placeholders is not None else 0,
            "prefetch_complete": False,
            "sidecar_loaded": True,
        }
        self._dataset_metadata[key] = meta
        logger.debug(
            "[THETA][DEBUG][SIDECAR][LOAD] asset=%s key=%s ts_unit=%s start=%s end=%s rows=%s placeholders=%s",
            getattr(asset, "symbol", asset),
            key,
            ts_unit,
            normalized_min,
            normalized_max,
            meta["rows"],
            placeholders,
        )
        return meta

    def _update_pandas_data(self, asset, quote, length, timestep, start_dt=None, require_quote_data: bool = False):
        """
        Get asset data and update the self.pandas_data dictionary.

        Parameters
        ----------
        asset : Asset
            The asset to get data for.
        quote : Asset
            The quote asset to use. For example, if asset is "SPY" and quote is "USD", the data will be for "SPY/USD".
        length : int
            The number of data points to get.
        timestep : str
            The timestep to use. For example, "1minute" or "1hour" or "1day".

        Returns
        -------
        dict
            A dictionary with the keys being the asset and the values being the PandasData objects.
        """
        # DEBUG: Log when strike 157 is requested
        if hasattr(asset, 'strike') and asset.strike == 157:
            import traceback
            logger.info(f"\n[DEBUG STRIKE 157] _update_pandas_data called for asset: {asset}")
            logger.info(f"[DEBUG STRIKE 157] Traceback:\n{''.join(traceback.format_stack())}")

        asset_separated = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(asset_separated, tuple):
            asset_separated, quote_asset = asset_separated

        if asset_separated.asset_type == "option":
            expiry = asset_separated.expiration
            if self.is_weekend(expiry):
                logger.info(f"\nSKIP: Expiry {expiry} date is a weekend, no contract exists: {asset_separated}")
                return None

        # Get the start datetime and timestep unit
        start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(
            length, timestep, start_dt, start_buffer=START_BUFFER
        )
        current_dt = self.get_datetime()

        requested_length = max(length, 1)
        requested_start = self._normalize_default_timezone(start_datetime)
        window_start = self._normalize_default_timezone(self.datetime_start - START_BUFFER)
        if requested_start is None or (window_start is not None and window_start < requested_start):
            requested_start = window_start
        start_threshold = requested_start + START_BUFFER if requested_start is not None else None
        start_for_fetch = requested_start or start_datetime
        # Always target full backtest coverage on first fetch; reuse thereafter
        if ts_unit == "day":
            try:
                end_date = self.datetime_end.date() if hasattr(self.datetime_end, "date") else self.datetime_end
            except Exception:
                end_date = self.datetime_end
            end_requirement = datetime.combine(end_date, datetime.max.time())
            try:
                end_requirement = self.tzinfo.localize(end_requirement)
            except Exception:
                end_requirement = end_requirement.replace(tzinfo=getattr(self, "tzinfo", None))
            end_requirement = self.to_default_timezone(end_requirement) if hasattr(self, "to_default_timezone") else end_requirement
        else:
            end_requirement = self._normalize_default_timezone(self.datetime_end)
        # Align day requests to the last known trading day before datetime_end to avoid off-by-one churn.
        if ts_unit == "day":
            try:
                trading_days = thetadata_helper.get_trading_dates(
                    asset_separated,
                    start_for_fetch or self.datetime_start,
                    end_requirement or self.datetime_end,
                )
                if trading_days:
                    last_trading_day = trading_days[-1]
                    end_requirement = datetime.combine(last_trading_day, datetime.max.time()).replace(tzinfo=end_requirement.tzinfo)
                    logger.debug(
                        "[THETA][DEBUG][END_ALIGNMENT] asset=%s/%s last_trading_day=%s aligned_end=%s",
                        asset_separated,
                        quote_asset,
                        last_trading_day,
                        end_requirement,
                    )
            except Exception:
                logger.debug("[THETA][DEBUG][END_ALIGNMENT] failed to align end_requirement for day bars", exc_info=True)
        # Log when minute/hour data is requested in day mode - this is allowed when explicitly
        # requested by the strategy (e.g., get_historical_prices with timestep="minute").
        # The implicit→day alignment happens upstream in _pull_source_symbol_bars.
        current_mode = getattr(self, "_timestep", None)
        if current_mode == "day" and ts_unit in {"minute", "hour"}:
            logger.debug(
                "[THETA][DEBUG][MINUTE_IN_DAY_MODE] _update_pandas_data ts_unit=%s current_mode=day asset=%s length=%s require_quote_data=%s | allowing explicit request",
                ts_unit,
                asset_separated,
                requested_length,
                require_quote_data,
            )
        logger.debug(
            "[THETA][DEBUG][UPDATE_ENTRY] asset=%s quote=%s timestep=%s length=%s requested_start=%s start_for_fetch=%s target_end=%s current_dt=%s",
            asset_separated,
            quote_asset,
            ts_unit,
            requested_length,
            requested_start,
            start_for_fetch,
            end_requirement,
            current_dt,
        )
        expiration_dt = self._option_expiration_end(asset_separated)
        if expiration_dt is not None and end_requirement is not None and expiration_dt < end_requirement:
            end_requirement = expiration_dt

        canonical_key, legacy_key = self._build_dataset_keys(asset_separated, quote_asset, ts_unit)
        dataset_key = canonical_key
        cached_data = None
        for lookup_key in (canonical_key, legacy_key):
            candidate = self.pandas_data.get(lookup_key)
            if candidate is not None:
                # Only use cached data if its timestep matches what we're requesting.
                # This prevents using day data when minute data is requested (or vice versa).
                if candidate.timestep == ts_unit:
                    cached_data = candidate
                    dataset_key = lookup_key
                    break
                else:
                    logger.debug(
                        "[THETA][DEBUG][CACHE_SKIP] Found data under key=%s but timestep mismatch: cached=%s requested=%s",
                        lookup_key,
                        candidate.timestep,
                        ts_unit,
                    )

        if cached_data is not None and canonical_key not in self.pandas_data:
            self.pandas_data[canonical_key] = cached_data
            self._data_store[canonical_key] = cached_data

        existing_meta = self._dataset_metadata.get(canonical_key)
        if existing_meta is None and legacy_key in self._dataset_metadata:
            existing_meta = self._dataset_metadata[legacy_key]
            if existing_meta is not None:
                self._dataset_metadata[canonical_key] = existing_meta
        if existing_meta is None:
            existing_meta = self._load_sidecar_metadata(canonical_key, asset_separated, ts_unit)

        existing_data = self.pandas_data.get(dataset_key)
        if existing_data is not None and ts_unit == "day":
            # Refresh metadata from the actual dataframe to avoid stale end dates caused by tz shifts.
            has_quotes = self._frame_has_quote_columns(existing_data.df)
            self._record_metadata(canonical_key, existing_data.df, existing_data.timestep, asset_separated, has_quotes=has_quotes)
            existing_meta = self._dataset_metadata.get(canonical_key)
            try:
                df_idx = pd.to_datetime(existing_data.df.index)
                logger.debug(
                    "[THETA][DEBUG][DAY_METADATA_REBUILD] asset=%s/%s df_min=%s df_max=%s rows=%s rebuilt_start=%s rebuilt_end=%s",
                    asset_separated,
                    quote_asset,
                    df_idx.min(),
                    df_idx.max(),
                    len(df_idx),
                    existing_meta.get("start") if existing_meta else None,
                    existing_meta.get("end") if existing_meta else None,
                )
            except Exception:
                logger.debug("[THETA][DEBUG][DAY_METADATA_REBUILD] failed to log dataframe bounds", exc_info=True)

        # Fast-path reuse: if we already have a dataframe that covers the needed window, skip all fetch/ffill work.
        # IMPORTANT: Only reuse if the cached data's timestep matches what we're requesting.
        # Otherwise we might reuse day data when minute data was requested (or vice versa).
        #
        # CRITICAL FIX (2025-12-07): For OPTIONS, we must be extra careful about fast-reuse.
        # Each option strike/expiration is a unique instrument that needs its own data.
        # Don't reuse cached data for options unless it's for the EXACT same strike/expiration.
        # The canonical_key includes the full Asset (with strike/expiration), but we add an
        # explicit check here as a defensive measure.
        is_option = getattr(asset_separated, 'asset_type', None) == 'option'

        if existing_data is not None and existing_data.timestep == ts_unit:
            df_idx = existing_data.df.index
            if len(df_idx):
                idx = pd.to_datetime(df_idx)
                if idx.tz is None:
                    idx = idx.tz_localize(pytz.UTC)
                else:
                    idx = idx.tz_convert(pytz.UTC)
                coverage_start = idx.min()
                coverage_end = idx.max()
                # Use date-level comparison for both day and minute data, but ensure both
                # timestamps are in the same timezone before extracting date. Otherwise
                # UTC midnight (Nov 3 00:00 UTC = Nov 2 19:00 EST) would incorrectly match
                # a local date requirement of Nov 3.
                if coverage_end is not None and end_requirement is not None:
                    # Convert both to the same timezone (use end_requirement's timezone)
                    target_tz = end_requirement.tzinfo
                    if target_tz is not None and coverage_end.tzinfo is not None:
                        coverage_end_local = coverage_end.astimezone(target_tz)
                    else:
                        coverage_end_local = coverage_end
                    coverage_end_cmp = coverage_end_local.date()
                    end_requirement_cmp = end_requirement.date()
                else:
                    coverage_end_cmp = coverage_end.date() if coverage_end is not None else None
                    end_requirement_cmp = end_requirement.date() if end_requirement is not None else None
                end_ok = coverage_end_cmp is not None and end_requirement_cmp is not None and coverage_end_cmp >= end_requirement_cmp

                if (
                    coverage_start is not None
                    and requested_start is not None
                    and coverage_start <= requested_start + START_BUFFER
                    and end_ok
                ):
                    # CRITICAL FIX (2025-12-07): For options, verify the cached data is for
                    # the EXACT same strike/expiration. Options are unique instruments and
                    # data for one strike cannot be reused for another.
                    if is_option:
                        # Get the asset that was used to cache this data
                        cached_asset = None
                        if isinstance(dataset_key, tuple) and len(dataset_key) >= 1:
                            cached_asset = dataset_key[0]

                        # Verify strike and expiration match exactly
                        if cached_asset is None or not isinstance(cached_asset, Asset):
                            logger.info(
                                "[THETA][CACHE][FAST_REUSE][OPTION_SKIP] Cannot verify cached asset for option %s - fetching fresh data",
                                asset_separated,
                            )
                            # Don't use fast-reuse, continue to fetch
                        elif (
                            getattr(cached_asset, 'strike', None) != getattr(asset_separated, 'strike', None)
                            or getattr(cached_asset, 'expiration', None) != getattr(asset_separated, 'expiration', None)
                            or getattr(cached_asset, 'right', None) != getattr(asset_separated, 'right', None)
                        ):
                            logger.info(
                                "[THETA][CACHE][FAST_REUSE][OPTION_MISMATCH] Cached data for %s does not match requested option %s - fetching fresh data",
                                cached_asset,
                                asset_separated,
                            )
                            # Don't use fast-reuse, continue to fetch
                        else:
                            # Option matches exactly, safe to reuse
                            meta = self._dataset_metadata.get(canonical_key, {}) or {}
                            if not meta.get("ffilled"):
                                meta["ffilled"] = True
                            if meta.get("prefetch_complete") is None:
                                meta["prefetch_complete"] = True
                            self._dataset_metadata[canonical_key] = meta
                            logger.info(
                                "[THETA][CACHE][FAST_REUSE][OPTION] asset=%s/%s (%s) strike=%s exp=%s -> reuse",
                                asset_separated,
                                quote_asset,
                                ts_unit,
                                getattr(asset_separated, 'strike', None),
                                getattr(asset_separated, 'expiration', None),
                            )
                            return None
                    else:
                        # Non-option asset - use standard fast-reuse
                        meta = self._dataset_metadata.get(canonical_key, {}) or {}
                        if not meta.get("ffilled"):
                            meta["ffilled"] = True
                        if meta.get("prefetch_complete") is None:
                            meta["prefetch_complete"] = True
                        self._dataset_metadata[canonical_key] = meta
                        logger.info(
                            "[THETA][CACHE][FAST_REUSE] asset=%s/%s (%s) covers start=%s end=%s needed_start=%s needed_end=%s -> reuse (date-level comparison)",
                            asset_separated,
                            quote_asset,
                            ts_unit,
                            coverage_start,
                            coverage_end,
                            requested_start,
                            end_requirement,
                        )
                        return None

        if cached_data is not None and existing_meta is None:
            has_quotes = self._frame_has_quote_columns(cached_data.df)
            self._record_metadata(canonical_key, cached_data.df, cached_data.timestep, asset_separated, has_quotes=has_quotes)
            existing_meta = self._dataset_metadata.get(canonical_key)

        existing_data = cached_data
        existing_start = None
        existing_end = None
        existing_has_quotes = bool(existing_meta.get("has_quotes")) if existing_meta else False

        if existing_data is not None and existing_meta and existing_meta.get("timestep") == ts_unit:
            existing_start = existing_meta.get("start")
            existing_rows = existing_meta.get("rows", 0)
            existing_end = existing_meta.get("end")

            # Fill missing metadata with actual dataframe bounds
            if (existing_start is None or existing_end is None) and len(existing_data.df.index) > 0:
                if existing_start is None:
                    existing_start = self._normalize_default_timezone(existing_data.df.index[0])
                if existing_end is None:
                    existing_end = self._normalize_default_timezone(existing_data.df.index[-1])

            # DEBUG-LOG: Cache validation entry
            logger.debug(
                "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][CACHE_VALIDATION][ENTRY] asset=%s timestep=%s | "
                "REQUESTED: start=%s start_threshold=%s end_requirement=%s length=%d | "
                "EXISTING: start=%s end=%s rows=%d",
                asset_separated.symbol if hasattr(asset_separated, 'symbol') else str(asset_separated),
                ts_unit,
                requested_start.isoformat() if requested_start else None,
                start_threshold.isoformat() if start_threshold else None,
                end_requirement.isoformat() if end_requirement else None,
                requested_length,
                existing_start.isoformat() if existing_start else None,
                existing_end.isoformat() if existing_end else None,
                existing_rows
            )

            # NOTE: Removed "existing_start <= start_threshold" check (2025-12-06)
            # This check invalidated cache for assets like TQQQ where the requested start date
            # (e.g., 2011-07-xx for 200-day MA lookback) is before the asset's inception date
            # (TQQQ started 2012-05-31). The cache helper's _validate_cache_frame() already
            # validates that all required trading days are present. If the asset didn't trade
            # before 2012-05-31, there ARE no trading days to miss, so the cache is valid.
            # The row count check (existing_rows >= requested_length) and end check (end_ok)
            # are sufficient to determine cache validity.
            start_ok = existing_start is not None

            # DEBUG-LOG: Start validation result
            logger.debug(
                "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][START_VALIDATION] asset=%s | "
                "start_ok=%s | "
                "existing_start=%s start_threshold=%s | "
                "reasoning=%s",
                asset_separated.symbol if hasattr(asset_separated, 'symbol') else str(asset_separated),
                start_ok,
                existing_start.isoformat() if existing_start else None,
                start_threshold.isoformat() if start_threshold else None,
                "existing_start is not None (threshold check removed - see NOTE above)" if start_ok else "existing_start is None"
            )

            tail_placeholder = existing_meta.get("tail_placeholder", False)
            end_ok = True

            # DEBUG-LOG: End validation entry
            logger.debug(
                "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][END_VALIDATION][ENTRY] asset=%s | "
                "end_requirement=%s existing_end=%s tail_placeholder=%s",
                asset_separated.symbol if hasattr(asset_separated, 'symbol') else str(asset_separated),
                end_requirement.isoformat() if end_requirement else None,
                existing_end.isoformat() if existing_end else None,
                tail_placeholder
            )

            if end_requirement is not None:
                if existing_end is None:
                    end_ok = False
                    logger.debug(
                        "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][END_VALIDATION][RESULT] asset=%s | "
                        "end_ok=FALSE | reason=existing_end_is_None",
                        asset_separated.symbol if hasattr(asset_separated, 'symbol') else str(asset_separated)
                    )
                else:
                    # FIX: For both day and minute data, use date-only comparison
                    # For day data: prevents false negatives when existing_end is midnight and end_requirement is later
                    # For minute data: minute data legitimately ends at market close (7:59 PM), not midnight
                    # IMPORTANT: Convert to same timezone before extracting date to avoid UTC/local mismatch
                    if hasattr(existing_end, 'tzinfo') and hasattr(end_requirement, 'tzinfo'):
                        target_tz = end_requirement.tzinfo
                        if target_tz is not None and existing_end.tzinfo is not None:
                            existing_end_local = existing_end.astimezone(target_tz)
                        else:
                            existing_end_local = existing_end
                    else:
                        existing_end_local = existing_end
                    existing_end_date = existing_end_local.date() if hasattr(existing_end_local, 'date') else existing_end_local
                    end_requirement_date = end_requirement.date() if hasattr(end_requirement, 'date') else end_requirement
                    existing_end_cmp = existing_end_date
                    end_requirement_cmp = end_requirement_date
                    # Allow 3-day tolerance - ThetaData may not have the most recent data
                    end_tolerance = timedelta(days=3)

                    if existing_end_cmp >= end_requirement_cmp - end_tolerance:
                        end_ok = True
                        logger.debug(
                            "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][END_VALIDATION][RESULT] asset=%s | "
                            "end_ok=TRUE | reason=existing_end_meets_requirement | "
                            "existing_end=%s end_requirement=%s tolerance=%s ts_unit=%s",
                            asset_separated.symbol if hasattr(asset_separated, 'symbol') else str(asset_separated),
                            existing_end.isoformat(),
                            end_requirement.isoformat(),
                            end_tolerance,
                            ts_unit
                        )
                    else:
                        # existing_end is still behind the required window
                        end_ok = False
                        logger.debug(
                            "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][END_VALIDATION][RESULT] asset=%s | "
                            "end_ok=FALSE | reason=existing_end_less_than_requirement | "
                            "existing_end=%s end_requirement=%s ts_unit=%s",
                            asset_separated.symbol if hasattr(asset_separated, 'symbol') else str(asset_separated),
                            existing_end.isoformat(),
                            end_requirement.isoformat(),
                            ts_unit
                        )

            cache_covers = (
                start_ok
                and existing_rows >= requested_length
                and end_ok
            )

            # DEBUG-LOG: Final cache decision
            logger.debug(
                "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][CACHE_DECISION] asset=%s | "
                "cache_covers=%s | "
                "start_ok=%s rows_ok=%s (existing=%d >= requested=%d) end_ok=%s",
                asset_separated.symbol if hasattr(asset_separated, 'symbol') else str(asset_separated),
                cache_covers,
                start_ok,
                existing_rows >= requested_length,
                existing_rows,
                requested_length,
                end_ok
            )

            if cache_covers:
                if (
                    expiration_dt is not None
                    and end_requirement is not None
                    and expiration_dt == end_requirement
                    and not existing_meta.get("expiration_notice")
                ):
                    logger.debug(
                        "[THETA][DEBUG][THETADATA-PANDAS] Reusing cached data for %s/%s through option expiry %s.",
                        asset_separated,
                        quote_asset,
                        asset_separated.expiration,
                    )
                    existing_meta["expiration_notice"] = True
                else:
                    logger.debug(
                        "[THETA][DEBUG][THETADATA-PANDAS] cache covers %s/%s (%s) from %s to %s; length=%s rows=%s -> reuse",
                        asset_separated,
                        quote_asset,
                        ts_unit,
                        existing_start,
                        existing_end,
                        requested_length,
                        existing_rows,
                    )
                return None

            reasons = []
            # NOTE: Only check if existing_start is None (matching fix above at line 780)
            if existing_start is None:
                reasons.append("start")
            if existing_rows < requested_length:
                reasons.append("rows")
            if not end_ok:
                reasons.append("end")
            logger.debug(
                "[THETA][DEBUG][THETADATA-PANDAS] refreshing cache for %s/%s (%s); reasons=%s "
                "(existing_start=%s requested_start=%s existing_end=%s end_requirement=%s existing_rows=%s needed_rows=%s)",
                asset_separated,
                quote_asset,
                ts_unit,
                ",".join(reasons) or "unknown",
                existing_start,
                requested_start,
                existing_end,
                end_requirement,
                existing_rows,
                requested_length,
            )
            if existing_meta is not None and existing_meta.get("prefetch_complete"):
                # The cache was marked complete but doesn't cover our required end date.
                # This can happen if the cache is stale or backtest dates changed.
                # Clear the prefetch_complete flag and try to fetch more data.
                logger.info(
                    "[THETA][CACHE][STALE] asset=%s/%s (%s) prefetch_complete but coverage insufficient; "
                    "clearing flag to allow refetch. existing_end=%s target_end=%s",
                    asset_separated,
                    quote_asset,
                    ts_unit,
                    existing_end,
                    end_requirement,
                )
                existing_meta["prefetch_complete"] = False
                self._dataset_metadata[canonical_key] = existing_meta
            logger.info(
                "[THETA][CACHE][REFRESH] asset=%s/%s (%s) dt=%s start_needed=%s end_needed=%s reasons=%s rows_have=%s rows_need=%s",
                asset_separated,
                quote_asset,
                ts_unit,
                current_dt,
                requested_start,
                end_requirement,
                ",".join(reasons) or "unknown",
                existing_rows,
                requested_length,
            )

        # Check if we have data for this asset
        if existing_data is not None:
            asset_data_df = existing_data.df
            data_start_datetime = asset_data_df.index[0]
            data_end_datetime = asset_data_df.index[-1]

            # Get the timestep of the data
            data_timestep = existing_data.timestep

            coverage_start = (
                self._normalize_default_timezone(existing_start)
                if existing_start is not None
                else self._normalize_default_timezone(data_start_datetime)
            )
            coverage_end = (
                self._normalize_default_timezone(existing_end)
                if existing_end is not None
                else self._normalize_default_timezone(data_end_datetime)
            )

            end_missing = False
            if end_requirement is not None:
                if coverage_end is None:
                    end_missing = True
                else:
                    coverage_end_cmp = coverage_end.date() if ts_unit == "day" else coverage_end
                    end_requirement_cmp = end_requirement.date() if ts_unit == "day" else end_requirement
                    end_missing = coverage_end_cmp < end_requirement_cmp

            # If the timestep is the same, we don't need to update the data
            if data_timestep == ts_unit:
                # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                start_buffer_ok = (
                    coverage_start is not None
                    and start_for_fetch is not None
                    and (coverage_start - start_for_fetch) < START_BUFFER
                )
                if start_buffer_ok and not end_missing:
                    return None

            # When daily bars are requested we should never "downgrade" to minute/hour requests.
            # Doing so forces the helper to download massive minute ranges and resample, which is
            # both slow (multi-minute runs) and introduces price drift vs Polygon/Yahoo.
            # Instead, rely on the Theta EOD endpoint for official day data, even if minute data is already cached.
            if ts_unit == "day" and data_timestep in {"minute", "hour"}:
                logger.debug(
                    "[THETA][DEBUG][THETADATA-PANDAS] day bars requested while cache holds %s data; forcing EOD fetch",
                    data_timestep,
                )

            # Hourly requests can leverage minute data, but should not force fresh minute downloads
            # unless the cache truly lacks coverage. Keep the existing minute cache instead of lowering
            # ts_unit for the fetch.
            if ts_unit == "hour" and data_timestep == "minute":
                if (data_start_datetime - start_datetime) < START_BUFFER:
                    return None

        # Download data from ThetaData
        # Get ohlc data from ThetaData
        date_time_now = self.get_datetime()
        logger.debug(
            "[THETA][DEBUG][THETADATA-PANDAS] fetch asset=%s quote=%s length=%s timestep=%s start=%s end=%s",
            asset_separated,
            quote_asset,
            length,
            timestep,
            start_for_fetch,
            end_requirement,
        )
        df_ohlc = thetadata_helper.get_price_data(
            self._username,
            self._password,
            asset_separated,
            start_for_fetch,
            end_requirement,
            timespan=ts_unit,
            quote_asset=quote_asset,
            dt=date_time_now,
            datastyle="ohlc",
            include_after_hours=True,  # Default to True for extended hours data
            preserve_full_history=True,
        )

        if df_ohlc is None or df_ohlc.empty:
            expired_reason = (
                expiration_dt is not None
                and end_requirement is not None
                and expiration_dt == end_requirement
            )
            if expired_reason:
                logger.debug(
                    "[THETA][DEBUG][THETADATA-PANDAS] No new OHLC rows for %s/%s (%s); option expired on %s. Keeping cached data.",
                    asset_separated,
                    quote_asset,
                    ts_unit,
                    asset_separated.expiration,
                )
                if existing_meta is not None:
                    existing_meta["expiration_notice"] = True
                return None
            raise ValueError(
                f"No OHLC data returned for {asset_separated} / {quote_asset} ({ts_unit}) "
                f"start={start_datetime} end={end_requirement}; refusing to proceed with empty dataset."
            )

        df = df_ohlc
        quotes_attached = False
        quotes_enabled = require_quote_data or existing_has_quotes

        # Quote data (bid/ask) is only available for intraday data (minute, hour, second)
        # For daily+ data, only use OHLC
        if self._use_quote_data and ts_unit in ["minute", "hour", "second"] and quotes_enabled:
            try:
                df_quote = thetadata_helper.get_price_data(
                    self._username,
                    self._password,
                    asset_separated,
                    start_for_fetch,
                    end_requirement,
                    timespan=ts_unit,
                    quote_asset=quote_asset,
                    dt=date_time_now,
                    datastyle="quote",
                    include_after_hours=True,  # Default to True for extended hours data
                    preserve_full_history=True,
                )
            except Exception as exc:
                logger.exception(
                    "ThetaData quote download failed for %s / %s (%s)",
                    asset_separated,
                    quote_asset,
                    ts_unit,
                )
                raise

            # If the quote dataframe is empty, continue with OHLC but log
            if df_quote is None or df_quote.empty:
                logger.warning(f"No QUOTE data returned for {asset_separated} / {quote_asset} ({ts_unit}); continuing without quotes.")
            else:
                # Combine the ohlc and quote data using outer join to preserve all data
                # Use forward fill for missing quote values (ThetaData's recommended approach)
                timestamp_columns = ['last_trade_time', 'last_bid_time', 'last_ask_time']
                df = pd.concat([df_ohlc, df_quote], axis=1, join='outer')
                df = self._combine_duplicate_columns(df, timestamp_columns)
                quotes_attached = True

                # Theta includes duplicate metadata columns (symbol/strike/right/expiration); merge them once.
                duplicate_names = df.columns[df.columns.duplicated()].unique().tolist()
                if duplicate_names:
                    df = self._combine_duplicate_columns(df, duplicate_names)

                # Forward fill missing quote values and timestamp metadata
                quote_columns = ['bid', 'ask', 'bid_size', 'ask_size', 'bid_condition', 'ask_condition', 'bid_exchange', 'ask_exchange']
                forward_fill_columns = [
                    col
                    for col in quote_columns + timestamp_columns
                    if col in df.columns
                ]
                quotes_ffilled = False
                quotes_ffill_rows = None
                quotes_ffill_remaining = None
                if forward_fill_columns:
                    should_ffill = True
                    if existing_meta:
                        prev_ffilled = existing_meta.get("quotes_ffilled")
                        prev_rows = existing_meta.get("quotes_ffill_rows")
                        prev_end = existing_meta.get("data_end")
                        if prev_ffilled and prev_rows is not None:
                            current_rows = len(df)
                            current_end = None
                            try:
                                if "datetime" in df.columns:
                                    current_end = pd.to_datetime(df["datetime"]).max()
                                else:
                                    current_end = pd.to_datetime(df.index).max()
                                if isinstance(current_end, pd.Timestamp):
                                    current_end = current_end.to_pydatetime()
                                current_end = self._normalize_default_timezone(current_end)
                            except Exception:
                                current_end = None

                            end_tolerance = timedelta(hours=12) if ts_unit in ["minute", "hour", "second"] else timedelta(days=0)
                            if (
                                current_rows <= prev_rows
                                and prev_end is not None
                                and current_end is not None
                                and current_end <= prev_end + end_tolerance
                            ):
                                should_ffill = False
                                logger.debug(
                                    "[THETA][DEBUG][THETADATA-PANDAS][FFILL] Skipping forward fill for %s/%s (%s); already applied to %s rows",
                                    asset_separated,
                                    quote_asset,
                                    ts_unit,
                                    prev_rows,
                                )

                    if should_ffill:
                        # IMPORTANT: Use TIME-GAP detection to prevent stale weekend/after-hours data
                        # from being filled into the first trading bar of a new session.
                        # Row-count limits don't work because there are no intermediate rows between
                        # Friday close and Monday 9:30 AM - the data jumps directly.
                        # Instead, we detect actual TIME gaps and prevent ffill across them.

                        # Define max time gap threshold for forward-fill (in minutes)
                        if ts_unit == "minute":
                            max_gap_minutes = 120  # 2 hours - allows filling within a session
                        elif ts_unit == "hour":
                            max_gap_minutes = 240  # 4 hours
                        elif ts_unit == "second":
                            max_gap_minutes = 120  # 2 hours
                        else:
                            max_gap_minutes = 0  # No forward-fill for day+ data

                        if max_gap_minutes > 0 and isinstance(df.index, pd.DatetimeIndex):
                            # Calculate time gaps between consecutive rows
                            time_diff = df.index.to_series().diff()

                            # Identify "session boundaries" where gap exceeds threshold
                            # These are places where we should NOT forward-fill
                            gap_threshold = pd.Timedelta(minutes=max_gap_minutes)
                            session_boundaries = time_diff > gap_threshold

                            # Count how many session boundaries we found
                            num_boundaries = session_boundaries.sum()
                            if num_boundaries > 0:
                                logger.debug(
                                    "[THETA][DEBUG][THETADATA-PANDAS][FFILL] Found %d session boundaries (gaps > %d min) for %s/%s",
                                    num_boundaries, max_gap_minutes, asset_separated, quote_asset,
                                )

                                # For rows at session boundaries, set quote columns to NaN BEFORE ffill
                                # This breaks the ffill chain so stale data doesn't propagate across sessions
                                for col in forward_fill_columns:
                                    if col in df.columns:
                                        # Set NaN at session boundaries to prevent stale data from propagating
                                        # But only if the current value is already NaN (don't overwrite real data)
                                        boundary_and_nan = session_boundaries & df[col].isna()
                                        # Actually, we need to mark the BOUNDARY rows so ffill doesn't reach them
                                        # The trick is: we temporarily set non-NaN values at boundaries to NaN,
                                        # do ffill, then restore. But simpler: just don't ffill across boundaries.
                                        pass  # We'll handle this differently below

                                # Alternative approach: segment-wise ffill
                                # Create segment IDs based on session boundaries
                                segment_ids = session_boundaries.cumsum()

                                # Forward fill within each segment only
                                for col in forward_fill_columns:
                                    if col in df.columns:
                                        # Group by segment and forward-fill within each group
                                        df[col] = df.groupby(segment_ids)[col].ffill()

                                logger.debug(
                                    "[THETA][DEBUG][THETADATA-PANDAS][FFILL] Applied segment-wise forward-fill for %s/%s (%s) across %d segments",
                                    asset_separated, quote_asset, ts_unit, segment_ids.max() + 1 if len(segment_ids) > 0 else 0,
                                )
                            else:
                                # No session boundaries - safe to ffill normally
                                df[forward_fill_columns] = df[forward_fill_columns].ffill()
                                logger.debug(
                                    "[THETA][DEBUG][THETADATA-PANDAS][FFILL] Forward-filled quote columns for %s/%s (%s) - no session boundaries",
                                    asset_separated, quote_asset, ts_unit,
                                )
                        elif max_gap_minutes > 0:
                            # Index is not DatetimeIndex, fall back to simple ffill
                            df[forward_fill_columns] = df[forward_fill_columns].ffill()
                            logger.debug(
                                "[THETA][DEBUG][THETADATA-PANDAS][FFILL] Forward-filled quote columns for %s/%s (%s) - non-datetime index",
                                asset_separated, quote_asset, ts_unit,
                            )
                        else:
                            logger.debug(
                                "[THETA][DEBUG][THETADATA-PANDAS][FFILL] Skipping quote forward-fill for %s/%s (%s) - day+ data",
                                asset_separated, quote_asset, ts_unit,
                            )

                        quotes_ffilled = True
                        quotes_ffill_rows = len(df)

                        # Log how much forward filling occurred
                        if 'bid' in df.columns and 'ask' in df.columns:
                            remaining_nulls = df[['bid', 'ask']].isna().sum().sum()
                            quotes_ffill_remaining = remaining_nulls
                            if remaining_nulls > 0:
                                logger.info(f"Forward-filled missing quote values for {asset_separated}. {remaining_nulls} nulls remain after time-gap-aware ffill.")

        if df is None or df.empty:
            return None

        def _prep_frame(base_df: pd.DataFrame) -> pd.DataFrame:
            frame = base_df
            if isinstance(frame, pd.DataFrame) and "datetime" in frame.columns:
                frame = frame.set_index("datetime")
            if not isinstance(frame.index, pd.DatetimeIndex):
                frame.index = pd.to_datetime(frame.index, utc=True)
            index_tz = getattr(frame.index, "tz", None)
            if index_tz is None:
                frame.index = frame.index.tz_localize(pytz.UTC)
            else:
                frame.index = frame.index.tz_convert(pytz.UTC)
            return frame.sort_index()

        def _process_frame(frame: pd.DataFrame):
            metadata_frame_local = frame.copy()
            cleaned_df_local = frame
            placeholder_mask_local = None
            placeholder_rows_local = 0
            leading_placeholder_local = False
            if "missing" in cleaned_df_local.columns:
                placeholder_mask_local = cleaned_df_local["missing"].astype(bool)
                placeholder_rows_local = int(placeholder_mask_local.sum())
                if placeholder_rows_local and len(placeholder_mask_local):
                    leading_placeholder_local = bool(placeholder_mask_local.iloc[0])
                cleaned_df_local = cleaned_df_local.loc[~placeholder_mask_local].copy()
                cleaned_df_local = cleaned_df_local.drop(columns=["missing"], errors="ignore")
            else:
                cleaned_df_local = cleaned_df_local.copy()

            if cleaned_df_local.empty:
                logger.debug(
                    "[THETA][DEBUG][THETADATA-PANDAS] All merged rows for %s/%s were placeholders; retaining raw merge for diagnostics.",
                    asset_separated,
                    quote_asset,
                )
                cleaned_df_local = metadata_frame_local.drop(columns=["missing"], errors="ignore").copy()

            metadata_start_override_local = None
            if leading_placeholder_local and len(metadata_frame_local):
                earliest_index = metadata_frame_local.index[0]
                if isinstance(earliest_index, pd.Timestamp):
                    earliest_index = earliest_index.to_pydatetime()
                metadata_start_override_local = earliest_index

            data_start_candidate_local = cleaned_df_local.index.min() if not cleaned_df_local.empty else None
            data_end_candidate_local = cleaned_df_local.index.max() if not cleaned_df_local.empty else None
            return (
                metadata_frame_local,
                cleaned_df_local,
                placeholder_mask_local,
                placeholder_rows_local,
                leading_placeholder_local,
                metadata_start_override_local,
                data_start_candidate_local,
                data_end_candidate_local,
            )

        def _covers_window(frame: Optional[pd.DataFrame], start_dt: Optional[datetime], end_dt: Optional[datetime]) -> bool:
            if frame is None or frame.empty or start_dt is None or end_dt is None:
                return False
            try:
                idx = pd.to_datetime(frame.index)
                if idx.tz is None:
                    idx = idx.tz_localize(pytz.UTC)
                else:
                    idx = idx.tz_convert(pytz.UTC)
                min_dt = idx.min()
                max_dt = idx.max()
            except Exception:
                return False
            return min_dt.date() <= start_dt.date() and max_dt.date() >= end_dt.date()

        merged_df = df
        if isinstance(merged_df, pd.DataFrame) and "datetime" in merged_df.columns:
            merged_df = merged_df.set_index("datetime")
        if (
            existing_data is not None
            and existing_data.timestep == ts_unit
            and existing_data.df is not None
            and not existing_data.df.empty
        ):
            if merged_df is None or merged_df.empty:
                merged_df = existing_data.df.copy()
            else:
                merged_df = pd.concat([existing_data.df, merged_df]).sort_index()
                merged_df = merged_df[~merged_df.index.duplicated(keep="last")]

        merged_df = _prep_frame(merged_df)
        (
            metadata_frame,
            cleaned_df,
            placeholder_mask,
            placeholder_rows,
            leading_placeholder,
            metadata_start_override,
            data_start_candidate,
            data_end_candidate,
        ) = _process_frame(merged_df)

        if ts_unit == "day" and not _covers_window(metadata_frame, requested_start, end_requirement):
            # Reload from the freshly written cache to avoid running on a truncated in-memory frame.
            cache_file = thetadata_helper.build_cache_filename(asset_separated, ts_unit, "ohlc")
            cache_df = thetadata_helper.load_cache(cache_file)
            if cache_df is not None and not cache_df.empty:
                logger.debug(
                    "[THETA][DEBUG][THETADATA-PANDAS] reloading daily cache from disk for %s/%s due to coverage gap (requested=%s->%s)",
                    asset_separated,
                    quote_asset,
                    requested_start,
                    end_requirement,
                )
                merged_df = _prep_frame(cache_df)
                (
                    metadata_frame,
                    cleaned_df,
                    placeholder_mask,
                    placeholder_rows,
                    leading_placeholder,
                    metadata_start_override,
                    data_start_candidate,
                    data_end_candidate,
                ) = _process_frame(merged_df)
        data = Data(asset_separated, cleaned_df, timestep=ts_unit, quote=quote_asset)
        data.strict_end_check = True
        logger.debug(
            "[THETA][DEBUG][DATA_OBJ] asset=%s/%s (%s) rows=%s idx_min=%s idx_max=%s placeholders=%s ffilled=%s",
            asset_separated,
            quote_asset,
            ts_unit,
            len(cleaned_df) if cleaned_df is not None else 0,
            cleaned_df.index.min() if cleaned_df is not None and len(cleaned_df) else None,
            cleaned_df.index.max() if cleaned_df is not None and len(cleaned_df) else None,
            placeholder_rows,
            meta.get("ffilled") if 'meta' in locals() else None,
        )
        requested_history_start = metadata_start_override
        if requested_history_start is None and existing_meta is not None:
            requested_history_start = existing_meta.get("start")
        if requested_history_start is None:
            requested_history_start = start_for_fetch
        if isinstance(requested_history_start, pd.Timestamp):
            requested_history_start = requested_history_start.to_pydatetime()
        effective_floor = requested_history_start or data.datetime_start
        if effective_floor is not None:
            data.requested_datetime_start = effective_floor
        pandas_data_update = self._set_pandas_data_keys([data])
        if pandas_data_update is not None:
            enriched_update: Dict[tuple, Data] = {}
            for key, data_obj in pandas_data_update.items():
                enriched_update[key] = data_obj
                if isinstance(key, tuple) and len(key) == 2:
                    enriched_update[(key[0], key[1], data_obj.timestep)] = data_obj
            # Add the keys (legacy + timestep-aware) to the caches
            self.pandas_data.update(enriched_update)
            self._data_store.update(enriched_update)
            if ts_unit == "day":
                # Signal to the strategy executor that we're effectively running on daily cadence.
                if getattr(self, "_timestep", None) != "day":
                    self._timestep = "day"
                # Refresh the cached date index so daily iteration can advance efficiently.
                try:
                    self._date_index = self.update_date_index()
                except Exception:
                    logger.debug("[THETA][DEBUG][THETADATA-PANDAS] Failed to rebuild date index for daily cache.", exc_info=True)
        rows_override = len(metadata_frame) if placeholder_rows else None
        self._record_metadata(
            canonical_key,
            metadata_frame,
            ts_unit,
            asset_separated,
            has_quotes=quotes_attached,
            start_override=metadata_start_override,
            rows_override=rows_override,
            data_start_override=data_start_candidate,
            data_end_override=data_end_candidate,
            data_rows_override=len(cleaned_df),
        )
        meta = self._dataset_metadata.get(canonical_key, {}) or {}
        legacy_meta = self._dataset_metadata.get(legacy_key)
        meta["prefetch_complete"] = True
        meta["target_start"] = requested_start
        meta["target_end"] = end_requirement
        meta["ffilled"] = True

        if quotes_attached:
            if quotes_ffill_rows is None and existing_meta is not None:
                quotes_ffill_rows = existing_meta.get("quotes_ffill_rows")
            if existing_meta is not None and quotes_ffill_remaining is None:
                quotes_ffill_remaining = existing_meta.get("quotes_nulls_remaining")
            meta["quotes_ffilled"] = bool(meta.get("quotes_ffilled") or quotes_ffilled)
            if quotes_ffill_rows is not None:
                meta["quotes_ffill_rows"] = quotes_ffill_rows
            if quotes_ffill_remaining is not None:
                meta["quotes_nulls_remaining"] = quotes_ffill_remaining
        elif existing_meta is not None and existing_meta.get("quotes_ffilled"):
            meta["quotes_ffilled"] = True

        self._dataset_metadata[canonical_key] = meta
        if legacy_meta is not None:
            legacy_meta.update(meta)
            self._dataset_metadata[legacy_key] = legacy_meta
        if ts_unit == "day" and placeholder_mask is not None and len(placeholder_mask):
            try:
                tail_missing = bool(placeholder_mask.iloc[-1])
                if tail_missing:
                    last_idx = pd.to_datetime(metadata_frame.index).max()
                    meta["tail_missing_date"] = last_idx.date() if hasattr(last_idx, "date") else last_idx
                    if end_requirement is not None and hasattr(last_idx, "date"):
                        try:
                            end_req_date = end_requirement.date()
                            last_missing_date = last_idx.date()
                            if last_missing_date >= end_req_date:
                                meta["tail_missing_permanent"] = True
                        except Exception:
                            logger.debug("[THETA][DEBUG][TAIL_PLACEHOLDER] failed to compare missing vs end_requirement", exc_info=True)
                    logger.debug(
                        "[THETA][DEBUG][TAIL_PLACEHOLDER] asset=%s/%s last_missing_date=%s target_end=%s permanent=%s",
                        asset_separated,
                        quote_asset,
                        meta.get("tail_missing_date"),
                        end_requirement,
                        meta.get("tail_missing_permanent"),
                    )
            except Exception:
                logger.debug("[THETA][DEBUG][TAIL_PLACEHOLDER] failed to compute tail placeholder metadata", exc_info=True)
            self._dataset_metadata[canonical_key] = meta
            if legacy_meta is not None:
                legacy_meta.update(meta)
                self._dataset_metadata[legacy_key] = legacy_meta

        coverage_end = meta.get("data_end") or meta.get("end")
        if ts_unit == "day":
            try:
                coverage_end = pd.to_datetime(metadata_frame.index).max()
                logger.debug(
                    "[THETA][DEBUG][COVERAGE_END] asset=%s/%s (%s) coverage_end_index=%s",
                    asset_separated,
                    quote_asset,
                    ts_unit,
                    coverage_end,
                )
            except Exception:
                pass
        logger.debug(
            "[THETA][DEBUG][COVERAGE_CHECK] asset=%s/%s (%s) coverage_start=%s coverage_end=%s target_start=%s target_end=%s data_rows=%s placeholders=%s",
            asset_separated,
            quote_asset,
            ts_unit,
            meta.get("data_start"),
            coverage_end,
            requested_start,
            end_requirement,
            meta.get("data_rows"),
            meta.get("placeholders"),
        )
        if end_requirement is not None:
            if coverage_end is None:
                raise ValueError(
                    f"ThetaData coverage for {asset_separated}/{quote_asset} ({ts_unit}) has no end timestamp "
                    f"while target end is {end_requirement}."
                )
            # For both day and minute data, compare at the date level.
            # Minute data legitimately ends at end of after-hours trading (not midnight),
            # so comparing full timestamps would fail incorrectly.
            # IMPORTANT: Convert to same timezone before extracting date to avoid UTC/local mismatch
            if hasattr(coverage_end, 'tzinfo') and hasattr(end_requirement, 'tzinfo'):
                target_tz = end_requirement.tzinfo
                if target_tz is not None and coverage_end.tzinfo is not None:
                    coverage_end_local = coverage_end.astimezone(target_tz)
                else:
                    coverage_end_local = coverage_end
            else:
                coverage_end_local = coverage_end
            coverage_end_cmp = coverage_end_local.date()
            end_requirement_cmp = end_requirement.date()
            # Allow tolerance of up to 3 days at the end - ThetaData may not have the most recent data
            days_behind = (end_requirement_cmp - coverage_end_cmp).days if end_requirement_cmp > coverage_end_cmp else 0
            END_TOLERANCE_DAYS = 3
            if days_behind > 0 and days_behind <= END_TOLERANCE_DAYS:
                # Use INFO - this is expected behavior (data lag within tolerance), not an error.
                logger.info(
                    "[THETA][COVERAGE][TOLERANCE] asset=%s/%s (%s) data is %s day(s) behind target_end=%s; allowing within tolerance",
                    asset_separated,
                    quote_asset,
                    ts_unit,
                    days_behind,
                    end_requirement,
                )
            if coverage_end_cmp < end_requirement_cmp and days_behind > END_TOLERANCE_DAYS:
                logger.error(
                    "[THETA][ERROR][COVERAGE] asset=%s/%s (%s) coverage_end=%s target_end=%s rows=%s placeholders=%s days_behind=%s",
                    asset_separated,
                    quote_asset,
                    ts_unit,
                    coverage_end,
                    end_requirement,
                    meta.get("rows"),
                    meta.get("placeholders"),
                    days_behind,
                )
                logger.error(
                    "[THETA][ERROR][COVERAGE][DIAGNOSTICS] requested_start=%s start_for_fetch=%s data_start=%s data_end=%s requested_length=%s prefetch_complete=%s",
                    requested_start,
                    start_for_fetch,
                    meta.get("data_start"),
                    meta.get("data_end"),
                    requested_length,
                    meta.get("prefetch_complete"),
                )
                raise ValueError(
                    f"ThetaData coverage for {asset_separated}/{quote_asset} ({ts_unit}) ends at {coverage_end} "
                    f"but target end is {end_requirement}; aborting repeated refreshes."
                )
        if meta.get("tail_placeholder") and not meta.get("tail_missing_permanent"):
            raise ValueError(
                f"ThetaData cache for {asset_separated}/{quote_asset} ({ts_unit}) ends with placeholders; "
                f"cannot trade on incomplete data (target_end={end_requirement})."
            )
        if legacy_key not in self._dataset_metadata:
            try:
                self._dataset_metadata[legacy_key] = self._dataset_metadata.get(canonical_key, {})
            except Exception:
                pass

    @staticmethod
    def _combine_duplicate_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Deduplicate duplicate-named columns, preferring the first non-null entry per row."""
        for column in columns:
            if column not in df.columns:
                continue
            selection = df.loc[:, column]
            if isinstance(selection, pd.DataFrame):
                combined = selection.bfill(axis=1).ffill(axis=1).iloc[:, 0]
                df = df.drop(columns=column)
                df[column] = combined
        return df


    def _pull_source_symbol_bars(
        self,
        asset,
        length,
        timestep=None,
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
    ):
        # When timestep is not explicitly specified, align to the current backtesting mode
        # to avoid accidental minute-for-day fallback. Explicit minute/hour requests are
        # allowed - if a strategy explicitly asks for minute data, that's intentional.
        current_mode = getattr(self, "_timestep", None)
        if timestep is None and current_mode == "day":
            timestep = "day"
            logger.debug(
                "[THETA][DEBUG][TIMESTEP_ALIGN] Implicit request aligned to day mode for asset=%s length=%s",
                asset,
                length,
            )
        dt = self.get_datetime()
        requested_length = self.estimate_requested_length(length, timestep=timestep)
        logger.debug(
            "[THETA][DEBUG][THETADATA-PANDAS] request asset=%s quote=%s timestep=%s length=%s inferred_length=%s at %s",
            asset,
            quote,
            timestep,
            length,
            requested_length,
            dt,
        )
        self._update_pandas_data(asset, quote, requested_length, timestep, dt)
        response = super()._pull_source_symbol_bars(
            asset, length, timestep, timeshift, quote, exchange, include_after_hours
        )
        if response is None:
            return None
        effective_timestep = timestep or "minute"
        if isinstance(response, pd.DataFrame) and effective_timestep == "day":
            finalized = self._finalize_day_frame(response, dt, requested_length, timeshift, asset=asset)
            if finalized is None or finalized.empty:
                return None
            return finalized
        return response

    # Get pricing data for an asset for the entire backtesting period
    def get_historical_prices_between_dates(
        self,
        asset,
        timestep="minute",
        quote=None,
        exchange=None,
        include_after_hours=True,
        start_date=None,
        end_date=None,
    ):
        inferred_length = self.estimate_requested_length(
            None, start_date=start_date, end_date=end_date, timestep=timestep
        )
        self._update_pandas_data(asset, quote, inferred_length, timestep, end_date)

        response = super()._pull_source_symbol_bars_between_dates(
            asset, timestep, quote, exchange, include_after_hours, start_date, end_date
        )

        if response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote)
        final_df = getattr(bars, "df", None)
        final_rows = len(final_df) if final_df is not None else 0
        logger.debug(
            "[THETA][DEBUG][FETCH][PANDAS][FINAL] asset=%s quote=%s length=%s timestep=%s start=%s end=%s rows=%s",
            getattr(asset, "symbol", asset) if not isinstance(asset, str) else asset,
            getattr(quote, "symbol", quote),
            inferred_length,
            timestep,
            start_date,
            end_date,
            final_rows,
        )
        return bars

    def get_yesterday_dividends(self, assets, quote=None):
        """Fetch Theta dividends via the corporate actions API to guarantee coverage.

        IMPORTANT: ThetaData returns UNADJUSTED dividend amounts (pre-split).
        We must adjust them by the cumulative split factor to get the correct
        per-share amount in today's (post-split) terms.

        NOTE: ThetaData has known data quality issues with phantom dividends
        (e.g., TQQQ 2014-09-18 shows $0.41 that doesn't exist in other sources).
        This is a ThetaData data quality issue that should be reported to their support.
        """
        if not hasattr(self, "_theta_dividend_cache"):
            self._theta_dividend_cache = {}

        current_date = self._datetime.date() if hasattr(self._datetime, "date") else self._datetime
        result = {}
        for asset in assets:
            cache = self._theta_dividend_cache.get(asset)
            if cache is None:
                cache = {}
                start_day = getattr(self, "datetime_start", None)
                end_day = getattr(self, "datetime_end", None)
                start_date = start_day.date() if hasattr(start_day, "date") else current_date - timedelta(days=365)
                end_date = end_day.date() if hasattr(end_day, "date") else current_date
                try:
                    events = thetadata_helper._get_theta_dividends(asset, start_date, end_date, self._username, self._password)
                    # Also fetch splits to adjust dividend amounts
                    splits = thetadata_helper._get_theta_splits(asset, start_date, end_date, self._username, self._password)

                    # Build cumulative split factor map (for each date, what factor to divide by)
                    if splits is not None and not splits.empty:
                        sorted_splits = splits.sort_values("event_date")
                        # Calculate cumulative factor for each potential dividend date
                        # A dividend on date D needs to be divided by all splits that occurred AFTER D
                        split_dates = sorted_splits["event_date"].dt.date.tolist()
                        split_ratios = sorted_splits["ratio"].tolist()

                        def get_cumulative_factor(div_date):
                            """Get the cumulative split factor for a dividend on div_date."""
                            factor = 1.0
                            for split_date, ratio in zip(split_dates, split_ratios):
                                if split_date > div_date and ratio > 0 and ratio != 1.0:
                                    factor *= ratio
                            return factor
                    else:
                        def get_cumulative_factor(div_date):
                            return 1.0

                    if events is not None and not events.empty:
                        for _, row in events.iterrows():
                            event_dt = row.get("event_date")
                            amount = row.get("cash_amount", 0)
                            if pd.notna(event_dt) and amount:
                                div_date = event_dt.date()

                                # Adjust dividend amount by cumulative split factor
                                cumulative_factor = get_cumulative_factor(div_date)
                                adjusted_amount = float(amount) / cumulative_factor if cumulative_factor != 0 else float(amount)
                                cache[div_date] = adjusted_amount
                                if cumulative_factor != 1.0:
                                    logger.debug(
                                        "[THETA][DIVIDENDS] %s dividend on %s: raw=%.6f adjusted=%.6f (factor=%.2f)",
                                        getattr(asset, "symbol", asset),
                                        div_date,
                                        amount,
                                        adjusted_amount,
                                        cumulative_factor,
                                    )
                        if cache:
                            logger.debug(
                                "[THETA][DIVIDENDS] cached %d entries for %s (%s -> %s)",
                                len(cache),
                                getattr(asset, "symbol", asset),
                                min(cache.keys()),
                                max(cache.keys()),
                            )
                    else:
                        logger.debug(
                            "[THETA][DIVIDENDS] no dividend rows returned for %s between %s and %s",
                            getattr(asset, "symbol", asset),
                            start_date,
                            end_date,
                        )
                except Exception as exc:
                    logger.debug(
                        "[THETA][DEBUG][DIVIDENDS] Failed to load corporate actions for %s: %s",
                        getattr(asset, "symbol", asset),
                        exc,
                    )
                self._theta_dividend_cache[asset] = cache

            dividend = cache.get(current_date, 0.0)
            if dividend:
                logger.info(
                    "[THETA][DIVIDENDS] %s dividend on %s = %.6f",
                    getattr(asset, "symbol", asset),
                    current_date,
                    dividend,
                )
            result[asset] = dividend

        return AssetsMapping(result)

    def get_last_price(self, asset, timestep="minute", quote=None, exchange=None, **kwargs) -> Union[float, Decimal, None]:
        sample_length = 5
        dt = self.get_datetime()
        # In day mode, use day data for price lookups instead of defaulting to minute.
        # This prevents unnecessary minute data downloads at end of day-mode backtests.
        current_mode = getattr(self, "_timestep", None)
        if current_mode == "day" and timestep == "minute":
            timestep = "day"
            logger.debug(
                "[THETA][DEBUG][TIMESTEP_ALIGN] get_last_price aligned from minute to day for asset=%s",
                asset,
            )

        self._update_pandas_data(asset, quote, sample_length, timestep, dt, require_quote_data=True)
        _, ts_unit = self.get_start_datetime_and_ts_unit(
            sample_length, timestep, dt, start_buffer=START_BUFFER
        )
        source = None
        tuple_key = self.find_asset_in_data_store(asset, quote, ts_unit)
        legacy_hit = False
        frame_last_dt = None
        frame_last_close = None
        if tuple_key is not None:
            data = self.pandas_data.get(tuple_key)
            if data is None and isinstance(tuple_key, tuple) and len(tuple_key) == 3:
                legacy_tuple_key = (tuple_key[0], tuple_key[1])
                data = self.pandas_data.get(legacy_tuple_key)
                if data is not None:
                    legacy_hit = True
            elif isinstance(tuple_key, tuple) and len(tuple_key) != 3:
                legacy_hit = True
            if data is not None and hasattr(data, "df"):
                close_series = data.df.get("close")
                if close_series is None:
                    return super().get_last_price(asset=asset, quote=quote, exchange=exchange)
                closes = close_series.dropna()
                # Remove placeholder rows (missing=True) from consideration.
                if "missing" in data.df.columns:
                    missing_mask = data.df.loc[closes.index, "missing"]
                    closes = closes[~missing_mask.fillna(True)]
                # Ignore non-positive prices which indicate bad ticks.
                closes = closes[closes > 0]
                if closes.empty:
                    logger.debug(
                        "[THETA][DEBUG][THETADATA-PANDAS] get_last_price found no valid closes for %s/%s; returning None (likely expired).",
                        asset,
                        quote or Asset("USD", "forex"),
                    )
                    return None
                closes = closes.tail(sample_length)
                source = "pandas_dataset"
                if len(closes):
                    frame_last_dt = closes.index[-1]
                    frame_last_close = closes.iloc[-1]
                    try:
                        frame_last_dt = frame_last_dt.isoformat()
                    except AttributeError:
                        frame_last_dt = str(frame_last_dt)
        value = super().get_last_price(asset=asset, quote=quote, exchange=exchange)
        logger.debug(
            "[THETA][DEBUG][THETADATA-PANDAS] get_last_price resolved via %s for %s/%s (close=%s)",
            source or "super",
            asset,
            quote or Asset("USD", "forex"),
            value,
        )
        _parity_log(
            "[THETA][DEBUG][PARITY][LAST_PRICE][THETA][DEBUG][PANDAS] asset=%s quote=%s dt=%s value=%s source=%s tuple_key=%s legacy_key_used=%s ts_unit=%s frame_last_dt=%s frame_last_close=%s",
            getattr(asset, "symbol", asset),
            getattr(quote, "symbol", quote) if quote else "USD",
            dt.isoformat() if hasattr(dt, "isoformat") else str(dt),
            value,
            source or "super",
            tuple_key,
            legacy_hit,
            ts_unit,
            frame_last_dt,
            float(frame_last_close) if frame_last_close is not None else None,
        )

        return value

    def get_price_snapshot(self, asset, quote=None, timestep="minute", **kwargs) -> Optional[Dict[str, object]]:
        """Return the latest OHLC + quote snapshot for the requested asset."""
        sample_length = 5
        dt = self.get_datetime()
        # In day mode, use day data for price snapshots instead of defaulting to minute.
        # This prevents unnecessary minute data downloads at end of day-mode backtests.
        current_mode = getattr(self, "_timestep", None)
        if current_mode == "day" and timestep == "minute":
            timestep = "day"
            logger.debug(
                "[THETA][DEBUG][TIMESTEP_ALIGN] get_price_snapshot aligned from minute to day for asset=%s",
                asset,
            )
        self._update_pandas_data(asset, quote, sample_length, timestep, dt)
        _, ts_unit = self.get_start_datetime_and_ts_unit(
            sample_length, timestep, dt, start_buffer=START_BUFFER
        )

        tuple_key = self.find_asset_in_data_store(asset, quote, ts_unit)
        data = None
        if tuple_key is not None:
            data = self.pandas_data.get(tuple_key)
            if data is None and isinstance(tuple_key, tuple) and len(tuple_key) == 3:
                legacy_tuple_key = (tuple_key[0], tuple_key[1])
                data = self.pandas_data.get(legacy_tuple_key)

        if data is None or not hasattr(data, "get_price_snapshot"):
            logger.debug(
                "[THETA][DEBUG][THETADATA-PANDAS] get_price_snapshot unavailable for %s/%s (tuple_key=%s).",
                asset,
                quote or Asset("USD", "forex"),
                tuple_key,
            )
            return None

        try:
            snapshot = data.get_price_snapshot(dt)
            logger.debug(
                "[THETA][DEBUG][THETADATA-PANDAS] get_price_snapshot succeeded for %s/%s: %s",
                asset,
                quote or Asset("USD", "forex"),
                snapshot,
            )
            return snapshot
        except ValueError as e:
            # Handle case where requested date is after available data (e.g., end of backtest)
            if "after the available data's end" in str(e):
                logger.debug(
                    "[THETA][DEBUG][THETADATA-PANDAS] get_price_snapshot date %s after data end for %s/%s; returning None",
                    dt,
                    asset,
                    quote or Asset("USD", "forex"),
                )
                return None
            raise

    def get_historical_prices(
        self,
        asset: Asset | str,
        length: int,
        timestep: str = "minute",
        timeshift: int | timedelta | None = None,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        include_after_hours: bool = True,
        return_polars: bool = False,
    ):
        if return_polars:
            raise ValueError("ThetaData backtesting currently supports pandas output only.")

        current_dt = self.get_datetime()
        start_requirement, ts_unit = self.get_start_datetime_and_ts_unit(
            length,
            timestep,
            current_dt,
            start_buffer=START_BUFFER,
        )
        bars = super().get_historical_prices(
            asset=asset,
            length=length,
            timestep=timestep,
            timeshift=timeshift,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours,
            return_polars=False,
        )
        if bars is not None and hasattr(bars, "df") and bars.df is not None:
            try:
                # Drop any future bars to avoid lookahead when requesting intraday data
                if ts_unit == "minute":
                    effective_now = self.to_default_timezone(self.get_datetime())
                    try:
                        idx_converted = bars.df.index.tz_convert(effective_now.tzinfo)
                    except Exception:
                        idx_converted = bars.df.index
                    mask = idx_converted <= effective_now
                    pruned = bars.df[mask]
                    if pruned.empty and len(bars.df):
                        pruned = bars.df[idx_converted < effective_now]
                    bars.df = pruned
            except Exception:
                pass
        if bars is None or getattr(bars, "df", None) is None or bars.df.empty:
            logger.debug(
                "[THETA][DEBUG][FETCH][THETA][DEBUG][PANDAS] asset=%s quote=%s length=%s timestep=%s timeshift=%s current_dt=%s "
                "rows=0 first_ts=None last_ts=None columns=None",
                getattr(asset, "symbol", asset) if not isinstance(asset, str) else asset,
                getattr(quote, "symbol", quote),
                length,
                timestep,
                timeshift,
                current_dt,
            )
            return bars

        df = bars.df
        dataset_key = self.find_asset_in_data_store(asset, quote, ts_unit)
        candidate_data = None
        if dataset_key is not None:
            candidate_data = self.pandas_data.get(dataset_key)
            if candidate_data is None and isinstance(dataset_key, tuple) and len(dataset_key) == 3:
                legacy_key = (dataset_key[0], dataset_key[1])
                candidate_data = self.pandas_data.get(legacy_key)
        normalized_requirement = self._normalize_default_timezone(start_requirement)
        normalized_current_dt = self._normalize_default_timezone(current_dt)
        normalized_data_start = None
        if candidate_data is not None and getattr(candidate_data, "df", None) is not None and not candidate_data.df.empty:
            normalized_data_start = self._normalize_default_timezone(candidate_data.df.index.min())
        if (
            normalized_current_dt is not None
            and normalized_data_start is not None
            and normalized_current_dt < normalized_data_start
        ):
            logger.debug(
                "[THETA][DEBUG][FETCH][THETA][DEBUG][PANDAS] asset=%s quote=%s length=%s timestep=%s timeshift=%s current_dt=%s "
                "occurs before first real bar %s – returning None",
                getattr(asset, "symbol", asset) if not isinstance(asset, str) else asset,
                getattr(quote, "symbol", quote),
                length,
                timestep,
                timeshift,
                normalized_current_dt,
                normalized_data_start,
            )
            return None
        rows = len(df)
        columns = list(df.columns)
        first_ts = df["datetime"].iloc[0] if "datetime" in df.columns else df.index[0]
        last_ts = df["datetime"].iloc[-1] if "datetime" in df.columns else df.index[-1]

        logger.debug(
            "[THETA][DEBUG][FETCH][THETA][DEBUG][PANDAS] asset=%s quote=%s length=%s timestep=%s timeshift=%s current_dt=%s rows=%s "
            "first_ts=%s last_ts=%s columns=%s",
            getattr(asset, "symbol", asset) if not isinstance(asset, str) else asset,
            getattr(quote, "symbol", quote),
            length,
            timestep,
            timeshift,
            current_dt,
            rows,
            first_ts,
            last_ts,
            columns,
        )
        return bars

    def get_quote(self, asset, timestep="minute", quote=None, exchange=None, **kwargs):
        """
        Get quote data for an asset during backtesting.

        Parameters
        ----------
        asset : Asset object
            The asset for which the quote is needed.
        timestep : str, optional
            The timestep to use for the data.
        quote : Asset object, optional
            The quote asset for cryptocurrency pairs.
        exchange : str, optional
            The exchange to get the quote from.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        Quote
            A Quote object with the quote information.
        """
        dt = self.get_datetime()

        # Log quote request details for debugging (options vs other assets)
        if hasattr(asset, 'asset_type') and asset.asset_type == Asset.AssetType.OPTION:
            logger.debug(
                "[THETA][QUOTE] Option request: symbol=%s expiration=%s strike=%s right=%s dt=%s timestep=%s",
                asset.symbol,
                asset.expiration,
                asset.strike,
                asset.right,
                dt.isoformat() if hasattr(dt, 'isoformat') else dt,
                timestep
            )
        else:
            logger.debug(
                "[THETA][QUOTE] Asset request: symbol=%s dt=%s timestep=%s",
                getattr(asset, "symbol", asset) if not isinstance(asset, str) else asset,
                dt.isoformat() if hasattr(dt, 'isoformat') else dt,
                timestep
            )

        self._update_pandas_data(asset, quote, 1, timestep, dt, require_quote_data=True)

        # [INSTRUMENTATION] Capture in-memory dataframe state after _update_pandas_data
        debug_enabled = True

        base_asset = asset[0] if isinstance(asset, tuple) else asset
        quote_asset = quote if quote else Asset("USD", "forex")
        _, ts_unit = self.get_start_datetime_and_ts_unit(1, timestep, dt, start_buffer=START_BUFFER)
        canonical_key, legacy_key = self._build_dataset_keys(base_asset, quote_asset, ts_unit)
        data_obj = self.pandas_data.get(canonical_key)
        if data_obj is None:
            data_obj = self.pandas_data.get(legacy_key)
        if data_obj is not None and hasattr(data_obj, 'df'):
            df = data_obj.df
            if df is not None and len(df) > 0:
                # Get first and last 5 rows
                head_df = df.head(5)
                tail_df = df.tail(5)

                # Format columns to show
                cols_to_show = ['bid', 'ask', 'mid_price', 'close'] if hasattr(asset, 'asset_type') and asset.asset_type == Asset.AssetType.OPTION else ['close']
                available_cols = [col for col in cols_to_show if col in df.columns]

                # Get timezone info
                tz_info = "NO_TZ"
                if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
                    tz_info = str(df.index.tz)

                logger.debug(
                    "[THETA][DEBUG][QUOTE][THETA][DEBUG][PANDAS][DATAFRAME_STATE] asset=%s | total_rows=%d | timestep=%s | index_type=%s | timezone=%s",
                    getattr(asset, "symbol", asset),
                    len(df),
                    data_obj.timestep,
                    type(df.index).__name__,
                    tz_info
                )

                # Log datetime range with timezone
                if isinstance(df.index, pd.DatetimeIndex):
                    first_dt_str = df.index[0].isoformat() if hasattr(df.index[0], 'isoformat') else str(df.index[0])
                    last_dt_str = df.index[-1].isoformat() if hasattr(df.index[-1], 'isoformat') else str(df.index[-1])
                    logger.debug(
                        "[THETA][DEBUG][QUOTE][THETA][DEBUG][PANDAS][DATETIME_RANGE] asset=%s | first_dt=%s | last_dt=%s | tz=%s",
                        getattr(asset, "symbol", asset),
                        first_dt_str,
                        last_dt_str,
                        tz_info
                    )

                    # CRITICAL: Show tail with explicit datetime index to catch time-travel bug
                    if debug_enabled and len(available_cols) > 0:
                        logger.debug(
                            "[THETA][DEBUG][QUOTE][THETA][DEBUG][PANDAS][DATAFRAME_HEAD] asset=%s | first_5_rows (with datetime index):\n%s",
                            getattr(asset, "symbol", asset),
                            head_df[available_cols].to_string()
                        )
                        logger.debug(
                            "[THETA][DEBUG][QUOTE][THETA][DEBUG][PANDAS][DATAFRAME_TAIL] asset=%s | last_5_rows (with datetime index):\n%s",
                            getattr(asset, "symbol", asset),
                            tail_df[available_cols].to_string()
                        )

                        # Show tail datetime values explicitly
                        tail_datetimes = [dt.isoformat() if hasattr(dt, 'isoformat') else str(dt) for dt in tail_df.index]
                        logger.debug(
                            "[THETA][DEBUG][QUOTE][THETA][DEBUG][PANDAS][TAIL_DATETIMES] asset=%s | tail_index=%s",
                            getattr(asset, "symbol", asset),
                            tail_datetimes
                        )
            else:
                logger.debug(
                    "[THETA][DEBUG][QUOTE][THETA][DEBUG][PANDAS][DATAFRAME_STATE] asset=%s | EMPTY_DATAFRAME",
                    getattr(asset, "symbol", asset)
                )
        else:
            logger.debug(
                "[THETA][DEBUG][QUOTE][THETA][DEBUG][PANDAS][DATAFRAME_STATE] asset=%s | NO_DATA_FOUND_IN_STORE",
                getattr(asset, "symbol", asset)
            )

        quote_obj = super().get_quote(asset=asset, quote=quote, exchange=exchange)

        # [INSTRUMENTATION] Final quote result with all details
        logger.debug(
            "[THETA][DEBUG][QUOTE][THETA][DEBUG][PANDAS][RESULT] asset=%s quote=%s current_dt=%s bid=%s ask=%s mid=%s last=%s source=%s",
            getattr(asset, "symbol", asset) if not isinstance(asset, str) else asset,
            getattr(quote, "symbol", quote),
            dt,
            getattr(quote_obj, "bid", None) if quote_obj else None,
            getattr(quote_obj, "ask", None) if quote_obj else None,
            getattr(quote_obj, "mid_price", None) if quote_obj else None,
            getattr(quote_obj, "last_price", None) if quote_obj else None,
            getattr(quote_obj, "source", None) if quote_obj else None,
        )

        return quote_obj

    def get_chains(self, asset):
        """
        Get option chains using cached implementation (matches Polygon pattern).

        Parameters
        ----------
        asset : Asset
            The asset to get data for.

        Returns
        -------
        Chains:
            A Chains entity object (dict subclass) with the structure:
            {
                "Multiplier": 100,
                "Exchange": "SMART",
                "Chains": {
                    "CALL": {
                        "2023-07-31": [100.0, 101.0, ...],
                        ...
                    },
                    "PUT": {
                        "2023-07-31": [100.0, 101.0, ...],
                        ...
                    }
                }
            }
        """
        from lumibot.entities import Chains

        constraints = getattr(self, "_chain_constraints", None)
        chains_dict = thetadata_helper.get_chains_cached(
            username=self._username,
            password=self._password,
            asset=asset,
            current_date=self.get_datetime().date(),
            chain_constraints=constraints,
        )

        # Wrap in Chains entity for modern API
        return Chains(chains_dict)
