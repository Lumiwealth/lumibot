from decimal import Decimal
from typing import Dict, Optional, Union, List

import logging
import pandas as pd
import pytz
import subprocess
from datetime import date, datetime, timedelta

from lumibot.data_sources import PandasData
from lumibot.entities import Asset, Data
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

    IS_BACKTESTING_BROKER = True

    # Enable fallback to last_price when bid/ask quotes are unavailable for options
    option_quote_fallback_allowed = True

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

    def _record_metadata(self, key, frame: pd.DataFrame, ts_unit: str, asset: Asset) -> None:
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
                start = dt_index.min().to_pydatetime()
                end = dt_index.max().to_pydatetime()
            else:
                start = end = None
            rows = len(frame)

        normalized_start = self._normalize_default_timezone(start)
        normalized_end = self._normalize_default_timezone(end)

        metadata: Dict[str, object] = {
            "timestep": ts_unit,
            "start": normalized_start,
            "end": normalized_end,
            "rows": rows,
        }
        metadata["empty_fetch"] = frame is None or frame.empty

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

    def _update_pandas_data(self, asset, quote, length, timestep, start_dt=None):
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

        search_asset = asset
        asset_separated = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(search_asset, tuple):
            asset_separated, quote_asset = search_asset
        else:
            search_asset = (search_asset, quote_asset)

        if asset_separated.asset_type == "option":
            expiry = asset_separated.expiration
            if self.is_weekend(expiry):
                logger.info(f"\nSKIP: Expiry {expiry} date is a weekend, no contract exists: {asset_separated}")
                return None

        # Get the start datetime and timestep unit
        start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(
            length, timestep, start_dt, start_buffer=START_BUFFER
        )

        requested_length = length
        requested_start = self._normalize_default_timezone(start_datetime)
        start_threshold = requested_start + START_BUFFER if requested_start is not None else None
        current_dt = self.get_datetime()
        end_requirement = self.datetime_end if ts_unit == "day" else current_dt
        end_requirement = self._normalize_default_timezone(end_requirement)
        expiration_dt = self._option_expiration_end(asset_separated)
        if expiration_dt is not None and end_requirement is not None and expiration_dt < end_requirement:
            end_requirement = expiration_dt

        existing_data = self.pandas_data.get(search_asset)
        if existing_data is not None and search_asset not in self._dataset_metadata:
            self._record_metadata(search_asset, existing_data.df, existing_data.timestep, asset_separated)
        existing_meta = self._dataset_metadata.get(search_asset)

        if existing_data is not None and existing_meta and existing_meta.get("timestep") == ts_unit:
            existing_start = existing_meta.get("start")
            existing_rows = existing_meta.get("rows", 0)
            existing_end = existing_meta.get("end")

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

            start_ok = (
                existing_start is not None
                and (start_threshold is None or existing_start <= start_threshold)
            )

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
                "existing_start <= start_threshold" if start_ok else
                ("start_threshold is None" if start_threshold is None else "existing_start > start_threshold")
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
                    # FIX: For daily data, use date-only comparison instead of datetime comparison
                    # This prevents false negatives when existing_end is midnight and end_requirement is later the same day
                    if ts_unit == "day":
                        existing_end_date = existing_end.date() if hasattr(existing_end, 'date') else existing_end
                        end_requirement_date = end_requirement.date() if hasattr(end_requirement, 'date') else end_requirement
                        existing_end_cmp = existing_end_date
                        end_requirement_cmp = end_requirement_date
                    else:
                        existing_end_cmp = existing_end
                        end_requirement_cmp = end_requirement

                    if existing_end_cmp > end_requirement_cmp:
                        end_ok = True
                        logger.debug(
                            "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][END_VALIDATION][RESULT] asset=%s | "
                            "end_ok=TRUE | reason=existing_end_exceeds_requirement | "
                            "existing_end=%s end_requirement=%s ts_unit=%s",
                            asset_separated.symbol if hasattr(asset_separated, 'symbol') else str(asset_separated),
                            existing_end.isoformat(),
                            end_requirement.isoformat(),
                            ts_unit
                        )
                    elif existing_end_cmp == end_requirement_cmp:
                        weekday = existing_end.weekday() if hasattr(existing_end, "weekday") else None
                        placeholder_on_weekend = tail_placeholder and weekday is not None and weekday >= 5
                        placeholder_empty_fetch = tail_placeholder and existing_meta.get("empty_fetch")
                        end_ok = (not tail_placeholder) or placeholder_on_weekend or placeholder_empty_fetch

                        logger.debug(
                            "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][END_VALIDATION][EXACT_MATCH] asset=%s | "
                            "existing_end == end_requirement | "
                            "weekday=%s placeholder_on_weekend=%s placeholder_empty_fetch=%s | "
                            "end_ok=%s ts_unit=%s",
                            asset_separated.symbol if hasattr(asset_separated, 'symbol') else str(asset_separated),
                            weekday,
                            placeholder_on_weekend,
                            placeholder_empty_fetch,
                            end_ok,
                            ts_unit
                        )
                    else:
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
            if existing_start is None or (start_threshold is not None and existing_start > start_threshold):
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

        # Check if we have data for this asset
        if search_asset in self.pandas_data:
            asset_data = self.pandas_data[search_asset]
            asset_data_df = asset_data.df
            data_start_datetime = asset_data_df.index[0]

            # Get the timestep of the data
            data_timestep = asset_data.timestep

            # If the timestep is the same, we don't need to update the data
            if data_timestep == ts_unit:
                # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                if (data_start_datetime - start_datetime) < START_BUFFER:
                    return None

            # Always try to get the lowest timestep possible because we can always resample
            # If day is requested then make sure we at least have data that's less than a day
            if ts_unit == "day":
                if data_timestep == "minute":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return None
                    else:
                        # We don't have enough data, so we need to get more (but in minutes)
                        ts_unit = "minute"
                elif data_timestep == "hour":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return None
                    else:
                        # We don't have enough data, so we need to get more (but in hours)
                        ts_unit = "hour"

            # If hour is requested then make sure we at least have data that's less than an hour
            if ts_unit == "hour":
                if data_timestep == "minute":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return None
                    else:
                        # We don't have enough data, so we need to get more (but in minutes)
                        ts_unit = "minute"

        # Download data from ThetaData
        # Get ohlc data from ThetaData
        date_time_now = self.get_datetime()
        logger.debug(
            "[THETA][DEBUG][THETADATA-PANDAS] fetch asset=%s quote=%s length=%s timestep=%s start=%s end=%s",
            asset_separated,
            quote_asset,
            length,
            timestep,
            start_datetime,
            self.datetime_end,
        )
        df_ohlc = thetadata_helper.get_price_data(
            self._username,
            self._password,
            asset_separated,
            start_datetime,
            self.datetime_end,
            timespan=ts_unit,
            quote_asset=quote_asset,
            dt=date_time_now,
            datastyle="ohlc",
            include_after_hours=True  # Default to True for extended hours data
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
            else:
                logger.warning(f"No OHLC data returned for {asset_separated} / {quote_asset} ({ts_unit}); skipping cache update.")
            cache_df = thetadata_helper.load_cache(
                thetadata_helper.build_cache_filename(asset_separated, ts_unit, "ohlc")
            )
            if cache_df is not None and len(cache_df) > 0:
                placeholder_data = Data(asset_separated, cache_df, timestep=ts_unit, quote=quote_asset)
                placeholder_update = self._set_pandas_data_keys([placeholder_data])
                if placeholder_update:
                    self.pandas_data.update(placeholder_update)
                    self._data_store.update(placeholder_update)
                    self._record_metadata(search_asset, placeholder_data.df, ts_unit, asset_separated)
                    logger.debug(
                        "[THETA][DEBUG][THETADATA-PANDAS] refreshed metadata from cache for %s/%s (%s) after empty fetch.",
                        asset_separated,
                        quote_asset,
                        ts_unit,
                    )
            return None

        df = df_ohlc

        # Quote data (bid/ask) is only available for intraday data (minute, hour, second)
        # For daily+ data, only use OHLC
        if self._use_quote_data and ts_unit in ["minute", "hour", "second"]:
            try:
                df_quote = thetadata_helper.get_price_data(
                    self._username,
                    self._password,
                    asset_separated,
                    start_datetime,
                    self.datetime_end,
                    timespan=ts_unit,
                    quote_asset=quote_asset,
                    dt=date_time_now,
                    datastyle="quote",
                    include_after_hours=True  # Default to True for extended hours data
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
                if forward_fill_columns:
                    df[forward_fill_columns] = df[forward_fill_columns].ffill()

                    # Log how much forward filling occurred
                    if 'bid' in df.columns and 'ask' in df.columns:
                        remaining_nulls = df[['bid', 'ask']].isna().sum().sum()
                        if remaining_nulls > 0:
                            logger.info(f"Forward-filled missing quote values for {asset_separated}. {remaining_nulls} nulls remain at start of data.")

        if df is None or df.empty:
            return None

        data = Data(asset_separated, df, timestep=ts_unit, quote=quote_asset)
        pandas_data_update = self._set_pandas_data_keys([data])
        if pandas_data_update is not None:
            # Add the keys to the self.pandas_data dictionary
            self.pandas_data.update(pandas_data_update)
            self._data_store.update(pandas_data_update)
        self._record_metadata(search_asset, data.df, ts_unit, asset_separated)

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
            "[THETA][DEBUG][FETCH][THETA][DEBUG][PANDAS][FINAL] asset=%s quote=%s length=%s timestep=%s timeshift=%s current_dt=%s rows=%s",
            getattr(asset, "symbol", asset) if not isinstance(asset, str) else asset,
            getattr(quote, "symbol", quote),
            length,
            timestep,
            timeshift,
            current_dt,
            final_rows,
        )
        return bars

    def get_last_price(self, asset, timestep="minute", quote=None, exchange=None, **kwargs) -> Union[float, Decimal, None]:
        sample_length = 5
        dt = self.get_datetime()
        self._update_pandas_data(asset, quote, sample_length, timestep, dt)
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

        snapshot = data.get_price_snapshot(dt)
        logger.debug(
            "[THETA][DEBUG][THETADATA-PANDAS] get_price_snapshot succeeded for %s/%s: %s",
            asset,
            quote or Asset("USD", "forex"),
            snapshot,
        )
        return snapshot

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
        rows = len(df)
        columns = list(df.columns)
        if "datetime" in df.columns:
            first_ts = df["datetime"].iloc[0]
            last_ts = df["datetime"].iloc[-1]
        else:
            first_ts = df.index[0]
            last_ts = df.index[-1]

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

        # [INSTRUMENTATION] Log full asset details for options
        if hasattr(asset, 'asset_type') and asset.asset_type == Asset.AssetType.OPTION:
            logger.debug(
                "[THETA][DEBUG][QUOTE][THETA][DEBUG][PANDAS][OPTION_REQUEST] symbol=%s expiration=%s strike=%s right=%s current_dt=%s timestep=%s",
                asset.symbol,
                asset.expiration,
                asset.strike,
                asset.right,
                dt.isoformat() if hasattr(dt, 'isoformat') else dt,
                timestep
            )
        else:
            logger.debug(
                "[THETA][DEBUG][QUOTE][THETA][DEBUG][PANDAS][REQUEST] asset=%s current_dt=%s timestep=%s",
                getattr(asset, "symbol", asset) if not isinstance(asset, str) else asset,
                dt.isoformat() if hasattr(dt, 'isoformat') else dt,
                timestep
            )

        self._update_pandas_data(asset, quote, 1, timestep, dt)

        # [INSTRUMENTATION] Capture in-memory dataframe state after _update_pandas_data
        debug_enabled = True

        search_asset = (asset, quote if quote else Asset("USD", "forex"))
        data_obj = self.pandas_data.get(search_asset)
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
