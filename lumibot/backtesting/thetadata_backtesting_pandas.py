import json
import logging
import math
import os
import subprocess
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Union

import pandas as pd
import pytz

from lumibot.credentials import THETADATA_CONFIG
from lumibot.data_sources import PandasData
from lumibot.entities import Asset, AssetsMapping, Data
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

    # Do not fall back to trade-derived OHLC when bid/ask quotes are unavailable for options.
    # Backtests should not trigger expensive option OHLC downloads as an implicit quote fallback.
    option_quote_fallback_allowed = False

    @staticmethod
    def _compute_prefetch_complete(
        meta: Dict[str, object],
        *,
        requested_start: Optional[datetime],
        effective_start_buffer: timedelta,
        end_requirement: Optional[datetime],
        ts_unit: str,
        requested_length: int,
    ) -> bool:
        """Return True when a cached dataset satisfies the requested coverage window.

        IMPORTANT: `prefetch_complete` is a performance optimization flag used to skip redundant
        downloader work in hot loops. It must never be set True when coverage is insufficient,
        otherwise backtests can thrash (STALE → REFRESH → STALE ...) on every bar.
        """
        try:
            if bool(meta.get("negative_cache")):
                return True
            if bool(meta.get("tail_missing_permanent")):
                return True
        except Exception:
            pass

        coverage_start = meta.get("data_start") or meta.get("start")
        coverage_end = meta.get("data_end") or meta.get("end")
        rows_have = meta.get("data_rows") or meta.get("rows") or 0

        try:
            rows_have_int = int(rows_have)  # type: ignore[arg-type]
        except Exception:
            rows_have_int = 0

        start_ok = True
        if requested_start is not None:
            if coverage_start is None:
                start_ok = False
            else:
                try:
                    if isinstance(coverage_start, pd.Timestamp):
                        coverage_start = coverage_start.to_pydatetime()
                    start_ok = coverage_start <= requested_start + effective_start_buffer
                except Exception:
                    start_ok = False

        end_ok = True
        if end_requirement is not None:
            if coverage_end is None:
                end_ok = False
            else:
                try:
                    if isinstance(coverage_end, pd.Timestamp):
                        coverage_end = coverage_end.to_pydatetime()
                    if ts_unit == "day":
                        end_ok = coverage_end.date() >= end_requirement.date()  # type: ignore[union-attr]
                    else:
                        end_ok = coverage_end >= end_requirement  # type: ignore[operator]
                except Exception:
                    end_ok = False

        length_ok = rows_have_int >= int(requested_length)
        return bool(start_ok and end_ok and length_ok)

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
        # Do not enable option quote fallback to trade-derived OHLC in ThetaData backtests.
        # ThetaData option NBBO is the preferred source for pricing; when bid/ask is missing we
        # treat the contract as unpriceable at that moment rather than downloading sparse trades.
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, pandas_data=pandas_data,
                         allow_option_quote_fallback=False, **kwargs)

        # Default to minute; broker can flip to day for daily strategies.
        self._timestep = self.MIN_TIMESTEP
        # PERF: Avoid scanning the entire pandas_data store on every quote/snapshot call to infer day-mode.
        # This flag is set eagerly when day data is loaded.
        self._effective_day_mode = None
        # Cadence detector: some intraday strategies also request daily history (e.g., indicators).
        # Do NOT let those day-series requests force the entire backtest into "day" cadence.
        self._cadence_last_dt = None
        self._observed_intraday_cadence = False

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
        self._negative_option_cache = set()

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

        # When a Data Downloader is configured, LumiBot must never touch local ThetaTerminal
        # processes. Starting/killing a local ThetaTerminal can steal the single licensed Theta
        # session and take down the downloader.
        if not (os.environ.get("DATADOWNLOADER_BASE_URL") or "").strip():
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
                    start_dt = datetime.combine(start_date, datetime.min.time())
                    end_dt = datetime.combine(end_date, datetime.max.time())
                    base_tz = getattr(dt_index, "tz", None) or pytz.UTC
                    # IMPORTANT: for pytz timezones, use `localize()` (not `replace(tzinfo=...)`)
                    # to avoid "LMT" offsets like -04:56 which break coverage comparisons.
                    if hasattr(base_tz, "localize"):
                        start_dt = base_tz.localize(start_dt)
                        end_dt = base_tz.localize(end_dt)
                    else:
                        start_dt = start_dt.replace(tzinfo=base_tz)
                        end_dt = end_dt.replace(tzinfo=base_tz)
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
        metadata["has_ohlc"] = self._frame_has_ohlc_columns(frame)

        last_real_ts = None
        if frame is not None and not frame.empty and "missing" in frame.columns:
            placeholder_flags = frame["missing"].fillna(False).astype(bool)
            metadata["placeholders"] = int(placeholder_flags.sum())
            metadata["tail_placeholder"] = bool(placeholder_flags.iloc[-1])
            if placeholder_flags.shape[0] and bool(placeholder_flags.all()):
                metadata["empty_fetch"] = True
            try:
                real_rows = frame.loc[~placeholder_flags]
                if not real_rows.empty:
                    last_real_ts = pd.to_datetime(real_rows.index).max()
            except Exception:
                last_real_ts = None
        else:
            metadata["placeholders"] = 0
            metadata["tail_placeholder"] = False
            if not metadata["empty_fetch"]:
                metadata["empty_fetch"] = False

        if last_real_ts is not None:
            metadata["last_real_ts"] = self._normalize_default_timezone(last_real_ts.to_pydatetime())
        else:
            metadata["last_real_ts"] = None

        # Only treat an option as "permanently missing" when we have fetched through its expiration
        # date and still have no real rows. An empty fetch for a single day/minute window is common
        # for illiquid contracts and must NOT disable future refetches (it breaks strategies that
        # rely on sparse last-trade prints over multi-year windows).
        if metadata.get("empty_fetch") and getattr(asset, "asset_type", None) == Asset.AssetType.OPTION:
            try:
                expiration = getattr(asset, "expiration", None)
                if (
                    isinstance(expiration, date)
                    and normalized_data_end is not None
                    and hasattr(normalized_data_end, "date")
                    and normalized_data_end.date() >= expiration
                ):
                    metadata["negative_cache"] = True
            except Exception:
                pass

            # Backtesting often probes many expirations/strikes repeatedly while an option has no
            # historical coverage yet (472 / placeholder-only). Avoid day-after-day refetch storms
            # by caching "empty" results for a short TTL, while still allowing refetches later in
            # the contract's lifetime (we do NOT treat this as permanently missing unless we have
            # fetched through expiration above).
            if not metadata.get("negative_cache"):
                try:
                    if normalized_data_end is not None:
                        metadata["empty_fetch_until"] = normalized_data_end + timedelta(days=7)
                except Exception:
                    pass

        # Preserve runtime cache flags that should not be reset by metadata refreshes
        # (e.g., day-mode metadata rebuilds during _update_pandas_data).
        for flag_key in (
            "prefetch_complete",
            "ffilled",
            "sidecar_loaded",
            "negative_cache",
            "empty_fetch_until",
            "quotes_missing_permanent",
            "tail_missing_permanent",
        ):
            if flag_key in previous_meta:
                metadata[flag_key] = previous_meta.get(flag_key)

        if getattr(asset, "asset_type", None) == Asset.AssetType.OPTION:
            metadata["expiration"] = asset.expiration

        if metadata.get("expiration") != previous_meta.get("expiration"):
            metadata["expiration_notice"] = False
        else:
            metadata["expiration_notice"] = previous_meta.get("expiration_notice", False)

        self._dataset_metadata[key] = metadata
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "[THETA][DEBUG][METADATA][WRITE] key=%s ts=%s start=%s end=%s data_start=%s data_end=%s rows=%s placeholders=%s has_quotes=%s has_ohlc=%s",
                key,
                ts_unit,
                metadata.get("start"),
                metadata.get("end"),
                metadata.get("data_start"),
                metadata.get("data_end"),
                metadata.get("rows"),
                metadata.get("placeholders"),
                metadata.get("has_quotes"),
                metadata.get("has_ohlc"),
            )

    def _frame_has_quote_columns(self, frame: Optional[pd.DataFrame]) -> bool:
        if frame is None or frame.empty:
            return False
        quote_markers = {"bid", "ask", "bid_size", "ask_size", "last_trade_time", "last_bid_time", "last_ask_time"}
        return any(col in frame.columns for col in quote_markers)

    def _frame_has_ohlc_columns(self, frame: Optional[pd.DataFrame]) -> bool:
        if frame is None or frame.empty:
            return False
        required = {"open", "high", "low", "close"}
        if not required.issubset(set(frame.columns)):
            return False

        # Some internal repair paths can create OHLC columns filled with nulls; treat that as "no OHLC".
        try:
            for col in required:
                series = frame.get(col)
                if series is None:
                    continue
                if pd.to_numeric(series, errors="coerce").notna().any():
                    return True
        except Exception:
            for col in required:
                series = frame.get(col)
                if series is None:
                    continue
                try:
                    if series.notna().any():
                        return True
                except Exception:
                    continue
        return False

    def _finalize_day_frame(
        self,
        pandas_df: Optional[pd.DataFrame],
        current_dt: datetime,
        requested_length: int,
        timeshift: Optional[timedelta],
        asset: Optional[Asset] = None,  # DEBUG-LOG: Added for logging
    ) -> Optional[pd.DataFrame]:
        debug_enabled = logger.isEnabledFor(logging.DEBUG)

        # DEBUG-LOG: Method entry with full parameter context
        if debug_enabled:
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][ENTRY] asset=%s current_dt=%s requested_length=%s timeshift=%s input_shape=%s input_columns=%s input_index_type=%s input_has_tz=%s input_index_sample=%s",
                getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                current_dt,
                requested_length,
                timeshift,
                pandas_df.shape if pandas_df is not None else "NONE",
                list(pandas_df.columns) if pandas_df is not None else "NONE",
                type(pandas_df.index).__name__ if pandas_df is not None else "NONE",
                getattr(pandas_df.index, "tz", None) if pandas_df is not None else "NONE",
                list(pandas_df.index[:5]) if pandas_df is not None and len(pandas_df) > 0 else "EMPTY",
            )

        if pandas_df is None or pandas_df.empty:
            # DEBUG-LOG: Early return for empty input
            if debug_enabled:
                logger.debug(
                    "[THETA][DEBUG][PANDAS][FINALIZE][EMPTY_INPUT] asset=%s returning_none_or_empty=True",
                    getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                )
            return pandas_df

        frame = pandas_df.copy()
        if "datetime" in frame.columns:
            frame = frame.set_index("datetime")

        frame.index = pd.to_datetime(frame.index)

        # DEBUG-LOG: Timezone state before localization
        if debug_enabled:
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][TZ_CHECK] asset=%s frame_index_tz=%s target_tz=%s needs_localization=%s frame_shape=%s",
                getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                frame.index.tz,
                self.tzinfo,
                frame.index.tz is None,
                frame.shape,
            )

        if frame.index.tz is None:
            frame.index = frame.index.tz_localize(pytz.UTC)
        localized_index = frame.index.tz_convert(self.tzinfo)
        normalized_for_cutoff = localized_index.normalize()

        # DEBUG-LOG: After localization
        if debug_enabled:
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][LOCALIZED] asset=%s localized_index_tz=%s localized_sample=%s",
                getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                localized_index.tz,
                list(localized_index[:3]) if len(localized_index) > 0 else "EMPTY",
            )

        cutoff = self.to_default_timezone(current_dt).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        cutoff_mask = normalized_for_cutoff <= cutoff

        # DEBUG-LOG: Cutoff filtering state
        if debug_enabled:
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][CUTOFF] asset=%s cutoff=%s cutoff_mask_true=%s cutoff_mask_false=%s",
                getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                cutoff,
                int(cutoff_mask.sum()) if hasattr(cutoff_mask, "sum") else "N/A",
                int((~cutoff_mask).sum()) if hasattr(cutoff_mask, "sum") else "N/A",
            )

        if timeshift and not isinstance(timeshift, int):
            cutoff_mask &= normalized_for_cutoff <= (cutoff - timeshift)
            # DEBUG-LOG: After timeshift adjustment
            if debug_enabled:
                logger.debug(
                    "[THETA][DEBUG][PANDAS][FINALIZE][TIMESHIFT_ADJUSTED] asset=%s timeshift=%s new_cutoff=%s cutoff_mask_true=%s",
                    getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                    timeshift,
                    cutoff - timeshift,
                    int(cutoff_mask.sum()) if hasattr(cutoff_mask, "sum") else "N/A",
                )

        frame = frame.loc[cutoff_mask]
        localized_index = localized_index[cutoff_mask]
        normalized_for_cutoff = normalized_for_cutoff[cutoff_mask]

        # DEBUG-LOG: After cutoff filtering
        if debug_enabled:
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][AFTER_CUTOFF] asset=%s shape=%s index_range=%s",
                getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                frame.shape,
                (localized_index[0], localized_index[-1]) if len(localized_index) > 0 else ("EMPTY", "EMPTY"),
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
        if debug_enabled:
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][NORMALIZED_INDEX] asset=%s shape=%s index_sample=%s",
                getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                frame.shape,
                list(normalized_index[:3]) if len(normalized_index) > 0 else "EMPTY",
            )

        expected_last_dt = self.to_default_timezone(current_dt).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        expected_last_dt_utc = expected_last_dt.astimezone(pytz.UTC)
        target_index = pd.date_range(end=expected_last_dt_utc, periods=requested_length, freq="D", tz=pytz.UTC).tz_convert(self.tzinfo)

        # DEBUG-LOG: Target index details
        if debug_enabled:
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][TARGET_INDEX] asset=%s target_length=%s target_range=%s",
                getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                len(target_index),
                (target_index[0], target_index[-1]) if len(target_index) > 0 else ("EMPTY", "EMPTY"),
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
        if debug_enabled:
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][PLACEHOLDER_MASK] asset=%s placeholder_true=%s placeholder_false=%s value_columns=%s",
                getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                int(placeholder_mask.sum()) if hasattr(placeholder_mask, "sum") else "N/A",
                int((~placeholder_mask).sum()) if hasattr(placeholder_mask, "sum") else "N/A",
                value_columns,
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
        if debug_enabled:
            try:
                missing_count = int(frame["missing"].sum())
                logger.debug(
                    "[THETA][DEBUG][PANDAS][FINALIZE][MISSING_FINAL] asset=%s missing_true=%s missing_false=%s total_rows=%s",
                    getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                    missing_count,
                    len(frame) - missing_count,
                    len(frame),
                )
            except Exception as e:
                logger.debug(
                    "[THETA][DEBUG][PANDAS][FINALIZE][MISSING_FINAL] asset=%s error=%s",
                    getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                    str(e),
                )

        # DEBUG-LOG: Return value
        if debug_enabled:
            logger.debug(
                "[THETA][DEBUG][PANDAS][FINALIZE][RETURN] asset=%s shape=%s columns=%s index_range=%s",
                getattr(asset, "symbol", asset) if asset else "UNKNOWN",
                frame.shape,
                list(frame.columns),
                (frame.index[0], frame.index[-1]) if len(frame) > 0 else ("EMPTY", "EMPTY"),
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

    def _update_pandas_data(
        self,
        asset,
        quote,
        length,
        timestep,
        start_dt=None,
        require_quote_data: bool = False,
        require_ohlc_data: bool = True,
        snapshot_only: bool = False,
    ):
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
        asset_separated = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(asset_separated, tuple):
            asset_separated, quote_asset = asset_separated

        asset_type_value = str(getattr(asset_separated, "asset_type", "")).lower()
        symbol_upper = str(getattr(asset_separated, "symbol", "") or "").upper()
        index_symbols = {
            "SPX", "SPXW",
            "RUT", "RUTW",
            "VIX", "VIXW",
            "NDX", "NDXP",
            "XSP", "DJX", "OEX", "XEO",
        }
        is_option_asset = asset_type_value == "option"
        # Index symbols are represented as plain Assets (asset_type="stock") in many strategies,
        # so we key off the symbol list. Do NOT treat option contracts on an index (e.g., SPX 0DTE)
        # as "index assets" for cache/coverage logic.
        is_index_asset = asset_type_value == "index" or (not is_option_asset and symbol_upper in index_symbols)

        if asset_separated.asset_type == "option":
            expiry = asset_separated.expiration
            if self.is_weekend(expiry):
                logger.info(f"\nSKIP: Expiry {expiry} date is a weekend, no contract exists: {asset_separated}")
                return None

        if is_option_asset and asset_separated in self._negative_option_cache:
            logger.info(
                "[THETA][CACHE][NEGATIVE] asset=%s/%s (%s) marked permanently missing; skipping refetch.",
                asset_separated,
                quote_asset,
                timestep,
            )
            return None

        # Get the start datetime and timestep unit.
        #
        # PERFORMANCE: For point-in-time option quote/price checks (length≈1–5 bars) a 5-day buffer is
        # overkill and can force the downloader to include prior trading days. For intraday option
        # quotes/prices we only need the current trading day for mark/greeks calculations; broader
        # history pulls can still request a larger `length` (or provide an explicit start_dt).
        effective_start_buffer = START_BUFFER
        try:
            _, ts_unit_preview = self.convert_timestep_str_to_timedelta(timestep)
        except Exception:
            ts_unit_preview = None
        if (
            getattr(asset_separated, "asset_type", None) == "option"
            and start_dt is not None
            and isinstance(length, int)
            and length <= 5
            and ts_unit_preview in {"minute", "hour", "day"}
        ):
            # Quote probes (and snapshot-only fetches) should stay as tight as possible for performance.
            # Trade-only last-price probes, however, need a small lookback so early-session requests
            # (e.g., 09:30) can still see a prior-day print when the current day has no trades yet.
            effective_start_buffer = timedelta(0) if (require_quote_data or snapshot_only) else timedelta(days=1)

        start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(
            length, timestep, start_dt, start_buffer=effective_start_buffer
        )
        if ts_unit == "day":
            self._effective_day_mode = True
        current_dt = self.get_datetime()

        requested_length = max(length, 1)
        requested_start = self._normalize_default_timezone(start_datetime)
        window_start = self._normalize_default_timezone(self.datetime_start - START_BUFFER)
        if requested_start is None:
            requested_start = window_start
        elif asset_separated.asset_type != "option" and window_start is not None and window_start < requested_start:
            # For non-options, prefetch the full backtest window once for performance.
            requested_start = window_start
        start_threshold = requested_start + effective_start_buffer if requested_start is not None else None
        start_for_fetch = requested_start or start_datetime
        # For non-options we target full backtest coverage on first fetch; reuse thereafter.
        # Options are too numerous/expensive to prefetch to backtest end, so only fetch up to the current sim time.
        end_anchor = current_dt
        if start_dt is not None:
            try:
                end_anchor = self._normalize_default_timezone(start_dt) or current_dt
            except Exception:
                end_anchor = current_dt

        # Index OHLC is a small surface area (vs option chains) and strategies often request it
        # repeatedly throughout an intraday run (indicators, settlement, risk checks). If we only
        # fetch up to the current simulation timestamp, we end up incrementally extending the same
        # cache thousands of times (O(N^2) merges over a year window).
        #
        # Prefetching through the backtest end keeps behavior deterministic while allowing the
        # Data() accessors to slice by `dt` (no lookahead as long as consumers pass `dt`).
        if (
            not snapshot_only
            and require_ohlc_data
            and asset_type_value == "index"
            and asset_separated.asset_type != "option"
            and self.datetime_end is not None
        ):
            try:
                normalized_end = self._normalize_default_timezone(self.datetime_end)
                if normalized_end is not None:
                    end_anchor = normalized_end
            except Exception:
                pass
        if end_anchor is not None and self.datetime_end is not None:
            try:
                normalized_end = self._normalize_default_timezone(self.datetime_end)
                if normalized_end is not None and end_anchor > normalized_end:
                    end_anchor = normalized_end
            except Exception:
                pass

            if ts_unit == "day":
                try:
                    end_date_source = end_anchor if asset_separated.asset_type == "option" else self.datetime_end
                    end_date = end_date_source.date() if hasattr(end_date_source, "date") else end_date_source
                except Exception:
                    end_date = end_anchor if asset_separated.asset_type == "option" else self.datetime_end
                end_requirement = datetime.combine(end_date, datetime.max.time())
                try:
                    end_requirement = self.tzinfo.localize(end_requirement)
                except Exception:
                    end_requirement = end_requirement.replace(tzinfo=getattr(self, "tzinfo", None))
                end_requirement = (
                    self.to_default_timezone(end_requirement) if hasattr(self, "to_default_timezone") else end_requirement
                )
            else:
                end_requirement = end_anchor
                # PERFORMANCE + RELIABILITY: For point-in-time intraday option quote/price checks,
                # fetching only up to the current simulation timestamp can cause repeated "stale/refetch"
                # loops (especially pre-market when no bars exist yet). Prefetch through the session close
                # for the current trading day so the same contract doesn't refetch every minute.
                if (
                    start_dt is not None
                    and isinstance(length, int)
                    and length <= 5
                    and ts_unit in {"minute", "hour"}
                    and not snapshot_only
                    and (
                        asset_type_value == "option"
                        or (asset_type_value in {"stock", "index"} and not require_quote_data)
                    )
                ):
                    # NOTE: This is perf-critical for option scanners, but it's also correctness-critical
                    # for stock/index intraday "last trade" probes:
                    #
                    # Without aligning to the trading-session close, we can end up repeatedly reusing
                    # stale prior-day closes for intraday timestamps when the cache coverage heuristic
                    # only checks small `length` windows (observed in SPX Copy2/Copy3 cold-cache runs).
                    try:
                        # PERF: `get_trading_days()` is expensive (calendar lookup + schedule build).
                        # Point-in-time option quote/price checks can call `_update_pandas_data()` tens
                        # of thousands of times in a single backtest, so cache the per-session close
                        # datetime by (market, date).
                        from lumibot.tools.helpers import get_trading_days

                        market = os.environ.get("BACKTESTING_MARKET", "NYSE")
                        # NOTE: Do not reuse `_session_close_cache` here.
                        # The small-window "align to session close" logic above intentionally caches
                        # the *forward* session close for a given date (which may be AFTER the current
                        # `end_requirement` timestamp). For the end-coverage clamp we need the *last*
                        # session close at or before the end timestamp, so cache it separately to
                        # avoid collisions that prevent clamping on weekends/holidays.
                        close_cache = getattr(self, "_session_close_cache_last", None)
                        if close_cache is None:
                            close_cache = {}
                            self._session_close_cache_last = close_cache

                        cache_date = end_requirement.date() if hasattr(end_requirement, "date") else end_requirement
                        cache_key = (market, cache_date)
                        cached_close = close_cache.get(cache_key)
                        if cached_close is None and cache_key not in close_cache:
                            schedule = get_trading_days(
                                market=market,
                                start_date=end_requirement,
                                end_date=end_requirement + timedelta(days=2),
                                tzinfo=self.tzinfo,
                            )
                            cached_close = None
                            if not schedule.empty:
                                cached_close = schedule.iloc[0]["market_close"]
                            close_cache[cache_key] = cached_close

                        if cached_close is not None and cached_close > end_requirement:
                            end_requirement = cached_close
                            # Clamp to the backtest end so we don't fetch beyond the simulation window.
                            try:
                                normalized_end = self._normalize_default_timezone(self.datetime_end)
                                if normalized_end is not None and end_requirement > normalized_end:
                                    end_requirement = normalized_end
                            except Exception:
                                pass
                    except Exception:
                        logger.debug(
                            "[THETA][DEBUG][END_REQUIREMENT] failed to align intraday option end_requirement",
                            exc_info=True,
                        )
                # CORRECTNESS + PERFORMANCE: For index intraday data in Theta backtests, the provider is
                # regular-session (RTH) bounded (e.g. ~09:30–16:00 ET for SPX, with early closes on
                # holidays) and does not provide bars through 23:59/UTC-midnight.
                #
                # If we require coverage through the backtest end bound (often 23:59 or 18:59 ET
                # depending on how end dates are serialized), the cache can become impossible to satisfy
                # and we can enter a perpetual STALE→REFRESH loop (observed in:
                # - SPX0DTEHybridStrangle prod runs dominated by v3/index/history/ohlc
                # - acceptance SPX short straddle runs stuck on SPXW minute prefetch).
                #
                # Clamp the intraday end requirement down to the *last trading session close* at or
                # before the end requirement datetime so "covered through close" is considered complete
                # even when the backtest end falls on a weekend/holiday.
                if (
                    is_index_asset
                    and not snapshot_only
                    and ts_unit in {"minute", "hour"}
                    and end_requirement is not None
                ):
                    try:
                        from lumibot.tools.helpers import get_trading_days

                        market = os.environ.get("BACKTESTING_MARKET", "NYSE")
                        close_cache = getattr(self, "_session_close_cache", None)
                        if close_cache is None:
                            close_cache = {}
                            self._session_close_cache = close_cache

                        cache_date = end_requirement.date()
                        cache_key = (market, cache_date)
                        if cache_key in close_cache:
                            cached_close = close_cache.get(cache_key)
                        else:
                            # Include a small lookback window so holidays/weekends resolve to the prior
                            # session close (e.g., 2025-12-25 holiday should clamp to 2025-12-24 early close).
                            schedule = get_trading_days(
                                market=market,
                                start_date=end_requirement - timedelta(days=7),
                                end_date=end_requirement + timedelta(days=2),
                                tzinfo=self.tzinfo,
                            )
                            cached_close = None
                            if not schedule.empty:
                                closes = schedule["market_close"]
                                candidates = closes[closes <= end_requirement]
                                if not candidates.empty:
                                    cached_close = candidates.iloc[-1]
                                else:
                                    cached_close = closes.iloc[0]
                            close_cache[cache_key] = cached_close

                        if cached_close is not None and end_requirement > cached_close:
                            logger.debug(
                                "[THETA][DEBUG][END_REQUIREMENT] clamping index intraday end_requirement to session close: %s -> %s",
                                end_requirement,
                                cached_close,
                            )
                            end_requirement = cached_close
                    except Exception:
                        logger.debug(
                            "[THETA][DEBUG][END_REQUIREMENT] failed to clamp index end_requirement to session close",
                            exc_info=True,
                        )
        else:
            end_requirement = (
                end_anchor
                if asset_separated.asset_type == "option"
                else self._normalize_default_timezone(self.datetime_end)
            )
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
            # PERF + CORRECTNESS: Normalize `prefetch_complete` after rebuilding metadata so stale
            # sidecars can't cause per-bar STALE/REFRESH loops.
            try:
                if existing_meta is not None:
                    existing_meta["prefetch_complete"] = self._compute_prefetch_complete(
                        existing_meta,
                        requested_start=requested_start,
                        effective_start_buffer=effective_start_buffer,
                        end_requirement=end_requirement,
                        ts_unit=ts_unit,
                        requested_length=requested_length,
                    )
                    self._dataset_metadata[canonical_key] = existing_meta
            except Exception:
                logger.debug("[THETA][DEBUG][PREFETCH_COMPLETE] failed to recompute after day metadata rebuild", exc_info=True)
            if logger.isEnabledFor(logging.DEBUG):
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
            # Fast-reuse must respect the *type* of data requested: quote-only caches should not
            # satisfy OHLC-only requests (trade-only APIs like get_last_price / get_historical_prices).
            cached_meta = self._dataset_metadata.get(canonical_key) or {}
            cached_has_quotes = bool(cached_meta.get("has_quotes")) or self._frame_has_quote_columns(existing_data.df)
            meta_has_ohlc = cached_meta.get("has_ohlc")
            if meta_has_ohlc is None:
                cached_has_ohlc = self._frame_has_ohlc_columns(existing_data.df)
            elif bool(meta_has_ohlc):
                cached_has_ohlc = self._frame_has_ohlc_columns(existing_data.df)
            else:
                cached_has_ohlc = False
            reuse_ok = True
            if require_quote_data and not cached_has_quotes:
                reuse_ok = False
            if require_ohlc_data and not cached_has_ohlc:
                reuse_ok = False

            # PERF: avoid per-call pandas index conversions. Prefer cached metadata or Data's
            # datetime_start/end (constant-time) rather than `pd.to_datetime(df.index)` each bar.
            coverage_start = cached_meta.get("data_start") or cached_meta.get("start")
            coverage_end = cached_meta.get("data_end") or cached_meta.get("end")

            if coverage_start is None or coverage_end is None:
                try:
                    if coverage_start is None:
                        coverage_start = self._normalize_default_timezone(getattr(existing_data, "datetime_start", None))
                    if coverage_end is None:
                        coverage_end = self._normalize_default_timezone(getattr(existing_data, "datetime_end", None))
                except Exception:
                    pass

            if coverage_start is None or coverage_end is None:
                df_idx = existing_data.df.index
                if len(df_idx):
                    idx = pd.to_datetime(df_idx)
                    if len(idx):
                        coverage_start = self._normalize_default_timezone(idx.min())
                        coverage_end = self._normalize_default_timezone(idx.max())

            if coverage_start is not None and coverage_end is not None:
                # If this dataset lacked sidecar metadata (common for older caches), persist the
                # computed bounds once so subsequent reuse checks are O(1).
                if not cached_meta.get("data_start") or not cached_meta.get("data_end"):
                    meta = dict(cached_meta)
                    meta.setdefault("timestep", ts_unit)
                    meta.setdefault("has_quotes", cached_has_quotes)
                    meta.setdefault("has_ohlc", cached_has_ohlc)
                    meta["data_start"] = meta.get("data_start") or coverage_start
                    meta["data_end"] = meta.get("data_end") or coverage_end
                    meta["data_rows"] = meta.get("data_rows") or len(existing_data.df)
                    meta["start"] = meta.get("start") or coverage_start
                    meta["end"] = meta.get("end") or coverage_end
                    meta["rows"] = meta.get("rows") or len(existing_data.df)
                    self._dataset_metadata[canonical_key] = meta
                    cached_meta = meta

            if coverage_end is not None and end_requirement is not None:
                try:
                    coverage_end_cmp = coverage_end.date()
                    end_requirement_cmp = end_requirement.date()
                except Exception:
                    coverage_end_cmp = coverage_end
                    end_requirement_cmp = end_requirement
            else:
                coverage_end_cmp = coverage_end.date() if coverage_end is not None else None
                end_requirement_cmp = end_requirement.date() if end_requirement is not None else None

            end_ok = (
                coverage_end_cmp is not None
                and end_requirement_cmp is not None
                and coverage_end_cmp >= end_requirement_cmp
            )

            if reuse_ok and (
                coverage_start is not None
                and requested_start is not None
                and coverage_start <= requested_start + effective_start_buffer
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
                            logger.debug(
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
                        logger.debug(
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
        existing_quotes_missing = bool(existing_meta.get("quotes_missing_permanent")) if existing_meta else False
        existing_has_ohlc = bool(existing_meta.get("has_ohlc", True)) if existing_meta else True

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

            # CORRECTNESS: Some older sidecar metadata (or externally-warmed caches) can carry an
            # incorrect `prefetch_complete=True` even when `existing_end` no longer meets the
            # current `end_requirement` (e.g., cache is slightly behind the requested backtest end).
            # Normalize it here so we don't emit thousands of per-bar STALE logs.
            try:
                existing_meta["prefetch_complete"] = self._compute_prefetch_complete(
                    existing_meta,
                    requested_start=requested_start,
                    effective_start_buffer=effective_start_buffer,
                    end_requirement=end_requirement,
                    ts_unit=ts_unit,
                    requested_length=requested_length,
                )
                self._dataset_metadata[canonical_key] = existing_meta
                legacy_meta = self._dataset_metadata.get(legacy_key)
                if legacy_meta is not None:
                    legacy_meta.update(existing_meta)
                    self._dataset_metadata[legacy_key] = legacy_meta
            except Exception:
                logger.debug("[THETA][DEBUG][PREFETCH_COMPLETE] failed to normalize before validation", exc_info=True)

            # DEBUG-LOG: Cache validation entry
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][CACHE_VALIDATION][ENTRY] asset=%s timestep=%s | "
                    "REQUESTED: start=%s start_threshold=%s end_requirement=%s length=%d | "
                    "EXISTING: start=%s end=%s rows=%d",
                    asset_separated.symbol if hasattr(asset_separated, "symbol") else str(asset_separated),
                    ts_unit,
                    requested_start,
                    start_threshold,
                    end_requirement,
                    requested_length,
                    existing_start,
                    existing_end,
                    existing_rows,
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
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][START_VALIDATION] asset=%s | "
                    "start_ok=%s | "
                    "existing_start=%s start_threshold=%s | "
                    "reasoning=%s",
                    asset_separated.symbol if hasattr(asset_separated, "symbol") else str(asset_separated),
                    start_ok,
                    existing_start,
                    start_threshold,
                    "existing_start is not None (threshold check removed - see NOTE above)" if start_ok else "existing_start is None",
                )

            tail_placeholder = existing_meta.get("tail_placeholder", False)
            tail_missing_permanent = bool(existing_meta.get("tail_missing_permanent")) if existing_meta else False
            end_ok = True

            # DEBUG-LOG: End validation entry
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][END_VALIDATION][ENTRY] asset=%s | "
                    "end_requirement=%s existing_end=%s tail_placeholder=%s",
                    asset_separated.symbol if hasattr(asset_separated, "symbol") else str(asset_separated),
                    end_requirement,
                    existing_end,
                    tail_placeholder,
                )

            if end_requirement is not None:
                if existing_end is None:
                    end_ok = False
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][END_VALIDATION][RESULT] asset=%s | "
                            "end_ok=FALSE | reason=existing_end_is_None",
                            asset_separated.symbol if hasattr(asset_separated, "symbol") else str(asset_separated),
                        )
                else:
                    # Cache coverage end checks:
                    #
                    # - Day bars: compare *dates* (Theta daily bars are often timestamped at 00:00 UTC, which can
                    #   appear as the prior-day evening in ET; date-only comparisons avoid false "stale" flags).
                    # - Minute/hour/second: compare full timestamps. A date-only comparison (or multi-day tolerance)
                    #   can silently reuse stale intraday datasets across days, breaking determinism and producing
                    #   incorrect trades (observed in SPX Copy2/Copy3 cold-cache runs: intraday prices reused the
                    #   prior-day close for multiple days).
                    #
                    # IMPORTANT: Convert to the same timezone before comparing to avoid UTC/local mismatch.
                    if hasattr(existing_end, 'tzinfo') and hasattr(end_requirement, 'tzinfo'):
                        target_tz = end_requirement.tzinfo
                        if target_tz is not None and existing_end.tzinfo is not None:
                            existing_end_local = existing_end.astimezone(target_tz)
                        else:
                            existing_end_local = existing_end
                    else:
                        existing_end_local = existing_end
                    if ts_unit == "day":
                        existing_end_cmp = (
                            existing_end_local.date() if hasattr(existing_end_local, "date") else existing_end_local
                        )
                        end_requirement_cmp = (
                            end_requirement.date() if hasattr(end_requirement, "date") else end_requirement
                        )
                        end_tolerance = timedelta(0)
                    else:
                        existing_end_cmp = existing_end_local
                        end_requirement_cmp = end_requirement
                        end_tolerance = timedelta(0)

                    if existing_end_cmp >= end_requirement_cmp - end_tolerance:
                        end_ok = True
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][END_VALIDATION][RESULT] asset=%s | "
                                "end_ok=TRUE | reason=existing_end_meets_requirement | "
                                "existing_end=%s end_requirement=%s tolerance=%s ts_unit=%s",
                                asset_separated.symbol if hasattr(asset_separated, "symbol") else str(asset_separated),
                                existing_end,
                                end_requirement,
                                end_tolerance,
                                ts_unit,
                            )
                    else:
                        # existing_end is still behind the required window
                        end_ok = False
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                "[DEBUG][BACKTEST][THETA][DEBUG][PANDAS][END_VALIDATION][RESULT] asset=%s | "
                                "end_ok=FALSE | reason=existing_end_less_than_requirement | "
                                "existing_end=%s end_requirement=%s ts_unit=%s",
                                asset_separated.symbol if hasattr(asset_separated, "symbol") else str(asset_separated),
                                existing_end,
                                end_requirement,
                                ts_unit,
                            )

            # PERF: If the cache metadata says the tail is permanently missing (placeholder coverage through
            # the requested end), treat the end check as satisfied. Without this, backtests can thrash on
            # every bar (STALE → REFRESH loops) trying to heal placeholder trading days that are expected
            # to remain unavailable for this run.
            if not end_ok and tail_missing_permanent:
                end_ok = True
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "[THETA][DEBUG][END_VALIDATION] asset=%s | end_ok forced TRUE due to tail_missing_permanent",
                        asset_separated.symbol if hasattr(asset_separated, "symbol") else str(asset_separated),
                    )

            if (
                require_quote_data
                and existing_meta
                and existing_meta.get("prefetch_complete")
                and not existing_has_quotes
            ):
                if not existing_quotes_missing:
                    existing_meta["quotes_missing_permanent"] = True
                    self._dataset_metadata[canonical_key] = existing_meta
                    existing_quotes_missing = True
                    logger.info(
                        "[THETA][CACHE][QUOTES_MISSING] asset=%s/%s (%s) no quote columns after fetch; "
                        "treating as permanent for this run.",
                        asset_separated,
                        quote_asset,
                        ts_unit,
                    )

            quotes_ok = (
                (not require_quote_data)
                or existing_has_quotes
                or existing_quotes_missing
            )

            cache_covers = (
                start_ok
                and existing_rows >= requested_length
                and end_ok
                and quotes_ok
                and (not require_ohlc_data or existing_has_ohlc)
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
                # Mark as forward-filled/complete for reuse semantics. Tests and downstream cache
                # logic expect these flags to stay truthy once a dataset is considered usable.
                if existing_meta is not None:
                    if not existing_meta.get("ffilled"):
                        existing_meta["ffilled"] = True
                    existing_meta["prefetch_complete"] = True
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
            if require_quote_data and not existing_has_quotes and not existing_quotes_missing:
                reasons.append("quotes")
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

            # Backtesting can legitimately see placeholder-only windows for illiquid option contracts.
            # When we detect a recent empty fetch, avoid repeatedly re-submitting identical requests
            # on every subsequent bar/day (it can dominate runtime for long windows).
            if is_option_asset and existing_meta is not None and existing_meta.get("empty_fetch") and not existing_meta.get("negative_cache"):
                try:
                    empty_until = existing_meta.get("empty_fetch_until")
                    normalized_current_dt = self._normalize_default_timezone(current_dt)
                    if empty_until is not None and normalized_current_dt is not None and normalized_current_dt < empty_until:
                        logger.info(
                            "[THETA][CACHE][EMPTY_TTL] asset=%s/%s (%s) empty fetch cached; skipping refetch until %s (dt=%s)",
                            asset_separated,
                            quote_asset,
                            ts_unit,
                            empty_until,
                            normalized_current_dt,
                        )
                        return None
                except Exception:
                    pass
            if existing_meta is not None and existing_meta.get("prefetch_complete"):
                if is_option_asset and existing_meta.get("negative_cache"):
                    logger.info(
                        "[THETA][CACHE][NEGATIVE] asset=%s/%s (%s) placeholder-only cache; skipping refetch. existing_end=%s target_end=%s",
                        asset_separated,
                        quote_asset,
                        ts_unit,
                        existing_end,
                        end_requirement,
                    )
                    return None
                # If the cached dataset already covers the requirement, treat it as reusable and
                # avoid thrashing (STALE → REFRESH loops). This is especially important for
                # option quote datasets in cold-local/warm-S3 production runs.
                quotes_ok = (not require_quote_data) or existing_has_quotes or existing_quotes_missing
                if (
                    start_ok
                    and end_ok
                    and existing_rows >= requested_length
                    and quotes_ok
                    and (not require_ohlc_data or existing_has_ohlc)
                ):
                    return None
                if is_index_asset and end_ok and existing_rows >= requested_length:
                    logger.info(
                        "[THETA][CACHE][REUSE] asset=%s/%s (%s) coverage meets requirement; skipping refetch. existing_end=%s target_end=%s",
                        asset_separated,
                        quote_asset,
                        ts_unit,
                        existing_end,
                        end_requirement,
                    )
                    return None
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
                    and (coverage_start - start_for_fetch) < effective_start_buffer
                )
                if start_buffer_ok and not end_missing:
                    if require_quote_data and not existing_has_quotes:
                        pass
                    elif require_ohlc_data and not existing_has_ohlc:
                        pass
                    else:
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
                if (data_start_datetime - start_datetime) < effective_start_buffer:
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
        df_quote = None
        wants_ohlc = bool(require_ohlc_data)
        use_quotes_flag = bool(getattr(self, "_use_quote_data", False)) or bool(require_quote_data)
        wants_quotes = bool(use_quotes_flag) and ts_unit in {"minute", "hour", "second"} and bool(require_quote_data)

        # Memory: for non-option assets, avoid pulling/returning the full on-disk cache frame.
        #
        # Production backtests can reuse a cache namespace that already contains multi-year intraday
        # history for a symbol (e.g. NVDA minute bars from a prior 2013→2025 run). Returning that
        # entire frame to the backtester can exceed the ECS task memory limit, especially on refresh
        # boundaries near the end of the requested window (observed as BotManager ERROR_CODE_CRASH
        # with no Python traceback / logs ending abruptly).
        #
        # Options are handled differently: placeholder rows / negative caching are useful to avoid
        # re-fetch storms, so we continue to preserve full-history semantics there.
        #
        # Day bars are small on disk even for multi-year backtests, so preserving full history is
        # safe and keeps cache/coverage behavior consistent (and avoids surprising truncations when
        # refreshing cached daily data).
        preserve_full_history = bool(is_option_asset or ts_unit == "day")

        # -------------------------------------------------------------------------------------
        # NDX UNDERLYING PROXY (ThetaData coverage gap)
        # -------------------------------------------------------------------------------------
        # ThetaData support confirmed they do not provide NDX index/underlying history (only NDX options).
        # In practice, /v3/index/history/* for NDX can return placeholder all-zero OHLC or NO_DATA, which
        # makes backtests thrash (empty dataset → repeated refetch attempts).
        #
        # Strategy code must continue to trade NDX options, but the platform needs a usable underlying
        # price series for `Asset("NDX", asset_type=INDEX)` (signals, moneyness, valuation, etc).
        #
        # Solution: proxy NDX underlying bars/quotes via a liquid Theta-covered instrument (QQQ),
        # scaled into NDX "points" units. This is explicit (logged once per run) and scoped to the
        # ThetaData backtesting path so other providers remain unaffected.
        ndx_proxy_symbol: Optional[str] = None
        ndx_proxy_factor: Optional[float] = None
        # IMPORTANT: Do not infer "index-ness" from the symbol alone.
        # `Asset("NDX")` defaults to a stock by design; only explicit `asset_type=INDEX` should be proxied.
        if (
            not is_option_asset
            and getattr(asset_separated, "asset_type", None) == Asset.AssetType.INDEX
        ):
            symbol_upper = str(getattr(asset_separated, "symbol", "") or "").upper()
            if symbol_upper in {"NDX", "NDXP"}:
                ndx_proxy_symbol = "QQQ"
                # Heuristic: NDX is an index level while QQQ is an ETF price. The ratio moves slowly
                # over time (fees/dividend timing), but is stable enough to serve as a fast proxy.
                # If we later add a low-cost daily calibration path from NDX options EOD, it can
                # override this constant factor without changing call sites.
                ndx_proxy_factor = 41.0

        def _log_ndx_proxy_once() -> None:
            if not ndx_proxy_symbol:
                return
            notices = getattr(self, "_thetadata_index_proxy_notices", None)
            if notices is None:
                notices = set()
                setattr(self, "_thetadata_index_proxy_notices", notices)
            key = f"{getattr(asset_separated, 'symbol', asset_separated)}->{ndx_proxy_symbol}"
            if key in notices:
                return
            notices.add(key)
            logger.warning(
                "[THETA][INDEX_PROXY] %s underlying is not available from ThetaData; proxying via %s (factor=%s).",
                getattr(asset_separated, "symbol", asset_separated),
                ndx_proxy_symbol,
                ndx_proxy_factor,
            )

        def _apply_ndx_proxy_scaling(frame: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
            if frame is None or getattr(frame, "empty", True) or not ndx_proxy_symbol or not ndx_proxy_factor:
                return frame
            # Shallow copy: keep memory stable for multi-year cached frames.
            frame = frame.copy(deep=False)
            price_columns = ("open", "high", "low", "close", "bid", "ask", "mid_price", "price")
            for col in price_columns:
                if col in frame.columns:
                    frame[col] = pd.to_numeric(frame[col], errors="coerce") * float(ndx_proxy_factor)
            # Index underlyings do not have meaningful share volume/splits/dividends in our backtests.
            if "volume" in frame.columns:
                frame["volume"] = 0.0
            if "dividend" in frame.columns:
                frame["dividend"] = 0.0
            if "stock_splits" in frame.columns:
                frame["stock_splits"] = 0.0
            return frame

        def _fetch_ohlc():
            fetch_asset = asset_separated
            if ndx_proxy_symbol:
                _log_ndx_proxy_once()
                fetch_asset = Asset(symbol=ndx_proxy_symbol, asset_type="stock")
            frame = thetadata_helper.get_price_data(
                fetch_asset,
                start_for_fetch,
                end_requirement,
                timespan=ts_unit,
                quote_asset=quote_asset,
                dt=date_time_now,
                datastyle="ohlc",
                include_after_hours=True,  # Default to True for extended hours data
                preserve_full_history=preserve_full_history,
                # Day bars use Theta's EOD endpoint which can return `close=0` on days with no trades.
                # For options we still need NBBO (bid/ask) so `get_quote()` and ThetaData option
                # mark-to-market / fill logic can price illiquid days even when there are no trades.
                include_eod_nbbo=bool(
                    getattr(self, "_use_quote_data", False)
                    and ts_unit == "day"
                    and getattr(asset_separated, "asset_type", None) == "option"
                ),
            )
            return _apply_ndx_proxy_scaling(frame)

        def _fetch_quote():
            fetch_asset = asset_separated
            if ndx_proxy_symbol:
                _log_ndx_proxy_once()
                fetch_asset = Asset(symbol=ndx_proxy_symbol, asset_type="stock")
            frame = thetadata_helper.get_price_data(
                fetch_asset,
                start_for_fetch,
                end_requirement,
                timespan=ts_unit,
                quote_asset=quote_asset,
                dt=date_time_now,
                datastyle="quote",
                include_after_hours=True,  # Default to True for extended hours data
                preserve_full_history=preserve_full_history,
            )
            return _apply_ndx_proxy_scaling(frame)

        df_ohlc = None
        if wants_ohlc:
            if wants_quotes:
                # Performance: OHLC + QUOTE requests are independent network calls. Fetch them concurrently
                # for first-touch cache misses (common for 0DTE option strategies), then merge.
                from concurrent.futures import ThreadPoolExecutor

                with ThreadPoolExecutor(max_workers=2) as executor:
                    future_ohlc = executor.submit(_fetch_ohlc)
                    future_quote = executor.submit(_fetch_quote)
                    df_ohlc = future_ohlc.result()
                    df_quote = future_quote.result()
            else:
                df_ohlc = _fetch_ohlc()

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
        else:
            # Quote-only fetch (used for option MTM / quote checks). Missing quotes are not fatal:
            # the caller can fall back to OHLC (last trade) or forward-fill MTM as needed.
            if wants_quotes:
                try:
                    df_quote = _fetch_quote()
                except Exception:
                    logger.exception(
                        "ThetaData quote download failed for %s / %s (%s)",
                        asset_separated,
                        quote_asset,
                        ts_unit,
                    )
                    return None
            if df_quote is None or getattr(df_quote, "empty", True):
                logger.warning(
                    "No QUOTE data returned for %s / %s (%s); continuing without updating cache.",
                    asset_separated,
                    quote_asset,
                    ts_unit,
                )
                return None
            df = df_quote

        quotes_attached = bool((not wants_ohlc) and wants_quotes)
        quotes_ffilled = False
        quotes_ffill_rows = None
        quotes_ffill_remaining = None

        # Quote data (bid/ask) is only available for intraday data (minute, hour, second)
        # For daily+ data, only use OHLC
        if wants_ohlc and wants_quotes:
            if df_quote is None:
                # Sequential fallback: should be rare, but keeps behavior robust if executor path didn't run.
                try:
                    df_quote = _fetch_quote()
                except Exception:
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
                timestamp_columns = ['last_trade_time', 'last_bid_time', 'last_ask_time']
                quotes_attached = True

                quote_columns = ['bid', 'ask', 'bid_size', 'ask_size', 'bid_condition', 'ask_condition', 'bid_exchange', 'ask_exchange']
                # PERFORMANCE: Theta quote responses include a lot of redundant metadata columns
                # (symbol/strike/right/expiration/etc) that already exist in the OHLC frame.
                # Concatenating them creates many duplicate column names and forces expensive
                # de-dup logic (transpose-heavy bfill/ffill). Keep only the actionable quote
                # fields + timestamp metadata before merging.
                quote_keep = [col for col in quote_columns + timestamp_columns if col in df_quote.columns]
                df_quote_reduced = df_quote.loc[:, quote_keep]
                overlapping = [col for col in df_quote_reduced.columns if col in df_ohlc.columns]
                if overlapping:
                    df_quote_reduced = df_quote_reduced.drop(columns=overlapping)

                # Combine the ohlc and quote data using outer join to preserve all data.
                df = pd.concat([df_ohlc, df_quote_reduced], axis=1, join='outer')

                # Safety net: if duplicates remain (unexpected), combine them once.
                duplicate_names = df.columns[df.columns.duplicated()].unique().tolist()
                if duplicate_names:
                    df = self._combine_duplicate_columns(df, duplicate_names)

                # Forward fill missing quote values and timestamp metadata
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
            # NOTE: `frame` can be very large for long-window minute backtests (e.g. multi-year
            # equity OHLC caches). Avoid unconditional deep copies here or we can exceed the ECS
            # task memory limit in production backtests (observed as exit code -9 / OOMKilled).
            #
            # `metadata_frame_local` is treated as read-only and used only for coverage/placeholder
            # diagnostics, so it is safe to reference `frame` directly.
            metadata_frame_local = frame
            cleaned_df_local = frame
            placeholder_mask_local = None
            placeholder_rows_local = 0
            leading_placeholder_local = False
            if "missing" in cleaned_df_local.columns:
                placeholder_mask_local = cleaned_df_local["missing"].astype(bool)
                placeholder_rows_local = int(placeholder_mask_local.sum())
                if placeholder_rows_local and len(placeholder_mask_local):
                    leading_placeholder_local = bool(placeholder_mask_local.iloc[0])
                # PERF: avoid copying the entire dataframe when the "missing" column is present but
                # contains no placeholders (common on warm caches). Dropping a column is a cheap
                # metadata operation; boolean row filtering is not.
                if placeholder_rows_local:
                    # Drop placeholder rows but avoid an extra deep copy. `.loc[...]` already
                    # returns a new frame; calling `.copy()` here can double peak RSS.
                    cleaned_df_local = cleaned_df_local.loc[~placeholder_mask_local]
                cleaned_df_local = cleaned_df_local.drop(columns=["missing"], errors="ignore")
            else:
                # Create a new DataFrame object without deep-copying the underlying blocks. The
                # Data() constructor mutates columns/index in-place, so we want an owned frame,
                # but we don't need to duplicate the raw numeric arrays.
                cleaned_df_local = cleaned_df_local.copy(deep=False)

            if cleaned_df_local.empty:
                logger.debug(
                    "[THETA][DEBUG][THETADATA-PANDAS] All merged rows for %s/%s were placeholders; retaining raw merge for diagnostics.",
                    asset_separated,
                    quote_asset,
                )
                cleaned_df_local = metadata_frame_local.drop(columns=["missing"], errors="ignore").copy(deep=False)

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
                # PERF/ROBUSTNESS: Avoid concat-with-all-NA warnings and preserve existing columns when
                # the newly fetched frame is quote-only (no OHLC). The previous concat+dedupe behavior
                # could drop existing OHLC values if the "new" row had NaNs for those columns.
                existing_df = existing_data.df
                new_df = merged_df
                if existing_df.index.has_duplicates:
                    existing_df = existing_df.loc[~existing_df.index.duplicated(keep="last")]
                if new_df.index.has_duplicates:
                    new_df = new_df.loc[~new_df.index.duplicated(keep="last")]

                union_cols = existing_df.columns.union(new_df.columns)
                existing_aligned = existing_df.reindex(columns=union_cols)
                new_aligned = new_df.reindex(columns=union_cols)

                # New data should override existing values, but only where it is non-null.
                # (combine_first preserves existing values when the new frame has NaNs for a column.)
                merged_df = new_aligned.combine_first(existing_aligned).sort_index()

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
        # For minute data we want strict cache boundary enforcement so missing bars force a refresh.
        # For daily data, the backtester queries intraday timestamps (09:30/16:00) while the latest
        # completed daily bar represents the prior session; allow Data.check_data's tolerance window.
        data.strict_end_check = ts_unit != "day"
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
                # Signal daily cadence ONLY when we haven't observed intraday stepping.
                #
                # Some intraday strategies still request daily history for indicators; those requests
                # must not flip the entire backtest into day cadence (it breaks intraday option quotes).
                if not getattr(self, "_observed_intraday_cadence", False) and getattr(self, "_timestep", None) != "day":
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
        meta["prefetch_complete"] = self._compute_prefetch_complete(
            meta,
            requested_start=requested_start,
            effective_start_buffer=effective_start_buffer,
            end_requirement=end_requirement,
            ts_unit=ts_unit,
            requested_length=requested_length,
        )
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

        if require_quote_data and is_option_asset and not meta.get("has_quotes") and meta.get("data_rows"):
            if not meta.get("quotes_missing_permanent"):
                meta["quotes_missing_permanent"] = True
                self._dataset_metadata[canonical_key] = meta
                if legacy_meta is not None:
                    legacy_meta.update(meta)
                    self._dataset_metadata[legacy_key] = legacy_meta
                logger.info(
                    "[THETA][CACHE][QUOTES_MISSING] asset=%s/%s (%s) fetched without quotes; "
                    "treating missing quotes as permanent for this run.",
                    asset_separated,
                    quote_asset,
                    ts_unit,
                )

        if is_option_asset and meta.get("negative_cache"):
            self._negative_option_cache.add(asset_separated)
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
            is_option_asset = str(getattr(asset_separated, "asset_type", "")).lower() == "option"
            if coverage_end is None:
                if is_option_asset:
                    # Data gaps are normal for options that don't trade every day.
                    logger.warning(
                        "[THETA][COVERAGE][NO_END] asset=%s/%s (%s) has no end timestamp while target_end=%s; "
                        "continuing with available data",
                        asset_separated,
                        quote_asset,
                        ts_unit,
                        end_requirement,
                    )
                    return  # Continue without crashing
                raise ValueError(
                    f"[THETA][COVERAGE][NO_END] asset={asset_separated}/{quote_asset} ({ts_unit}) has no end timestamp "
                    f"while target_end={end_requirement}; refusing to proceed for non-option asset."
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
                if is_option_asset:
                    # Data gaps are normal for options that don't trade every day.
                    logger.warning(
                        "[THETA][COVERAGE][GAP] asset=%s/%s (%s) coverage_end=%s target_end=%s rows=%s placeholders=%s days_behind=%s; "
                        "continuing with available data (data gaps are normal for illiquid options)",
                        asset_separated,
                        quote_asset,
                        ts_unit,
                        coverage_end,
                        end_requirement,
                        meta.get("rows"),
                        meta.get("placeholders"),
                        days_behind,
                    )
                    logger.debug(
                        "[THETA][COVERAGE][GAP][DIAGNOSTICS] requested_start=%s start_for_fetch=%s data_start=%s data_end=%s requested_length=%s prefetch_complete=%s",
                        requested_start,
                        start_for_fetch,
                        meta.get("data_start"),
                        meta.get("data_end"),
                        requested_length,
                        meta.get("prefetch_complete"),
                    )
                else:
                    raise ValueError(
                        f"[THETA][COVERAGE][GAP] asset={asset_separated}/{quote_asset} ({ts_unit}) coverage_end={coverage_end} "
                        f"target_end={end_requirement} rows={meta.get('rows')} placeholders={meta.get('placeholders')} "
                        f"days_behind={days_behind}; refusing to proceed for non-option asset."
                    )
        if meta.get("tail_placeholder") and not meta.get("tail_missing_permanent"):
            if is_option_asset:
                # Placeholders at the end mean the option didn't trade for those dates; continue.
                logger.warning(
                    "[THETA][COVERAGE][TAIL_PLACEHOLDER] asset=%s/%s (%s) ends with placeholders (target_end=%s); "
                    "continuing with available data (asset may not have traded during this period)",
                    asset_separated,
                    quote_asset,
                    ts_unit,
                    end_requirement,
                )
            elif is_index_asset:
                clamp_end = meta.get("last_real_ts") or meta.get("data_end") or meta.get("end")
                if clamp_end is not None and end_requirement is not None:
                    if clamp_end < end_requirement:
                        logger.warning(
                            "[THETA][COVERAGE][TAIL_PLACEHOLDER] asset=%s/%s (%s) ends with placeholders; "
                            "clamping backtest end to %s (target_end=%s).",
                            asset_separated,
                            quote_asset,
                            ts_unit,
                            clamp_end,
                            end_requirement,
                        )
                        self.datetime_end = clamp_end
                meta["tail_missing_permanent"] = True
            else:
                raise ValueError(
                    f"[THETA][COVERAGE][TAIL_PLACEHOLDER] asset={asset_separated}/{quote_asset} ({ts_unit}) ends with placeholders "
                    f"(target_end={end_requirement}); refusing to proceed for non-option asset."
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
                # PERFORMANCE: `bfill/ffill(axis=1)` triggers a transpose on every call which is
                # extremely expensive for wide, high-row-count frames (options minute data).
                # A simple left-to-right `combine_first` achieves the same "first non-null wins"
                # semantics without transposing.
                combined = selection.iloc[:, 0]
                for idx in range(1, selection.shape[1]):
                    combined = combined.combine_first(selection.iloc[:, idx])
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
        # Align requests to the current backtesting mode to avoid accidental intraday downloads
        # during day-cadence backtests.
        current_mode = getattr(self, "_timestep", None)
        if (
            current_mode == "day"
            and isinstance(timestep, str)
            and timestep.lower() in {"minute", "hour", "second"}
        ):
            logger.debug(
                "[THETA][DEBUG][TIMESTEP_ALIGN] Aligning %s request to day mode for asset=%s length=%s",
                timestep,
                asset,
                length,
            )
            timestep = "day"
        elif timestep is None and current_mode == "day":
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
        current_mode = getattr(self, "_timestep", None)
        if current_mode == "day" and isinstance(timestep, str) and timestep.lower() in {"minute", "hour", "second"}:
            logger.debug(
                "[THETA][DEBUG][TIMESTEP_ALIGN] Aligning %s between-dates request to day mode for asset=%s",
                timestep,
                asset,
            )
            timestep = "day"
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
        dt = self.get_datetime()
        self._update_cadence_from_dt(dt)
        # In day mode, use day data for price lookups instead of defaulting to minute.
        # This prevents unnecessary minute data downloads at end of day-mode backtests.
        # NOTE: Do not infer day-mode from "any day data exists" because intraday strategies may
        # still request daily history for indicators.
        current_mode = getattr(self, "_timestep", None)

        if current_mode == "day" and timestep == "minute":
            timestep = "day"
            logger.debug(
                "[THETA][DEBUG][TIMESTEP_ALIGN] get_last_price aligned from minute to day for asset=%s",
                asset,
            )

        # PERFORMANCE: `get_last_price()` is "last trade" semantics, not a historical-series API.
        # For daily option last-price checks, fetching a 5-day window can cause unnecessary placeholder
        # churn (Theta 472 / empty) during contract selection. Use a minimal window and rely on the
        # existing progressive lookback (30/252 days) only when the current day has no trades.
        sample_length = 5
        if timestep == "day" and getattr(asset, "asset_type", None) == Asset.AssetType.OPTION:
            sample_length = 1

        # PERF: cache last trade results within the same backtest datetime.
        # Backtest internals and user strategies may ask for the same asset's last price multiple
        # times per bar (e.g., option selection + fills + risk checks). In backtesting, the
        # last-trade at a given dt is immutable.
        last_price_cache_dt = getattr(self, "_last_price_cache_dt", None)
        if last_price_cache_dt != dt:
            self._last_price_cache_dt = dt
            self._last_price_cache = {}
        cache_key = (asset, quote, exchange, timestep)
        cached = getattr(self, "_last_price_cache", {}).get(cache_key, None)
        if cache_key in getattr(self, "_last_price_cache", {}):
            return cached

        # Trade-only: do not require quote columns. Quotes are used explicitly via get_quote()/snapshots
        # for mark-to-market and fills, never via get_last_price().
        self._update_pandas_data(asset, quote, sample_length, timestep, dt, require_quote_data=False)
        _, ts_unit = self.get_start_datetime_and_ts_unit(
            sample_length, timestep, dt, start_buffer=START_BUFFER
        )
        source = None
        tuple_key = self.find_asset_in_data_store(asset, quote, ts_unit)
        legacy_hit = False
        frame_last_dt = None
        frame_last_close = None

        def _resolve_last_trade_close(key: object) -> Optional[float]:
            """Return the most recent positive close (trade) at-or-before dt for the dataset key."""
            nonlocal frame_last_dt, frame_last_close

            data_obj = self.pandas_data.get(key)
            if data_obj is None and isinstance(key, tuple) and len(key) == 3:
                data_obj = self.pandas_data.get((key[0], key[1]))
            if data_obj is None or not hasattr(data_obj, "df") or data_obj.df is None:
                return None

            df = data_obj.df
            close_series = df.get("close")
            if close_series is None or len(df.index) == 0:
                return None

            try:
                iter_count = data_obj.get_iter_count(dt)
                closes = close_series.iloc[: iter_count + 1]
                if "missing" in df.columns:
                    try:
                        missing_mask = df["missing"].iloc[: iter_count + 1].astype(bool)
                    except Exception:
                        missing_mask = df["missing"].iloc[: iter_count + 1] == 1
                    closes = closes[~missing_mask.fillna(True)]
            except Exception:
                # Defensive fallback: filter by timestamp if iter lookup fails.
                try:
                    closes = close_series.loc[close_series.index <= dt]
                except Exception:
                    closes = close_series

            closes = pd.to_numeric(closes, errors="coerce").dropna()
            closes = closes[closes > 0]
            if len(closes) == 0:
                return None

            frame_last_dt = closes.index[-1]
            frame_last_close = closes.iloc[-1]
            try:
                frame_last_dt = frame_last_dt.isoformat()
            except AttributeError:
                frame_last_dt = str(frame_last_dt)
            return float(frame_last_close)
        if tuple_key is not None:
            if isinstance(tuple_key, tuple) and len(tuple_key) != 3:
                legacy_hit = True

            value = _resolve_last_trade_close(tuple_key)
            source = "pandas_dataset" if value is not None else None
        else:
            value = None

        # If we still don't have a last trade for an option in day-cadence, progressively expand lookback.
        #
        # Rationale: live brokers commonly return the most recent prior trade even when the current day has no
        # prints. Many options (especially far OTM LEAPS) can go weeks/months without prints. Returning None here
        # causes strategies to incorrectly treat contracts as untradeable (even when a stale last trade exists).
        if (
            value is None
            and getattr(asset, "asset_type", None) == Asset.AssetType.OPTION
            and timestep == "day"
            and tuple_key is not None
        ):
            meta = self._dataset_metadata.setdefault(tuple_key, {})
            attempted = bool(meta.get("last_trade_lookback_attempted", False))

            if not attempted:
                meta["last_trade_lookback_attempted"] = True

                for lookback_days in (30, 252):
                    if lookback_days <= sample_length:
                        continue

                    try:
                        self._update_pandas_data(
                            asset,
                            quote,
                            lookback_days,
                            timestep,
                            dt,
                            require_quote_data=False,
                        )
                    except Exception:
                        continue

                    value = _resolve_last_trade_close(tuple_key)
                    if value is not None:
                        source = f"pandas_dataset_lookback_{lookback_days}"
                        meta["last_trade_lookback_days"] = lookback_days
                        break

        # As a fallback (e.g., empty dataframe), defer to the base implementation which is
        # still trade-based (no quote/mid contamination).
        if value is None:
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

        try:
            self._last_price_cache[cache_key] = value
        except Exception:
            pass

        return value

    def get_price_snapshot(self, asset, quote=None, timestep="minute", **kwargs) -> Optional[Dict[str, object]]:
        """Return the latest OHLC + quote snapshot for the requested asset."""
        sample_length = 5
        dt = self.get_datetime()
        # In day mode, use day data for price snapshots instead of defaulting to minute.
        # This prevents unnecessary minute data downloads at end of day-mode backtests.
        # FIX (2025-12-12): Also check if any existing data uses "day" timestep to detect day mode
        current_mode = getattr(self, "_timestep", None)

        # PERF: determine "effective day mode" once per backtest instead of scanning the entire store on
        # every snapshot call (pandas_data can contain tens of thousands of option frames).
        if current_mode != "day":
            effective_day_mode = getattr(self, "_effective_day_mode", None)
            if effective_day_mode is None:
                effective_day_mode = any(
                    getattr(data, "timestep", None) == "day" for data in self.pandas_data.values()
                )
                self._effective_day_mode = effective_day_mode
            if effective_day_mode:
                current_mode = "day"
        else:
            self._effective_day_mode = True
        if current_mode == "day" and timestep == "minute":
            timestep = "day"
            logger.debug(
                "[THETA][DEBUG][TIMESTEP_ALIGN] get_price_snapshot aligned from minute to day for asset=%s",
                asset,
            )

        # PERF: cache snapshots within the same backtest datetime.
        # Portfolio valuation (and other internals) may request snapshots repeatedly within a bar.
        snapshot_cache_dt = getattr(self, "_price_snapshot_cache_dt", None)
        if snapshot_cache_dt != dt:
            self._price_snapshot_cache_dt = dt
            self._price_snapshot_cache = {}
        snapshot_cache_key = (asset, quote, timestep)
        snapshot_cache = getattr(self, "_price_snapshot_cache", {})
        if snapshot_cache_key in snapshot_cache:
            return snapshot_cache.get(snapshot_cache_key)

        asset_for_check = asset[0] if isinstance(asset, tuple) else asset
        require_quote_data = getattr(asset_for_check, "asset_type", None) == "option"
        require_ohlc_data = True
        ts_unit = None
        try:
            _, ts_unit = self.convert_timestep_str_to_timedelta(timestep)
            if require_quote_data and ts_unit in {"minute", "hour", "second"}:
                require_ohlc_data = False
        except Exception:
            require_ohlc_data = True

        # PERF: Portfolio valuation can request snapshots for many assets each bar. Avoid calling
        # `_update_pandas_data()` when the in-memory frame already covers `dt` and has the required
        # columns (quote-only for options, OHLC for everything else).
        should_refresh = True
        candidate_data = None
        if ts_unit is not None:
            try:
                quote_asset = quote if quote is not None else Asset("USD", "forex")
                canonical_key, legacy_key = self._build_dataset_keys(asset_for_check, quote_asset, ts_unit)
                candidate_data = self.pandas_data.get(canonical_key) or self.pandas_data.get(legacy_key)
                if candidate_data is not None and getattr(candidate_data, "timestep", None) == ts_unit:
                    candidate_df = getattr(candidate_data, "df", None)
                    if candidate_df is not None and not candidate_df.empty:
                        has_required = True
                        if require_quote_data and not self._frame_has_quote_columns(candidate_df):
                            has_required = False
                        if require_ohlc_data and not self._frame_has_ohlc_columns(candidate_df):
                            has_required = False
                        if has_required:
                            data_end = getattr(candidate_data, "datetime_end", None)
                            if data_end is None:
                                data_end = self._normalize_default_timezone(candidate_df.index.max())
                            normalized_dt = self._normalize_default_timezone(dt) if dt is not None else None
                            normalized_end = self._normalize_default_timezone(data_end) if data_end is not None else None
                            if normalized_dt is not None and normalized_end is not None and normalized_dt <= normalized_end:
                                should_refresh = False
            except Exception:
                should_refresh = True

        if should_refresh:
            self._update_pandas_data(
                asset,
                quote,
                sample_length,
                timestep,
                dt,
                require_quote_data=require_quote_data,
                require_ohlc_data=require_ohlc_data,
            )
        if ts_unit is None:
            try:
                _, ts_unit = self.convert_timestep_str_to_timedelta(timestep)
            except Exception:
                ts_unit = None

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
            try:
                self._price_snapshot_cache[snapshot_cache_key] = snapshot
            except Exception:
                pass
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
                try:
                    self._price_snapshot_cache[snapshot_cache_key] = None
                except Exception:
                    pass
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
        # PandasData handles str->Asset coercion in its public API; our fast-path bypasses that,
        # so normalize here to preserve behaviour.
        asset_obj: Asset
        if isinstance(asset, str):
            asset_obj = Asset(symbol=asset)
        else:
            asset_obj = asset

        start_requirement, ts_unit = self.get_start_datetime_and_ts_unit(
            length,
            timestep,
            current_dt,
            start_buffer=START_BUFFER,
        )

        # PERFORMANCE: Avoid calling `_update_pandas_data()` for every `get_historical_prices()` call
        # when we already have a cached Data object that covers the requested dt.
        #
        # Why this matters:
        # - Some strategies call `get_historical_prices()` thousands of times per backtest (RSI/EMA
        #   every bar, multi-leg scanners, etc.).
        # - ThetaDataBacktestingPandas overrides `_pull_source_symbol_bars()` and *always* calls
        #   `_update_pandas_data()` before slicing. That is correct but can be extremely slow when
        #   the in-memory cache already covers the requested window.
        #
        # Fast-path:
        # - If the requested asset/timestep exists in `_data_store` and the cached dataframe already
        #   extends through `current_dt`, slice directly via PandasData._pull_source_symbol_bars()
        #   (which does not refresh) and skip the expensive update/merge logic.
        bars = None
        try:
            tuple_key = self.find_asset_in_data_store(asset_obj, quote, ts_unit)
            data_obj = self._data_store.get(tuple_key) if tuple_key is not None else None
            df_existing = getattr(data_obj, "df", None) if data_obj is not None else None
            if df_existing is not None and not df_existing.empty:
                try:
                    data_end = getattr(data_obj, "date_end", None) or df_existing.index.max()
                except Exception:
                    data_end = None
                try:
                    data_start = getattr(data_obj, "date_start", None) or df_existing.index.min()
                except Exception:
                    data_start = None
                if data_end is not None:
                    # Normalize timezone for safe comparisons.
                    try:
                        normalized_end = self._normalize_default_timezone(data_end)
                        normalized_now = self._normalize_default_timezone(current_dt)
                        normalized_start = self._normalize_default_timezone(data_start) if data_start is not None else None
                        normalized_required_start = self._normalize_default_timezone(start_requirement) if start_requirement is not None else None
                    except Exception:
                        normalized_end = None
                        normalized_now = None
                        normalized_start = None
                        normalized_required_start = None
                    if normalized_end is not None and normalized_now is not None and normalized_now <= normalized_end:
                        # Only apply the fast-path when the cached frame already covers the
                        # requested lookback window. This preserves existing behavior where we
                        # prefetch additional history (START_BUFFER) to seed indicators.
                        if normalized_required_start is not None and normalized_start is not None:
                            if normalized_start > normalized_required_start:
                                raise ValueError("cached frame missing required lookback; refresh needed")
                        response = PandasData._pull_source_symbol_bars(
                            self,
                            asset_obj,
                            length,
                            timestep=timestep,
                            timeshift=timeshift,
                            quote=quote,
                            exchange=exchange,
                            include_after_hours=include_after_hours,
                        )
                        if response is not None:
                            bars = self._parse_source_symbol_bars(response, asset_obj, quote=quote, length=length, return_polars=False)
        except Exception:
            bars = None

        if bars is None:
            bars = super().get_historical_prices(
                asset=asset_obj,
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

    def _update_cadence_from_dt(self, dt) -> None:
        """Detect intraday cadence so daily-history requests don't flip the whole run to day mode.

        This is used to prevent a common failure mode:
        - Intraday strategies request daily history for indicators.
        - If we infer "day mode" from the mere presence of day data, intraday option quote/ohlc
          requests can be incorrectly aligned to daily cadence and explode into 472/no-data churn.

        Cadence detection must be conservative:
        - Repeated calls at the same strategy datetime must NOT mark a run as intraday.
        - Daily strategies may execute multiple lifecycle hooks per trading day; those should also
          not flip cadence.
        """
        if dt is None:
            return

        try:
            if isinstance(dt, pd.Timestamp):
                dt = dt.to_pydatetime()
        except Exception:
            pass

        last_dt = getattr(self, "_cadence_last_dt", None)
        if last_dt is not None:
            try:
                if isinstance(last_dt, pd.Timestamp):
                    last_dt = last_dt.to_pydatetime()
            except Exception:
                pass

            try:
                delta_s = abs((dt - last_dt).total_seconds())
                # Any sub-6h step strongly indicates an intraday backtest (minute/hour cadence).
                if 0 < delta_s < 6 * 3600:
                    self._observed_intraday_cadence = True
            except Exception:
                pass

        self._cadence_last_dt = dt

    def get_quote(self, asset, quote=None, exchange=None, timestep="minute", **kwargs):
        """
        Get quote data for an asset during backtesting.

        Parameters
        ----------
        asset : Asset object
            The asset for which the quote is needed.
        quote : Asset object, optional
            The quote asset for cryptocurrency pairs.
        exchange : str, optional
            The exchange to get the quote from.
        timestep : str, optional
            The timestep to use for the data. Defaults to ``"minute"`` but is auto-aligned to ``"day"``
            when the backtest is running in daily cadence.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        Quote
            A Quote object with the quote information.
        """
        snapshot_only = bool(kwargs.pop("snapshot_only", False))
        dt = self.get_datetime()
        self._update_cadence_from_dt(dt)

        # Day-cadence alignment: in day mode, prefer day data for quote lookups to avoid downloading
        # full intraday quote history for long backtests.
        #
        # NOTE: Order-fill logic can still request intraday "snapshot_only" quotes when needed
        # (see `BacktestingBroker._try_fill_with_quote`) without forcing all quote consumers onto
        # minute-level data.
        current_mode = getattr(self, "_timestep", None)
        self._effective_day_mode = current_mode == "day"

        if current_mode == "day" and timestep == "minute" and not snapshot_only:
            timestep = "day"
            logger.debug(
                "[THETA][DEBUG][TIMESTEP_ALIGN] get_quote aligned from minute to day for asset=%s",
                getattr(asset, "symbol", asset) if not isinstance(asset, str) else asset,
            )

        # PERF: cache Quote objects within the same backtest datetime.
        # Many strategies call get_quote() multiple times per asset per bar (MTM, risk checks,
        # limit price estimation, etc.). In backtesting, the quote for a given dt is immutable.
        quote_cache_dt = getattr(self, "_quote_cache_dt", None)
        if quote_cache_dt != dt:
            self._quote_cache_dt = dt
            self._quote_cache = {}
        cache_key = (asset, quote, exchange, timestep)
        cached = getattr(self, "_quote_cache", {}).get(cache_key)
        if cached is not None:
            return cached

        # SNAPSHOT MODE (perf-critical)
        # -----------------------------
        # Some strategies (notably delta/strike scanners) need only a point-in-time NBBO for many
        # strikes that will never be traded. The normal caching path for intraday ThetaData quotes
        # is date-based and can download an entire trading day (~956 minute rows) per strike, which
        # is both slow and can stall long-running backtests in production.
        #
        # When snapshot_only=True, bypass parquet caching entirely and request only the minimal
        # intraday quote window around the current simulation timestamp.
        if snapshot_only:
            from lumibot.entities import Quote as QuoteEntity

            def _cache_and_return(obj: QuoteEntity):
                try:
                    self._quote_cache[cache_key] = obj
                except Exception:
                    pass
                return obj

            try:
                from lumibot.tools import thetadata_helper

                delta_td, ts_unit = self.convert_timestep_str_to_timedelta(timestep)
                if ts_unit not in {"minute", "second", "hour"}:
                    raise ValueError(f"snapshot_only unsupported for ts_unit={ts_unit}")

                ivl_ms = int(delta_td.total_seconds() * 1000)
                if ivl_ms <= 0:
                    ivl_ms = 60_000

                # PERF: Snapshot-only quote probes can happen many times per minute in option scanners.
                # If a given contract returns "no data" (472 / empty), avoid re-hitting the downloader
                # every single bar by caching that negative result for a short TTL.
                negative_cache = getattr(self, "_snapshot_negative_cache", None)
                if not isinstance(negative_cache, dict):
                    negative_cache = {}
                    self._snapshot_negative_cache = negative_cache
                cache_day = None
                try:
                    cache_day = dt.date()
                except Exception:
                    cache_day = None
                negative_key = (asset, ts_unit, cache_day)
                cached_skip_until = negative_cache.get(negative_key)
                if cached_skip_until is not None and dt is not None:
                    try:
                        if dt < cached_skip_until:
                            return _cache_and_return(
                                QuoteEntity(
                                    asset=asset,
                                    price=None,
                                    bid=None,
                                    ask=None,
                                    volume=None,
                                    timestamp=dt,
                                    bid_size=None,
                                    ask_size=None,
                                    raw_data=None,
                                )
                            )
                    except Exception:
                        pass

                # Request only a tiny window around dt.
                #
                # In daily-cadence backtests, ThetaData minute-aggregated quote bars are often
                # timestamped at the END of the minute (e.g., the first bar after the 09:30 open
                # can appear at 09:31). Use a small forward-looking window and take the first
                # quote in that window so option selection doesn't get stuck with missing NBBO.
                #
                # In intraday backtests, prefer a backward-looking window so we never read quotes
                # from the future bar.
                if current_mode == "day":
                    window_td = delta_td * 5
                    start_dt = dt
                    end_dt = dt + window_td
                else:
                    # ThetaData minute-aggregated quote bars are often timestamped at the END of the
                    # minute. At the session open this means the first "09:30" quote bar can appear
                    # at 09:31, so a strict backward-only window (dt-1m -> dt) frequently returns an
                    # empty/placeholder response and causes option scanners to conclude "no quotes".
                    #
                    # Use a small forward buffer so we can still find the first valid bar for the
                    # current dt without needing to download a full day of quotes.
                    window_td = delta_td * 5
                    start_dt = dt - delta_td
                    end_dt = dt + window_td

                    # PERF: Keep snapshot-only quote payloads small but stable. Bucket to a fixed
                    # intraday window so delta probes don't download full-session quote history and
                    # don't create one cache file per bar.
                    try:
                        bucket_td = timedelta(minutes=15)
                        bucket_minute = (dt.minute // 15) * 15
                        bucket_start = dt.replace(minute=bucket_minute, second=0, microsecond=0)
                        start_dt = bucket_start
                        end_dt = bucket_start + bucket_td
                    except Exception:
                        pass

                df_snapshot = thetadata_helper.get_historical_data_snapshot_cached(
                    asset,
                    start_dt,
                    end_dt,
                    ivl_ms,
                    datastyle="quote",
                    include_after_hours=True,
                    # Cache the full regular session for stability and to maximize reuse of warmed
                    # S3 objects (acceptance backtests require the warm-cache invariant).
                    prefer_full_session=True,
                )

                def _negative_ttl() -> timedelta:
                    if ts_unit == "second":
                        return timedelta(minutes=1)
                    if ts_unit == "hour":
                        return timedelta(hours=1)
                    return timedelta(minutes=15)

                is_empty = df_snapshot is None or getattr(df_snapshot, "empty", True)
                if not is_empty and isinstance(df_snapshot, pd.DataFrame) and "missing" in df_snapshot.columns:
                    try:
                        missing_flags = df_snapshot["missing"].fillna(False).astype(bool)
                        if bool(missing_flags.all()):
                            is_empty = True
                    except Exception:
                        pass

                if is_empty:
                    try:
                        negative_cache[negative_key] = dt + _negative_ttl()
                    except Exception:
                        pass
                    # Snapshot-only is intentionally non-caching: do not fall back to the day-chunk
                    # parquet fetch path, which is extremely expensive for delta probes and can
                    # introduce lookahead if later-session quotes are forward-filled.
                    return _cache_and_return(
                        QuoteEntity(
                            asset=asset,
                            price=None,
                            bid=None,
                            ask=None,
                            volume=None,
                            timestamp=dt,
                            bid_size=None,
                            ask_size=None,
                            raw_data=None,
                        )
                    )

                if current_mode == "day":
                    # Daily-cadence backtests use a small forward-looking snapshot window to
                    # account for end-of-minute timestamping (the first "09:30" quote bar can
                    # appear at ~09:31). Prefer the first two-sided NBBO row within that forward
                    # window to avoid one-sided quotes at the open causing strategies to skip
                    # otherwise tradeable contracts (acceptance runs rely on this behavior).
                    row = df_snapshot.iloc[0]
                    row_ts = df_snapshot.index[0]
                    try:
                        df_window = df_snapshot
                        try:
                            df_window = df_snapshot.loc[start_dt:end_dt]
                        except Exception:
                            df_window = df_snapshot

                        if isinstance(df_window, pd.DataFrame) and not df_window.empty:
                            col_map = {str(c).lower(): c for c in df_window.columns}
                            bid_col = col_map.get("bid")
                            ask_col = col_map.get("ask")
                            if bid_col is not None and ask_col is not None:
                                mask = df_window[bid_col].notna() & df_window[ask_col].notna()
                                if bool(mask.any()):
                                    first_idx = df_window.index[mask.argmax()]  # type: ignore[arg-type]
                                    row = df_window.loc[first_idx]
                                    row_ts = first_idx
                    except Exception:
                        pass
                else:
                    # Prefer the last bar at/before dt. If none exist (common at the open due to
                    # end-of-minute timestamping), fall forward to the first bar after dt.
                    try:
                        df_slice = df_snapshot.loc[:dt]
                        if not df_slice.empty:
                            row = df_slice.iloc[-1]
                            row_ts = df_slice.index[-1]
                        else:
                            df_future = df_snapshot.loc[dt:]
                            row = df_future.iloc[0] if not df_future.empty else df_snapshot.iloc[-1]
                            row_ts = df_future.index[0] if not df_future.empty else df_snapshot.index[-1]
                    except Exception:
                        row = df_snapshot.iloc[-1]
                        row_ts = df_snapshot.index[-1]

                def _coerce_positive(value):
                    try:
                        numeric = float(value)
                    except (TypeError, ValueError):
                        return None
                    if not math.isfinite(numeric) or numeric <= 0:
                        return None
                    return numeric

                bid = _coerce_positive(row.get("bid") if hasattr(row, "get") else None)
                ask = _coerce_positive(row.get("ask") if hasattr(row, "get") else None)

                bid_size = row.get("bid_size") if hasattr(row, "get") else None
                ask_size = row.get("ask_size") if hasattr(row, "get") else None
                volume = row.get("volume") if hasattr(row, "get") else None

                price = None
                if bid is not None and ask is not None:
                    price = (bid + ask) / 2.0
                elif bid is not None:
                    price = bid
                elif ask is not None:
                    price = ask
                else:
                    # ThetaData quote snapshots can sometimes omit actionable NBBO (bid/ask) for
                    # historical options while still providing a trade-derived close/last field.
                    # For snapshot-only probes (used for expiry/strike validation), treat that as a
                    # usable price signal so strategies can locate tradable contracts without
                    # forcing expensive full-day OHLC downloads.
                    for field in ("close", "price", "last", "last_trade", "last_trade_price"):
                        candidate = None
                        try:
                            candidate = row.get(field) if hasattr(row, "get") else None
                        except Exception:
                            candidate = None
                        candidate = _coerce_positive(candidate)
                        if candidate is not None:
                            price = candidate
                            break

                return _cache_and_return(
                    QuoteEntity(
                        asset=asset,
                        price=price,
                        bid=bid,
                        ask=ask,
                        volume=volume,
                        timestamp=row_ts,
                        bid_size=bid_size,
                        ask_size=ask_size,
                        raw_data=None,
                    )
                )
            except Exception:
                logger.debug("[THETA][QUOTE][SNAPSHOT_ONLY] failed", exc_info=True)
                return _cache_and_return(
                    QuoteEntity(
                        asset=asset,
                        price=None,
                        bid=None,
                        ask=None,
                        volume=None,
                        timestamp=dt,
                        bid_size=None,
                        ask_size=None,
                        raw_data=None,
                    )
                )

        # Log quote request details for debugging (options vs other assets).
        # Guard to avoid allocating strings/dicts when debug logging is disabled.
        if logger.isEnabledFor(logging.DEBUG):
            if hasattr(asset, "asset_type") and asset.asset_type == Asset.AssetType.OPTION:
                logger.debug(
                    "[THETA][QUOTE] Option request: symbol=%s expiration=%s strike=%s right=%s dt=%s timestep=%s",
                    asset.symbol,
                    asset.expiration,
                    asset.strike,
                    asset.right,
                    dt.isoformat() if hasattr(dt, "isoformat") else dt,
                    timestep,
                )
            else:
                logger.debug(
                    "[THETA][QUOTE] Asset request: symbol=%s dt=%s timestep=%s",
                    getattr(asset, "symbol", asset) if not isinstance(asset, str) else asset,
                    dt.isoformat() if hasattr(dt, "isoformat") else dt,
                    timestep,
                )

        # PERFORMANCE: `get_quote()` is called extremely frequently for option-heavy strategies
        # (e.g., intraday straddle MTM checks). `_update_pandas_data()` is correct but expensive,
        # even when the in-memory cache already fully covers the current date.
        #
        # Fast-path: if we already have quote columns in the cached dataframe and the current dt
        # is within the cached range, skip the refresh work and just read from the cache.
        should_refresh = True
        fast_data = None
        try:
            quote_asset = quote if quote is not None else Asset("USD", "forex")
            _, ts_unit = self.convert_timestep_str_to_timedelta(timestep)
            if ts_unit in {"minute", "hour", "second"}:
                canonical_key, legacy_key = self._build_dataset_keys(asset, quote_asset, ts_unit)
                candidate_data = self.pandas_data.get(canonical_key)
                if candidate_data is None:
                    candidate_data = self.pandas_data.get(legacy_key)

                if candidate_data is not None and getattr(candidate_data, "timestep", None) == ts_unit:
                    candidate_df = getattr(candidate_data, "df", None)
                    if candidate_df is not None and not candidate_df.empty and self._frame_has_quote_columns(candidate_df):
                        data_end = getattr(candidate_data, "datetime_end", None)
                        normalized_dt = self._normalize_default_timezone(dt) if dt is not None else None
                        normalized_end = self._normalize_default_timezone(data_end) if data_end is not None else None
                        if normalized_dt is not None and normalized_end is not None and normalized_dt <= normalized_end:
                            should_refresh = False
                            fast_data = candidate_data
        except Exception:
            should_refresh = True

        if should_refresh:
            require_ohlc_data = True
            try:
                _, ts_unit = self.convert_timestep_str_to_timedelta(timestep)
                if (
                    getattr(asset, "asset_type", None) == Asset.AssetType.OPTION
                    and ts_unit in {"minute", "hour", "second"}
                ):
                    require_ohlc_data = False
            except Exception:
                require_ohlc_data = True

            self._update_pandas_data(
                asset,
                quote,
                1,
                timestep,
                dt,
                require_quote_data=True,
                require_ohlc_data=require_ohlc_data,
                snapshot_only=snapshot_only,
            )

        quote_obj = None
        # Fast-path: build a Quote directly from the cached Data object without calling
        # Data.get_quote() (which is wrapped in `check_data` and allocates dicts per call).
        if fast_data is not None:
            try:
                from lumibot.entities import Quote

                iter_count = fast_data.get_iter_count(dt)

                def _get(column: str):
                    if column not in fast_data.datalines:
                        return None
                    value = fast_data.datalines[column].dataline[iter_count]
                    if value is None:
                        return None
                    try:
                        if pd.isna(value):
                            return None
                    except Exception:
                        pass
                    return value

                close = _get("close")
                bid = _get("bid")
                ask = _get("ask")
                bid_size = _get("bid_size")
                ask_size = _get("ask_size")
                volume = _get("volume")

                # Match PandasData.get_quote(): treat non-positive bid/ask as missing.
                for side_key, side_val in (("bid", bid), ("ask", ask)):
                    if side_val is None:
                        continue
                    try:
                        numeric_val = float(side_val)
                    except (TypeError, ValueError):
                        continue
                    if numeric_val <= 0:
                        if side_key == "bid":
                            bid = None
                        else:
                            ask = None

                quote_obj = Quote(
                    asset=asset,
                    price=close,
                    bid=bid,
                    ask=ask,
                    volume=volume,
                    timestamp=dt,
                    bid_size=bid_size,
                    ask_size=ask_size,
                    raw_data=None,
                )
            except Exception:
                quote_obj = None

        if quote_obj is None:
            try:
                quote_obj = super().get_quote(asset=asset, quote=quote, exchange=exchange)
            except Exception:
                # Missing data (placeholders / no trades / sparse NBBO) is expected for many option
                # contracts at many timestamps. Avoid raising and triggering high-volume error logs
                # from Strategy.get_quote(); return an "empty" Quote instead.
                from lumibot.entities import Quote

                quote_obj = Quote(asset=asset, timestamp=dt)

        # ThetaData quote history for options can omit actionable NBBO while still surfacing a trade-derived
        # "close" field.
        #
        # Prefer quote-derived (bid/ask) pricing when available:
        # - If NBBO is present, set Quote.price to a mid/bid/ask-derived value.
        # - If NBBO is missing, preserve any existing `price` value (often trade close) so callers that
        #   expect a numeric Quote.price (including backtest tests) remain functional.
        #
        # Important: Quote.price does not imply actionable two-sided NBBO exists; execution/fill logic must
        # still prefer bid/ask when available.
        if quote_obj is not None and getattr(asset, "asset_type", None) == Asset.AssetType.OPTION:
            bid = getattr(quote_obj, "bid", None)
            ask = getattr(quote_obj, "ask", None)
            try:
                bid = float(bid) if bid is not None else None
            except (TypeError, ValueError):
                bid = None
            try:
                ask = float(ask) if ask is not None else None
            except (TypeError, ValueError):
                ask = None

            if bid is not None and ask is not None and bid > 0 and ask > 0:
                quote_obj.price = (bid + ask) / 2.0
            elif bid is not None and bid > 0:
                quote_obj.price = bid
            elif ask is not None and ask > 0:
                quote_obj.price = ask
            else:
                # Preserve the existing price (often trade close) but ensure it's finite and positive.
                existing = getattr(quote_obj, "price", None)
                try:
                    existing = float(existing) if existing is not None else None
                except (TypeError, ValueError):
                    existing = None
                if existing is None or (not math.isfinite(existing)) or existing <= 0:
                    existing = None
                quote_obj.price = existing

        # [INSTRUMENTATION] Final quote result with all details
        if logger.isEnabledFor(logging.DEBUG):
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

        if quote_obj is not None:
            try:
                self._quote_cache[cache_key] = quote_obj
            except Exception:
                pass

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

        current_date = self.get_datetime().date()
        constraints = getattr(self, "_chain_constraints", None) or {}

        # PERF: intraday option strategies often ask for chains repeatedly (sometimes directly via
        # Strategy.get_chains, not via OptionsHelper). If we allow ThetaData's default horizon
        # (up to 2 years for equities) the chain builder will issue one strike-list request per
        # expiration, which can be thousands of requests (especially for SPX/SPXW daily expirations).
        #
        # Apply a conservative default max-expiration bound for intraday backtests unless the
        # strategy explicitly configured a different max_expiration_date via `_chain_constraints`.
        try:
            needs_default_max = not isinstance(constraints, dict) or constraints.get("max_expiration_date") is None
        except Exception:
            needs_default_max = True

        if needs_default_max and getattr(self, "_timestep", None) != "day":
            try:
                symbol_upper = (getattr(asset, "symbol", "") or "").upper()
                asset_type = str(getattr(asset, "asset_type", "") or "").lower()
                is_index_like = asset_type == "index" or symbol_upper in {
                    "SPX",
                    "SPXW",
                    "NDX",
                    "NDXP",
                    "RUT",
                    "RUTW",
                    "VIX",
                    "VIXW",
                    "XSP",
                    "DJX",
                    "OEX",
                    "XEO",
                }
                # Intraday backtests are especially sensitive to chain build fanout. Keep the
                # default horizon bounded (vs Theta's multi-year default), but conservative enough
                # not to break common 30-60DTE strategies. Strategies that truly need a different
                # horizon should set `_chain_constraints["max_expiration_date"]`.
                max_days_out = 45 if is_index_like else 60

                base_date = current_date
                try:
                    min_dt = constraints.get("min_expiration_date") if isinstance(constraints, dict) else None
                    if isinstance(min_dt, datetime):
                        min_dt = min_dt.date()
                    if isinstance(min_dt, date) and min_dt > base_date:
                        base_date = min_dt
                except Exception:
                    base_date = current_date

                max_expiration = base_date + timedelta(days=max_days_out)
                if isinstance(constraints, dict):
                    constraints = dict(constraints)
                else:
                    constraints = {}
                constraints["max_expiration_date"] = max_expiration
            except Exception:
                pass

        # PERF: `get_chains_cached()` hits parquet (local/S3) and `Chains(...)` normalizes expiry keys.
        # Option-heavy strategies can call `get_chains()` hundreds of times per trading day, which
        # becomes the dominant CPU cost in year-long backtests. Cache per (symbol, date, constraints)
        # within the backtest process; invalidate automatically as the simulated date advances.
        cache_date = getattr(self, "_chains_cache_date", None)
        if cache_date != current_date:
            self._chains_cache_date = current_date
            self._chains_cache = {}

        try:
            constraints_key = json.dumps(constraints, sort_keys=True, default=str)
        except Exception:
            constraints_key = str(constraints)

        cache_key = (getattr(asset, "symbol", str(asset)), str(getattr(asset, "asset_type", "")), current_date, constraints_key)
        cached = getattr(self, "_chains_cache", {}).get(cache_key)
        if cached is not None:
            return cached

        chains_dict = thetadata_helper.get_chains_cached(
            asset=asset,
            current_date=current_date,
            chain_constraints=constraints,
        )

        chains = Chains(chains_dict)
        try:
            self._chains_cache[cache_key] = chains
        except Exception:
            pass

        return chains
