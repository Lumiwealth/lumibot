from __future__ import annotations

import json
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from lumibot.constants import LUMIBOT_CACHE_FOLDER, LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset
from lumibot.tools.backtest_cache import CacheMode, get_backtest_cache
from lumibot.tools.data_downloader_queue_client import queue_request
from lumibot.tools.ibkr_secdef import (
    IbkrFuturesExchangeAmbiguousError,
    select_futures_exchange_from_secdef_search_payload,
)
from lumibot.tools.parquet_series_cache import ParquetSeriesCache

logger = logging.getLogger(__name__)

CACHE_SUBFOLDER = "ibkr"

# IBKR Client Portal Gateway caps historical responses at ~1000 datapoints per call.
IBKR_HISTORY_MAX_POINTS = 1000
IBKR_DEFAULT_CRYPTO_VENUE = "ZEROHASH"
IBKR_DEFAULT_HISTORY_SOURCE = "Trades"
IBKR_DEFAULT_FUTURES_EXCHANGE_FALLBACK = "CME"
IBKR_DEFAULT_INDEX_HISTORY_SOURCE = "Midpoint"
# Per-request period cap for daily stock/index history.
#
# Rationale: IBKR Client Portal Gateway caps daily-history responses at ~1000
# data points per call (``IBKR_HISTORY_MAX_POINTS``). Live testing against the
# production gateway on 2026-04-17 showed that ``period=5y`` already hits that
# ceiling (1000 bars returned, covering ~4 trading years), while ``period=2y``
# returned 323 bars and ``period=180d`` (the prior value) returned only ~125
# bars. Asking for less than the cap is pure inefficiency: multi-year
# backtests would need 40+ round-trips per symbol, and in practice the loop
# was completing after a single iteration (the initial fetch near real-now
# filled the cache, then the coverage check skipped the real historical
# window for every subsequent simulation date, resulting in flat "today"
# prices for the entire historical range — observed in Peter Credit Spreads
# backtest a83663bd where VIX was constant 14.2 across 2,010 simulated days
# and max drawdown was ``-94%``).
#
# With ``5y``:
#   - Each call returns up to 1000 bars (~4 years of daily data).
#   - An 8-year backtest is covered in ~2 paginated chunks per symbol/source.
#   - Short requests (e.g. "2 bars ending <date>") still receive a valid
#     response because IBKR honours ``startTime`` and returns the N bars
#     ending at that time, bounded by 1000.
#
# If a future need arises for sub-daily stock/index bars with very long
# windows, extend ``_history_period_for_request`` to pick a smaller period
# tailored to the bar size rather than tightening this cap.
IBKR_STOCK_INDEX_DAILY_MAX_PERIOD = "5y"
IBKR_DAILY_GAP_RETRY_TTL_SECONDS = 24 * 60 * 60
IBKR_DAILY_GAP_REPAIR_TOTAL_BUDGET_SECONDS = 15.0
IBKR_DAILY_GAP_REPAIR_MAX_MONTHS_PER_SERIES = 12

IBKR_CONID_NEGATIVE_CACHE_TTL_SECONDS = 24 * 60 * 60  # 24h (persisted via BacktestCacheManager when enabled)


class IbkrFuturesConidLookupError(RuntimeError):
    """Raised when IBKR cannot resolve a futures conid for a root/expiration.

    This is treated as a data-availability issue (not a platform crash) so backtests can
    complete while logging loudly.
    """


_FUTURES_EXCHANGE_CACHE: Dict[str, str] = {}
_FUTURES_EXCHANGE_CACHE_LOADED = False

_NEGATIVE_CONID_CACHE: Dict[str, Dict[str, Any]] = {}
_NEGATIVE_CONID_CACHE_LOADED = False
_IBKR_EQUITY_ACTIONS_CACHE: Dict[str, pd.DataFrame] = {}
_RUNTIME_CONID_CACHE: Dict[str, int] = {}
_RUNTIME_HISTORY_NO_DATA_WINDOWS: Dict[str, Tuple[datetime, datetime]] = {}
_RUNTIME_DAILY_GAP_CHECKED_WINDOWS: set[tuple[str, str, str]] = set()
_RUNTIME_DAILY_GAP_REPAIR_SECONDS_USED = 0.0
_DISABLE_CONIDS_REMOTE_UPLOAD = False
_LOGGED_CONIDS_REMOTE_UPLOAD_DISABLE = False
_LOGGED_HISTORY_ALIASES: set[str] = set()


def _truthy_env(var_name: str, default: str = "false") -> bool:
    raw = os.environ.get(var_name, default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _is_access_denied_error(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    return "access denied" in msg or "accessdenied" in msg


def _normalize_asset_type(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if "." in raw:
        raw = raw.split(".")[-1]
    return raw


def _alias_asset_for_ibkr_history(asset: Asset) -> tuple[Asset, Optional[str]]:
    """Map known strategy-facing index aliases to IBKR history symbols.

    Some strategy code uses weekly option roots (for example `SPXW`) as the
    index symbol for convenience. IBKR history resolves the underlying index as
    `SPX`, not `SPXW`. Keep this mapping local to history requests so option
    chains/orders are unaffected.
    """
    asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
    symbol = str(getattr(asset, "symbol", "") or "").strip().upper()
    if asset_type == "index" and symbol == "SPXW":
        return Asset("SPX", asset_type=Asset.AssetType.INDEX), "SPXW->SPX"
    return asset, None


def _enable_futures_bid_ask_derivation() -> bool:
    """Whether to derive bid/ask quotes for futures from Bid_Ask + Midpoint history.

    Default is disabled because:
    - Futures backtests in LumiBot are intended to fill off TRADES/OHLC by default.
    - IBKR Client Portal Bid_Ask/Midpoint history can be flaky and adds 2x request volume.
    """
    return os.environ.get("LUMIBOT_IBKR_ENABLE_FUTURES_BID_ASK", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }

def _futures_exchange_cache_file() -> Path:
    return Path(LUMIBOT_CACHE_FOLDER) / CACHE_SUBFOLDER / "futures_exchanges.json"


def _load_futures_exchange_cache() -> None:
    global _FUTURES_EXCHANGE_CACHE_LOADED
    if _FUTURES_EXCHANGE_CACHE_LOADED:
        return
    _FUTURES_EXCHANGE_CACHE_LOADED = True
    path = _futures_exchange_cache_file()
    if not path.exists():
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    if isinstance(payload, dict):
        for k, v in payload.items():
            if not k or not v:
                continue
            _FUTURES_EXCHANGE_CACHE[str(k).strip().upper()] = str(v).strip().upper()


def _persist_futures_exchange_cache() -> None:
    path = _futures_exchange_cache_file()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(_FUTURES_EXCHANGE_CACHE, indent=2, sort_keys=True), encoding="utf-8")
    except Exception:
        return
    try:
        cache = get_backtest_cache()
        cache.on_local_update(path, payload={"provider": "ibkr", "type": "futures_exchanges"})
    except Exception:
        pass


def _negative_conid_cache_file() -> Path:
    return Path(LUMIBOT_CACHE_FOLDER) / CACHE_SUBFOLDER / "conids_negative.json"


def _load_negative_conid_cache() -> None:
    """Load negative conid cache (best-effort) and prune stale entries."""
    global _NEGATIVE_CONID_CACHE_LOADED
    if _NEGATIVE_CONID_CACHE_LOADED:
        return
    _NEGATIVE_CONID_CACHE_LOADED = True

    path = _negative_conid_cache_file()
    cache_manager = get_backtest_cache()
    try:
        cache_manager.ensure_local_file(path, payload={"provider": "ibkr", "type": "conids_negative"})
    except Exception:
        pass

    if not path.exists():
        return

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        return

    now = float(time.time())
    changed = False
    for key, value in payload.items():
        if not isinstance(key, str) or not key:
            continue
        if not isinstance(value, dict):
            continue
        ts = value.get("ts")
        try:
            ts_f = float(ts)
        except Exception:
            ts_f = None
        if ts_f is None or (now - ts_f) > IBKR_CONID_NEGATIVE_CACHE_TTL_SECONDS:
            changed = True
            continue
        _NEGATIVE_CONID_CACHE[key] = value

    if changed:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(_NEGATIVE_CONID_CACHE, indent=2, sort_keys=True), encoding="utf-8")
            try:
                cache_manager.on_local_update(path, payload={"provider": "ibkr", "type": "conids_negative"})
            except Exception:
                pass
        except Exception:
            pass


def _record_negative_conid(*, key: str, reason: str, message: str) -> None:
    """Persist a negative cache marker so we stop hammering IBKR for invalid roots/expirations."""
    if not key:
        return
    _load_negative_conid_cache()
    now = float(time.time())
    _NEGATIVE_CONID_CACHE[key] = {"ts": now, "reason": str(reason), "message": str(message)}

    path = _negative_conid_cache_file()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(_NEGATIVE_CONID_CACHE, indent=2, sort_keys=True), encoding="utf-8")
    except Exception:
        return
    try:
        cache_manager = get_backtest_cache()
        cache_manager.on_local_update(path, payload={"provider": "ibkr", "type": "conids_negative"})
    except Exception:
        pass


def _clear_negative_conid(*, key: str) -> None:
    """Remove a stale negative conid marker after a later successful resolution."""
    if not key:
        return
    _load_negative_conid_cache()
    if key not in _NEGATIVE_CONID_CACHE:
        return
    _NEGATIVE_CONID_CACHE.pop(key, None)

    path = _negative_conid_cache_file()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(_NEGATIVE_CONID_CACHE, indent=2, sort_keys=True), encoding="utf-8")
    except Exception:
        return
    try:
        cache_manager = get_backtest_cache()
        cache_manager.on_local_update(path, payload={"provider": "ibkr", "type": "conids_negative"})
    except Exception:
        pass


def _date_from_yyyymmdd(value: str) -> Optional[date]:
    raw = str(value or "").strip()
    if not (raw.isdigit() and len(raw) == 8):
        return None
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except Exception:
        return None


def _expiration_date_only(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _is_future_or_current_expiration(value: Any) -> bool:
    expiration_date = _expiration_date_only(value)
    if expiration_date is None:
        return False
    return expiration_date >= datetime.now(timezone.utc).date()


def _resolve_futures_exchange(symbol: str) -> str:
    symbol_upper = str(symbol or "").strip().upper()
    if not symbol_upper:
        raise RuntimeError("IBKR futures exchange resolution requires a non-empty symbol")

    _load_futures_exchange_cache()
    cached = _FUTURES_EXCHANGE_CACHE.get(symbol_upper)
    if cached:
        return cached

    base_url = _downloader_base_url()
    url = f"{base_url}/ibkr/iserver/secdef/search"
    payload = queue_request(url=url, querystring={"symbol": symbol_upper, "secType": "FUT"}, headers=None, timeout=None)
    if payload is None:
        raise RuntimeError(f"IBKR secdef/search returned no payload for FUT symbol={symbol_upper!r}")

    exchange = select_futures_exchange_from_secdef_search_payload(symbol_upper, payload)
    _FUTURES_EXCHANGE_CACHE[symbol_upper] = exchange
    _persist_futures_exchange_cache()
    return exchange


def _us_futures_closed_interval(start_local: datetime, end_local: datetime) -> bool:
    """Return True if US futures are fully closed in [start_local, end_local).

    This is a deliberately simple rule-based calendar used to avoid repeated downloader fetches
    for known closed windows (daily maintenance + weekends). It is not intended to encode every
    CME holiday/early-close rule; those can still produce longer gaps that require vendor data.
    """
    try:
        start_ts = pd.Timestamp(start_local)
        end_ts = pd.Timestamp(end_local)
        if start_ts.tzinfo is None:
            start_ts = start_ts.tz_localize(LUMIBOT_DEFAULT_PYTZ)
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize(LUMIBOT_DEFAULT_PYTZ)
        start_ts = start_ts.tz_convert("America/New_York")
        end_ts = end_ts.tz_convert("America/New_York")
        if end_ts <= start_ts:
            return True
    except Exception:
        return False

    def _next_open(ts: pd.Timestamp) -> pd.Timestamp:
        ts = ts.tz_convert("America/New_York")
        dow = int(ts.weekday())  # Mon=0 .. Sun=6
        t = ts.time()

        # Saturday: closed all day; next open is Sunday 18:00 ET.
        if dow == 5:
            days = 1
            candidate = (ts + pd.Timedelta(days=days)).normalize() + pd.Timedelta(hours=18)
            return candidate.tz_localize("America/New_York") if candidate.tzinfo is None else candidate

        # Sunday: closed until 18:00 ET.
        if dow == 6:
            open_ts = ts.normalize() + pd.Timedelta(hours=18)
            open_ts = open_ts.tz_localize("America/New_York") if open_ts.tzinfo is None else open_ts
            return ts if ts >= open_ts else open_ts

        # Weekdays: closed daily 17:00–18:00 ET.
        if t >= datetime.min.replace(hour=17, minute=0, second=0).time() and t < datetime.min.replace(hour=18, minute=0, second=0).time():
            reopen = ts.normalize() + pd.Timedelta(hours=18)
            reopen = reopen.tz_localize("America/New_York") if reopen.tzinfo is None else reopen
            return reopen

        return ts

    try:
        next_open = _next_open(start_ts)
        return bool(next_open >= end_ts)
    except Exception:
        return False


@dataclass(frozen=True)
class IbkrConidKey:
    asset_type: str
    symbol: str
    quote_symbol: str
    exchange: str
    expiration: str

    def to_key(self) -> str:
        return "|".join(
            [
                self.asset_type or "",
                self.symbol or "",
                self.quote_symbol or "",
                self.exchange or "",
                self.expiration or "",
            ]
        )


def get_price_data(
    *,
    asset: Asset,
    quote: Optional[Asset],
    timestep: str,
    start_dt: datetime,
    end_dt: datetime,
    exchange: Optional[str] = None,
    include_after_hours: bool = True,
    source: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch IBKR historical bars (via the Data Downloader) and cache to parquet.

    This helper mirrors the ThetaData cache pattern:
    - local parquet under `LUMIBOT_CACHE_FOLDER/ibkr/...`
    - optional S3 mirroring via BacktestCacheManager
    - best-effort negative caching via `missing=True` placeholder rows (only for true NO_DATA)
    """
    # v4.5.1: defensive coercion. Callers occasionally hand us a bare string
    # symbol (observed in Alpha Picks 2026-04-19 local backtest: upstream code
    # path leaked a string through to this helper, producing misleading
    # "IBKR history fetch failed for None/USD ...: 'str' object has no
    # attribute 'symbol'" errors that obscured the real bug). Accepting a
    # string here is a one-line safety net that lets the rest of the pipeline
    # work, and the log message becomes accurate.
    if isinstance(asset, str):
        asset = Asset(symbol=asset)

    start_utc = _to_utc(start_dt)
    end_utc = _to_utc(end_dt)
    if start_utc > end_utc:
        start_utc, end_utc = end_utc, start_utc
    start_local = start_utc.astimezone(LUMIBOT_DEFAULT_PYTZ)
    end_local = end_utc.astimezone(LUMIBOT_DEFAULT_PYTZ)

    asset, history_alias = _alias_asset_for_ibkr_history(asset)
    if history_alias:
        alias_key = f"{history_alias}:{timestep}"
        if alias_key not in _LOGGED_HISTORY_ALIASES:
            logger.info("IBKR history symbol alias applied: %s", history_alias)
            _LOGGED_HISTORY_ALIASES.add(alias_key)

    asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
    asset_auto_expiry = getattr(asset, "auto_expiry", None)
    if asset_type == "future" and getattr(asset, "expiration", None) is None and not asset_auto_expiry:
        raise ValueError(
            "IBKR futures require an explicit expiration on Asset(asset_type='future') unless "
            "`auto_expiry` is set. Use asset_type='cont_future' for continuous futures."
        )
    effective_exchange = exchange
    if asset_type in {"future", "cont_future"} and not effective_exchange:
        try:
            effective_exchange = _resolve_futures_exchange(getattr(asset, "symbol", ""))
        except IbkrFuturesExchangeAmbiguousError:
            raise
        except Exception as exc:
            fallback = (os.environ.get("IBKR_FUTURES_EXCHANGE") or IBKR_DEFAULT_FUTURES_EXCHANGE_FALLBACK).strip().upper()
            logger.warning(
                "IBKR futures exchange auto-resolution failed for %s: %s. Falling back to %s",
                getattr(asset, "symbol", None),
                exc,
                fallback,
            )
            effective_exchange = fallback
    if asset_type == "crypto" and not effective_exchange:
        effective_exchange = (os.environ.get("IBKR_CRYPTO_VENUE") or IBKR_DEFAULT_CRYPTO_VENUE).strip().upper()

    # Treat the env var as explicit too.
    #
    # WHY: If the user explicitly chooses a history source via env vars (for example `Trades`),
    # we must not silently derive/augment bid/ask from other sources because that would change
    # execution semantics in backtests.
    env_source_raw = os.environ.get("IBKR_HISTORY_SOURCE")
    env_source_was_explicit = False
    if env_source_raw is not None:
        trimmed = env_source_raw.strip()
        if trimmed and trimmed.lower() != "none":
            env_source_was_explicit = True

    source_was_explicit = source is not None or env_source_was_explicit
    history_source = _normalize_history_source(source)
    # IBKR index history is frequently unavailable with `Trades`. Force midpoint bars for index
    # requests unless a per-call source was explicitly provided.
    if source is None and asset_type == "index":
        history_source = IBKR_DEFAULT_INDEX_HISTORY_SOURCE

    # Normalize timestep classification once so callers can pass "day", "1d", "1day", etc.
    try:
        _bar, _bar_seconds, timestep_component = _timestep_to_ibkr_bar(timestep)
    except Exception:
        timestep_component = _timestep_component(timestep)

    # Continuous futures
    #
    # IMPORTANT (expired explicit futures support):
    # IBKR's Client Portal API does not reliably expose conids for *expired* futures contracts.
    # To backtest `cont_future` deterministically (and to support explicit expired contracts),
    # LumiBot uses a local conid registry (`ibkr/conids.json`) populated via a one-time TWS
    # backfill. `cont_future` data is stitched by resolving each contract month using LumiBot's
    # roll schedule (see `_resolve_cont_future_segments`), then fetching bars per-expiration.

    is_roll_wrapper = bool(
        asset_type == "cont_future"
        or (
            asset_type == "future"
            and getattr(asset, "expiration", None) is None
            and asset_auto_expiry
        )
    )

    # Cont-futures + Auto-expiry futures
    #
    # Behavior: for IBKR we treat `Asset(asset_type='cont_future')` and
    # `Asset(asset_type='future', auto_expiry=...)` as synthetic roll wrappers.
    #
    # Rationale: backtests must match live semantics and must not depend on `date.today()` for
    # selecting a contract month.
    if is_roll_wrapper:
        segments = _resolve_cont_future_segments(asset=asset, start_dt=start_utc, end_dt=end_utc, exchange=effective_exchange)
        if not segments:
            logger.error(
                "IBKR futures roll wrapper could not resolve any roll segments for %s (type=%s exchange=%s). Returning empty bars.",
                getattr(asset, "symbol", None),
                asset_type,
                effective_exchange,
            )
            return pd.DataFrame()
        # Ensure the *user-facing* cont_future asset (used in orders/positions) carries the
        # correct contract metadata (multiplier/min_tick). Otherwise PnL and tick rounding will
        # be wrong even if we fetch bars for the right underlying expirations.
        #
        # We copy metadata from the first roll segment, since multiplier/minTick are stable
        # across expirations for a given root (e.g., MES, ES).
        try:
            first_asset, _, _ = segments[0]
            _maybe_apply_future_contract_metadata(asset=first_asset, exchange=effective_exchange)
            first_multiplier = getattr(first_asset, "multiplier", None)
            if first_multiplier not in (None, 0, 1):
                try:
                    asset.multiplier = first_multiplier  # type: ignore[assignment]
                except Exception:
                    pass
            first_min_tick = getattr(first_asset, "min_tick", None)
            if first_min_tick not in (None, 0):
                try:
                    setattr(asset, "min_tick", first_min_tick)
                except Exception:
                    pass
        except Exception:
            pass

        frames: list[pd.DataFrame] = []
        for i, (seg_asset, seg_start, seg_end) in enumerate(segments):
            # Clamp each segment to the requested window.
            seg_start = _to_utc(seg_start)
            seg_end = _to_utc(seg_end)

            # IMPORTANT: `Strategy.get_last_price()` is evaluated at bar boundaries, and our
            # futures backtesting semantics treat "last price at dt" as the last completed bar
            # (i.e., previous bar close).
            #
            # At roll boundaries, the *first* bar of the new contract often occurs exactly one
            # minute before the roll trigger (`roll_dt + 1 minute` in `futures_roll`), so the
            # previous-bar lookup at the roll timestamp needs the new contract's final pre-roll
            # minute available.
            #
            # Fix: for every segment after the first, widen the fetch window by 1 minute on the
            # left so the stitched series contains that preceding bar. We rely on "keep=last"
            # de-duping so the newer contract overrides overlaps deterministically.
            if i > 0:
                seg_start = seg_start - timedelta(minutes=1)

            seg_start = max(seg_start, start_utc)
            seg_end = min(_to_utc(seg_end), end_utc)
            if seg_start >= seg_end:
                continue
            df_seg = get_price_data(
                asset=seg_asset,
                quote=quote,
                timestep=timestep,
                start_dt=seg_start,
                end_dt=seg_end,
                exchange=effective_exchange,
                include_after_hours=include_after_hours,
                source=source,
            )
            if df_seg is not None and not df_seg.empty:
                frames.append(df_seg)
        if not frames:
            return pd.DataFrame()
        stitched = pd.concat(frames, axis=0)
        stitched = stitched[~stitched.index.duplicated(keep="last")]
        stitched = stitched.sort_index()
        return stitched.loc[(stitched.index >= start_local) & (stitched.index <= end_local)]

    # IMPORTANT (IBKR crypto daily semantics):
    # IBKR's `bar=1d` history is not a clean midnight-to-midnight 24/7 day series for crypto.
    # Daily-cadence strategies in LumiBot typically advance the simulation clock at midnight in
    # the strategy timezone. If we treat IBKR daily bars as authoritative, the series often
    # "ends" at a non-midnight timestamp and can lag by days, which triggers Data.checker()
    # stale-end errors and repeated refreshes (extremely slow; looks like "missing BTC data").
    #
    # Fix: for crypto only, derive daily bars from intraday history and align them to midnight
    # buckets in `LUMIBOT_DEFAULT_PYTZ`.
    if asset_type == "crypto" and str(timestep_component).endswith("day"):
        return _get_crypto_daily_bars(
            asset=asset,
            quote=quote,
            start_dt=start_utc,
            end_dt=end_utc,
            exchange=effective_exchange,
            include_after_hours=include_after_hours,
            source=history_source,
        )

    if asset_type in {"future", "cont_future"} and str(timestep_component).endswith("day"):
        _maybe_apply_future_contract_metadata(asset=asset, exchange=effective_exchange)
        return _get_futures_daily_bars(
            asset=asset,
            quote=quote,
            start_dt=start_utc,
            end_dt=end_utc,
            exchange=effective_exchange,
            include_after_hours=include_after_hours,
            source=history_source,
        )

    if asset_type in {"future", "cont_future"}:
        _maybe_apply_future_contract_metadata(asset=asset, exchange=effective_exchange)

    cache_file = _cache_file_for(
        asset=asset,
        quote=quote,
        timestep=timestep,
        exchange=effective_exchange,
        source=history_source,
        include_after_hours=include_after_hours,
    )
    runtime_no_data_key = str(cache_file)
    cache_manager = get_backtest_cache()

    try:
        cache_manager.ensure_local_file(
            cache_file,
            payload=_remote_payload(
                asset,
                quote,
                timestep,
                effective_exchange,
                history_source,
                include_after_hours=include_after_hours,
            ),
        )
    except Exception:
        pass

    df_cache = _read_cache_frame(cache_file)
    # If this series came from a cached parquet (e.g., prefilled via TWS), it may not include the
    # synthetic bid/ask fallback columns that we add when decoding Client Portal history. For
    # non-explicit history sources, populate bid/ask from close so quote-based fill logic (and
    # SMART_LIMIT) remains functional without forcing extra history requests.
    if (
        not source_was_explicit
        and not df_cache.empty
        and "close" in df_cache.columns
        and (("bid" not in df_cache.columns) or ("ask" not in df_cache.columns))
    ):
        df_cache = df_cache.copy()
        close = pd.to_numeric(df_cache.get("close"), errors="coerce")

        if "bid" in df_cache.columns:
            bid = pd.to_numeric(df_cache.get("bid"), errors="coerce")
        else:
            bid = pd.Series(index=df_cache.index, dtype="float64")
        df_cache["bid"] = bid.where(~bid.isna(), close)

        if "ask" in df_cache.columns:
            ask = pd.to_numeric(df_cache.get("ask"), errors="coerce")
        else:
            ask = pd.Series(index=df_cache.index, dtype="float64")
        df_cache["ask"] = ask.where(~ask.isna(), close)
    if not df_cache.empty:
        coverage_start = df_cache.index.min()
        coverage_end = df_cache.index.max()
    else:
        coverage_start = None
        coverage_end = None

    # Detect disjoint cached segments.
    #
    # IBKR parquet caches (especially when hydrated from remote S3) can contain disjoint segments
    # where `coverage_start..coverage_end` spans a large range but the requested window is only
    # partially covered (or not covered near one boundary). If we only look at global min/max
    # coverage we can incorrectly treat a request as a cache hit and return empty/underfilled bars.
    window_slice = pd.DataFrame()
    window_cov_start = None
    window_cov_end = None
    try:
        if coverage_start is not None and coverage_end is not None:
            window_slice = df_cache.loc[(df_cache.index >= start_local) & (df_cache.index <= end_local)]
            if not window_slice.empty:
                window_cov_start = window_slice.index.min()
                window_cov_end = window_slice.index.max()
    except Exception:
        window_slice = pd.DataFrame()
        window_cov_start = None
        window_cov_end = None

    # IBKR history can legitimately omit the very last bar(s) of a window (e.g., missing the final
    # 1–2 minutes of the day). When this happens, repeatedly trying to "fill to the end" creates
    # unnecessary downloader traffic and can wedge CI acceptance runs.
    #
    # Treat the cached series as "good enough" if it's within 2 bars of the requested end.
    #
    # Futures also have an expected daily maintenance gap (~1 hour). If a request window begins
    # during a closed period and the cache starts at the next session open, do not try to fetch
    # the closed interval (it will return empty and can trigger retry loops).
    end_tolerance = timedelta(0)
    start_tolerance = timedelta(0)
    bar_step = timedelta(0)
    try:
        ibkr_bar, _, _ = _timestep_to_ibkr_bar(timestep)

        def _bar_delta(bar: str) -> timedelta:
            b = (bar or "").strip().lower()
            if b.endswith("min"):
                return timedelta(minutes=int(b.removesuffix("min") or "1"))
            if b.endswith("h"):
                return timedelta(hours=int(b.removesuffix("h") or "1"))
            if b.endswith("d"):
                return timedelta(days=int(b.removesuffix("d") or "1"))
            return timedelta(0)

        bar_step = _bar_delta(ibkr_bar)
        end_tolerance = bar_step * 3
        start_tolerance = bar_step * 3
        if asset_type in {"future", "cont_future"}:
            start_tolerance = max(start_tolerance, timedelta(hours=1))
    except Exception:
        end_tolerance = timedelta(0)
        start_tolerance = timedelta(0)
        bar_step = timedelta(0)

    window_start_gap_closed = (
        asset_type in {"future", "cont_future"}
        and window_cov_start is not None
        and start_local < window_cov_start
        and _us_futures_closed_interval(start_local, window_cov_start)
    )
    cache_start_gap_closed = (
        asset_type in {"future", "cont_future"}
        and coverage_start is not None
        and start_local < coverage_start
        and _us_futures_closed_interval(start_local, coverage_start)
    )
    window_end_gap_closed = (
        asset_type in {"future", "cont_future"}
        and window_cov_end is not None
        and end_local > window_cov_end
        and _us_futures_closed_interval(window_cov_end + bar_step, end_local)
    )
    cache_end_gap_closed = (
        asset_type in {"future", "cont_future"}
        and coverage_end is not None
        and end_local > coverage_end
        and _us_futures_closed_interval(coverage_end + bar_step, end_local)
    )

    needs_fetch = (
        coverage_start is None
        or coverage_end is None
        # If the requested window has no rows at all (even though the overall cache has a broad
        # min/max range), treat it as a cache miss and fetch that specific segment.
        or (coverage_start is not None and coverage_end is not None and window_slice.empty)
        # Missing coverage near the requested boundaries (disjoint segments within the window).
        or (
            window_cov_start is not None
            and start_local < window_cov_start
            and not window_start_gap_closed
            and (start_tolerance <= timedelta(0) or (window_cov_start - start_local) > start_tolerance)
        )
        or (
            window_cov_end is not None
            and end_local > window_cov_end
            and not window_end_gap_closed
            and (end_tolerance <= timedelta(0) or (end_local - window_cov_end) > end_tolerance)
        )
        or (
            coverage_start is not None
            and start_local < coverage_start
            and not cache_start_gap_closed
            and (start_tolerance <= timedelta(0) or (coverage_start - start_local) > start_tolerance)
        )
        or (
            end_local > coverage_end
            and not cache_end_gap_closed
            and (end_tolerance <= timedelta(0) or (end_local - coverage_end) > end_tolerance)
        )
    )
    blocked_window = _RUNTIME_HISTORY_NO_DATA_WINDOWS.get(runtime_no_data_key)
    if blocked_window is not None:
        blocked_start, blocked_end = blocked_window
        if start_utc >= blocked_start and end_utc <= blocked_end:
            needs_fetch = False
    # Persisted no-data suppression:
    #
    # `_record_missing_window()` writes placeholder markers to parquet so we can skip repeated
    # no-data fetches across runs (not only within this process). If the requested window is fully
    # bracketed by placeholder markers and contains no real bars, treat it as a cache hit.
    if needs_fetch and _window_is_placeholder_covered(df_cache, start_local=start_local, end_local=end_local):
        needs_fetch = False

    # Opt-in trace: log why we decided to hit the network, so we can audit measured-pass cache misses.
    if needs_fetch and os.environ.get("LUMIBOT_CACHE_MISS_DEBUG"):
        try:
            logger.warning(
                "[CACHE_MISS] %s %s ts=%s start_local=%s end_local=%s cov_start=%s cov_end=%s "
                "win_cov_start=%s win_cov_end=%s win_empty=%s start_tol=%s end_tol=%s "
                "win_start_gap_closed=%s win_end_gap_closed=%s cache_start_gap_closed=%s cache_end_gap_closed=%s",
                getattr(asset, "symbol", None),
                asset_type,
                timestep,
                start_local,
                end_local,
                coverage_start,
                coverage_end,
                window_cov_start,
                window_cov_end,
                bool(window_slice.empty) if coverage_start is not None else None,
                start_tolerance,
                end_tolerance,
                window_start_gap_closed,
                window_end_gap_closed,
                cache_start_gap_closed,
                cache_end_gap_closed,
            )
        except Exception:
            pass

    if needs_fetch:
        segments: list[tuple[datetime, datetime]] = []
        if coverage_start is None or coverage_end is None or window_slice.empty:
            segments.append((start_utc, end_utc))
        else:
            # Prefer window-local coverage for disjoint-segment detection.
            effective_start = window_cov_start or coverage_start
            effective_end = window_cov_end or coverage_end

            # If the requested window has no overlap with the cached window, do NOT try to "bridge"
            # the gap. Fetch exactly the requested window and merge it into the cache as a disjoint
            # segment. Bridging can turn a 1-hour request into months of downloads.
            if end_local < effective_start or start_local > effective_end:
                segments.append((start_utc, end_utc))
            else:
                if effective_start is not None and start_local < effective_start:
                    segments.append((start_utc, effective_start.astimezone(timezone.utc)))
                if effective_end is not None and end_local > effective_end:
                    segments.append((effective_end.astimezone(timezone.utc), end_utc))

        for seg_start, seg_end in segments:
            if seg_start >= seg_end:
                continue
            prev_max = df_cache.index.max() if not df_cache.empty else None
            try:
                fetched = _fetch_history_between_dates(
                    asset=asset,
                    quote=quote,
                    timestep=timestep,
                    start_dt=seg_start,
                    end_dt=seg_end,
                    exchange=effective_exchange,
                    include_after_hours=include_after_hours,
                    source=history_source,
                    source_was_explicit=source_was_explicit,
                )
            except Exception as exc:
                # Avoid crashing the entire backtest on entitlement/session issues. Return an empty
                # frame so strategies can continue with a loud error in logs.
                logger.error(
                    "IBKR history fetch failed for %s/%s timestep=%s exchange=%s source=%s: %s",
                    getattr(asset, "symbol", None),
                    getattr(quote, "symbol", None) if quote else None,
                    timestep,
                    effective_exchange,
                    history_source,
                    exc,
                )
                terminal_no_data = _is_terminal_no_data_error(exc)
                # If IBKR explicitly reports a terminal no-data condition (for example
                # "Chart data unavailable"), record the missing window so we don't hammer the same
                # request on every subsequent iteration.
                if terminal_no_data:
                    try:
                        # Suppress repeat fetches for the same cached series within this process.
                        existing_block = _RUNTIME_HISTORY_NO_DATA_WINDOWS.get(runtime_no_data_key)
                        if existing_block is None:
                            _RUNTIME_HISTORY_NO_DATA_WINDOWS[runtime_no_data_key] = (start_utc, end_utc)
                        else:
                            _RUNTIME_HISTORY_NO_DATA_WINDOWS[runtime_no_data_key] = (
                                min(existing_block[0], start_utc),
                                max(existing_block[1], end_utc),
                            )
                        _record_missing_window(
                            asset=asset,
                            quote=quote,
                            timestep=timestep,
                            exchange=effective_exchange,
                            source=history_source,
                            include_after_hours=include_after_hours,
                            # Mark the whole requested window for this get_price_data call so
                            # subsequent iterations don't re-submit near-identical failing slices.
                            start_dt=_to_utc(start_utc),
                            end_dt=_to_utc(end_utc),
                        )
                        # Reload to include the newly written missing markers.
                        df_cache = _read_cache_frame(cache_file)
                    except Exception:
                        pass
                fetched = pd.DataFrame()
                if terminal_no_data:
                    # No-data terminal errors are not recoverable by trying more segments in the
                    # same iteration/window.
                    break
            if fetched is not None and not fetched.empty:
                merged = _merge_frames(df_cache, fetched)
                _write_cache_frame(cache_file, merged)
                df_cache = merged
                # IBKR can return the "latest available" bars even when the requested cursor_end is
                # beyond the true available range (holiday/early close/entitlement gaps). In that
                # case, `fetched` may contain *no newer bars* than the existing cache. Without an
                # explicit negative cache marker, the caller will keep re-submitting the same
                # history request as the backtest clock advances.
                try:
                    new_max = df_cache.index.max() if not df_cache.empty else None
                    if prev_max is not None and new_max is not None and new_max <= prev_max:
                        prev_max_utc = _to_utc(prev_max.to_pydatetime() if hasattr(prev_max, "to_pydatetime") else prev_max)
                        seg_start_utc = _to_utc(seg_start)
                        seg_end_utc = _to_utc(seg_end)
                        is_tail_extension = abs((seg_start_utc - prev_max_utc).total_seconds()) <= 1.0 and seg_end_utc > prev_max_utc
                        if is_tail_extension:
                            missing_start = prev_max_utc + timedelta(seconds=1)
                            if missing_start < seg_end_utc:
                                _record_missing_window(
                                    asset=asset,
                                    quote=quote,
                                    timestep=timestep,
                                    exchange=effective_exchange,
                                    source=history_source,
                                    include_after_hours=include_after_hours,
                                    start_dt=missing_start,
                                    end_dt=seg_end_utc,
                                )
                                df_cache = _read_cache_frame(cache_file)
                except Exception:
                    pass

    if (
        not df_cache.empty
        and asset_type in {"stock", "index"}
        and str(timestep_component).endswith("day")
    ):
        df_cache = _repair_us_stock_index_daily_gaps(
            df_cache,
            cache_file=cache_file,
            asset=asset,
            quote=quote,
            timestep=timestep,
            start_dt=start_utc,
            end_dt=end_utc,
            exchange=effective_exchange,
            include_after_hours=include_after_hours,
            source=history_source,
            source_was_explicit=source_was_explicit,
        )

    if df_cache.empty:
        return df_cache

    # Best-effort: derive actionable bid/ask quotes for crypto *minute* bars so quote-based fills
    # behave realistically (buy at ask, sell at bid). IBKR history does not return separate
    # bid/ask fields, so we reconstruct them from Bid_Ask + Midpoint when needed.
    #
    # IMPORTANT (performance): do not do this for daily series (and avoid doing it for large
    # multi-month windows unless required) because it multiplies request volume.
    if (not source_was_explicit) and asset_type == "crypto" and str(timestep_component).endswith("minute"):
        df_aug, changed = _maybe_augment_crypto_bid_ask(
            df_cache=df_cache,
            asset=asset,
            quote=quote,
            timestep=timestep,
            start_dt=start_utc,
            end_dt=end_utc,
            exchange=effective_exchange,
            include_after_hours=include_after_hours,
        )
        if changed:
            _write_cache_frame(cache_file, df_aug)
            df_cache = df_aug

    if (
        _enable_futures_bid_ask_derivation()
        and (not source_was_explicit)
        and asset_type in {"future", "cont_future"}
        and str(timestep_component).endswith(("minute", "hour"))
    ):
        df_aug, changed = _maybe_augment_futures_bid_ask(
            df_cache=df_cache,
            asset=asset,
            quote=quote,
            timestep=timestep,
            start_dt=start_utc,
            end_dt=end_utc,
            exchange=effective_exchange,
            include_after_hours=include_after_hours,
        )
        if changed:
            _write_cache_frame(cache_file, df_aug)
            df_cache = df_aug

    if asset_type in {"stock", "index"} and str(timestep_component).endswith("day"):
        # Align cached daily bars to session close BEFORE slicing by [start_local, end_local].
        # This avoids same-day lookahead at market open when IBKR timestamps day bars near
        # session open/midnight boundaries.
        aligned_cache = _align_stock_index_daily_to_session_close(df_cache)
        if not aligned_cache.index.equals(df_cache.index):
            _write_cache_frame(cache_file, aligned_cache)
        df_cache = aligned_cache

    # Enrich daily equity cache with corporate actions so dividend accounting and splits are
    # consistent with other providers. This is intentionally best-effort.
    if asset_type == "stock" and str(timestep_component).endswith("day"):
        enriched_cache, changed = _append_equity_corporate_actions_daily(df_cache, asset)
        normalized_cache, normalized_changed = _normalize_equity_daily_prices_for_splits(enriched_cache)
        if changed or normalized_changed:
            _write_cache_frame(cache_file, normalized_cache)
        df_cache = normalized_cache

    # Remove placeholder rows from the returned frame (but keep them in cache).
    frame = df_cache.loc[(df_cache.index >= start_local) & (df_cache.index <= end_local)].copy()
    if "missing" in frame.columns:
        frame = frame[~frame["missing"].fillna(False)]
        frame = frame.drop(columns=["missing"], errors="ignore")
    if asset_type in {"stock", "index"} and str(timestep_component).endswith("day"):
        frame = _repair_isolated_split_spikes_daily(frame)
    placeholder_covered = _window_is_placeholder_covered(df_cache, start_local=start_local, end_local=end_local)
    if not frame.empty:
        try:
            covers_requested_window = frame_covers_requested_window(
                frame,
                asset=asset,
                timestep=timestep,
                start_dt=start_utc,
                end_dt=end_utc,
            )
        except Exception:
            covers_requested_window = False
        if not covers_requested_window:
            logger.warning(
                "IBKR cached history remained underfilled after refresh for %s/%s timestep=%s exchange=%s source=%s; "
                "returning available real bars instead of synthesizing an empty dataset "
                "(placeholder_covered=%s)",
                getattr(asset, "symbol", None),
                getattr(quote, "symbol", None) if quote else None,
                timestep,
                effective_exchange,
                history_source,
                placeholder_covered,
            )
    elif placeholder_covered:
        return frame
    return frame


def _frame_has_actionable_bid_ask(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    if "bid" not in df.columns or "ask" not in df.columns:
        return False
    bid = pd.to_numeric(df["bid"], errors="coerce")
    ask = pd.to_numeric(df["ask"], errors="coerce")
    spread = ask - bid
    return bool((spread > 0).any())


def _align_stock_index_daily_to_session_close(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize IBKR stock/index daily timestamps to the session close.

    Why:
    - IBKR day bars are often timestamped near UTC midnight, which appears as ~04:00/05:00 ET.
    - Daily backtests that run at market open can then incorrectly treat the current session's
      bar as already available.
    - Re-indexing day bars to 16:00 ET aligns them with end-of-session semantics used by
      other providers (for example ThetaData day bars at 21:00 UTC in winter).
    """
    if df is None or df.empty:
        return df

    frame = df.sort_index().copy()
    idx = pd.DatetimeIndex(frame.index)
    if idx.tz is None:
        idx = idx.tz_localize(LUMIBOT_DEFAULT_PYTZ)
    else:
        idx = idx.tz_convert(LUMIBOT_DEFAULT_PYTZ)

    aligned_idx = idx.normalize() + pd.Timedelta(hours=16)
    frame.index = aligned_idx
    return _merge_frames(pd.DataFrame(), frame)


def _normalize_split_ratio(value: Any) -> Optional[float]:
    try:
        ratio = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(ratio) or ratio <= 0 or math.isclose(ratio, 1.0):
        return None
    return ratio


def _split_row_needs_price_adjustment(close: pd.Series, split_pos: int, ratio: float) -> bool:
    """Return True when the local split window still has a raw split-level jump.

    IBKR daily cache rows can be mixed: older rows may already be split-adjusted while
    a newly appended tail around a fresh split is still raw. A per-split continuity test
    prevents double-adjusting the already-normalized part of the cache.
    """
    if split_pos <= 0 or split_pos >= len(close):
        return False

    prev_close = close.iat[split_pos - 1]
    split_close = close.iat[split_pos]
    if (
        pd.isna(prev_close)
        or pd.isna(split_close)
        or prev_close <= 0
        or split_close <= 0
    ):
        return False

    observed = float(prev_close) / float(split_close)
    if observed <= 0:
        return False

    # In adjusted space, adjacent closes should be closer to 1x. In raw space,
    # the adjacent ratio should be closer to the split ratio.
    raw_score = abs(math.log(observed / ratio))
    adjusted_score = abs(math.log(observed))
    return raw_score < adjusted_score


def _find_split_raw_segment_start(close: pd.Series, split_pos: int, ratio: float) -> int:
    """Find where a mixed raw pre-split cache segment begins.

    If a cache was partially refreshed after a split, the rows immediately before the split
    can be raw while older rows are already adjusted. In that case there is usually a
    persistent split-factor level jump at the cache splice point. Return that splice row;
    if none is found, the whole history before the split is treated as raw.
    """
    for pos in range(split_pos - 1, 0, -1):
        prev_close = close.iat[pos - 1]
        cur_close = close.iat[pos]
        if (
            pd.isna(prev_close)
            or pd.isna(cur_close)
            or prev_close <= 0
            or cur_close <= 0
        ):
            continue
        observed = float(cur_close) / float(prev_close)
        if observed <= 0:
            continue
        raw_boundary_score = abs(math.log(observed / ratio))
        continuity_score = abs(math.log(observed))
        if raw_boundary_score < continuity_score:
            return pos
    return 0


def _normalize_equity_daily_prices_for_splits(frame: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    """Normalize IBKR stock daily bars into split-adjusted price space.

    LumiBot's stock backtesting providers generally expose Yahoo-style split-adjusted
    OHLC to strategies. IBKR daily cache data can arrive raw for newly appended split
    tails while older cached rows are already continuous. This function repairs only
    the split dates whose local price jump still looks raw, marks the frame as
    `_split_adjusted`, and lets the broker ledger skip share multiplication for the
    adjusted frame.
    """
    if frame is None or frame.empty or "stock_splits" not in frame.columns or "close" not in frame.columns:
        return frame, False

    out = frame.sort_index().copy()
    marker_all_adjusted = False
    if "_split_adjusted" in out.columns:
        marker = out["_split_adjusted"]
        try:
            marker_all_adjusted = bool(marker.fillna(False).astype(bool).all())
        except Exception:
            marker_all_adjusted = False

    close = pd.to_numeric(out["close"], errors="coerce")
    splits = pd.to_numeric(out["stock_splits"], errors="coerce").fillna(0.0)
    split_positions = [i for i, value in enumerate(splits.to_numpy()) if _normalize_split_ratio(value) is not None]
    if not split_positions:
        if marker_all_adjusted:
            return out, False
        out["_split_adjusted"] = True
        return out, True

    price_cols = [
        col
        for col in ("open", "high", "low", "close", "bid", "ask", "last", "vwap")
        if col in out.columns
    ]
    volume_cols = [col for col in ("volume",) if col in out.columns]
    dividend_cols = [col for col in ("dividend",) if col in out.columns]

    changed = False
    adjusted_splits = 0
    for split_pos in split_positions:
        ratio = _normalize_split_ratio(splits.iat[split_pos])
        if ratio is None:
            continue
        if not _split_row_needs_price_adjustment(close, split_pos, ratio):
            continue

        segment_start = _find_split_raw_segment_start(close, split_pos, ratio)
        before_mask = pd.Series(False, index=out.index)
        before_mask.iloc[segment_start:split_pos] = True
        for col in price_cols + dividend_cols:
            out.loc[before_mask, col] = pd.to_numeric(out.loc[before_mask, col], errors="coerce") / ratio
        for col in volume_cols:
            out.loc[before_mask, col] = pd.to_numeric(out.loc[before_mask, col], errors="coerce") * ratio

        close = pd.to_numeric(out["close"], errors="coerce")
        changed = True
        adjusted_splits += 1

    if not marker_all_adjusted or not out["_split_adjusted"].fillna(False).astype(bool).all():
        out["_split_adjusted"] = True
        changed = True

    if adjusted_splits:
        logger.warning("IBKR daily split normalization adjusted %s split boundary/boundaries.", adjusted_splits)
    return out, changed


def _repair_isolated_split_spikes_daily(df: pd.DataFrame) -> pd.DataFrame:
    """Repair isolated split-like spikes in daily stock/index bars.

    Why:
    - Some IBKR daily stock/index series can contain a one-day bar scaled by a split factor
      (2x/3x/...) that immediately reverts the next day.
    - This creates impossible +100%/+200% followed by -50%/-66% portfolio jumps.

    Scope:
    - Only isolated one-day anomalies are repaired.
    - Persistent level shifts are left untouched.
    """
    if df is None or df.empty or "close" not in df.columns or len(df) < 3:
        return df

    frame = df.sort_index().copy()
    candidate_cols = [c for c in ("open", "high", "low", "close", "bid", "ask", "last", "vwap") if c in frame.columns]
    if not candidate_cols:
        return frame

    numeric = {col: pd.to_numeric(frame[col], errors="coerce").copy() for col in candidate_cols}
    close = numeric["close"]

    factors = (2.0, 3.0, 4.0, 5.0, 10.0)
    factor_tol = 0.25
    reversion_tol = 0.25
    adjusted_rows = 0

    def _near(value: float, target: float, rel_tol: float) -> bool:
        if value <= 0 or target <= 0:
            return False
        return abs(value - target) <= abs(target) * rel_tol

    def _row_scales_like(idx: int, factor: float, upward: bool) -> bool:
        # Require most OHLC fields to indicate the same factor jump.
        ratios: list[float] = []
        for col in ("open", "high", "low", "close"):
            series = numeric.get(col)
            if series is None:
                continue
            prev_val = series.iat[idx - 1]
            cur_val = series.iat[idx]
            if pd.isna(prev_val) or pd.isna(cur_val) or prev_val <= 0 or cur_val <= 0:
                continue
            ratio = (cur_val / prev_val) if upward else (prev_val / cur_val)
            ratios.append(float(ratio))
        if len(ratios) < 2:
            return False
        hits = sum(1 for ratio in ratios if _near(ratio, factor, factor_tol))
        return hits >= max(2, len(ratios) - 1)

    def _trailing_level_stable(last_idx: int) -> bool:
        # Guard against mutating genuine regime shifts by requiring a stable
        # trailing level before we adjust a terminal-row spike.
        start = max(0, last_idx - 6)
        trailing = close.iloc[start:last_idx].dropna()
        if len(trailing) < 3:
            return False
        ref = float(trailing.median())
        if ref <= 0:
            return False
        return abs(float(close.iat[last_idx - 1]) / ref - 1.0) <= 0.35

    for i in range(1, len(frame) - 1):
        prev_close = close.iat[i - 1]
        cur_close = close.iat[i]
        next_close = close.iat[i + 1]
        if (
            pd.isna(prev_close)
            or pd.isna(cur_close)
            or pd.isna(next_close)
            or prev_close <= 0
            or cur_close <= 0
            or next_close <= 0
        ):
            continue

        action = None
        for factor in factors:
            # Upward spike on day i that reverts on day i+1.
            if (
                _near(float(cur_close / prev_close), factor, factor_tol)
                and _near(float(next_close / prev_close), 1.0, reversion_tol)
                and _row_scales_like(i, factor, upward=True)
            ):
                action = ("divide", factor)
                break

            # Downward spike on day i that reverts on day i+1.
            if (
                _near(float(prev_close / cur_close), factor, factor_tol)
                and _near(float(next_close / prev_close), 1.0, reversion_tol)
                and _row_scales_like(i, factor, upward=False)
            ):
                action = ("multiply", factor)
                break

        if action is None:
            continue

        op, factor = action
        for col, series in numeric.items():
            val = series.iat[i]
            if pd.isna(val):
                continue
            series.iat[i] = (val / factor) if op == "divide" else (val * factor)
        close = numeric["close"]
        adjusted_rows += 1

    # Terminal-row fallback:
    # In rolling backtests we often evaluate up to the current day only, so the latest row does
    # not yet have the next-day reversion available. Detect obvious split-factor spikes on that
    # final row using trailing-level stability as a safety check.
    last_i = len(frame) - 1
    if last_i >= 1 and _trailing_level_stable(last_i):
        prev_close = close.iat[last_i - 1]
        cur_close = close.iat[last_i]
        if (
            not pd.isna(prev_close)
            and not pd.isna(cur_close)
            and prev_close > 0
            and cur_close > 0
        ):
            terminal_action = None
            for factor in factors:
                if _near(float(cur_close / prev_close), factor, factor_tol) and _row_scales_like(last_i, factor, upward=True):
                    terminal_action = ("divide", factor)
                    break
                if _near(float(prev_close / cur_close), factor, factor_tol) and _row_scales_like(last_i, factor, upward=False):
                    terminal_action = ("multiply", factor)
                    break

            if terminal_action is not None:
                op, factor = terminal_action
                for col, series in numeric.items():
                    val = series.iat[last_i]
                    if pd.isna(val):
                        continue
                    series.iat[last_i] = (val / factor) if op == "divide" else (val * factor)
                close = numeric["close"]
                adjusted_rows += 1

    if adjusted_rows > 0:
        for col, series in numeric.items():
            frame[col] = series
        logger.warning("IBKR daily split-spike repair adjusted %s row(s).", adjusted_rows)

    return frame


def _resolve_cont_future_segments(*, asset: Asset, start_dt: datetime, end_dt: datetime, exchange: Optional[str]) -> list[tuple[Asset, datetime, datetime]]:
    """Resolve a `cont_future` asset into a list of explicit futures contract segments.

    This follows LumiBot's roll schedule (`lumibot.tools.futures_roll`) so that backtests
    match live broker semantics and remain consistent across backtesting environments.
    """
    try:
        from lumibot.tools import futures_roll
    except Exception:
        return []

    start_utc = _to_utc(start_dt)
    end_utc = _to_utc(end_dt)
    if start_utc > end_utc:
        start_utc, end_utc = end_utc, start_utc

    try:
        schedule = futures_roll.build_roll_schedule(asset, start_utc, end_utc, year_digits=2)
    except Exception:
        schedule = []
    if not schedule:
        return []

    segments: list[tuple[Asset, datetime, datetime]] = []
    for contract_symbol, seg_start, seg_end in schedule:
        year, month = _parse_contract_year_month(contract_symbol)
        expiration = _contract_expiration_date(asset.symbol, year=year, month=month)
        contract_asset = Asset(asset.symbol, asset_type=Asset.AssetType.FUTURE, expiration=expiration)
        # Validate that we can resolve an explicit conid for this contract month.
        try:
            _resolve_conid(asset=contract_asset, quote=None, exchange=exchange)
        except Exception as exc:
            # Do not crash the whole backtest because one contract month cannot resolve.
            #
            # This most commonly happens when:
            # - The root symbol is invalid (no contracts returned), or
            # - IBKR cannot discover a conid for an expired contract month via Client Portal.
            logger.error(
                "IBKR roll segment conid resolution failed for %s %s (%s): %s",
                getattr(asset, "symbol", None),
                expiration,
                contract_symbol,
                exc,
            )
            continue
        resolved_expiration = _expiration_date_only(getattr(contract_asset, "_ibkr_resolved_expiration", None))
        if resolved_expiration is not None and resolved_expiration != expiration:
            logger.warning(
                "IBKR roll segment %s computed expiration %s but IBKR lists %s; using listed expiration for history fetches.",
                contract_symbol,
                expiration,
                resolved_expiration,
            )
            contract_asset = Asset(asset.symbol, asset_type=Asset.AssetType.FUTURE, expiration=resolved_expiration)
        segments.append((contract_asset, _to_utc(seg_start), _to_utc(seg_end)))
    return segments


def _parse_contract_year_month(contract_symbol: str) -> tuple[int, int]:
    """Parse a futures contract symbol (e.g., MESZ25) into (year, month)."""
    symbol = (contract_symbol or "").strip().upper()
    if len(symbol) < 3:
        raise ValueError(f"Invalid contract symbol: {contract_symbol!r}")

    month_code = symbol[-3:-2]
    year_text = symbol[-2:]
    try:
        year_two = int(year_text)
    except Exception as exc:
        raise ValueError(f"Invalid futures year in {contract_symbol!r}") from exc

    # Assumption: 20xx is the relevant range for our backtests.
    year = 2000 + year_two

    try:
        from lumibot.tools import futures_roll

        reverse = {v: k for k, v in getattr(futures_roll, "_FUTURES_MONTH_CODES", {}).items()}
    except Exception:
        reverse = {"H": 3, "M": 6, "U": 9, "Z": 12}

    month = reverse.get(month_code)
    if month is None:
        raise ValueError(f"Invalid futures month code {month_code!r} in {contract_symbol!r}")
    return year, int(month)


def _contract_expiration_date(root_symbol: str, *, year: int, month: int):
    """Best-effort expiration date for a futures contract based on the roll rules."""
    try:
        from lumibot.tools import futures_roll

        rule = futures_roll.ROLL_RULES.get(str(root_symbol).upper())
        anchor = getattr(rule, "anchor", None) if rule else None

        if anchor == "third_last_business_day":
            expiry = futures_roll._third_last_business_day(year, month)
        elif anchor == "last_friday":
            expiry = futures_roll._last_friday_trading_day(year, month)
        elif anchor == "cl_last_trade":
            expiry = futures_roll._cl_last_trade_date(year, month)
        elif anchor == "mcl_last_trade":
            expiry = futures_roll._mcl_last_trade_date(year, month)
        else:
            # Default anchor for CME equity index futures is third Friday.
            expiry = futures_roll._third_friday(year, month)
        return expiry.date()
    except Exception:
        # Safe fallback: third Friday.
        from datetime import date, timedelta

        first = date(year, month, 1)
        days_until_friday = (4 - first.weekday()) % 7
        first_friday = first + timedelta(days=days_until_friday)
        third_friday = first_friday + timedelta(days=14)
        return third_friday


def _get_cached_bars_for_source(
    *,
    asset: Asset,
    quote: Optional[Asset],
    timestep: str,
    start_dt: datetime,
    end_dt: datetime,
    exchange: Optional[str],
    include_after_hours: bool,
    source: str,
) -> pd.DataFrame:
    start_utc = _to_utc(start_dt)
    end_utc = _to_utc(end_dt)
    if start_utc > end_utc:
        start_utc, end_utc = end_utc, start_utc
    start_local = start_utc.astimezone(LUMIBOT_DEFAULT_PYTZ)
    end_local = end_utc.astimezone(LUMIBOT_DEFAULT_PYTZ)

    history_source = _normalize_history_source(source)
    cache_file = _cache_file_for(
        asset=asset,
        quote=quote,
        timestep=timestep,
        exchange=exchange,
        source=history_source,
        include_after_hours=include_after_hours,
    )
    cache_manager = get_backtest_cache()
    try:
        cache_manager.ensure_local_file(
            cache_file,
            payload=_remote_payload(
                asset,
                quote,
                timestep,
                exchange,
                history_source,
                include_after_hours=include_after_hours,
            ),
        )
    except Exception:
        pass

    df_cache = _read_cache_frame(cache_file)
    if not df_cache.empty:
        coverage_start = df_cache.index.min()
        coverage_end = df_cache.index.max()
    else:
        coverage_start = None
        coverage_end = None

    end_tolerance = timedelta(0)
    try:
        ibkr_bar, _, _ = _timestep_to_ibkr_bar(timestep)

        def _bar_delta(bar: str) -> timedelta:
            b = (bar or "").strip().lower()
            if b.endswith("min"):
                return timedelta(minutes=int(b.removesuffix("min") or "1"))
            if b.endswith("h"):
                return timedelta(hours=int(b.removesuffix("h") or "1"))
            if b.endswith("d"):
                return timedelta(days=int(b.removesuffix("d") or "1"))
            return timedelta(0)

        end_tolerance = _bar_delta(ibkr_bar) * 3
    except Exception:
        end_tolerance = timedelta(0)

    needs_fetch = (
        coverage_start is None
        or coverage_end is None
        or start_local < coverage_start
        or (end_local > coverage_end and (end_tolerance <= timedelta(0) or (end_local - coverage_end) > end_tolerance))
    )

    if needs_fetch:
        segments: list[tuple[datetime, datetime]] = []
        if coverage_start is None or coverage_end is None:
            segments.append((start_utc, end_utc))
        else:
            if end_local < coverage_start or start_local > coverage_end:
                segments.append((start_utc, end_utc))
            else:
                if start_local < coverage_start:
                    segments.append((start_utc, coverage_start.astimezone(timezone.utc)))
                if end_local > coverage_end:
                    segments.append((coverage_end.astimezone(timezone.utc), end_utc))

        for seg_start, seg_end in segments:
            if seg_start >= seg_end:
                continue
            prev_max = df_cache.index.max() if not df_cache.empty else None
            try:
                fetched = _fetch_history_between_dates(
                    asset=asset,
                    quote=quote,
                    timestep=timestep,
                    start_dt=seg_start,
                    end_dt=seg_end,
                    exchange=exchange,
                    include_after_hours=include_after_hours,
                    source=history_source,
                    source_was_explicit=True,
                )
            except Exception as exc:
                logger.error(
                    "IBKR history fetch failed for %s/%s timestep=%s exchange=%s source=%s: %s",
                    getattr(asset, "symbol", None),
                    getattr(quote, "symbol", None) if quote else None,
                    timestep,
                    exchange,
                    history_source,
                    exc,
                )
                fetched = pd.DataFrame()
            if fetched is not None and not fetched.empty:
                merged = _merge_frames(df_cache, fetched)
                _write_cache_frame(cache_file, merged)
                df_cache = merged
                # IBKR can return the "latest available" bars even when the requested cursor_end is
                # beyond the true available range (holiday/early close/entitlement gaps). In that
                # case, `fetched` may contain *no newer bars* than the existing cache, and if we do
                # nothing we'll keep re-submitting the same history request in a loop as the
                # backtest clock advances.
                #
                # Negative-cache this "stale end" by recording a missing window that extends
                # coverage to the requested bound. The placeholder rows are filtered out before
                # returning bars, so this does not create synthetic liquidity.
                try:
                    new_max = df_cache.index.max() if not df_cache.empty else None
                    if prev_max is not None and new_max is not None and new_max <= prev_max:
                        prev_max_utc = _to_utc(prev_max.to_pydatetime() if hasattr(prev_max, "to_pydatetime") else prev_max)
                        seg_start_utc = _to_utc(seg_start)
                        seg_end_utc = _to_utc(seg_end)
                        is_tail_extension = abs((seg_start_utc - prev_max_utc).total_seconds()) <= 1.0 and seg_end_utc > prev_max_utc
                        if is_tail_extension:
                            # Start the missing window just *after* the last real bar to avoid
                            # clobbering the bar at `prev_max` when merging placeholder rows.
                            missing_start = prev_max_utc + timedelta(seconds=1)
                            if missing_start >= seg_end_utc:
                                continue
                            _record_missing_window(
                                asset=asset,
                                quote=quote,
                                timestep=timestep,
                                exchange=exchange,
                                source=history_source,
                                include_after_hours=include_after_hours,
                                start_dt=missing_start,
                                end_dt=seg_end_utc,
                            )
                            # Keep the in-memory view in sync for any further segment checks.
                            df_cache = _read_cache_frame(cache_file)
                except Exception:
                    pass

    if df_cache.empty:
        return df_cache

    frame = df_cache.loc[(df_cache.index >= start_local) & (df_cache.index <= end_local)].copy()
    if "missing" in frame.columns:
        frame = frame[~frame["missing"].fillna(False)]
        frame = frame.drop(columns=["missing"], errors="ignore")
    return frame


def _maybe_augment_crypto_bid_ask(
    *,
    df_cache: pd.DataFrame,
    asset: Asset,
    quote: Optional[Asset],
    timestep: str,
    start_dt: datetime,
    end_dt: datetime,
    exchange: Optional[str],
    include_after_hours: bool,
) -> tuple[pd.DataFrame, bool]:
    if df_cache is None or df_cache.empty:
        return df_cache, False
    if _frame_has_actionable_bid_ask(df_cache):
        return df_cache, False

    try:
        bid_ask = _get_cached_bars_for_source(
            asset=asset,
            quote=quote,
            timestep=timestep,
            start_dt=start_dt,
            end_dt=end_dt,
            exchange=exchange,
            include_after_hours=include_after_hours,
            source="Bid_Ask",
        )
        midpoint = _get_cached_bars_for_source(
            asset=asset,
            quote=quote,
            timestep=timestep,
            start_dt=start_dt,
            end_dt=end_dt,
            exchange=exchange,
            include_after_hours=include_after_hours,
            source="Midpoint",
        )
    except Exception:
        return df_cache, False

    derived = _derive_bid_ask_from_bid_ask_and_midpoint(bid_ask, midpoint)
    if derived is None or derived.empty:
        return df_cache, False

    updated = df_cache.copy()
    updated.loc[derived.index, "bid"] = derived["bid"]
    updated.loc[derived.index, "ask"] = derived["ask"]

    # Any residual NaNs fall back to the trade/mark close.
    if "close" in updated.columns:
        updated["bid"] = pd.to_numeric(updated.get("bid"), errors="coerce").where(
            ~pd.to_numeric(updated.get("bid"), errors="coerce").isna(),
            pd.to_numeric(updated.get("close"), errors="coerce"),
        )
        updated["ask"] = pd.to_numeric(updated.get("ask"), errors="coerce").where(
            ~pd.to_numeric(updated.get("ask"), errors="coerce").isna(),
            pd.to_numeric(updated.get("close"), errors="coerce"),
        )

    if not _frame_has_actionable_bid_ask(updated):
        return df_cache, False

    return updated, True


def _maybe_augment_futures_bid_ask(
    *,
    df_cache: pd.DataFrame,
    asset: Asset,
    quote: Optional[Asset],
    timestep: str,
    start_dt: datetime,
    end_dt: datetime,
    exchange: Optional[str],
    include_after_hours: bool,
) -> tuple[pd.DataFrame, bool]:
    if not _enable_futures_bid_ask_derivation():
        return df_cache, False
    if df_cache is None or df_cache.empty:
        return df_cache, False
    if _frame_has_actionable_bid_ask(df_cache):
        return df_cache, False

    try:
        bid_ask = _get_cached_bars_for_source(
            asset=asset,
            quote=quote,
            timestep=timestep,
            start_dt=start_dt,
            end_dt=end_dt,
            exchange=exchange,
            include_after_hours=include_after_hours,
            source="Bid_Ask",
        )
        midpoint = _get_cached_bars_for_source(
            asset=asset,
            quote=quote,
            timestep=timestep,
            start_dt=start_dt,
            end_dt=end_dt,
            exchange=exchange,
            include_after_hours=include_after_hours,
            source="Midpoint",
        )
    except Exception:
        return df_cache, False

    derived = _derive_bid_ask_from_bid_ask_and_midpoint(bid_ask, midpoint)
    if derived is None or derived.empty:
        return df_cache, False

    updated = df_cache.copy()
    updated.loc[derived.index, "bid"] = derived["bid"]
    updated.loc[derived.index, "ask"] = derived["ask"]

    if "close" in updated.columns:
        updated["bid"] = pd.to_numeric(updated.get("bid"), errors="coerce").where(
            ~pd.to_numeric(updated.get("bid"), errors="coerce").isna(),
            pd.to_numeric(updated.get("close"), errors="coerce"),
        )
        updated["ask"] = pd.to_numeric(updated.get("ask"), errors="coerce").where(
            ~pd.to_numeric(updated.get("ask"), errors="coerce").isna(),
            pd.to_numeric(updated.get("close"), errors="coerce"),
        )

    if not _frame_has_actionable_bid_ask(updated):
        return df_cache, False

    return updated, True


def _fetch_history_between_dates(
    *,
    asset: Asset,
    quote: Optional[Asset],
    timestep: str,
    start_dt: datetime,
    end_dt: datetime,
    exchange: Optional[str],
    include_after_hours: bool,
    source: str,
    source_was_explicit: bool,
    _period_override: Optional[str] = None,
    _record_missing_on_empty: bool = True,
    _queue_timeout: Optional[float] = None,
    _max_timeout_attempts: Optional[int] = None,
) -> pd.DataFrame:
    conid = _resolve_conid(asset=asset, quote=quote, exchange=exchange)
    bar, bar_seconds, _cache_timestep = _timestep_to_ibkr_bar(timestep)
    asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
    # IBKR's `continuous=true` is IBKR-specific roll behavior. For LumiBot `cont_future` assets
    # we prefer our own synthetic roll (explicit contract series per expiration) so parity is
    # stable across data providers. Only request IBKR "continuous" when we truly do not have an
    # explicit expiration to anchor the contract.
    continuous = bool(asset_type == "cont_future" and getattr(asset, "expiration", None) is None)
    period = _period_override or _history_period_for_request(asset_type=asset_type, bar=bar, source=source)

    cursor_end = _to_utc(end_dt)
    start_dt = _to_utc(start_dt)
    chunks: list[pd.DataFrame] = []

    # Opt-in trace: log every real network fetch + caller, to audit cache-miss root causes.
    if os.environ.get("LUMIBOT_CACHE_MISS_DEBUG"):
        try:
            import traceback
            stack = "".join(traceback.format_stack(limit=10)[:-1])
            logger.warning(
                "[FETCH] sym=%s type=%s ts=%s start=%s end=%s source=%s\n%s",
                getattr(asset, "symbol", None),
                asset_type,
                timestep,
                start_dt.isoformat(),
                end_dt.isoformat(),
                source,
                stack,
            )
        except Exception:
            pass

    # Fetch backwards (end -> start) to accommodate IBKR's 1000 datapoint cap.
    while cursor_end > start_dt:
        payload = _ibkr_history_request(
            conid=conid,
            period=period,
            bar=bar,
            start_time=cursor_end,
            exchange=exchange,
            include_after_hours=include_after_hours,
            continuous=continuous,
            source=source,
            queue_timeout=_queue_timeout,
            max_timeout_attempts=_max_timeout_attempts,
        )

        # IBKR typically returns {"data":[...]} (empty list means no data).
        data = payload.get("data") if isinstance(payload, dict) else None
        if not data:
            # If we already fetched earlier chunks, keep them and stop paging.
            # CME weekend/maintenance gaps (Fri 4pm CT → Sun 5pm CT, nightly 4–5pm CT)
            # make a backward-paging window land in a no-trading range and return empty
            # even when the surrounding bars are valid. Raising here discards every
            # chunk we already collected and poisons the cache with a broad
            # missing-window marker — then every subsequent call for the same asset
            # takes the slow retry path. Break out with what we have.
            if chunks:
                break

            if _record_missing_on_empty:
                _record_missing_window(
                    asset=asset,
                    quote=quote,
                    timestep=timestep,
                    exchange=exchange,
                    source=source,
                    include_after_hours=include_after_hours,
                    start_dt=start_dt,
                    end_dt=cursor_end,
                )
            return pd.DataFrame()

        df = _history_payload_to_frame(data, source_was_explicit=source_was_explicit)
        if df.empty:
            # Same rationale as the empty-data branch above: an intermediate empty
            # page during backward pagination must not discard earlier chunks.
            if chunks:
                break

            if _record_missing_on_empty:
                _record_missing_window(
                    asset=asset,
                    quote=quote,
                    timestep=timestep,
                    exchange=exchange,
                    source=source,
                    include_after_hours=include_after_hours,
                    start_dt=start_dt,
                    end_dt=cursor_end,
                )
            return pd.DataFrame()

        chunks.append(df)

        earliest = df.index.min()
        if earliest is None:
            break
        if earliest <= start_dt:
            break

        # Move cursor backwards.
        #
        # IBKR history bounds are effectively inclusive, and we de-dupe on merge anyway, so it is
        # safer to continue from `earliest` instead of subtracting a whole bar (which can skip the
        # requested start boundary for coarse bars like 1h/1d).
        next_cursor_end = earliest
        if next_cursor_end >= cursor_end:
            next_cursor_end = earliest - pd.Timedelta(seconds=bar_seconds)
        cursor_end = next_cursor_end

        # Do not assume `len(df) < 1000` implies we're at the start of history.
        # IBKR can return fewer bars due to gaps/vendor behavior; breaking early can leave large
        # holes (and can trigger stale-end refresh loops in daily backtests).

    if not chunks:
        return pd.DataFrame()

    merged = pd.concat(chunks, axis=0).sort_index()
    merged = merged[~merged.index.duplicated(keep="last")]
    # IMPORTANT: Do not clamp to the requested window here.
    #
    # IBKR can return the "latest available" bars even when the requested window is in the
    # future (or otherwise outside the available range). We still want to persist those bars
    # to the cache to warm future requests and avoid repeatedly hammering the downloader.
    #
    # The caller (`get_price_data`) performs the final slice for the requested time range.
    return merged


def _history_period_for_request(*, asset_type: str, bar: str, source: str) -> str:
    normalized_bar = (bar or "").strip().lower()
    if asset_type in {"stock", "index"} and normalized_bar.endswith("d"):
        return IBKR_STOCK_INDEX_DAILY_MAX_PERIOD
    return _max_period_for_bar(bar)


def frame_covers_requested_window(
    df: pd.DataFrame,
    *,
    asset: Asset,
    timestep: str,
    start_dt: datetime,
    end_dt: datetime,
) -> bool:
    if df is None or df.empty:
        return False

    asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
    try:
        bar, _, _ = _timestep_to_ibkr_bar(timestep)
    except Exception:
        return False

    tolerance = timedelta(0)
    normalized_bar = (bar or "").strip().lower()
    if normalized_bar.endswith("min"):
        tolerance = timedelta(minutes=int(normalized_bar.removesuffix("min") or "1")) * 3
    elif normalized_bar.endswith("h"):
        tolerance = timedelta(hours=int(normalized_bar.removesuffix("h") or "1")) * 3
    elif normalized_bar.endswith("d"):
        tolerance = timedelta(days=int(normalized_bar.removesuffix("d") or "1")) * 3

    frame = df
    if asset_type in {"stock", "index"} and normalized_bar.endswith("d"):
        try:
            frame = _align_stock_index_daily_to_session_close(df)
        except Exception:
            frame = df

    start_local = pd.Timestamp(start_dt)
    end_local = pd.Timestamp(end_dt)
    coverage_start = frame.index.min()
    coverage_end = frame.index.max()
    if coverage_start is None or coverage_end is None:
        return False

    try:
        if coverage_start.tzinfo is not None and start_local.tzinfo is None:
            start_local = start_local.tz_localize(coverage_start.tzinfo)
        elif coverage_start.tzinfo is None and start_local.tzinfo is not None:
            start_local = start_local.tz_localize(None)
        elif coverage_start.tzinfo is not None and start_local.tzinfo is not None:
            start_local = start_local.tz_convert(coverage_start.tzinfo)
    except Exception:
        pass

    try:
        if coverage_end.tzinfo is not None and end_local.tzinfo is None:
            end_local = end_local.tz_localize(coverage_end.tzinfo)
        elif coverage_end.tzinfo is None and end_local.tzinfo is not None:
            end_local = end_local.tz_localize(None)
        elif coverage_end.tzinfo is not None and end_local.tzinfo is not None:
            end_local = end_local.tz_convert(coverage_end.tzinfo)
    except Exception:
        pass

    return bool(
        coverage_start <= (start_local + tolerance)
        and coverage_end >= (end_local - tolerance)
    )


def _downloader_history_meta(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    meta = payload.get("_botspot_meta")
    if not isinstance(meta, dict):
        return {}
    if str(meta.get("provider") or "").strip().lower() != "ibkr":
        return {}
    return meta


def _ensure_cacheable_downloader_history_payload(payload: Any) -> None:
    meta = _downloader_history_meta(payload)
    if not meta:
        return
    classification = str(meta.get("classification") or "").strip().lower()
    cache_policy = str(meta.get("cache_write_policy") or "").strip().lower()
    if classification in {"complete", "explicit_no_data"} and cache_policy in {"allow", "negative_only"}:
        return
    raise RuntimeError(
        "IBKR downloader returned a non-cacheable history payload "
        f"(classification={classification or 'unknown'} cache_write_policy={cache_policy or 'unknown'})"
    )


def _ibkr_history_request(
    *,
    conid: int,
    period: str,
    bar: str,
    start_time: datetime,
    exchange: Optional[str],
    include_after_hours: bool,
    continuous: bool,
    source: str,
    queue_timeout: Optional[float] = None,
    max_timeout_attempts: Optional[int] = None,
) -> Dict[str, Any]:
    base_url = _downloader_base_url()
    url = f"{base_url}/ibkr/iserver/marketdata/history"
    # IBKR Client Portal history endpoint interprets `startTime` as UTC.
    #
    # If we format `startTime` in a local timezone (e.g. America/New_York) while IBKR treats it
    # as UTC, paginating in 1000-bar chunks can create DST-sized holes (~4h in summer, ~5h in
    # winter). We have observed these holes as ~4h02/~5h02 gaps at chunk boundaries in cached
    # parquet files, which then cascades into stale-bar execution and parity failures.
    start_time_utc = _to_utc(start_time)
    query = {
        "conid": str(int(conid)),
        "period": period,
        "bar": bar,
        "outsideRth": "true" if include_after_hours else "false",
        "source": source,
        "startTime": start_time_utc.strftime("%Y%m%d-%H:%M:%S"),
    }
    if continuous:
        query["continuous"] = "true"
    if exchange:
        query["exchange"] = str(exchange)

    queue_kwargs: Dict[str, Any] = {
        "url": url,
        "querystring": query,
        "headers": None,
        "timeout": queue_timeout,
    }
    if max_timeout_attempts is not None:
        queue_kwargs["max_timeout_attempts"] = max_timeout_attempts
    result = queue_request(**queue_kwargs)
    if result is None:
        return {}
    if isinstance(result, dict) and result.get("error"):
        err = str(result.get("error") or "")
        # IBKR occasionally rejects large day windows (e.g. 1000d) with "Chart data unavailable"
        # for symbols that do return data over shorter periods. Fall back to smaller windows before
        # surfacing a hard failure.
        if "chart data unavailable" in err.lower() and str(bar).lower().endswith("d"):
            for fallback_period in ("1y", "6m", "3m", "1m"):
                fallback_query = dict(query)
                fallback_query["period"] = fallback_period
                fallback_kwargs = dict(queue_kwargs)
                fallback_kwargs["querystring"] = fallback_query
                fallback_result = queue_request(**fallback_kwargs)
                if fallback_result is None:
                    continue
                if isinstance(fallback_result, dict) and fallback_result.get("error"):
                    continue
                return fallback_result
        # Do not treat entitlement errors as NO_DATA; surface them to the caller.
        raise RuntimeError(f"IBKR history error: {result.get('error')}")
    _ensure_cacheable_downloader_history_payload(result)
    return result


def _history_payload_to_frame(data: Any, *, source_was_explicit: bool) -> pd.DataFrame:
    df = pd.DataFrame(data)
    if df.empty:
        return df

    # IBKR payload columns: t(ms), o, h, l, c, v.
    rename = {"t": "timestamp", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
    df = df.rename(columns=rename)
    if "timestamp" not in df.columns:
        return pd.DataFrame()

    ts = pd.to_datetime(df["timestamp"], unit="ms", utc=True, errors="coerce")
    df = df.drop(columns=["timestamp"], errors="ignore")
    df.index = ts
    df = df[~df.index.isna()]
    df = df.sort_index()
    df.index = df.index.tz_convert(LUMIBOT_DEFAULT_PYTZ)
    df["missing"] = False
    # Default quote fields:
    #
    # When callers did not explicitly request a history `source`, we populate bid/ask with the
    # close as a fallback so quote-based fill logic remains functional even before we derive a
    # real spread (Bid_Ask + Midpoint).
    #
    # When the caller explicitly requests a history `source` (e.g., `source="Trades"` in a
    # deterministic parity suite), we intentionally DO NOT synthesize bid/ask so the engine
    # uses OHLC fills.
    if (not source_was_explicit) and "close" in df.columns:
        df["bid"] = df["close"]
        df["ask"] = df["close"]
    return df


def _derive_bid_ask_from_bid_ask_and_midpoint(
    bid_ask: pd.DataFrame,
    midpoint: pd.DataFrame,
) -> pd.DataFrame:
    """Derive per-bar bid/ask quotes using IBKR Bid_Ask + Midpoint history.

    IBKR's Client Portal history endpoint returns OHLC bars for different "sources":
    - Trades: prints-based bars
    - Midpoint: midpoint bars
    - Bid_Ask: IBKR-style BID_ASK bars (historically: open/low use bid, close/high use ask)

    The payload does NOT include separate bid/ask fields. For backtesting fills, we want a
    stable bid/ask at each bar timestamp. The best-effort reconstruction is:
    - ask_close = Bid_Ask.close
    - mid_close = Midpoint.close
    - bid_close = 2 * mid_close - ask_close

    The result is clamped defensively to avoid negative/inverted spreads.
    """
    if bid_ask is None or bid_ask.empty or midpoint is None or midpoint.empty:
        return pd.DataFrame()
    if "close" not in bid_ask.columns or "close" not in midpoint.columns:
        return pd.DataFrame()

    joined = (
        pd.concat(
            [
                bid_ask[["close"]].rename(columns={"close": "ask_close"}),
                midpoint[["close"]].rename(columns={"close": "mid_close"}),
            ],
            axis=1,
            join="inner",
        )
        .dropna()
        .copy()
    )
    if joined.empty:
        return pd.DataFrame()

    ask = pd.to_numeric(joined["ask_close"], errors="coerce")
    mid = pd.to_numeric(joined["mid_close"], errors="coerce")
    bid = 2 * mid - ask

    out = pd.DataFrame(index=joined.index)
    out["bid"] = bid
    out["ask"] = ask

    invalid = (
        out["bid"].isna()
        | out["ask"].isna()
        | (out["bid"] <= 0)
        | (out["ask"] <= 0)
        | (out["bid"] > out["ask"])
    )
    if invalid.any():
        mid_valid = mid > 0
        use_mid = invalid & mid_valid
        out.loc[use_mid, "bid"] = mid[use_mid]
        out.loc[use_mid, "ask"] = mid[use_mid]

        # If midpoint itself is invalid (<=0), leave as NaN so callers can fall back to
        # the trade/mark close instead of propagating negative prices into fills.
        use_nan = invalid & ~mid_valid
        if use_nan.any():
            out.loc[use_nan, "bid"] = float("nan")
            out.loc[use_nan, "ask"] = float("nan")

    return out


def _merge_frames(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        merged = incoming
    elif incoming is None or incoming.empty:
        merged = existing
    else:
        merged = pd.concat([existing, incoming], axis=0)
    if merged is None or merged.empty:
        return merged

    merged = merged.sort_index(kind="mergesort")
    if "missing" in merged.columns:
        merged["missing"] = merged["missing"].fillna(False)
        missing_mask = merged["missing"].astype(bool)
        real_indexes = merged.index[~missing_mask]
        if len(real_indexes):
            merged = merged[~(missing_mask & merged.index.isin(real_indexes))]
    merged = merged[~merged.index.duplicated(keep="last")]
    return merged


def _expected_us_daily_sessions(
    *,
    start_dt: datetime,
    end_dt: datetime,
) -> list[pd.Timestamp]:
    """Return NYSE session-close timestamps in the end-exclusive request window."""
    from lumibot.tools.helpers import get_trading_days

    start_local = pd.Timestamp(_to_utc(start_dt)).tz_convert(LUMIBOT_DEFAULT_PYTZ)
    end_local = pd.Timestamp(_to_utc(end_dt)).tz_convert(LUMIBOT_DEFAULT_PYTZ)
    if start_local >= end_local:
        return []
    schedule = get_trading_days(
        market="NYSE",
        start_date=start_local.date(),
        end_date=(end_local + pd.Timedelta(days=1)).date(),
        tzinfo=LUMIBOT_DEFAULT_PYTZ,
    )
    if schedule is None or schedule.empty:
        return []
    closes = pd.DatetimeIndex(schedule["market_close"])
    closes = closes[(closes >= start_local) & (closes < end_local)]
    return list(closes)


def _retryable_us_daily_sessions(
    df_cache: pd.DataFrame,
    *,
    start_dt: datetime,
    end_dt: datetime,
    now: Optional[datetime] = None,
) -> list[pd.Timestamp]:
    """Find completed NYSE sessions without a real daily bar and eligible for retry.

    Legacy placeholders have no retry timestamp, so they are eligible once. New placeholders
    suppress another repair until their retry timestamp expires.
    """
    if df_cache is None or df_cache.empty:
        return []

    now_utc = pd.Timestamp(now or datetime.now(timezone.utc))
    if now_utc.tzinfo is None:
        now_utc = now_utc.tz_localize(timezone.utc)
    else:
        now_utc = now_utc.tz_convert(timezone.utc)

    frame = df_cache.copy()
    idx = pd.DatetimeIndex(frame.index)
    if idx.tz is None:
        idx = idx.tz_localize(LUMIBOT_DEFAULT_PYTZ)
    else:
        idx = idx.tz_convert(LUMIBOT_DEFAULT_PYTZ)
    frame.index = idx

    missing_mask = (
        frame["missing"].fillna(False).astype(bool)
        if "missing" in frame.columns
        else pd.Series(False, index=frame.index)
    )
    real_frame = frame.loc[~missing_mask]
    if real_frame.empty:
        return []

    requested_start = max(
        _to_utc(start_dt),
        real_frame.index.min().to_pydatetime().astimezone(timezone.utc),
    )
    requested_end = min(_to_utc(end_dt), now_utc.to_pydatetime())
    expected = _expected_us_daily_sessions(start_dt=requested_start, end_dt=requested_end)
    if not expected:
        return []

    real_dates = {ts.date() for ts in pd.DatetimeIndex(real_frame.index)}
    marker_retry_after: Dict[date, pd.Timestamp] = {}
    if bool(missing_mask.any()) and "missing_retry_after" in frame.columns:
        marker_rows = frame.loc[missing_mask, ["missing_retry_after"]]
        parsed = pd.to_datetime(marker_rows["missing_retry_after"], utc=True, errors="coerce")
        for marker_ts, retry_after in parsed.items():
            if pd.isna(retry_after):
                continue
            session_date = pd.Timestamp(marker_ts).tz_convert(LUMIBOT_DEFAULT_PYTZ).date()
            previous = marker_retry_after.get(session_date)
            if previous is None or retry_after > previous:
                marker_retry_after[session_date] = retry_after

    retryable: list[pd.Timestamp] = []
    for session_close in expected:
        session_date = session_close.date()
        if session_date in real_dates:
            continue
        retry_after = marker_retry_after.get(session_date)
        if retry_after is not None and retry_after > now_utc:
            continue
        retryable.append(session_close)
    return retryable


def _daily_missing_placeholders(
    sessions: list[pd.Timestamp],
    *,
    retry_after: datetime,
) -> pd.DataFrame:
    if not sessions:
        return pd.DataFrame()
    count = len(sessions)
    index = pd.DatetimeIndex(sessions).tz_convert(LUMIBOT_DEFAULT_PYTZ)
    index = index.normalize() + pd.Timedelta(hours=16)
    return pd.DataFrame(
        {
            "open": [pd.NA] * count,
            "high": [pd.NA] * count,
            "low": [pd.NA] * count,
            "close": [pd.NA] * count,
            "volume": [pd.NA] * count,
            "missing": [True] * count,
            "missing_retry_after": [retry_after.isoformat()] * count,
        },
        index=index,
    )


def _repair_us_stock_index_daily_gaps(
    df_cache: pd.DataFrame,
    *,
    cache_file: Path,
    asset: Asset,
    quote: Optional[Asset],
    timestep: str,
    start_dt: datetime,
    end_dt: datetime,
    exchange: Optional[str],
    include_after_hours: bool,
    source: str,
    source_was_explicit: bool,
) -> pd.DataFrame:
    """Best-effort lazy repair for internal US stock/index daily cache gaps."""
    global _RUNTIME_DAILY_GAP_REPAIR_SECONDS_USED

    quote_symbol = str(getattr(quote, "symbol", "USD") or "USD").strip().upper()
    normalized_exchange = str(exchange or "").strip().upper()
    if quote_symbol != "USD" or normalized_exchange not in {
        "",
        "SMART",
        "NYSE",
        "NASDAQ",
        "ARCA",
        "AMEX",
        "IEX",
    }:
        return df_cache

    check_key = (
        str(cache_file),
        _to_utc(start_dt).date().isoformat(),
        _to_utc(end_dt).date().isoformat(),
    )
    if check_key in _RUNTIME_DAILY_GAP_CHECKED_WINDOWS:
        return df_cache
    _RUNTIME_DAILY_GAP_CHECKED_WINDOWS.add(check_key)

    aligned = _align_stock_index_daily_to_session_close(df_cache)
    gaps = _retryable_us_daily_sessions(
        aligned,
        start_dt=start_dt,
        end_dt=end_dt,
    )
    if not gaps:
        return aligned

    months: Dict[tuple[int, int], list[pd.Timestamp]] = {}
    for session in gaps:
        months.setdefault((session.year, session.month), []).append(session)

    attempted: list[pd.Timestamp] = []
    working = aligned
    for (year, month), sessions in list(months.items())[
        :IBKR_DAILY_GAP_REPAIR_MAX_MONTHS_PER_SERIES
    ]:
        remaining_budget = (
            IBKR_DAILY_GAP_REPAIR_TOTAL_BUDGET_SECONDS
            - _RUNTIME_DAILY_GAP_REPAIR_SECONDS_USED
        )
        if remaining_budget <= 0:
            break

        month_start = pd.Timestamp(year=year, month=month, day=1, tz=LUMIBOT_DEFAULT_PYTZ)
        next_month = month_start + pd.offsets.MonthBegin(1)
        started = time.perf_counter()
        try:
            fetched = _fetch_history_between_dates(
                asset=asset,
                quote=quote,
                timestep=timestep,
                start_dt=month_start.to_pydatetime(),
                end_dt=next_month.to_pydatetime(),
                exchange=exchange,
                include_after_hours=include_after_hours,
                source=source,
                source_was_explicit=source_was_explicit,
                _period_override="1m",
                _record_missing_on_empty=False,
                _queue_timeout=max(0.1, remaining_budget),
                _max_timeout_attempts=1,
            )
            attempted.extend(sessions)
            if fetched is not None and not fetched.empty:
                fetched = _align_stock_index_daily_to_session_close(fetched)
                working = _merge_frames(working, fetched)
        except Exception as exc:
            logger.warning(
                "IBKR daily gap repair skipped for %s %04d-%02d: %s",
                getattr(asset, "symbol", None),
                year,
                month,
                exc,
            )
        finally:
            _RUNTIME_DAILY_GAP_REPAIR_SECONDS_USED += time.perf_counter() - started

    if attempted:
        missing_mask = working["missing"].fillna(False).astype(bool)
        real_dates = {
            ts.date()
            for ts in pd.DatetimeIndex(working.index[~missing_mask]).tz_convert(
                LUMIBOT_DEFAULT_PYTZ
            )
        }
        unresolved = [session for session in attempted if session.date() not in real_dates]
        retry_after = datetime.now(timezone.utc) + timedelta(
            seconds=IBKR_DAILY_GAP_RETRY_TTL_SECONDS
        )
        working = _merge_frames(
            working,
            _daily_missing_placeholders(unresolved, retry_after=retry_after),
        )

    if not working.equals(df_cache):
        _write_cache_frame(cache_file, working)
    return working


def _window_is_placeholder_covered(
    df_cache: pd.DataFrame,
    *,
    start_local: datetime,
    end_local: datetime,
) -> bool:
    """Return True when [start_local, end_local] is fully covered by placeholder markers.

    IBKR uses `_record_missing_window()` to write `missing=True` marker rows at the start/end of a
    known no-data interval. On a fresh process, we should still honor those persisted markers and
    avoid re-submitting identical history requests for sub-windows inside that interval.
    """
    if df_cache is None or df_cache.empty or "missing" not in df_cache.columns:
        return False

    try:
        missing_mask = df_cache["missing"].fillna(False).astype(bool)
    except Exception:
        return False

    if not bool(missing_mask.any()):
        return False

    missing_index = pd.DatetimeIndex(df_cache.index[missing_mask]).sort_values()
    if len(missing_index) < 2:
        return False

    left_candidates = missing_index[missing_index <= start_local]
    right_candidates = missing_index[missing_index >= end_local]
    if len(left_candidates) == 0 or len(right_candidates) == 0:
        return False

    left = left_candidates.max()
    right = right_candidates.min()
    if left > right:
        return False

    between = df_cache.loc[(df_cache.index >= left) & (df_cache.index <= right)]
    if between.empty or "missing" not in between.columns:
        return False

    try:
        return bool(between["missing"].fillna(False).astype(bool).all())
    except Exception:
        return False


def _record_missing_window(
    *,
    asset: Asset,
    quote: Optional[Asset],
    timestep: str,
    exchange: Optional[str],
    source: str,
    include_after_hours: bool,
    start_dt: datetime,
    end_dt: datetime,
) -> None:
    # Add a bracketing placeholder window (two rows) to cache.
    cache_file = _cache_file_for(
        asset=asset,
        quote=quote,
        timestep=timestep,
        exchange=exchange,
        source=source,
        include_after_hours=include_after_hours,
    )
    cache_manager = get_backtest_cache()
    try:
        cache_manager.ensure_local_file(
            cache_file,
            payload=_remote_payload(
                asset,
                quote,
                timestep,
                exchange,
                source,
                include_after_hours=include_after_hours,
            ),
        )
    except Exception:
        pass

    df = _read_cache_frame(cache_file)
    retry_after = datetime.now(timezone.utc) + timedelta(
        seconds=IBKR_DAILY_GAP_RETRY_TTL_SECONDS
    )
    placeholder = pd.DataFrame(
        {
            "open": [pd.NA, pd.NA],
            "high": [pd.NA, pd.NA],
            "low": [pd.NA, pd.NA],
            "close": [pd.NA, pd.NA],
            "volume": [pd.NA, pd.NA],
            "missing": [True, True],
            "missing_retry_after": [retry_after.isoformat(), retry_after.isoformat()],
        },
        index=pd.DatetimeIndex([_to_utc(start_dt), _to_utc(end_dt)]).tz_convert(LUMIBOT_DEFAULT_PYTZ),
    )
    merged = _merge_frames(df, placeholder)
    _write_cache_frame(cache_file, merged)


def _crypto_day_bounds(start_local: datetime, end_local: datetime) -> tuple[datetime, datetime]:
    """Return inclusive midnight-to-midnight day bucket bounds in `LUMIBOT_DEFAULT_PYTZ`.

    LumiBot treats BACKTESTING_END as exclusive. If the requested end timestamp is exactly
    midnight, exclude that day from the derived daily series.
    """
    start_day = start_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = end_local.replace(hour=0, minute=0, second=0, microsecond=0)
    if end_local == end_day:
        end_day = end_day - pd.Timedelta(days=1)
    if end_day < start_day:
        end_day = start_day
    return start_day, end_day


def _derive_daily_from_intraday(
    intraday: pd.DataFrame,
    *,
    start_day: datetime,
    end_day: datetime,
) -> pd.DataFrame:
    """Derive daily OHLCV bars from an intraday OHLCV dataframe (crypto: 24/7 days)."""
    idx = pd.date_range(start=start_day, end=end_day, freq="D", tz=LUMIBOT_DEFAULT_PYTZ)
    if intraday is None or intraday.empty:
        out = pd.DataFrame(index=idx, columns=["open", "high", "low", "close", "volume", "missing"])
        out["missing"] = True
        return out

    df = intraday.copy()
    df.index = pd.to_datetime(df.index, utc=True, errors="coerce").tz_convert(LUMIBOT_DEFAULT_PYTZ)
    df = df[~df.index.isna()].sort_index()
    if df.empty:
        out = pd.DataFrame(index=idx, columns=["open", "high", "low", "close", "volume", "missing"])
        out["missing"] = True
        return out

    day_key = df.index.normalize()
    grouped = df.groupby(day_key)
    daily = pd.DataFrame(
        {
            "open": grouped["open"].first(),
            "high": grouped["high"].max(),
            "low": grouped["low"].min(),
            "close": grouped["close"].last(),
            "volume": grouped["volume"].sum(min_count=1) if "volume" in df.columns else pd.NA,
        }
    )
    daily_idx = pd.DatetimeIndex(daily.index)
    if daily_idx.tz is None:
        daily.index = daily_idx.tz_localize(LUMIBOT_DEFAULT_PYTZ)
    else:
        daily.index = daily_idx.tz_convert(LUMIBOT_DEFAULT_PYTZ)
    daily = daily.sort_index()
    daily["missing"] = False

    daily = daily.reindex(idx)
    close = pd.to_numeric(daily.get("close"), errors="coerce")
    daily["missing"] = daily["missing"].fillna(True) | close.isna()

    # IBKR crypto history is often effectively 24/5: weekend days may be absent even though
    # strategies are frequently configured as 24/7. To keep daily-cadence backtests stable
    # (no refresh loops / "missing BTC day"), forward-fill short gaps (<= 3 days) using the
    # prior close. This mirrors the existing Data.checker() tolerance window.
    if close is not None and not close.empty:
        filled_close = close.ffill(limit=3)
        filled_mask = close.isna() & filled_close.notna()
        if filled_mask.any():
            daily.loc[filled_mask, "close"] = filled_close[filled_mask]
            for col in ("open", "high", "low"):
                if col in daily.columns:
                    daily.loc[filled_mask, col] = pd.to_numeric(daily.loc[filled_mask, col], errors="coerce").fillna(
                        daily.loc[filled_mask, "close"]
                    )
            if "volume" in daily.columns:
                daily.loc[filled_mask, "volume"] = pd.to_numeric(daily.loc[filled_mask, "volume"], errors="coerce").fillna(0)
            daily.loc[filled_mask, "missing"] = False
    return daily


def _get_crypto_daily_bars(
    *,
    asset: Asset,
    quote: Optional[Asset],
    start_dt: datetime,
    end_dt: datetime,
    exchange: Optional[str],
    include_after_hours: bool,
    source: str,
) -> pd.DataFrame:
    """Return crypto daily bars aligned to midnight days in `LUMIBOT_DEFAULT_PYTZ`."""
    start_local = _to_utc(start_dt).astimezone(LUMIBOT_DEFAULT_PYTZ)
    end_local = _to_utc(end_dt).astimezone(LUMIBOT_DEFAULT_PYTZ)
    start_day, end_day = _crypto_day_bounds(start_local, end_local)

    exch = (exchange or os.environ.get("IBKR_CRYPTO_VENUE") or IBKR_DEFAULT_CRYPTO_VENUE).strip().upper()
    # IMPORTANT: keep derived daily bars in a separate cache namespace so we don't mix them with
    # legacy `bar=1d` results (which have different semantics and timestamps).
    derived_source = f"{source}_DERIVED_DAILY"
    cache_file = _cache_file_for(
        asset=asset,
        quote=quote,
        timestep="day",
        exchange=exch,
        source=derived_source,
        include_after_hours=include_after_hours,
    )
    cache = ParquetSeriesCache(
        cache_file,
        remote_payload=_remote_payload(
            asset,
            quote,
            "day",
            exch,
            derived_source,
            include_after_hours=include_after_hours,
        ),
    )
    cache.hydrate_remote()
    df_cache = cache.read()

    if not df_cache.empty:
        coverage_start = df_cache.index.min()
        coverage_end = df_cache.index.max()
    else:
        coverage_start = None
        coverage_end = None

    needs_fetch = (
        coverage_start is None
        or coverage_end is None
        or start_day < coverage_start
        or end_day > coverage_end
    )

    if needs_fetch:
        fetch_start = start_day if coverage_start is None else min(start_day, coverage_start)
        fetch_end = (end_day + pd.Timedelta(days=1)) if coverage_end is None else max(end_day + pd.Timedelta(days=1), coverage_end)

        hourly = _get_cached_bars_for_source(
            asset=asset,
            quote=quote,
            timestep="hour",
            start_dt=fetch_start,
            end_dt=fetch_end,
            exchange=exch,
            include_after_hours=include_after_hours,
            source=source,
        )
        daily = _derive_daily_from_intraday(hourly, start_day=fetch_start, end_day=(fetch_end - pd.Timedelta(days=1)))

        missing_days = daily.index[daily["missing"].fillna(True)]
        for day in missing_days:
            day_start = day
            day_end = day + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            minute = _get_cached_bars_for_source(
                asset=asset,
                quote=quote,
                timestep="minute",
                start_dt=day_start,
                end_dt=day_end,
                exchange=exch,
                include_after_hours=True,
                source=source,
            )
            if minute is None or minute.empty:
                continue
            filled = _derive_daily_from_intraday(minute, start_day=day_start, end_day=day_start)
            if not filled.empty and not bool(filled["missing"].iloc[0]):
                daily.loc[day_start, ["open", "high", "low", "close", "volume"]] = filled.iloc[0][
                    ["open", "high", "low", "close", "volume"]
                ]
                daily.loc[day_start, "missing"] = False

        merged = ParquetSeriesCache.merge(df_cache, daily)
        cache.write(
            merged,
            remote_payload=_remote_payload(
                asset,
                quote,
                "day",
                exch,
                derived_source,
                include_after_hours=include_after_hours,
            ),
        )
        df_cache = merged

    if df_cache.empty:
        return df_cache

    frame = df_cache.loc[(df_cache.index >= start_day) & (df_cache.index <= end_day)].copy()
    if "missing" in frame.columns:
        frame = frame[~frame["missing"].fillna(False)]
        frame = frame.drop(columns=["missing"], errors="ignore")
    if "close" in frame.columns:
        frame["bid"] = pd.to_numeric(frame.get("bid", frame["close"]), errors="coerce").fillna(frame["close"])
        frame["ask"] = pd.to_numeric(frame.get("ask", frame["close"]), errors="coerce").fillna(frame["close"])
    return frame


def _get_futures_daily_bars(
    *,
    asset: Asset,
    quote: Optional[Asset],
    start_dt: datetime,
    end_dt: datetime,
    exchange: Optional[str],
    include_after_hours: bool,
    source: str,
) -> pd.DataFrame:
    """Derive `day` bars aligned to the `us_futures` session (not midnight).

    This is intentionally session-based because futures strategies commonly use
    `self.set_market("us_futures")` and LumiBot's backtesting clock advances based on that calendar.
    """

    try:
        import pandas_market_calendars as mcal
    except Exception:
        return pd.DataFrame()

    start_utc = _to_utc(start_dt)
    end_utc = _to_utc(end_dt)
    if start_utc > end_utc:
        start_utc, end_utc = end_utc, start_utc
    start_local = start_utc.astimezone(LUMIBOT_DEFAULT_PYTZ)
    end_local = end_utc.astimezone(LUMIBOT_DEFAULT_PYTZ)

    cal = mcal.get_calendar("us_futures")
    schedule = cal.schedule(
        start_date=pd.Timestamp(start_utc.date()) - pd.Timedelta(days=2),
        end_date=pd.Timestamp(end_utc.date()) + pd.Timedelta(days=2),
    )
    if schedule is None or schedule.empty:
        return pd.DataFrame()

    session_start = pd.Timestamp(schedule["market_open"].min()).tz_convert("UTC").to_pydatetime()
    session_end = pd.Timestamp(schedule["market_close"].max()).tz_convert("UTC").to_pydatetime()
    if session_start >= session_end:
        return pd.DataFrame()

    # Prefer hourly bars for speed (deriving daily from minute across long windows is too slow).
    intraday = _get_cached_bars_for_source(
        asset=asset,
        quote=quote,
        timestep="hour",
        start_dt=session_start,
        end_dt=session_end,
        exchange=exchange,
        include_after_hours=include_after_hours,
        source=source,
    )
    intraday_timestep = "hour"
    if intraday is None or intraday.empty:
        intraday = _get_cached_bars_for_source(
            asset=asset,
            quote=quote,
            timestep="minute",
            start_dt=session_start,
            end_dt=session_end,
            exchange=exchange,
            include_after_hours=include_after_hours,
            source=source,
        )
        intraday_timestep = "minute"
        if intraday is None or intraday.empty:
            return pd.DataFrame()

    if _enable_futures_bid_ask_derivation():
        intraday, _ = _maybe_augment_futures_bid_ask(
            df_cache=intraday,
            asset=asset,
            quote=quote,
            timestep=intraday_timestep,
            start_dt=session_start,
            end_dt=session_end,
            exchange=exchange,
            include_after_hours=include_after_hours,
        )

    rows: list[dict[str, float]] = []
    idx: list[pd.Timestamp] = []
    minute_fallback: Optional[pd.DataFrame] = None
    for _, sess in schedule.iterrows():
        open_local = pd.Timestamp(sess["market_open"]).tz_convert("UTC").tz_convert(LUMIBOT_DEFAULT_PYTZ)
        close_local = pd.Timestamp(sess["market_close"]).tz_convert("UTC").tz_convert(LUMIBOT_DEFAULT_PYTZ)
        if close_local < start_local or open_local > end_local:
            continue
        window = intraday.loc[(intraday.index >= open_local) & (intraday.index <= close_local)]
        if window.empty and intraday_timestep != "minute":
            if minute_fallback is None:
                minute_fallback = _get_cached_bars_for_source(
                    asset=asset,
                    quote=quote,
                    timestep="minute",
                    start_dt=session_start,
                    end_dt=session_end,
                    exchange=exchange,
                    include_after_hours=include_after_hours,
                    source=source,
                )
                if _enable_futures_bid_ask_derivation():
                    minute_fallback, _ = _maybe_augment_futures_bid_ask(
                        df_cache=minute_fallback,
                        asset=asset,
                        quote=quote,
                        timestep="minute",
                        start_dt=session_start,
                        end_dt=session_end,
                        exchange=exchange,
                        include_after_hours=include_after_hours,
                    )
            if minute_fallback is not None and not minute_fallback.empty:
                window = minute_fallback.loc[(minute_fallback.index >= open_local) & (minute_fallback.index <= close_local)]
        if window.empty:
            continue

        open_px = float(window["open"].iloc[0]) if "open" in window.columns else float(window["close"].iloc[0])
        high_px = float(pd.to_numeric(window.get("high"), errors="coerce").max()) if "high" in window.columns else float(window["close"].max())
        low_px = float(pd.to_numeric(window.get("low"), errors="coerce").min()) if "low" in window.columns else float(window["close"].min())
        close_px = float(pd.to_numeric(window.get("close"), errors="coerce").iloc[-1])
        vol = float(pd.to_numeric(window.get("volume"), errors="coerce").fillna(0).sum()) if "volume" in window.columns else 0.0

        payload: dict[str, float] = {"open": open_px, "high": high_px, "low": low_px, "close": close_px, "volume": vol}
        if "bid" in window.columns:
            payload["bid"] = float(pd.to_numeric(window.get("bid"), errors="coerce").iloc[-1])
        if "ask" in window.columns:
            payload["ask"] = float(pd.to_numeric(window.get("ask"), errors="coerce").iloc[-1])

        rows.append(payload)
        idx.append(close_local)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, index=pd.DatetimeIndex(idx))
    df = df.sort_index()
    df.index = df.index.tz_convert(LUMIBOT_DEFAULT_PYTZ)
    return df.loc[(df.index >= start_local) & (df.index <= end_local)]


def _resolve_conid(*, asset: Asset, quote: Optional[Asset], exchange: Optional[str]) -> int:
    global _RUNTIME_CONID_CACHE

    cache_file = Path(LUMIBOT_CACHE_FOLDER) / CACHE_SUBFOLDER / "conids.json"
    cache_manager = get_backtest_cache()

    asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
    effective_exchange = exchange
    if asset_type in {"future", "cont_future"} and not effective_exchange:
        effective_exchange = _resolve_futures_exchange(getattr(asset, "symbol", ""))

    # Fast path: avoid repeated secdef/conid resolution in tight loops (for example minute-index
    # strategies repeatedly requesting SPX bars). For equivalent key variants, mirror historical
    # cache compatibility behavior.
    primary = _conid_key(asset=asset, quote=quote, exchange=effective_exchange)
    candidates = [primary.to_key()]
    if asset_type in {"future", "cont_future"}:
        if primary.quote_symbol:
            candidates.append(IbkrConidKey(primary.asset_type, primary.symbol, "", primary.exchange, primary.expiration).to_key())
        else:
            candidates.append(IbkrConidKey(primary.asset_type, primary.symbol, "USD", primary.exchange, primary.expiration).to_key())

    for key in candidates:
        cached_runtime = _RUNTIME_CONID_CACHE.get(key)
        if isinstance(cached_runtime, int) and cached_runtime > 0:
            return int(cached_runtime)

    try:
        cache_manager.ensure_local_file(cache_file, payload={"provider": "ibkr", "type": "conids"})
    except Exception:
        pass

    mapping: Dict[str, int] = {}
    if cache_file.exists():
        try:
            mapping = json.loads(cache_file.read_text(encoding="utf-8")) or {}
        except Exception:
            mapping = {}

    # Seed conids.json across cache namespaces.
    #
    # Production backtests often run with a fresh S3 cache version/prefix to simulate cold-cache
    # behavior. IBKR Client Portal cannot resolve conids for *expired* futures contracts, so
    # historical futures backtests depend on the shared conid registry (`ibkr/conids.json`).
    #
    # If the current cache namespace does not contain `conids.json`, fall back to the default
    # `v1` namespace and materialize it locally (and, when possible, upload it into the current
    # namespace) so we do not thrash the downloader in a hot loop.
    if (not mapping) and (not cache_file.exists()) and getattr(cache_manager, "enabled", False):
        settings = getattr(cache_manager, "_settings", None)
        try:
            backend = getattr(settings, "backend", None)
            bucket = str(getattr(settings, "bucket", "") or "")
            prefix = str(getattr(settings, "prefix", "") or "").strip("/")
            version = str(getattr(settings, "version", "") or "").strip("/")
        except Exception:
            backend = None
            bucket = ""
            prefix = ""
            version = ""

        if backend == "s3" and bucket and version and version != "v1":
            try:
                relative_path = cache_file.resolve().relative_to(Path(LUMIBOT_CACHE_FOLDER).resolve()).as_posix()
            except Exception:
                relative_path = f"{CACHE_SUBFOLDER}/conids.json"

            seed_components = [prefix, "v1", relative_path]
            seed_key = "/".join([c for c in seed_components if c])
            seed_mapping: Dict[str, int] = {}
            try:
                seed_mapping = _download_remote_conids_json(cache_manager, bucket=bucket, key=seed_key)
            except Exception as exc:
                if _is_not_found_error(cache_manager, exc):
                    seed_mapping = {}
                else:
                    seed_mapping = {}

            if seed_mapping:
                mapping = dict(seed_mapping)
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                cache_file.write_text(json.dumps(mapping, indent=2, sort_keys=True), encoding="utf-8")
                try:
                    # Best-effort: upload into the current cache namespace so subsequent runs can
                    # reuse without re-downloading from the seed namespace.
                    _merge_upload_conids_json(cache_manager, cache_file, mapping=mapping, required_keys=set())
                except Exception:
                    pass

    # Conid keying is not fully uniform across historical caches (some runs key futures with
    # quote_symbol="USD", others omit it). For robustness (and to avoid unnecessary remote
    # lookups), try a small set of equivalent keys before falling back to the downloader.

    for key in candidates:
        cached = mapping.get(key)
        if isinstance(cached, int) and cached > 0:
            _RUNTIME_CONID_CACHE[key] = int(cached)
            return cached

    if asset_type not in {"future", "cont_future"}:
        _load_negative_conid_cache()
        for key in candidates:
            neg_hit = _NEGATIVE_CONID_CACHE.get(key)
            if isinstance(neg_hit, dict):
                cached_msg = str(neg_hit.get("message") or "").strip() or (
                    f"IBKR conid lookup is negatively cached for {asset.symbol} (type={asset_type})."
                )
                logger.error("IBKR negative conid cache hit: %s", cached_msg)
                raise RuntimeError(cached_msg)

    if asset_type in {"future", "cont_future"} and primary.expiration:
        same_month_cached = _lookup_same_month_future_conid_from_mapping(mapping=mapping, key=primary)
        if same_month_cached is not None:
            conid, actual_expiration = same_month_cached
            actual_date = _date_from_yyyymmdd(actual_expiration)
            if actual_date is not None:
                setattr(asset, "_ibkr_resolved_expiration", actual_date)
            logger.warning(
                "IBKR conid registry has no exact key for %s expiring %s on %s; using unambiguous same-month contract %s.",
                primary.symbol,
                primary.expiration,
                primary.exchange,
                actual_expiration,
            )
            for key in candidates:
                _RUNTIME_CONID_CACHE[key] = int(conid)
            return int(conid)

    keys_added: set[str] = set()
    conid = _lookup_conid_remote(asset=asset, quote=quote, exchange=effective_exchange, mapping=mapping, keys_added=keys_added)
    # Always persist under the primary key for forward consistency.
    primary_key = primary.to_key()
    conid_int = int(conid)
    prior_primary = mapping.get(primary_key)
    mapping[primary_key] = conid_int
    _RUNTIME_CONID_CACHE[primary_key] = conid_int
    if prior_primary != conid_int:
        keys_added.add(primary_key)
    if asset_type in {"future", "cont_future"}:
        # Mirror the primary conid under both quote_symbol variants for compatibility with
        # historical caches and older in-flight backtests.
        if primary.quote_symbol:
            alt_key = IbkrConidKey(primary.asset_type, primary.symbol, "", primary.exchange, primary.expiration).to_key()
        else:
            alt_key = IbkrConidKey(primary.asset_type, primary.symbol, "USD", primary.exchange, primary.expiration).to_key()
        prior_alt = mapping.get(alt_key)
        mapping[alt_key] = conid_int
        _RUNTIME_CONID_CACHE[alt_key] = conid_int
        if prior_alt != conid_int:
            keys_added.add(alt_key)

    for key in set(candidates + [primary_key]):
        _clear_negative_conid(key=key)

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(mapping, indent=2, sort_keys=True), encoding="utf-8")
    try:
        _merge_upload_conids_json(cache_manager, cache_file, mapping=mapping, required_keys=keys_added)
    except Exception:
        pass
    return int(conid)


def _is_not_found_error(cache_manager, exc: Exception) -> bool:
    try:
        fn = getattr(cache_manager, "_is_not_found_error", None)
        if callable(fn):
            return bool(fn(exc))
    except Exception:
        pass
    msg = str(exc).lower()
    return any(token in msg for token in ("nosuchkey", "not found", "404", "no such key"))


def _is_terminal_no_data_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    # Data Downloader only emits this after the IBKR history response failed validation, was
    # rebuilt from smaller windows, and still could not produce a cacheable payload. Treat the
    # known malformed-payload variant as terminal for the requested window so backtests do not
    # repeatedly resubmit the same dead 1-minute range.
    if "ibkr history remained invalid after rebuild" in msg and "malformed_history_payload" in msg:
        return True
    return any(
        token in msg
        for token in (
            "chart data unavailable",
            "no data available",
            "does not have data",
            "asset does not exist",
            "unable to resolve ibkr conid",
            "ibkr conid lookup is negatively cached",
            "secdef/search returned no",
            # `_fetch_history_between_dates` raises this when IBKR pagination returns
            # empty before we covered the requested window. In practice this happens
            # for entitlement/stitching gaps (e.g. CONT_FUTURE 1-minute Trades) where
            # the data will never be served — so treat it as a terminal no-data
            # condition and persist a placeholder marker to skip future refetches.
            "pagination returned empty data before covering",
            "pagination returned an empty frame before covering",
        )
    )


def _download_remote_conids_json(cache_manager, *, bucket: str, key: str) -> Dict[str, int]:
    client = getattr(cache_manager, "_get_client", None)
    if not callable(client):
        return {}
    s3 = client()
    if not hasattr(s3, "get_object"):
        return {}
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response.get("Body")
    raw = b""
    if body is not None:
        raw = body.read()
        try:
            body.close()
        except Exception:
            pass
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except Exception:
        parsed = {}
    if not isinstance(parsed, dict):
        return {}
    out: Dict[str, int] = {}
    for k, v in parsed.items():
        try:
            iv = int(v)
        except Exception:
            continue
        if iv > 0:
            out[str(k)] = iv
    return out


def _persist_s3_marker(*, local_path: Path, remote_key: str) -> None:
    try:
        marker_path = local_path.with_suffix(local_path.suffix + ".s3key")
        marker_tmp = marker_path.with_suffix(marker_path.suffix + ".tmp")
        marker_path.parent.mkdir(parents=True, exist_ok=True)
        marker_tmp.write_text(remote_key, encoding="utf-8")
        os.replace(marker_tmp, marker_path)
    except Exception:
        pass


def _merge_upload_conids_json(
    cache_manager,
    local_path: Path,
    *,
    mapping: Dict[str, int],
    required_keys: set[str],
    max_attempts: int = 3,
) -> None:
    """Upload `ibkr/conids.json` with a merge-before-upload retry to reduce lost updates."""
    global _DISABLE_CONIDS_REMOTE_UPLOAD, _LOGGED_CONIDS_REMOTE_UPLOAD_DISABLE

    if _DISABLE_CONIDS_REMOTE_UPLOAD:
        return

    if not cache_manager.enabled or cache_manager.mode != CacheMode.S3_READWRITE:
        cache_manager.on_local_update(local_path, payload={"provider": "ibkr", "type": "conids"})
        return

    settings = getattr(cache_manager, "_settings", None)
    if settings is None or getattr(settings, "backend", None) != "s3":
        cache_manager.on_local_update(local_path, payload={"provider": "ibkr", "type": "conids"})
        return

    remote_key = cache_manager.remote_key_for(local_path, payload={"provider": "ibkr", "type": "conids"})
    if not remote_key:
        cache_manager.on_local_update(local_path, payload={"provider": "ibkr", "type": "conids"})
        return

    bucket = str(getattr(settings, "bucket", "") or "")
    if not bucket:
        cache_manager.on_local_update(local_path, payload={"provider": "ibkr", "type": "conids"})
        return

    client_fn = getattr(cache_manager, "_get_client", None)
    if not callable(client_fn):
        cache_manager.on_local_update(local_path, payload={"provider": "ibkr", "type": "conids"})
        return
    s3 = client_fn()
    if not hasattr(s3, "upload_file") or not hasattr(s3, "get_object"):
        cache_manager.on_local_update(local_path, payload={"provider": "ibkr", "type": "conids"})
        return

    # If this update didn't add anything new, a plain upload is fine.
    if not required_keys:
        try:
            s3.upload_file(str(local_path), bucket, remote_key)
        except Exception as exc:
            if _is_access_denied_error(exc):
                _DISABLE_CONIDS_REMOTE_UPLOAD = True
                if not _LOGGED_CONIDS_REMOTE_UPLOAD_DISABLE:
                    logger.warning("Disabling remote conids.json uploads due to AccessDenied: %s", exc)
                    _LOGGED_CONIDS_REMOTE_UPLOAD_DISABLE = True
                return
            raise
        _persist_s3_marker(local_path=local_path, remote_key=remote_key)
        return

    last_exc: Optional[Exception] = None
    for attempt in range(max_attempts):
        try:
            # Pull the freshest remote, union, then upload.
            try:
                remote = _download_remote_conids_json(cache_manager, bucket=bucket, key=remote_key)
            except Exception as exc:
                if _is_not_found_error(cache_manager, exc):
                    remote = {}
                else:
                    raise
            merged = dict(remote)
            merged.update(mapping)
            if merged != mapping:
                mapping.clear()
                mapping.update(merged)
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.write_text(json.dumps(mapping, indent=2, sort_keys=True), encoding="utf-8")

            s3.upload_file(str(local_path), bucket, remote_key)

            # Verify: ensure the keys we just added are present remotely.
            verified = _download_remote_conids_json(cache_manager, bucket=bucket, key=remote_key)
            if required_keys.issubset(set(verified.keys())):
                _persist_s3_marker(local_path=local_path, remote_key=remote_key)
                return

            # Lost update: retry after a short backoff.
            time.sleep(0.15 * (attempt + 1))
        except Exception as exc:
            last_exc = exc
            if _is_access_denied_error(exc):
                _DISABLE_CONIDS_REMOTE_UPLOAD = True
                if not _LOGGED_CONIDS_REMOTE_UPLOAD_DISABLE:
                    logger.warning("Disabling remote conids.json uploads due to AccessDenied: %s", exc)
                    _LOGGED_CONIDS_REMOTE_UPLOAD_DISABLE = True
                return
            time.sleep(0.15 * (attempt + 1))

    if last_exc is not None:
        logger.warning("IBKR conids.json merge-upload failed after retries: %s", last_exc)


def _lookup_conid_remote(
    *,
    asset: Asset,
    quote: Optional[Asset],
    exchange: Optional[str],
    mapping: Optional[Dict[str, int]] = None,
    keys_added: Optional[set[str]] = None,
) -> int:
    asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
    if asset_type in {"future", "cont_future"}:
        if getattr(asset, "expiration", None) is None and asset_type != "cont_future" and not getattr(asset, "auto_expiry", None):
            raise ValueError(
                "IBKR futures require an explicit expiration on Asset(asset_type='future'). "
                "Use asset_type='cont_future' for continuous futures."
            )
        return _lookup_conid_future(asset=asset, exchange=exchange, mapping=mapping, keys_added=keys_added)
    if asset_type in {"crypto"}:
        return _lookup_conid_crypto(asset=asset, quote=quote)

    preferred_sec_types: tuple[str, ...] = ()
    if asset_type == "stock":
        preferred_sec_types = ("STK",)
    elif asset_type == "index":
        preferred_sec_types = ("IND",)

    def _payload_item_matches_sec_type(item: dict, allowed: tuple[str, ...]) -> bool:
        if not allowed:
            return True
        direct = str(item.get("secType") or "").upper()
        if direct in allowed:
            return True
        sections = item.get("sections")
        if isinstance(sections, list):
            for section in sections:
                if isinstance(section, dict) and str(section.get("secType") or "").upper() in allowed:
                    return True
        return False

    def _extract_conid(payload: Any, allowed: tuple[str, ...]) -> Optional[int]:
        if not isinstance(payload, list):
            return None
        for item in payload:
            if not isinstance(item, dict):
                continue
            if not _payload_item_matches_sec_type(item, allowed):
                continue
            conid = item.get("conid")
            if conid is not None:
                return int(conid)
        return None

    base_url = _downloader_base_url()
    url = f"{base_url}/ibkr/iserver/secdef/search"

    if preferred_sec_types:
        for sec_type in preferred_sec_types:
            payload = queue_request(
                url=url,
                querystring={"symbol": asset.symbol, "secType": sec_type},
                headers=None,
                timeout=None,
            )
            conid = _extract_conid(payload, (sec_type,))
            if conid is not None:
                return int(conid)

    # Default: fall back to secdef search, but still filter ambiguous symbols by asset type.
    payload = queue_request(url=url, querystring={"symbol": asset.symbol}, headers=None, timeout=None)
    conid = _extract_conid(payload, preferred_sec_types)
    if conid is not None:
        return int(conid)

    message = f"Unable to resolve IBKR conid for {asset.symbol} (type={asset_type})"
    _record_negative_conid(key=_conid_key(asset=asset, quote=quote, exchange=exchange).to_key(), reason="no_conid", message=message)
    raise RuntimeError(message)


def _lookup_conid_crypto(*, asset: Asset, quote: Optional[Asset]) -> int:
    # Best-effort: IBKR crypto availability depends on region; conid mappings differ by venue.
    base_url = _downloader_base_url()
    url = f"{base_url}/ibkr/iserver/secdef/search"
    venue = (os.environ.get("IBKR_CRYPTO_VENUE") or IBKR_DEFAULT_CRYPTO_VENUE).strip().upper()
    desired_quote = str(getattr(quote, "symbol", "") or "").strip().upper() if quote is not None else ""
    payload = queue_request(
        url=url,
        querystring={"symbol": asset.symbol, "secType": "CRYPTO"},
        headers=None,
        timeout=None,
    )
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected IBKR secdef/search response for crypto: {payload}")
    fallback_any: Optional[int] = None
    fallback_quote: Optional[int] = None
    for entry in payload:
        entry_currency = str(entry.get("currency") or "").strip().upper()
        conid = entry.get("conid")
        if conid is not None and fallback_any is None:
            try:
                fallback_any = int(conid)
            except Exception:
                fallback_any = None
        sections = entry.get("sections") or []
        for section in sections:
            if str(section.get("secType") or "").upper() == "CRYPTO":
                if venue:
                    exch = str(section.get("exchange") or "").upper()
                    if venue not in exch:
                        # Keep a best-effort fallback if the quote matches but the venue doesn't.
                        if fallback_quote is None:
                            try:
                                fallback_quote = int(conid) if conid is not None else None
                            except Exception:
                                fallback_quote = None
                        continue
                section_currency = str(section.get("currency") or "").strip().upper()
                resolved_currency = section_currency or entry_currency
                if desired_quote and resolved_currency and desired_quote != resolved_currency:
                    continue
                if conid is not None:
                    return int(conid)
    # Fallback: prefer any quote-matching crypto conid even if the venue metadata doesn't match.
    if fallback_quote is not None:
        return int(fallback_quote)
    # Final fallback: accept the first conid only when the caller didn't request a specific quote.
    if fallback_any is not None and not desired_quote:
        return int(fallback_any)
    raise RuntimeError(
        f"Unable to resolve IBKR crypto conid for {asset.symbol}/{getattr(quote,'symbol',None)} "
        f"(venue={venue or 'AUTO'})."
    )


def _future_contract_date_strings(contract: Dict[str, Any]) -> list[str]:
    """Return IBKR date fields for a listed futures contract in priority order."""
    dates: list[str] = []
    for field in ("expirationDate", "ltd", "lastTradeDate", "lastTradeDay", "lastTrade"):
        raw = contract.get(field)
        if raw is None:
            continue
        value = str(raw).strip()
        if value.isdigit() and len(value) == 8 and value not in dates:
            dates.append(value)
    return dates


def _future_contract_conid(contract: Dict[str, Any]) -> Optional[int]:
    try:
        conid = int(contract.get("conid"))
    except Exception:
        return None
    return conid if conid > 0 else None


def _remember_future_conid_mapping(
    *,
    mapping: Optional[Dict[str, int]],
    keys_added: Optional[set[str]],
    symbol: str,
    exchange: str,
    expiration: str,
    conid: int,
) -> None:
    if mapping is None:
        return
    if not (expiration.isdigit() and len(expiration) == 8 and conid > 0):
        return
    key_blank = IbkrConidKey("future", symbol, "", exchange, expiration).to_key()
    key_usd = IbkrConidKey("future", symbol, "USD", exchange, expiration).to_key()
    for key in (key_blank, key_usd):
        prior = mapping.get(key)
        if prior != conid:
            mapping[key] = conid
            if keys_added is not None:
                keys_added.add(key)


def _select_future_contract_for_target(
    *,
    contracts: list[Dict[str, Any]],
    target: str,
) -> tuple[Optional[Dict[str, Any]], str, bool, list[str]]:
    """Select a contract by exact date, then by unambiguous same contract month.

    IBKR's listed `expirationDate`/`ltd` can differ from LumiBot's synthetic expiration
    anchor by a day when a third Friday is a holiday. Same-month fallback is still strict:
    it only succeeds when IBKR returns exactly one conid for the target YYYYMM.
    """
    same_month: Dict[int, tuple[Dict[str, Any], str]] = {}
    same_month_dates: set[str] = set()
    target_month = target[:6]

    for contract in contracts:
        if not isinstance(contract, dict):
            continue
        conid = _future_contract_conid(contract)
        if conid is None:
            continue
        dates = _future_contract_date_strings(contract)
        if not dates:
            continue
        canonical_date = dates[0]
        if target in dates:
            return contract, canonical_date, False, []
        for candidate in dates:
            if target_month and candidate[:6] == target_month:
                same_month_dates.add(candidate)
                same_month.setdefault(conid, (contract, canonical_date))

    if len(same_month) == 1:
        contract, canonical_date = next(iter(same_month.values()))
        return contract, canonical_date, True, sorted(same_month_dates)

    return None, "", False, sorted(same_month_dates)


def _lookup_same_month_future_conid_from_mapping(
    *,
    mapping: Dict[str, int],
    key: IbkrConidKey,
) -> Optional[tuple[int, str]]:
    target = str(key.expiration or "")
    if not (target.isdigit() and len(target) == 8):
        return None
    target_month = target[:6]
    prefixes = (
        IbkrConidKey(key.asset_type, key.symbol, "", key.exchange, "").to_key(),
        IbkrConidKey(key.asset_type, key.symbol, "USD", key.exchange, "").to_key(),
    )
    matches: Dict[int, str] = {}
    for raw_key, raw_conid in mapping.items():
        raw_key_text = str(raw_key)
        if not any(raw_key_text.startswith(prefix) for prefix in prefixes):
            continue
        expiration = raw_key_text.rsplit("|", 1)[-1]
        if expiration == target:
            continue
        if not (expiration.isdigit() and len(expiration) == 8 and expiration[:6] == target_month):
            continue
        try:
            conid = int(raw_conid)
        except Exception:
            continue
        if conid > 0:
            matches.setdefault(conid, expiration)

    if len(matches) == 1:
        conid, expiration = next(iter(matches.items()))
        return conid, expiration
    return None


def _lookup_conid_future(
    *,
    asset: Asset,
    exchange: Optional[str],
    mapping: Optional[Dict[str, int]] = None,
    keys_added: Optional[set[str]] = None,
) -> int:
    base_url = _downloader_base_url()
    url = f"{base_url}/ibkr/trsrv/futures"
    desired_exchange = (exchange or "").strip().upper()
    if not desired_exchange:
        try:
            desired_exchange = _resolve_futures_exchange(getattr(asset, "symbol", ""))
        except IbkrFuturesExchangeAmbiguousError:
            raise
        except Exception:
            desired_exchange = (os.environ.get("IBKR_FUTURES_EXCHANGE") or IBKR_DEFAULT_FUTURES_EXCHANGE_FALLBACK).strip().upper()

    symbol_upper = str(getattr(asset, "symbol", "") or "").strip().upper()
    expiration = getattr(asset, "expiration", None)
    target = expiration.strftime("%Y%m%d") if expiration is not None else ""

    # Negative cache: stop hammering IBKR for invalid roots/expirations.
    _load_negative_conid_cache()
    neg_root_key = IbkrConidKey("future", symbol_upper, "", desired_exchange, "").to_key()
    neg_target_key = IbkrConidKey("future", symbol_upper, "", desired_exchange, target).to_key() if target else ""
    neg_root_hit = _NEGATIVE_CONID_CACHE.get(neg_root_key)
    if isinstance(neg_root_hit, dict):
        cached_msg = str(neg_root_hit.get("message") or "").strip() or (
            f"IBKR futures conid lookup is negatively cached for {symbol_upper} on {desired_exchange} (target={target or 'front_month'})."
        )
        logger.error("IBKR negative conid cache hit: %s", cached_msg)
        raise IbkrFuturesConidLookupError(cached_msg)
    neg_target_hit = _NEGATIVE_CONID_CACHE.get(neg_target_key) if neg_target_key else None
    if isinstance(neg_target_hit, dict):
        cached_msg = str(neg_target_hit.get("message") or "").strip() or (
            f"IBKR futures conid lookup is negatively cached for {symbol_upper} on {desired_exchange} (target={target})."
        )
        if _is_future_or_current_expiration(expiration):
            logger.warning("Ignoring future-dated IBKR negative conid cache entry and retrying: %s", cached_msg)
        else:
            logger.error("IBKR negative conid cache hit: %s", cached_msg)
            raise IbkrFuturesConidLookupError(cached_msg)

    query = {"symbols": asset.symbol, "exchange": desired_exchange, "secType": "FUT"}
    payload = queue_request(url=url, querystring=query, headers=None, timeout=None)
    # Response shape: { "<symbol>": [ {conid, expirationDate, ...}, ... ] }
    if not isinstance(payload, dict):
        # Some gateways require secType=CONTFUT to list contracts.
        payload = queue_request(
            url=url,
            querystring={"symbols": asset.symbol, "exchange": desired_exchange, "secType": "CONTFUT"},
            headers=None,
            timeout=None,
        )
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected IBKR trsrv/futures response: {payload}")
    contracts = payload.get(asset.symbol) or payload.get(asset.symbol.upper()) or []
    if not isinstance(contracts, list) or not contracts:
        msg = f"No futures contracts returned for {symbol_upper} on {desired_exchange}"
        _record_negative_conid(key=neg_root_key, reason="no_contracts", message=msg)
        raise IbkrFuturesConidLookupError(msg)

    # Bulk-refresh: update conids.json with *all* returned contract months for this root+exchange.
    # This keeps the registry current via REST so we rarely/never need a new TWS backfill.
    if mapping is not None:
        for contract in contracts:
            if not isinstance(contract, dict):
                continue
            conid_int = _future_contract_conid(contract)
            if conid_int is None:
                continue

            for exp_str in _future_contract_date_strings(contract):
                _remember_future_conid_mapping(
                    mapping=mapping,
                    keys_added=keys_added,
                    symbol=symbol_upper,
                    exchange=desired_exchange,
                    expiration=exp_str,
                    conid=conid_int,
                )

    if expiration is not None:
        selected, actual_expiration, used_same_month, same_month_dates = _select_future_contract_for_target(
            contracts=contracts,
            target=target,
        )
        if selected is not None:
            conid_int = _future_contract_conid(selected)
            if conid_int is not None:
                if actual_expiration:
                    actual_date = _date_from_yyyymmdd(actual_expiration)
                    if actual_date is not None:
                        setattr(asset, "_ibkr_resolved_expiration", actual_date)
                    _remember_future_conid_mapping(
                        mapping=mapping,
                        keys_added=keys_added,
                        symbol=symbol_upper,
                        exchange=desired_exchange,
                        expiration=actual_expiration,
                        conid=conid_int,
                    )
                if used_same_month and actual_expiration:
                    _remember_future_conid_mapping(
                        mapping=mapping,
                        keys_added=keys_added,
                        symbol=symbol_upper,
                        exchange=desired_exchange,
                        expiration=target,
                        conid=conid_int,
                    )
                    logger.warning(
                        "IBKR did not list %s %s on %s exactly; using unambiguous same-month contract %s (conid=%s).",
                        symbol_upper,
                        target,
                        desired_exchange,
                        actual_expiration,
                        conid_int,
                    )
                elif actual_expiration and actual_expiration != target:
                    logger.warning(
                        "IBKR matched %s %s on %s by alternate date field; canonical listed date is %s (conid=%s).",
                        symbol_upper,
                        target,
                        desired_exchange,
                        actual_expiration,
                        conid_int,
                    )
                if neg_target_key:
                    _clear_negative_conid(key=neg_target_key)
                return int(conid_int)
        msg = (
            f"IBKR did not return a conid for {symbol_upper} expiring {target} on {desired_exchange}. "
            "If this is an expired contract, IBKR Client Portal cannot reliably discover it. "
            "Ensure the IBKR conid registry (`<cache>/ibkr/conids.json`, S3-mirrored) contains the "
            "missing expiration. New contracts are expected to auto-populate via REST; only older "
            "historical gaps require a one-time TWS backfill."
        )
        if same_month_dates:
            msg += (
                " IBKR returned same-month contract dates "
                f"{', '.join(same_month_dates)}, but they were not an unambiguous single contract."
            )
        if neg_target_key:
            _record_negative_conid(key=neg_target_key, reason="no_conid", message=msg)
        raise IbkrFuturesConidLookupError(msg)

    # Default: earliest expiration (front month) – used for smoke tests like MES.
    def _exp_key(item: Dict[str, Any]) -> int:
        try:
            return int(item.get("expirationDate") or 0)
        except Exception:
            return 0

    chosen = min(contracts, key=_exp_key)
    return int(chosen["conid"])


def _cache_file_for(
    *,
    asset: Asset,
    quote: Optional[Asset],
    timestep: str,
    exchange: Optional[str],
    source: str,
    include_after_hours: bool,
) -> Path:
    provider_root = Path(LUMIBOT_CACHE_FOLDER) / CACHE_SUBFOLDER
    asset_folder = _asset_folder(asset)
    _bar, _bar_seconds, timestep_component = _timestep_to_ibkr_bar(timestep)
    exch = (exchange or "").strip().upper() or "AUTO"
    symbol = _safe_component(getattr(asset, "symbol", "") or "symbol")
    quote_symbol = _safe_component(getattr(quote, "symbol", "") or "USD") if quote else "USD"
    expiration = getattr(asset, "expiration", None)
    exp_component = expiration.strftime("%Y%m%d") if expiration else ""
    source_component = _safe_component(source)
    session_component = "AHR" if bool(include_after_hours) else "RTH"
    suffix = f"_{exp_component}" if exp_component else ""
    filename = (
        f"{asset_folder}_{symbol}_{quote_symbol}_{timestep_component}_{exch}_{source_component}_{session_component}"
        f"{suffix}.parquet"
    )
    return provider_root / asset_folder / timestep_component / "bars" / filename


def _asset_folder(asset: Asset) -> str:
    asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
    if asset_type in {"crypto"}:
        return "crypto"
    if asset_type in {"future", "cont_future"}:
        return "future"
    return asset_type or "asset"


def _timestep_component(timestep: str) -> str:
    cleaned = str(timestep or "minute").strip().lower()
    return cleaned.replace(" ", "")


def _safe_component(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in value.upper())


def _timestep_to_ibkr_bar(timestep: str) -> Tuple[str, int, str]:
    raw = (timestep or "minute").strip().lower()
    raw = raw.replace(" ", "")
    raw = raw.replace("seconds", "second").replace("minutes", "minute").replace("hours", "hour").replace("days", "day")

    if raw in {"second", "1second", "s", "1s", "sec", "1sec"}:
        return "1sec", 1, "second"
    if raw.endswith("second"):
        qty = raw.removesuffix("second") or "1"
        seconds = int(qty)
        return f"{seconds}sec", seconds, f"{seconds}second"
    if raw.endswith("sec"):
        seconds = int(raw.removesuffix("sec") or "1")
        return f"{seconds}sec", seconds, f"{seconds}second"
    if raw.endswith("s") and raw[:-1].isdigit():
        seconds = int(raw[:-1] or "1")
        return f"{seconds}sec", seconds, f"{seconds}second"

    if raw in {"minute", "1minute", "m", "1m"}:
        return "1min", 60, "minute"
    if raw.endswith("minute"):
        qty = raw.removesuffix("minute") or "1"
        minutes = int(qty)
        return f"{minutes}min", minutes * 60, f"{minutes}minute"

    if raw in {"hour", "1hour", "h", "1h"}:
        return "1h", 60 * 60, "hour"
    if raw.endswith("hour"):
        qty = raw.removesuffix("hour") or "1"
        hours = int(qty)
        return f"{hours}h", hours * 60 * 60, f"{hours}hour"

    if raw in {"day", "1day", "d", "1d"}:
        return "1d", 24 * 60 * 60, "day"
    if raw.endswith("day"):
        qty = raw.removesuffix("day") or "1"
        days = int(qty)
        return f"{days}d", days * 24 * 60 * 60, f"{days}day"

    if raw.endswith("min"):
        minutes = int(raw.removesuffix("min") or "1")
        return f"{minutes}min", minutes * 60, f"{minutes}minute"

    raise ValueError(f"Unsupported IBKR timestep: {timestep}")


def _normalize_history_source(source: Optional[str]) -> str:
    raw = (source or os.environ.get("IBKR_HISTORY_SOURCE") or IBKR_DEFAULT_HISTORY_SOURCE).strip()
    if not raw:
        return IBKR_DEFAULT_HISTORY_SOURCE
    normalized = raw.strip().lower().replace("-", "_")
    if normalized in {"trades", "trade"}:
        return "Trades"
    if normalized in {"midpoint", "mid"}:
        return "Midpoint"
    if normalized in {"bid_ask", "bidask"}:
        return "Bid_Ask"
    raise ValueError(f"Unsupported IBKR history source '{source}'. Expected Trades, Midpoint, or Bid_Ask.")


def _get_cached_equity_actions(symbol: str, last_needed_datetime: Optional[datetime] = None) -> pd.DataFrame:
    """Return cached split/dividend actions for an equity symbol (best effort)."""
    key = str(symbol or "").strip().upper()
    if not key:
        return pd.DataFrame(columns=["Dividends", "Stock Splits"])
    cache_key = key
    if isinstance(last_needed_datetime, datetime):
        try:
            # Bucket by date to maximize in-process reuse while keeping "needed coverage" stable.
            cache_key = f"{key}:{last_needed_datetime.date().isoformat()}"
        except Exception:
            cache_key = key

    cached = _IBKR_EQUITY_ACTIONS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    actions = pd.DataFrame(columns=["Dividends", "Stock Splits"])
    try:
        from lumibot.tools.yahoo_helper import YahooHelper

        history = YahooHelper.get_symbol_data(
            key,
            interval="1d",
            caching=True,
            auto_adjust=False,
            last_needed_datetime=last_needed_datetime,
        )
        if history is not None and not history.empty:
            raw = history[["Dividends", "Stock Splits"]]
            raw = raw[(raw != 0).any(axis=1)].fillna(0)
        else:
            raw = None

        if raw is not None and not raw.empty:
            actions = raw.copy()
            actions.index = pd.to_datetime(actions.index, errors="coerce")
            actions = actions[~actions.index.isna()]
            if actions.index.tz is None:
                actions.index = actions.index.tz_localize(LUMIBOT_DEFAULT_PYTZ)
            else:
                actions.index = actions.index.tz_convert(LUMIBOT_DEFAULT_PYTZ)
            actions = actions.sort_index()
    except Exception as exc:
        logger.debug("IBKR equity corporate actions unavailable for %s: %s", key, exc)

    _IBKR_EQUITY_ACTIONS_CACHE[cache_key] = actions
    if cache_key != key and key not in _IBKR_EQUITY_ACTIONS_CACHE:
        _IBKR_EQUITY_ACTIONS_CACHE[key] = actions
    return actions


def _append_equity_corporate_actions_daily(frame: pd.DataFrame, asset: Asset) -> tuple[pd.DataFrame, bool]:
    """Append `dividend` + `stock_splits` columns to daily equity bars.

    WHY:
    - IBKR day bars do not include corporate actions in the history payload.
    - LumiBot dividend accounting reads these columns when present.
    - We enrich only stock/day bars, using Yahoo actions as a free, cached corporate-action
      source until a first-party IBKR corporate-actions endpoint is added to the downloader.
    """
    if frame is None or frame.empty:
        return frame, False
    if not _truthy_env("LUMIBOT_IBKR_ENRICH_EQUITY_CORPORATE_ACTIONS", "true"):
        return frame, False

    symbol = str(getattr(asset, "symbol", "") or "").strip().upper()
    if not symbol:
        return frame, False

    out = frame
    changed = False
    if "dividend" not in out.columns or "stock_splits" not in out.columns:
        out = out.copy()
        if "dividend" not in out.columns:
            out["dividend"] = 0.0
        if "stock_splits" not in out.columns:
            out["stock_splits"] = 0.0
        changed = True

    last_needed_datetime: Optional[datetime] = None
    try:
        if len(out.index):
            last_idx = pd.to_datetime(out.index, errors="coerce")
            if isinstance(last_idx, pd.DatetimeIndex) and len(last_idx) > 0 and not pd.isna(last_idx.max()):
                last_needed_datetime = last_idx.max().to_pydatetime()
    except Exception:
        last_needed_datetime = None

    actions = _get_cached_equity_actions(symbol, last_needed_datetime=last_needed_datetime)
    if actions.empty:
        return out, changed

    idx = pd.DatetimeIndex(out.index)
    if idx.tz is None:
        idx = idx.tz_localize(LUMIBOT_DEFAULT_PYTZ)
    else:
        idx = idx.tz_convert(LUMIBOT_DEFAULT_PYTZ)
    idx_dates = idx.date

    div_map: Dict[Any, float] = {}
    split_map: Dict[Any, float] = {}
    if "Dividends" in actions.columns:
        div_series = pd.to_numeric(actions["Dividends"], errors="coerce").fillna(0.0)
        div_map = div_series.groupby(actions.index.date).sum().to_dict()
    if "Stock Splits" in actions.columns:
        split_series = pd.to_numeric(actions["Stock Splits"], errors="coerce").fillna(0.0)
        split_map = split_series.groupby(actions.index.date).sum().to_dict()

    if div_map or split_map:
        out = out.copy()
        out["dividend"] = [float(div_map.get(day, 0.0)) for day in idx_dates]
        out["stock_splits"] = [float(split_map.get(day, 0.0)) for day in idx_dates]
        changed = True

    return out, changed


def _max_period_for_bar(bar: str) -> str:
    """Return an IBKR `period` that requests at most ~1000 datapoints for the bar size."""
    normalized = (bar or "").strip().lower()
    if normalized.endswith("sec"):
        multiplier = int(normalized.removesuffix("sec") or "1")
        return f"{IBKR_HISTORY_MAX_POINTS * multiplier}sec"
    if normalized.endswith("min"):
        multiplier = int(normalized.removesuffix("min") or "1")
        return f"{IBKR_HISTORY_MAX_POINTS * multiplier}min"
    if normalized.endswith("h"):
        multiplier = int(normalized.removesuffix("h") or "1")
        return f"{IBKR_HISTORY_MAX_POINTS * multiplier}h"
    if normalized.endswith("d"):
        multiplier = int(normalized.removesuffix("d") or "1")
        return f"{IBKR_HISTORY_MAX_POINTS * multiplier}d"
    return f"{IBKR_HISTORY_MAX_POINTS}min"


def _read_cache_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    if not isinstance(df.index, pd.DatetimeIndex):
        # Defensive: older caches might have a column index.
        if "datetime" in df.columns:
            df = df.set_index(pd.to_datetime(df["datetime"], utc=True, errors="coerce"))
            df = df.drop(columns=["datetime"], errors="ignore")
    df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
    df = df[~df.index.isna()]
    df = df.sort_index()
    df.index = df.index.tz_convert(LUMIBOT_DEFAULT_PYTZ)
    if "missing" in df.columns:
        df["missing"] = df["missing"].fillna(False)
    else:
        df["missing"] = False
    return df


def _write_cache_frame(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df_to_save = df.copy()
    if not isinstance(df_to_save.index, pd.DatetimeIndex):
        raise ValueError("IBKR cache frames must be indexed by datetime")
    df_to_save.to_parquet(path)
    try:
        get_backtest_cache().on_local_update(path, payload=_remote_payload_from_path(path))
    except Exception:
        pass


def _contract_info_cache_file(conid: int) -> Path:
    provider_root = Path(LUMIBOT_CACHE_FOLDER) / CACHE_SUBFOLDER
    return provider_root / "future" / "contracts" / f"CONID_{int(conid)}.json"


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, default=str, indent=2), encoding="utf-8")
    try:
        get_backtest_cache().on_local_update(path, payload=_remote_payload_from_path(path))
    except Exception:
        pass


def _fetch_contract_info(conid: int) -> Dict[str, Any]:
    base_url = _downloader_base_url()
    url = f"{base_url}/ibkr/iserver/contract/{int(conid)}/info"
    payload = queue_request(url=url, querystring=None, headers=None, timeout=None)
    if payload is None:
        return {}
    if isinstance(payload, dict) and payload.get("error"):
        raise RuntimeError(f"IBKR contract info error: {payload.get('error')}")
    if not isinstance(payload, dict):
        return {}
    return payload


def _maybe_apply_future_contract_metadata(*, asset: Asset, exchange: Optional[str]) -> None:
    """Best-effort: populate futures multiplier + min_tick for accurate PnL and tick rounding."""
    asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
    if asset_type not in {"future", "cont_future"}:
        return

    try:
        conid = _resolve_conid(asset=asset, quote=None, exchange=exchange)
    except Exception:
        return

    cache_file = _contract_info_cache_file(int(conid))
    cache = get_backtest_cache()
    try:
        cache.ensure_local_file(cache_file, payload={"provider": "ibkr", "type": "contract_info", "conid": int(conid)})
    except Exception:
        pass

    info = _read_json(cache_file)
    if not info:
        try:
            info = _fetch_contract_info(int(conid))
        except Exception:
            info = {}
        if info:
            _write_json(cache_file, info)

    if not info:
        return

    raw_mult = info.get("multiplier")
    try:
        mult_val = float(raw_mult) if raw_mult is not None else None
    except Exception:
        mult_val = None
    if mult_val and mult_val > 0:
        try:
            asset.multiplier = int(mult_val) if float(mult_val).is_integer() else mult_val  # type: ignore[assignment]
        except Exception:
            pass

    raw_tick = info.get("minTick") if "minTick" in info else info.get("min_tick")
    try:
        tick_val = float(raw_tick) if raw_tick is not None else None
    except Exception:
        tick_val = None
    if tick_val and tick_val > 0:
        try:
            setattr(asset, "min_tick", tick_val)
        except Exception:
            pass


def _remote_payload(
    asset: Asset,
    quote: Optional[Asset],
    timestep: str,
    exchange: Optional[str],
    source: str,
    include_after_hours: bool,
) -> Dict[str, object]:
    return {
        "provider": "ibkr",
        "symbol": getattr(asset, "symbol", None),
        "asset_type": str(getattr(asset, "asset_type", "") or ""),
        "quote": getattr(quote, "symbol", None) if quote else None,
        "timestep": timestep,
        "exchange": exchange,
        "source": source,
        "include_after_hours": bool(include_after_hours),
        "expiration": getattr(asset, "expiration", None).isoformat() if getattr(asset, "expiration", None) else None,
    }


def _remote_payload_from_path(path: Path) -> Dict[str, object]:
    return {"provider": "ibkr", "path": path.as_posix()}


def _conid_key(asset: Asset, quote: Optional[Asset], exchange: Optional[str]) -> IbkrConidKey:
    asset_type = _normalize_asset_type(getattr(asset, "asset_type", ""))
    symbol = str(getattr(asset, "symbol", "") or "")
    quote_symbol = str(getattr(quote, "symbol", "") or "") if quote else ""
    exch = (exchange or "").strip().upper()
    if asset_type == "crypto" and not exch:
        exch = (os.environ.get("IBKR_CRYPTO_VENUE") or IBKR_DEFAULT_CRYPTO_VENUE).strip().upper()
    expiration = ""
    if getattr(asset, "expiration", None) is not None:
        try:
            expiration = asset.expiration.strftime("%Y%m%d")  # type: ignore[union-attr]
        except Exception:
            expiration = str(asset.expiration)
    return IbkrConidKey(
        asset_type=asset_type,
        symbol=symbol,
        quote_symbol=quote_symbol,
        exchange=exch,
        expiration=expiration,
    )


def _downloader_base_url() -> str:
    return os.environ.get("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:8080").rstrip("/")


def _to_utc(dt_value: datetime) -> datetime:
    """Convert a datetime to UTC, treating naive datetimes as LumiBot local time.

    IMPORTANT: LumiBot uses `pytz` timezones. For pytz, you must NOT attach tzinfo via
    `datetime.replace(tzinfo=...)` because it can yield historical "LMT" offsets (e.g. -04:56
    for America/New_York) and create multi-hour gaps/misalignment in paginated history fetches.
    Use `tz.localize()` so DST rules apply correctly.
    """
    if isinstance(dt_value, pd.Timestamp):
        dt_value = dt_value.to_pydatetime()
    if dt_value.tzinfo is None:
        try:
            dt_value = LUMIBOT_DEFAULT_PYTZ.localize(dt_value)  # type: ignore[attr-defined]
        except Exception:
            # Fallback for non-pytz tzinfo implementations.
            dt_value = dt_value.replace(tzinfo=LUMIBOT_DEFAULT_PYTZ)
    return dt_value.astimezone(timezone.utc)
