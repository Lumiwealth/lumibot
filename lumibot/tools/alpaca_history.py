"""Shared provider bars for live analysis and backtests.

Only complete, closed calendar-month requests are persisted. A partition always
contains real provider observations, never reindexed/filled simulation prices.
Overlapping callers therefore cannot overwrite one another's partial windows.
The existing BacktestCacheManager owns the S3 namespace and credentials.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from lumibot import constants
from lumibot.data_sources.exceptions import InvalidBars
from lumibot.tools.parquet_series_cache import ParquetSeriesCache

logger = logging.getLogger(__name__)
_MAX_AGE = timedelta(hours=24)


def _value(value):
    return getattr(value, "value", value)


def _utc(value):
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def _next_month(stamp):
    return stamp + pd.offsets.MonthBegin(1)


def _frame(value, symbols):
    frame = value.df.copy()
    if not isinstance(frame.index, pd.MultiIndex):
        if len(symbols) != 1:
            raise InvalidBars("Alpaca multi-symbol bars require a symbol/timestamp index")
        frame = pd.concat([frame], keys=symbols, names=["symbol", "timestamp"])
    return frame


def _valid_rows(rows, start, stop):
    fields = ["open", "high", "low", "close", "volume"]
    if rows.empty or not set(fields) <= set(rows.columns):
        return False
    try:
        values = rows[fields].to_numpy(dtype=float)
        return (
            isinstance(rows.index, pd.DatetimeIndex)
            and not rows.index.hasnans
            and not rows.index.has_duplicates
            and bool(((rows.index >= start) & (rows.index < stop)).all())
            and bool(np.isfinite(values).all())
            and bool((values[:, :4] > 0).all())
            and bool((values[:, 4] >= 0).all())
            and bool((rows["low"] <= rows[["open", "close", "high"]].min(axis=1)).all())
            and bool((rows["high"] >= rows[["open", "close", "low"]].max(axis=1)).all())
        )
    except (ValueError, TypeError):
        return False


def fetch_alpaca_bars(client, request, method, *, now=None, refresh=False):
    """Run an Alpaca SDK bars read using the shared backtest Parquet cache.

    The return contract is the BarSet ``df`` consumed by LumiBot's adapters. Provider
    exceptions propagate. An unusable cache causes a real provider read, never an
    empty-success replacement. Explicit limits and unknown credential scopes bypass
    reuse because they cannot establish complete partition coverage.
    """
    lookup_started = time.perf_counter()
    raw_symbols = request.symbol_or_symbols
    symbols = [raw_symbols] if isinstance(raw_symbols, str) else list(raw_symbols)
    api_key = getattr(client, "_api_key", None)
    oauth_token = getattr(client, "_oauth_token", None)
    credential = oauth_token if isinstance(oauth_token, str) and oauth_token else api_key
    if (
        not isinstance(credential, str)
        or not credential
        or request.start is None
        or request.end is None
        or getattr(request, "limit", None) is not None
        or getattr(request, "sort", None) is not None
    ):
        return method(request)
    start, end = _utc(request.start), _utc(request.end)
    if end < start:
        return method(request)
    clock = _utc(now or datetime.now(timezone.utc))
    identity = {
        "schema": 1,
        "requestType": type(request).__name__,
        "timeframe": str(request.timeframe),
        "feed": _value(getattr(request, "feed", None)),
        "adjustment": _value(getattr(request, "adjustment", None)),
        "asof": getattr(request, "asof", None),
        "currency": _value(getattr(request, "currency", None)),
        # Default feed/entitlement can differ across credentials. Reuse between
        # chart and backtest with the same connection, never infer entitlements.
        "credentialScope": hashlib.sha256(credential.encode()).hexdigest(),
    }
    identity_hash = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    root = Path(constants.LUMIBOT_CACHE_FOLDER) / "alpaca" / "bars" / identity_hash
    cursor = start.normalize().replace(day=1)
    windows = []
    pieces = []
    hits = 0
    cache_errors = []
    fetched_at = []
    while cursor <= end:
        stop = _next_month(cursor)
        closed = stop <= clock - timedelta(days=1)
        missing = []
        for symbol in symbols:
            symbol_key = hashlib.sha256(symbol.encode()).hexdigest()
            cache = ParquetSeriesCache(
                root / symbol_key / f"{cursor:%Y-%m}.parquet",
                remote_payload={"provider": "alpaca", "type": "bars"},
                tz="UTC",
            )
            cached = pd.DataFrame()
            if closed and not refresh:
                cache.hydrate_remote()
                cached = cache.read()
            metadata = cached.attrs.get("alpacaPartition", {})
            try:
                fresh = clock - _utc(metadata["fetchedAt"]) < _MAX_AGE
                valid = (
                    metadata.get("identity") == identity_hash
                    and metadata.get("symbol") == symbol
                    and metadata.get("start") == cursor.isoformat()
                    and metadata.get("endExclusive") == stop.isoformat()
                    and timedelta(0) <= clock - _utc(metadata["fetchedAt"])
                )
            except (KeyError, TypeError, ValueError):
                fresh = valid = False
            if valid and fresh and _valid_rows(cached, cursor, stop):
                hits += 1
                fetched_at.append(metadata["fetchedAt"])
                pieces.append(pd.concat([cached], keys=[symbol], names=["symbol", "timestamp"]))
            else:
                missing.append((symbol, cache))
        if missing:
            windows.append((cursor, stop, closed, missing))
        cursor = stop

    # Adjacent misses with the same symbol set remain one SDK multi-symbol read.
    groups = []
    for window in windows:
        if (
            groups
            and groups[-1][-1][1] == window[0]
            and groups[-1][-1][2] == window[2]
            and [x[0] for x in groups[-1][-1][3]] == [x[0] for x in window[3]]
        ):
            groups[-1].append(window)
        else:
            groups.append([window])
    lookup_ms = (time.perf_counter() - lookup_started) * 1000
    provider_ms = write_ms = 0.0
    for group in groups:
        first, last = group[0], group[-1]
        group_symbols = [item[0] for item in first[3]]
        closed = first[2]
        changes = {
            "symbol_or_symbols": group_symbols,
            "start": (first[0] if closed else max(start, first[0])).to_pydatetime(),
            "end": (last[1] if closed else min(end, last[1])).to_pydatetime(),
        }
        params = request.model_copy(update=changes)
        provider_started = time.perf_counter()
        fetched = _frame(method(params), group_symbols)
        provider_ms += (time.perf_counter() - provider_started) * 1000
        fetched_at.append(clock.isoformat())
        # Persist each entire month, including any real gaps within it. Never
        # infer coverage from first/last bar alone or fill weekends/no-trade bars.
        for month_start, month_stop, is_closed, entries in group:
            for symbol, cache in entries:
                if symbol not in fetched.index.get_level_values(0):
                    continue
                rows = fetched.xs(symbol, level=0).copy()
                rows.index = pd.to_datetime(rows.index, utc=True)
                rows = rows[(rows.index >= month_start) & (rows.index < month_stop)]
                rows = rows[~rows.index.duplicated(keep="last")].sort_index()
                if rows.empty:
                    continue
                if not _valid_rows(rows, month_start, month_stop):
                    raise InvalidBars("Alpaca returned invalid OHLCV bars; shared cache was not written")
                if is_closed:
                    rows.attrs["alpacaPartition"] = {
                        "identity": identity_hash,
                        "symbol": symbol,
                        "start": month_start.isoformat(),
                        "endExclusive": month_stop.isoformat(),
                        "fetchedAt": clock.isoformat(),
                    }
                    write_started = time.perf_counter()
                    try:
                        cache.write(rows)
                    except Exception as exc:
                        cache_errors.append(type(exc).__name__)
                        logger.warning("Alpaca bar cache write failed; provider bars retained: %s", type(exc).__name__)
                    write_ms += (time.perf_counter() - write_started) * 1000
                pieces.append(pd.concat([rows], keys=[symbol], names=["symbol", "timestamp"]))
    if pieces:
        result = pd.concat(pieces).sort_index()
        result = result[~result.index.duplicated(keep="last")]
        times = pd.to_datetime(result.index.get_level_values(1), utc=True)
        result = result[(times >= start) & (times <= end)]
    else:
        result = pd.DataFrame(
            columns=["open", "high", "low", "close", "volume"],
            index=pd.MultiIndex.from_arrays([[], []], names=["symbol", "timestamp"]),
        )
    result.attrs["historyCache"] = {
        "kind": "lumibot_shared_bars",
        "hit": not groups,
        "partitionHits": hits,
        "providerReads": len(groups),
        "cacheErrors": cache_errors,
        "lookupMs": round(lookup_ms, 3),
        "providerMs": round(provider_ms, 3),
        "writeMs": round(write_ms, 3),
        "oldestFetchedAt": min(fetched_at) if fetched_at else None,
    }
    return SimpleNamespace(df=result)
