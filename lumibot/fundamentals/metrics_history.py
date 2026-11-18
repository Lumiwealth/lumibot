"""Stored daily fundamentals: every ``FUNDAMENTAL_METRICS`` value for many symbols, one row per
trading session, so a screen reads one ready table instead of loading price history for every
candidate.

A row for session ``D`` is exactly what ``SECFundamentals.get_metrics`` returns with
``as_of`` = ``D`` 16:00 New York and ``price`` = the close traded on ``D``: only filings public
by that close, and that day's close. A backtest asking at time ``T`` gets the latest row whose
session closed at or before ``T`` (a strategy running at the open of ``D`` sees ``D - 1``), so
a stored row never shows a price or filing from after the backtest clock.

Files: one Parquet file per calendar year, ``metrics_daily/v1/<year>.parquet`` in the SEC cache
directory and under ``sec/metrics_daily/v1/<year>.parquet`` in the shared backtest cache.
Anyone with SEC access and as-traded closes can build them with ``build_metrics_history`` and
``save_metrics_history``. When no file covers a symbol and date, LumiBot computes the value as
before; nothing is estimated or carried forward beyond ``MAX_ROW_AGE``.
"""
from __future__ import annotations

import json
import os
import threading
from datetime import date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any, Iterable, Mapping
from zoneinfo import ZoneInfo

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

    from .sec import SECFundamentals

HISTORY_VERSION = 1
NEW_YORK = ZoneInfo("America/New_York")
SESSION_CLOSE = time(16, 0)
# A stored row older than this (calendar days) is not used for a later date: long market
# closures are shorter, and a missing file or symbol must never look like an unchanged value.
MAX_ROW_AGE = timedelta(days=7)
ENV_SWITCH = "LUMIBOT_SEC_METRICS_HISTORY"  # "0" turns reading stored rows off (emergency only)
BASE_COLUMNS = ("date", "symbol", "available_at", "close", "cik", "reasons")


def remote_key(year: int) -> str:
    return f"sec/metrics_daily/v{HISTORY_VERSION}/{int(year)}.parquet"


def session_close(day: date) -> datetime:
    return datetime.combine(day, SESSION_CLOSE, tzinfo=NEW_YORK)


def reading_enabled() -> bool:
    return str(os.environ.get(ENV_SWITCH, "1")).strip().lower() not in {"0", "false", "no", "off"}


def _local_path(sec: "SECFundamentals", year: int):
    return sec._cache_path("metrics_daily", f"v{HISTORY_VERSION}", f"{int(year)}.parquet")


def build_metrics_history(
    sec: "SECFundamentals",
    closes: Mapping[str, "pd.Series"],
    metrics: Iterable[str] | None = None,
) -> "pd.DataFrame":
    """Rows for every (session, symbol) in ``closes``.

    ``closes`` maps symbol to a Series of closes indexed by session date. The closes must be the
    prices actually traded that day (not adjusted for later splits or dividends), because market
    cap is shares on that day's basis times that day's price. A missing or non-positive close
    gives price-based metrics of None with a reason, as ``get_metrics`` does.
    """
    import pandas as pd

    from .sec import FUNDAMENTAL_METRICS

    names = list(FUNDAMENTAL_METRICS) if metrics is None else [str(name) for name in metrics]
    rows: list[dict[str, Any]] = []
    for symbol, series in closes.items():
        symbol = str(symbol).upper().strip()
        for when, close in series.items():
            day = pd.Timestamp(when).date()
            price = float(close) if close is not None and pd.notna(close) and float(close) > 0 else None
            row: dict[str, Any] = {
                "date": day, "symbol": symbol, "available_at": session_close(day), "close": price,
            }
            try:
                result = sec.get_metrics(symbol, names, as_of=session_close(day), price=price)
                values, reasons, cik = result["values"], dict(result["reasons"]), result.get("cik")
                if price is None:
                    for name in names:
                        if values.get(name) is None and name in _price_metric_names():
                            reasons[name] = "no traded close for this session"
            except Exception as exc:
                values = {name: None for name in names}
                reasons = {name: f"SEC data unavailable: {type(exc).__name__}: {exc}" for name in names}
                cik = None
            row.update({name: values.get(name) for name in names})
            row["cik"] = cik
            row["reasons"] = json.dumps(reasons, sort_keys=True)
            rows.append(row)
    frame = pd.DataFrame(rows, columns=[*BASE_COLUMNS, *names])
    for name in names:
        frame[name] = pd.to_numeric(frame[name], errors="coerce")
    return frame


def _price_metric_names() -> frozenset[str]:
    from .sec import _PRICE_METRICS

    return _PRICE_METRICS


def save_metrics_history(sec: "SECFundamentals", frame: "pd.DataFrame") -> list[str]:
    """Merge ``frame`` into the yearly files (a stored session and symbol is replaced) and share them.

    Returns the shared cache keys written.
    """
    import pandas as pd

    written: list[str] = []
    if frame is None or frame.empty:
        return written
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    for year, part in frame.groupby(frame["date"].map(lambda day: day.year)):
        path = _local_path(sec, int(year))
        key = remote_key(int(year))
        if not path.exists():
            sec._pull_remote(path, key)
        if path.exists():
            existing = pd.read_parquet(path)
            existing["date"] = pd.to_datetime(existing["date"]).dt.date
            merged = pd.concat([existing, part], ignore_index=True)
        else:
            merged = part
        merged = merged.drop_duplicates(["date", "symbol"], keep="last").sort_values(["date", "symbol"])
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}-{threading.get_ident()}")
        merged.to_parquet(tmp, index=False)
        os.replace(tmp, path)
        sec._push_remote(path, key)
        written.append(key)
    return written


class MetricsHistory:
    """Reads stored rows for ``SECFundamentals.get_metrics_table`` (one file per year, loaded once)."""

    def __init__(self, sec: "SECFundamentals"):
        self._sec = sec
        self._years: dict[int, Any] = {}
        self._lock = threading.Lock()

    def _year(self, year: int):
        with self._lock:
            if year in self._years:
                return self._years[year]
        import pandas as pd

        path = _local_path(self._sec, year)
        frame = None
        try:
            if path.exists() or self._sec._pull_remote(path, remote_key(year)):
                frame = pd.read_parquet(path)
                frame["date"] = pd.to_datetime(frame["date"]).dt.date
                frame = frame.sort_values("date")
        except Exception:
            frame = None  # a broken file is the same as no file: values are computed as before
        with self._lock:
            self._years[year] = frame
        return frame

    def lookup(self, symbols: Iterable[str], metrics: list[str], as_of: datetime) -> dict[str, dict[str, Any]]:
        """``symbol -> {metric: value, "cik", "reasons"}`` for symbols with a usable stored row."""
        import pandas as pd

        as_of = as_of if as_of.tzinfo else as_of.replace(tzinfo=NEW_YORK)
        local = as_of.astimezone(NEW_YORK)
        latest_day = local.date() if local.time() >= SESSION_CLOSE else local.date() - timedelta(days=1)
        oldest_day = latest_day - MAX_ROW_AGE
        wanted = {str(symbol).upper().strip() for symbol in symbols}
        frames = [self._year(year) for year in sorted({oldest_day.year, latest_day.year})]
        frames = [frame for frame in frames if frame is not None and not frame.empty]
        if not frames or not wanted:
            return {}
        frame = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        if any(name not in frame.columns for name in metrics):
            return {}
        window = frame[(frame["date"] <= latest_day) & (frame["date"] >= oldest_day) & frame["symbol"].isin(wanted)]
        if window.empty:
            return {}
        last = window.sort_values("date").groupby("symbol", sort=False).tail(1)
        found: dict[str, dict[str, Any]] = {}
        for record in last.to_dict("records"):
            row = {name: (None if pd.isna(record[name]) else float(record[name])) for name in metrics}
            try:
                stored_reasons = json.loads(record.get("reasons") or "{}")
            except ValueError:
                stored_reasons = {}
            row["reasons"] = {name: stored_reasons.get(name, "not available") for name in metrics if row[name] is None}
            row["cik"] = record.get("cik")
            found[str(record["symbol"])] = row
        return found
