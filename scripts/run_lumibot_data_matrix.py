#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import requests

try:
    from dateutil.relativedelta import relativedelta
except Exception:  # pragma: no cover
    relativedelta = None

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

try:
    from zoneinfo import ZoneInfo

    US_EASTERN = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    US_EASTERN = None


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("LUMIBOT_DISABLE_UI", "1")
os.environ.setdefault("LUMIBOT_DISABLE_DOTENV", "1")

import lumibot
from lumibot.backtesting import YahooDataBacktesting
from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from lumibot.tools import ibkr_helper


WINDOWS_BY_INTERVAL: dict[str, list[str]] = {
    "minute": ["1d", "5d", "30d", "90d", "180d", "1y", "3y", "5y", "10y", "15y", "20y", "30y"],
    "hour": ["5d", "30d", "180d", "1y", "3y", "5y", "10y", "15y", "20y", "30y"],
    "day": ["30d", "180d", "1y", "3y", "5y", "10y", "15y", "20y", "30y"],
}

QUALITY_FLAT_THRESHOLDS = {
    "minute": 120,
    "hour": 36,
    "day": 10,
}

BAR_DELTAS = {
    "minute": timedelta(minutes=1),
    "hour": timedelta(hours=1),
    "day": timedelta(days=1),
}

REQUIRED_SYMBOLS = {
    "ibkr": {
        "indexes": ["VIX", "SPX", "NDX", "RUT"],
        "stocks": ["SPY", "AAPL", "TSLA", "REM", "MOBX"],
        "crypto": ["BTC", "ETH"],
    },
    "yahoo": {
        "indexes": ["VIX", "SPX", "NDX", "RUT"],
    },
}


@dataclass(frozen=True)
class CellConfig:
    provider: str
    symbol: str
    asset_type: str
    market: str
    quote_symbol: Optional[str] = None


class _HistoricalSeriesArtifactStrategy(Strategy):
    def initialize(self, parameters=None):
        interval = str((parameters or {}).get("interval") or "day")
        self.sleeptime = {"minute": "1M", "hour": "1H", "day": "1D"}[interval]
        self.include_cash_positions = True
        self.completed = False
        self.validation: dict[str, Any] = {}
        self.exact_window_df: Optional[pd.DataFrame] = None

    def on_trading_iteration(self):
        if self.completed:
            return

        params = self.parameters
        asset: Asset = params["asset"]
        quote: Asset | None = params.get("quote")
        interval = str(params["interval"])
        start_dt: datetime = params["start_dt"]
        end_dt: datetime = params["end_dt"]
        exchange = params.get("exchange")

        self.validation["lumibot_file"] = str(Path(lumibot.__file__).resolve())
        self.validation["strategy_datetime"] = self.get_datetime().astimezone(timezone.utc).isoformat()

        public_probe = self.get_historical_prices(
            asset,
            length=2,
            timestep=interval,
            quote=quote,
        )
        public_probe_df = getattr(public_probe, "df", None)
        if public_probe_df is None or public_probe_df.empty:
            raise RuntimeError(f"public get_historical_prices returned no data for {asset.symbol} {interval}")
        self.validation["public_probe_rows"] = int(len(public_probe_df.index))

        last_price = self.get_last_price(asset, quote=quote)
        if last_price is None:
            raise RuntimeError(f"get_last_price returned None for {asset.symbol} {interval}")
        self.validation["last_price"] = float(last_price)

        frame = _fetch_exact_window_frame(
            strategy=self,
            asset=asset,
            quote=quote,
            interval=interval,
            start_dt=start_dt,
            end_dt=end_dt,
            exchange=exchange,
        )
        self.exact_window_df = frame
        self.validation["history_rows"] = int(len(frame.index))
        self.validation["history_columns"] = list(frame.columns)

        for ts, row in frame.iterrows():
            dt_value = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
            close_value = _safe_float(row.get("close"))
            if close_value is not None:
                self.add_line(
                    f"{asset.symbol} {interval} close",
                    close_value,
                    dt=dt_value,
                    plot_name="close",
                    asset=asset,
                )
            open_value = _safe_float(row.get("open"))
            high_value = _safe_float(row.get("high"))
            low_value = _safe_float(row.get("low"))
            if None not in {open_value, high_value, low_value, close_value}:
                self.add_ohlc(
                    f"{asset.symbol} {interval} ohlc",
                    open=open_value,
                    high=high_value,
                    low=low_value,
                    close=close_value,
                    dt=dt_value,
                    plot_name="ohlc",
                    asset=asset,
                )

        self.completed = True


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_repo_env() -> None:
    if load_dotenv is None:
        return
    for candidate in (REPO_ROOT / ".env.local", REPO_ROOT / ".env"):
        if candidate.exists():
            load_dotenv(candidate, override=False)


def _normalize_provider(provider: str) -> str:
    value = str(provider or "").strip().lower()
    if value not in {"ibkr", "yahoo"}:
        raise ValueError(f"Unsupported provider {provider}")
    return value


def _normalize_asset_type(asset_type: str) -> str:
    value = str(asset_type or "").strip().lower()
    if value not in {"index", "stock", "crypto"}:
        raise ValueError(f"Unsupported asset_type {asset_type}")
    return value


def _build_asset(symbol: str, asset_type: str) -> Asset:
    asset_type = _normalize_asset_type(asset_type)
    if asset_type == "index":
        return Asset(symbol, asset_type=Asset.AssetType.INDEX)
    if asset_type == "stock":
        return Asset(symbol, asset_type=Asset.AssetType.STOCK)
    return Asset(symbol, asset_type=Asset.AssetType.CRYPTO)


def _build_quote_asset(quote_symbol: Optional[str]) -> Optional[Asset]:
    if not quote_symbol:
        return None
    return Asset(quote_symbol, asset_type=Asset.AssetType.FOREX)


def _window_to_start(end_dt: datetime, window: str) -> datetime:
    if window.endswith("y"):
        years = int(window[:-1])
        if relativedelta is not None:
            return end_dt - relativedelta(years=years)
        return end_dt - timedelta(days=365 * years)
    if window.endswith("d"):
        return end_dt - timedelta(days=int(window[:-1]))
    raise ValueError(f"Unsupported window {window}")


def _backtest_bounds_for_requested_end(*, requested_end: datetime, interval: str) -> tuple[datetime, datetime]:
    delta = BAR_DELTAS[interval]
    probe_span = delta * 2
    if interval == "minute":
        probe_span = max(probe_span, timedelta(hours=2))
    # Some backtest data sources normalize timestamps to completed bars, which can collapse a
    # one-bar probe into a zero-length window after flooring. Use a small two-bar window instead.
    return requested_end - probe_span, requested_end


def _normalize_case_status(case: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(case)
    outcome = str(normalized.get("outcome") or "").strip().lower()
    status = str(normalized.get("status") or "").strip().lower()
    if outcome in {"pass", "support-limit", "fail"}:
        normalized["status"] = outcome
        return normalized
    if status == "completed":
        normalized["outcome"] = "pass"
        normalized["status"] = "pass"
    elif status == "error":
        normalized["outcome"] = "support-limit" if _explicit_support_limit(normalized.get("error")) else "fail"
        normalized["status"] = normalized["outcome"]
    return normalized


def _estimate_public_history_length_for_window(
    *,
    interval: str,
    asset_type: str,
    start_dt: datetime,
    end_dt: datetime,
) -> int:
    span = max(end_dt - start_dt, BAR_DELTAS[interval])
    total_days = max(1, int(math.ceil(span.total_seconds() / 86400)))
    if interval == "day":
        if asset_type == "crypto":
            return total_days + 7
        trading_days = max(1, int(math.ceil(total_days * 5 / 7)))
        return trading_days + 10
    if interval == "hour":
        if asset_type == "crypto":
            return max(2, int(math.ceil(span.total_seconds() / 3600))) + 24
        trading_days = max(1, int(math.ceil(total_days * 5 / 7)))
        return trading_days * 8 + 16
    if asset_type == "crypto":
        return max(2, int(math.ceil(span.total_seconds() / 60))) + 1440
    trading_days = max(1, int(math.ceil(total_days * 5 / 7)))
    return trading_days * 390 + 390


def _fetch_exact_window_frame(
    *,
    strategy: Strategy,
    asset: Asset,
    quote: Asset | None,
    interval: str,
    start_dt: datetime,
    end_dt: datetime,
    exchange: Optional[str],
) -> pd.DataFrame:
    data_source = strategy.broker.data_source
    asset_arg: Asset | tuple[Asset, Asset]
    if quote is not None:
        asset_arg = (asset, quote)
    else:
        asset_arg = asset

    if hasattr(data_source, "get_historical_prices_between_dates"):
        bars = data_source.get_historical_prices_between_dates(
            asset_arg,
            timestep=interval,
            quote=quote,
            exchange=exchange,
            start_date=start_dt,
            end_date=end_dt,
        )
        exact_df = getattr(bars, "df", None)
        if exact_df is None or exact_df.empty:
            raise RuntimeError(f"exact-window history returned no data for {asset.symbol} {interval}")
        return _normalize_price_frame(exact_df)

    length = _estimate_public_history_length_for_window(
        interval=interval,
        asset_type=_normalize_asset_type(getattr(asset, "asset_type", "")),
        start_dt=start_dt,
        end_dt=end_dt,
    )
    bars = strategy.get_historical_prices(
        asset,
        length=length,
        timestep=interval,
        quote=quote,
    )
    fallback_df = getattr(bars, "df", None)
    if fallback_df is None or fallback_df.empty:
        raise RuntimeError(f"public fallback history returned no data for {asset.symbol} {interval}")
    frame = _normalize_price_frame(fallback_df)
    sliced = frame.loc[(frame.index >= start_dt) & (frame.index <= end_dt)]
    if sliced.empty:
        raise RuntimeError(f"public fallback history did not cover requested window for {asset.symbol} {interval}")
    return sliced


def _safe_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _run_lengths(values: list[Any]) -> tuple[int, Optional[int], Optional[int]]:
    if not values:
        return 0, None, None
    best = 1
    best_start = 0
    best_end = 0
    current = 1
    current_start = 0
    for index in range(1, len(values)):
        if values[index] == values[index - 1]:
            current += 1
            if current > best:
                best = current
                best_start = current_start
                best_end = index
        else:
            current = 1
            current_start = index
    return best, best_start, best_end


def _normalize_price_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame.columns = [str(col).strip().lower() for col in frame.columns]
    index = pd.DatetimeIndex(frame.index)
    if index.tz is None:
        index = index.tz_localize(timezone.utc)
    else:
        index = index.tz_convert(timezone.utc)
    frame.index = index
    frame = frame.sort_index()
    return frame


def _quality_issues_for_frame(
    *,
    frame: pd.DataFrame,
    interval: str,
    asset_type: str,
    requested_start: datetime,
    requested_end: datetime,
) -> tuple[list[str], bool]:
    issues: list[str] = []
    duplicate_timestamps = int(frame.index.duplicated().sum())
    if duplicate_timestamps:
        issues.append(f"duplicate_timestamps:{duplicate_timestamps}")

    if not frame.index.is_monotonic_increasing:
        issues.append("non_increasing_timestamps")

    flat_ohlc_values = list(
        zip(
            frame.get("open", pd.Series(dtype=float)).tolist(),
            frame.get("high", pd.Series(dtype=float)).tolist(),
            frame.get("low", pd.Series(dtype=float)).tolist(),
            frame.get("close", pd.Series(dtype=float)).tolist(),
        )
    )
    close_values = [value for value in pd.to_numeric(frame.get("close"), errors="coerce").dropna().tolist()]
    max_flat_ohlc_run, _, _ = _run_lengths(flat_ohlc_values)
    max_flat_close_run, _, _ = _run_lengths(close_values)
    threshold = QUALITY_FLAT_THRESHOLDS[interval]
    if max_flat_ohlc_run > threshold:
        issues.append(f"flat_ohlc_run:{max_flat_ohlc_run}")
    if max_flat_close_run > threshold:
        issues.append(f"flat_close_run:{max_flat_close_run}")
    if len(set(close_values)) < 3 and len(close_values) > 10:
        issues.append(f"suspiciously_few_unique_closes:{len(set(close_values))}")

    coverage_asset = _build_asset("COVERAGE", asset_type)
    coverage_ok = bool(
        ibkr_helper.frame_covers_requested_window(
            frame,
            asset=coverage_asset,
            start_dt=requested_start,
            end_dt=requested_end,
            timestep=interval,
        )
    )
    if not coverage_ok:
        issues.append("coverage_shortfall")

    return issues, coverage_ok


def _explicit_support_limit(error_text: str) -> bool:
    lowered = str(error_text or "").lower()
    markers = [
        "no data",
        "possibly delisted",
        "not available for starttime",
        "range must be within",
        "requested range must be within",
        "yfpricesmissingerror",
        "download failed",
        "unavailabetimestep",
        "does not have data with",
    ]
    return any(marker in lowered for marker in markers)


def _derive_failure_bucket(record: dict[str, Any]) -> Optional[str]:
    outcome = str(record.get("outcome") or "")
    if outcome in {"pass", "support-limit"}:
        return None
    error = str(record.get("error") or "").lower()
    issues = " ".join(record.get("quality_issues") or [])
    if "timeout" in error:
        return "provider throughput timeout"
    if "coverage_shortfall" in issues:
        return "provider-specific index handling bug"
    if "duplicate_timestamps" in issues or "non_increasing_timestamps" in issues or "flat_" in issues:
        return "lumibot ingestion bug"
    if "cache" in error:
        return "lumibot cache bug"
    if "symbol" in error or "conid" in error:
        return "symbol-mapping bug"
    return "provider hard limit" if _explicit_support_limit(error) else "lumibot ingestion bug"


def _stable_end_datetime(asset_type: str, now_utc: datetime) -> datetime:
    if asset_type == "crypto":
        return now_utc.replace(second=0, microsecond=0) - timedelta(minutes=5)
    if US_EASTERN is None:
        return now_utc - timedelta(days=1)

    local_now = now_utc.astimezone(US_EASTERN)
    close_today = datetime.combine(local_now.date(), dt_time(hour=16, minute=0), tzinfo=US_EASTERN)
    if local_now.weekday() < 5 and local_now >= close_today:
        return close_today.astimezone(timezone.utc)

    cursor = local_now.date() - timedelta(days=1)
    while cursor.weekday() >= 5:
        cursor -= timedelta(days=1)
    previous_close = datetime.combine(cursor, dt_time(hour=16, minute=0), tzinfo=US_EASTERN)
    return previous_close.astimezone(timezone.utc)


def _assert_repo_lumibot_import_path() -> str:
    resolved = Path(lumibot.__file__).resolve()
    if not resolved.is_relative_to(REPO_ROOT / "lumibot"):
        raise RuntimeError(f"Expected repo LumiBot import under {REPO_ROOT / 'lumibot'}, got {resolved}")
    return str(resolved)


def _require_prod_downloader() -> dict[str, str]:
    base_url = (os.environ.get("DATADOWNLOADER_BASE_URL") or "").strip().rstrip("/")
    api_key = (os.environ.get("DATADOWNLOADER_API_KEY") or "").strip()
    api_key_header = (os.environ.get("DATADOWNLOADER_API_KEY_HEADER") or "X-Downloader-Key").strip()
    if not base_url or not api_key:
        raise RuntimeError("Missing DATADOWNLOADER_BASE_URL / DATADOWNLOADER_API_KEY")
    if any(host in base_url for host in ("localhost", "127.0.0.1", "0.0.0.0")):
        raise RuntimeError(f"Refusing local downloader base URL for production verification: {base_url}")

    resp = requests.get(
        f"{base_url}/healthz",
        headers={api_key_header: api_key},
        timeout=10,
    )
    resp.raise_for_status()
    payload = resp.json()
    ibkr = payload.get("ibkr") if isinstance(payload, dict) else None
    if not isinstance(ibkr, dict) or not ibkr.get("enabled"):
        raise RuntimeError("Production downloader reachable but IBKR is not enabled")
    if ibkr.get("authenticated") is not True:
        raise RuntimeError("Production downloader reachable but IBKR is not authenticated")

    return {
        "base_url": base_url,
        "api_key_header": api_key_header,
    }


def _load_downloader_targets(path: Optional[str]) -> dict[tuple[str, str], dict[str, Any]]:
    if not path:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload.get("downloader_matrix") or payload.get("interval_summary") or []
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        out[(str(row.get("symbol") or ""), str(row.get("interval") or ""))] = row
    return out


def _windows_for_cell(
    *,
    provider: str,
    symbol: str,
    interval: str,
    downloader_targets: dict[tuple[str, str], dict[str, Any]],
) -> list[str]:
    if provider == "ibkr":
        target = downloader_targets.get((symbol, interval))
        if target:
            windows: list[str] = []
            max_reachable = target.get("max_reachable_window")
            first_support = target.get("first_support_limit_window")
            if isinstance(max_reachable, str) and max_reachable:
                windows.append(max_reachable)
            if isinstance(first_support, str) and first_support and first_support not in windows:
                windows.append(first_support)
            if windows:
                return windows
    return list(WINDOWS_BY_INTERVAL[interval])


def _datasource_for_provider(provider: str):
    if provider == "ibkr":
        return InteractiveBrokersRESTBacktesting
    if provider == "yahoo":
        return YahooDataBacktesting
    raise ValueError(provider)


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _append_run_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{_iso_now()}] {message}\n")


def _summarize_interval(
    *,
    provider: str,
    symbol: str,
    asset_type: str,
    interval: str,
    cases: list[dict[str, Any]],
    downloader_targets: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    successes = [case for case in cases if case.get("outcome") == "pass"]
    failures = [case for case in cases if case.get("outcome") == "fail"]
    support_limits = [case for case in cases if case.get("outcome") == "support-limit"]
    focus_case = successes[-1] if successes else (support_limits[0] if support_limits else (failures[0] if failures else None))
    max_reachable = successes[-1]["window"] if successes else None
    first_support_limit = support_limits[0]["window"] if support_limits else None

    if failures:
        status = "fail"
        confidence = "works inconsistently" if successes else "broken"
    elif support_limits:
        status = "support-limit"
        confidence = "proven" if (not successes or (max_reachable and max_reachable in WINDOWS_BY_INTERVAL[interval][-2:])) else "proven but shallow"
    elif successes:
        status = "pass"
        confidence = "proven" if max_reachable and max_reachable in WINDOWS_BY_INTERVAL[interval][-2:] else "proven but shallow"
    else:
        status = "not-run"
        confidence = None

    downloader_row = downloader_targets.get((symbol, interval)) if provider == "ibkr" else None
    backend = (focus_case or {}).get("backend") or (downloader_row or {}).get("backend")
    classification = (focus_case or {}).get("classification") or (downloader_row or {}).get("classification")
    cache_write_policy = (focus_case or {}).get("cache_write_policy") or (downloader_row or {}).get("cache_write_policy")
    failure_bucket = next((case.get("failure_bucket") for case in cases if case.get("failure_bucket")), None)
    artifact_path = (focus_case or {}).get("artifact_path")
    metadata_path = (focus_case or {}).get("metadata_path")
    validation_log_path = (focus_case or {}).get("validation_log_path")

    return {
        "provider": provider,
        "symbol": symbol,
        "asset_type": asset_type,
        "interval": interval,
        "status": status,
        "backend": backend,
        "classification": classification,
        "cache_write_policy": cache_write_policy,
        "max_reachable_window": max_reachable,
        "first_support_limit_window": first_support_limit,
        "row_count": (focus_case or {}).get("row_count") or 0,
        "first_timestamp": (focus_case or {}).get("first_timestamp"),
        "last_timestamp": (focus_case or {}).get("last_timestamp"),
        "quality_issues": ", ".join((focus_case or {}).get("quality_issues") or []),
        "failure_bucket": failure_bucket,
        "artifact_path": artifact_path,
        "metadata_path": metadata_path,
        "validation_log_path": validation_log_path,
        "confidence": confidence,
        "comparison_note": (downloader_row or {}).get("comparison_note", "not_compared"),
        "pass_count": len(successes),
        "fail_count": len(failures),
        "support_limit_count": len(support_limits),
    }


def _build_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# LumiBot Data Matrix",
        "",
        f"Generated: {report['generated_at']}",
        f"Repo LumiBot: {report['lumibot_file']}",
        "",
        "## Summary",
        "",
        "| Provider | Symbol | Asset | Interval | Status | Backend | Class | Cache | Max Reach | First Support Limit | Rows | First Timestamp | Last Timestamp | Quality Issues | Failure Bucket | Confidence | Artifact |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---: | --- | --- | --- | --- | --- | --- |",
    ]
    for row in report["lumibot_matrix"]:
        lines.append(
            "| {provider} | {symbol} | {asset_type} | {interval} | {status} | {backend} | {classification} | {cache_write_policy} | {max_reachable_window} | {first_support_limit_window} | {row_count} | {first_timestamp} | {last_timestamp} | {quality_issues} | {failure_bucket} | {confidence} | {artifact_path} |".format(
                **{k: ("" if v is None else str(v).replace("|", "/")) for k, v in row.items()}
            )
        )
    lines.extend(
        [
            "",
            "## Detailed Cases",
            "",
            "| Provider | Symbol | Interval | Window | Outcome | Status | Rows | Coverage | Last Price | Last Close | First Timestamp | Last Timestamp | Quality Issues | Artifact | Error |",
            "| --- | --- | --- | --- | --- | --- | ---: | --- | ---: | ---: | --- | --- | --- | --- | --- |",
        ]
    )
    for case in report["cases"]:
        lines.append(
            "| {provider} | {symbol} | {interval} | {window} | {outcome} | {status} | {row_count} | {coverage_ok} | {last_price} | {last_close} | {first_timestamp} | {last_timestamp} | {quality_issues} | {artifact_path} | {error} |".format(
                provider=case.get("provider") or "",
                symbol=case.get("symbol") or "",
                interval=case.get("interval") or "",
                window=case.get("window") or "",
                outcome=case.get("outcome") or "",
                status=case.get("status") or "",
                row_count=case.get("row_count") or 0,
                coverage_ok="yes" if case.get("coverage_ok") else "no",
                last_price="" if case.get("last_price") is None else case.get("last_price"),
                last_close="" if case.get("last_close") is None else case.get("last_close"),
                first_timestamp=case.get("first_timestamp") or "",
                last_timestamp=case.get("last_timestamp") or "",
                quality_issues=", ".join(case.get("quality_issues") or [])[:120],
                artifact_path=case.get("artifact_path") or "",
                error=str(case.get("error") or "").replace("|", "/")[:180],
            )
        )
    return "\n".join(lines) + "\n"


def _write_report_files(outdir: Path, report: dict[str, Any]) -> None:
    json_path = outdir / "lumibot_matrix.json"
    md_path = outdir / "lumibot_matrix.md"
    csv_path = outdir / "lumibot_matrix.csv"
    failure_path = outdir / "lumibot_failure_buckets.md"
    unsupported_path = outdir / "lumibot_unsupported_limits.md"

    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_build_markdown(report), encoding="utf-8")
    _write_csv(csv_path, report["lumibot_matrix"])

    failure_lines = ["# LumiBot Failure Buckets", ""]
    if report["failure_buckets"]:
        failure_lines.append("| Provider | Symbol | Asset | Interval | Window | Failure Bucket | Error |")
        failure_lines.append("| --- | --- | --- | --- | --- | --- | --- |")
        for item in report["failure_buckets"]:
            failure_lines.append(
                "| {provider} | {symbol} | {asset_type} | {interval} | {window} | {failure_bucket} | {error} |".format(
                    **{k: ("" if v is None else str(v).replace("|", "/")) for k, v in item.items()}
                )
            )
    else:
        failure_lines.append("No failures recorded.")
    failure_path.write_text("\n".join(failure_lines) + "\n", encoding="utf-8")

    unsupported_lines = ["# LumiBot Unsupported Limits", ""]
    if report["unsupported_limits"]:
        unsupported_lines.append("| Provider | Symbol | Asset | Interval | Max Reach | First Support Limit | Confidence |")
        unsupported_lines.append("| --- | --- | --- | --- | --- | --- | --- |")
        for item in report["unsupported_limits"]:
            unsupported_lines.append(
                "| {provider} | {symbol} | {asset_type} | {interval} | {max_reachable_window} | {first_support_limit_window} | {confidence} |".format(
                    **{k: ("" if v is None else str(v).replace("|", "/")) for k, v in item.items()}
                )
            )
    else:
        unsupported_lines.append("No unsupported limits recorded.")
    unsupported_path.write_text("\n".join(unsupported_lines) + "\n", encoding="utf-8")


def _write_runner_state(
    *,
    outdir: Path,
    state: str,
    current_case: Optional[dict[str, Any]] = None,
    completed_case: Optional[dict[str, Any]] = None,
    report_path: Optional[Path] = None,
) -> None:
    payload = {
        "updated_at": _iso_now(),
        "state": state,
        "lumibot_file": _assert_repo_lumibot_import_path(),
        "report_path": str(report_path) if report_path is not None else "",
        "current_case": current_case or None,
        "completed_case": completed_case or None,
    }
    (outdir / "lumibot_runner_state.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _case_key(provider: str, symbol: str, interval: str, window: str) -> tuple[str, str, str, str]:
    return provider, symbol, interval, window


def _run_case(
    *,
    cell: CellConfig,
    interval: str,
    window: str,
    end_dt: datetime,
    run_dir: Path,
    downloader_targets: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    asset = _build_asset(cell.symbol, cell.asset_type)
    quote = _build_quote_asset(cell.quote_symbol)
    requested_start = _window_to_start(end_dt, window)
    requested_end = end_dt
    backtest_start, backtest_end = _backtest_bounds_for_requested_end(requested_end=requested_end, interval=interval)
    indicators_file = run_dir / "indicators.html"
    metadata_path = run_dir / "run_metadata.json"
    validation_log_path = run_dir / "validation.log"
    log_file = run_dir / "run.log"

    record: dict[str, Any] = {
        "provider": cell.provider,
        "symbol": cell.symbol,
        "asset_type": cell.asset_type,
        "interval": interval,
        "window": window,
        "requested_start": requested_start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "status": None,
        "outcome": None,
        "backend": None,
        "classification": None,
        "cache_write_policy": None,
        "row_count": 0,
        "first_timestamp": None,
        "last_timestamp": None,
        "coverage_ok": False,
        "quality_issues": [],
        "failure_bucket": None,
        "artifact_path": str(indicators_file),
        "metadata_path": str(metadata_path),
        "validation_log_path": str(validation_log_path),
        "last_price": None,
        "last_close": None,
        "error": None,
        "lumibot_file": _assert_repo_lumibot_import_path(),
    }

    downloader_row = downloader_targets.get((cell.symbol, interval)) if cell.provider == "ibkr" else None
    if downloader_row:
        record["backend"] = downloader_row.get("backend")
        record["classification"] = downloader_row.get("classification")
        record["cache_write_policy"] = downloader_row.get("cache_write_policy")

    run_dir.mkdir(parents=True, exist_ok=True)
    datasource_class = _datasource_for_provider(cell.provider)

    params = {
        "asset": asset,
        "quote": quote,
        "interval": interval,
        "start_dt": requested_start,
        "end_dt": requested_end,
        "exchange": None,
    }
    if cell.provider == "ibkr" and cell.asset_type == "crypto":
        os.environ["IBKR_CRYPTO_VENUE"] = "ZEROHASH"

    previous_cwd = Path.cwd()
    try:
        os.chdir(run_dir)
        os.environ.pop("BACKTESTING_DATA_SOURCE", None)
        _append_run_log(
            log_file,
            "case_start "
            f"provider={cell.provider} symbol={cell.symbol} interval={interval} window={window} "
            f"backtest_start={backtest_start.isoformat()} backtest_end={backtest_end.isoformat()} "
            f"requested_start={requested_start.isoformat()} requested_end={requested_end.isoformat()}",
        )
        _, strategy = _HistoricalSeriesArtifactStrategy.run_backtest(
            datasource_class=datasource_class,
            backtesting_start=backtest_start,
            backtesting_end=backtest_end,
            market=cell.market,
            analyze_backtest=True,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=True,
            indicators_file=str(indicators_file),
            tearsheet_file=str(run_dir / "tearsheet.html"),
            quiet_logs=False,
            logfile=str(log_file),
            name=f"{cell.provider}_{cell.symbol}_{interval}_{window}",
            benchmark_asset=None,
            budget=100_000.0,
            parameters=params,
        )
        if strategy is None:
            raise RuntimeError("run_backtest returned no strategy instance")
        frame = strategy.exact_window_df
        if frame is None or frame.empty:
            raise RuntimeError("strategy did not persist exact_window_df")

        issues, coverage_ok = _quality_issues_for_frame(
            frame=frame,
            interval=interval,
            asset_type=cell.asset_type,
            requested_start=requested_start,
            requested_end=requested_end,
        )
        first_ts = frame.index.min()
        last_ts = frame.index.max()
        last_close = _safe_float(pd.to_numeric(frame.get("close"), errors="coerce").dropna().iloc[-1])
        last_price = _safe_float((strategy.validation or {}).get("last_price"))

        record.update(
            {
                "row_count": int(len(frame.index)),
                "first_timestamp": first_ts.isoformat() if first_ts is not None else None,
                "last_timestamp": last_ts.isoformat() if last_ts is not None else None,
                "coverage_ok": coverage_ok,
                "quality_issues": issues,
                "last_close": last_close,
                "last_price": last_price,
            }
        )

        if record["classification"] is None:
            record["classification"] = "complete" if coverage_ok and not issues else "partial"
        if record["cache_write_policy"] is None and cell.provider == "yahoo":
            record["cache_write_policy"] = "n/a"

        if coverage_ok and not issues and last_price is not None and last_price > 0:
            record["outcome"] = "pass"
        elif "coverage_shortfall" in issues:
            record["outcome"] = "support-limit"
            record["classification"] = "partial"
        else:
            record["outcome"] = "fail"
        record["status"] = record["outcome"]

        metadata = {
            "record": record,
            "strategy_validation": strategy.validation,
        }
        metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
        validation_log_path.write_text(json.dumps(strategy.validation, indent=2, sort_keys=True), encoding="utf-8")
        _append_run_log(
            log_file,
            "case_complete "
            f"outcome={record['outcome']} rows={record['row_count']} coverage_ok={record['coverage_ok']} "
            f"first_timestamp={record['first_timestamp']} last_timestamp={record['last_timestamp']}",
        )
    except Exception as exc:
        error_text = f"{type(exc).__name__}: {exc}"
        record["error"] = error_text
        if _explicit_support_limit(error_text):
            record["outcome"] = "support-limit"
            record["classification"] = "explicit_no_data"
        else:
            record["outcome"] = "fail"
        record["status"] = record["outcome"]
        metadata_path.write_text(json.dumps({"record": record}, indent=2, sort_keys=True), encoding="utf-8")
        validation_log_path.write_text(error_text + "\n", encoding="utf-8")
        _append_run_log(log_file, f"case_error error={error_text}")
    finally:
        os.chdir(previous_cwd)

    record["failure_bucket"] = _derive_failure_bucket(record)
    return record


def _assemble_report(
    *,
    cases: list[dict[str, Any]],
    cells: list[CellConfig],
    downloader_targets: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    lumibot_matrix: list[dict[str, Any]] = []
    for cell in cells:
        for interval in ("minute", "hour", "day"):
            matching = [
                case
                for case in cases
                if case.get("provider") == cell.provider
                and case.get("symbol") == cell.symbol
                and case.get("interval") == interval
            ]
            lumibot_matrix.append(
                _summarize_interval(
                    provider=cell.provider,
                    symbol=cell.symbol,
                    asset_type=cell.asset_type,
                    interval=interval,
                    cases=matching,
                    downloader_targets=downloader_targets,
                )
            )

    failure_buckets = [
        {
            "provider": case.get("provider"),
            "symbol": case.get("symbol"),
            "asset_type": case.get("asset_type"),
            "interval": case.get("interval"),
            "window": case.get("window"),
            "failure_bucket": case.get("failure_bucket"),
            "error": case.get("error"),
        }
        for case in cases
        if case.get("failure_bucket")
    ]
    unsupported_limits = [
        {
            "provider": row.get("provider"),
            "symbol": row.get("symbol"),
            "asset_type": row.get("asset_type"),
            "interval": row.get("interval"),
            "max_reachable_window": row.get("max_reachable_window"),
            "first_support_limit_window": row.get("first_support_limit_window"),
            "confidence": row.get("confidence"),
        }
        for row in lumibot_matrix
        if row.get("status") == "support-limit"
    ]
    return {
        "generated_at": _iso_now(),
        "lumibot_file": _assert_repo_lumibot_import_path(),
        "cases": cases,
        "lumibot_matrix": lumibot_matrix,
        "failure_buckets": failure_buckets,
        "unsupported_limits": unsupported_limits,
    }


def _build_cells(include_sol: bool) -> list[CellConfig]:
    cells: list[CellConfig] = []
    for symbol in REQUIRED_SYMBOLS["ibkr"]["indexes"]:
        cells.append(CellConfig(provider="ibkr", symbol=symbol, asset_type="index", market="NYSE"))
    for symbol in REQUIRED_SYMBOLS["ibkr"]["stocks"]:
        cells.append(CellConfig(provider="ibkr", symbol=symbol, asset_type="stock", market="NYSE"))
    for symbol in REQUIRED_SYMBOLS["ibkr"]["crypto"]:
        cells.append(CellConfig(provider="ibkr", symbol=symbol, asset_type="crypto", market="24/7", quote_symbol="USD"))
    if include_sol:
        cells.append(CellConfig(provider="ibkr", symbol="SOL", asset_type="crypto", market="24/7", quote_symbol="USD"))
    for symbol in REQUIRED_SYMBOLS["yahoo"]["indexes"]:
        cells.append(CellConfig(provider="yahoo", symbol=symbol, asset_type="index", market="NYSE"))
    return cells


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the LumiBot data verification matrix and emit artifact-backed reports.")
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--downloader-matrix-json", default="")
    parser.add_argument("--include-sol", action="store_true")
    parser.add_argument("--end-time-utc", default="")
    parser.add_argument("--providers", nargs="*", default=[])
    parser.add_argument("--symbols", nargs="*", default=[])
    parser.add_argument("--intervals", nargs="*", choices=["minute", "hour", "day"], default=["minute", "hour", "day"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _load_repo_env()
    _assert_repo_lumibot_import_path()
    _require_prod_downloader()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    json_path = outdir / "lumibot_matrix.json"
    downloader_targets = _load_downloader_targets(args.downloader_matrix_json or None)
    cells = _build_cells(include_sol=args.include_sol)
    provider_filter = {value.strip().lower() for value in args.providers if value.strip()}
    symbol_filter = {value.strip().upper() for value in args.symbols if value.strip()}
    if provider_filter:
        cells = [cell for cell in cells if cell.provider in provider_filter]
    if symbol_filter:
        cells = [cell for cell in cells if cell.symbol.upper() in symbol_filter]
    now_utc = datetime.now(timezone.utc) if not args.end_time_utc else datetime.fromisoformat(args.end_time_utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)

    cases: list[dict[str, Any]] = []
    existing_cases_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    if args.resume and json_path.exists():
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        loaded_cases = payload.get("cases") if isinstance(payload, dict) else None
        if isinstance(loaded_cases, list):
            cases = [_normalize_case_status(case) for case in loaded_cases if isinstance(case, dict)]
            existing_cases_by_key = {
                _case_key(str(case.get("provider") or ""), str(case.get("symbol") or ""), str(case.get("interval") or ""), str(case.get("window") or "")): case
                for case in cases
            }

    _write_runner_state(
        outdir=outdir,
        state="running",
        report_path=json_path,
    )

    for cell in cells:
        end_dt = _stable_end_datetime(cell.asset_type, now_utc)
        for interval in args.intervals:
            previous_pass = False
            windows = _windows_for_cell(
                provider=cell.provider,
                symbol=cell.symbol,
                interval=interval,
                downloader_targets=downloader_targets,
            )
            for window in windows:
                key = _case_key(cell.provider, cell.symbol, interval, window)
                existing = existing_cases_by_key.get(key)
                if existing is not None:
                    if existing.get("outcome") == "pass":
                        previous_pass = True
                        continue
                    if existing.get("outcome") == "support-limit":
                        break
                    if existing in cases:
                        cases.remove(existing)
                    existing_cases_by_key.pop(key, None)

                run_dir = outdir / "artifacts" / cell.provider / cell.symbol / interval / window
                requested_end = end_dt
                requested_start = _window_to_start(requested_end, window)
                _write_runner_state(
                    outdir=outdir,
                    state="running",
                    current_case={
                        "provider": cell.provider,
                        "symbol": cell.symbol,
                        "asset_type": cell.asset_type,
                        "interval": interval,
                        "window": window,
                        "requested_start": requested_start.isoformat(),
                        "requested_end": requested_end.isoformat(),
                        "run_dir": str(run_dir),
                    },
                    report_path=json_path,
                )
                case = _run_case(
                    cell=cell,
                    interval=interval,
                    window=window,
                    end_dt=end_dt,
                    run_dir=run_dir,
                    downloader_targets=downloader_targets,
                )
                if case.get("outcome") == "fail" and previous_pass and "coverage_shortfall" in (case.get("quality_issues") or []):
                    case["outcome"] = "support-limit"
                    case["failure_bucket"] = None
                    case["classification"] = "partial"
                cases.append(case)
                existing_cases_by_key[key] = case
                if case.get("outcome") == "pass":
                    previous_pass = True
                checkpoint = _assemble_report(cases=cases, cells=cells, downloader_targets=downloader_targets)
                _write_report_files(outdir, checkpoint)
                _write_runner_state(
                    outdir=outdir,
                    state="running",
                    completed_case={
                        "provider": case.get("provider"),
                        "symbol": case.get("symbol"),
                        "interval": case.get("interval"),
                        "window": case.get("window"),
                        "outcome": case.get("outcome"),
                        "artifact_path": case.get("artifact_path"),
                    },
                    report_path=json_path,
                )
                if case.get("outcome") != "pass":
                    break

    report = _assemble_report(cases=cases, cells=cells, downloader_targets=downloader_targets)
    _write_report_files(outdir, report)
    _write_runner_state(
        outdir=outdir,
        state="complete",
        report_path=json_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
