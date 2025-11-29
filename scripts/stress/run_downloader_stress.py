import json
import os
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import Dict, List

import requests

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def _request_healthz(base_url: str) -> Dict:
    url = base_url.rstrip("/") + "/healthz"
    start = time.perf_counter()
    response = requests.get(url, timeout=5)
    elapsed = time.perf_counter() - start
    payload = None
    try:
        payload = response.json()
    except Exception:
        payload = {"raw": response.text}
    return {"status": response.status_code, "elapsed": elapsed, "payload": payload}


def _single_minute_request(
    asset: Asset,
    day: datetime,
    username: str,
    password: str,
) -> Dict:
    start_dt = datetime.combine(day.date(), dt_time(9, 30))
    end_dt = start_dt + timedelta(minutes=1)
    start = time.perf_counter()
    df = thetadata_helper.get_historical_data(
        asset,
        start_dt,
        end_dt,
        ivl=60_000,
        username=username,
        password=password,
        datastyle="ohlc",
        include_after_hours=False,
    )
    elapsed = time.perf_counter() - start
    rows = 0 if df is None else len(df)
    return {"elapsed": elapsed, "rows": rows, "success": bool(rows)}


def _burst_request(
    asset: Asset,
    day: datetime,
    username: str,
    password: str,
) -> Dict:
    try:
        result = _single_minute_request(asset, day, username, password)
        result["date"] = day.strftime("%Y-%m-%d")
        return result
    except Exception as exc:
        return {
            "date": day.strftime("%Y-%m-%d"),
            "elapsed": None,
            "rows": 0,
            "success": False,
            "error": str(exc),
        }


def _summarize(results: List[Dict]) -> Dict:
    durations = [entry["elapsed"] for entry in results if entry["elapsed"] is not None]
    if not durations:
        return {"count": len(results), "success": 0, "errors": len(results)}
    durations.sort()
    success_count = sum(1 for entry in results if entry.get("success"))
    error_count = len(results) - success_count
    return {
        "count": len(results),
        "success": success_count,
        "errors": error_count,
        "min_s": min(durations),
        "max_s": max(durations),
        "median_s": statistics.median(durations),
        "p95_s": durations[int(0.95 * len(durations)) - 1] if len(durations) >= 1 else durations[-1],
    }


def run():
    base_url = os.environ.get("DATADOWNLOADER_BASE_URL")
    if not base_url:
        raise RuntimeError("DATADOWNLOADER_BASE_URL is required for stress testing")
    username = os.environ.get("THETADATA_USERNAME") or "stress_user"
    password = os.environ.get("THETADATA_PASSWORD") or "stress_pass"
    asset = Asset("SPY", Asset.AssetType.STOCK)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("logs/stress")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"stress_{timestamp}.json"

    pre_healthz = _request_healthz(base_url)
    single_run = _single_minute_request(asset, datetime.utcnow(), username, password)

    burst_days = [datetime(2024, 10, 1) + timedelta(days=offset) for offset in range(100)]
    burst_results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = {pool.submit(_burst_request, asset, day, username, password): day for day in burst_days}
        for future in as_completed(futures):
            burst_results.append(future.result())

    post_healthz = _request_healthz(base_url)

    payload = {
        "generated_at": datetime.utcnow().isoformat(),
        "base_url": base_url,
        "single_request": single_run,
        "burst_summary": _summarize(burst_results),
        "burst_details": burst_results,
        "healthz": {"before": pre_healthz, "after": post_healthz},
    }
    with output_path.open("w") as handle:
        json.dump(payload, handle, indent=2)
    print(f"Stress results saved to {output_path}")


if __name__ == "__main__":
    run()
