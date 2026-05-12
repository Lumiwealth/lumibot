#!/usr/bin/env python3
"""
Backfill IBKR futures bar caches via the local TWS / Gateway API for specific baseline windows.

Why
---
Client Portal (REST) historical bars can be flaky or unavailable in some environments, and
explicit expired futures backtests require reliable access to contract history. This script
uses the TWS API (reqHistoricalData) to populate LumiBot's IBKR parquet caches and upload
them to the configured remote cache (S3) so prod-like backtests can run without hitting
Client Portal for those windows.

Scope
-----
This script is intentionally scoped to the 3 user-approved DataBento baseline windows:
- MESFlipStrategy (MES, CME)
- GoldFlipMinuteStrategy (GC, COMEX)
- MESMomentumSMA9 (MES, CME)

It backfills TRADES OHLC bars for minute/hour/day across the exact baseline window, split
across the contracts implied by LumiBot's centralized roll schedule (futures_roll).

Secrets
-------
Reads S3 cache creds and config from `botspot_node/.env-local` (same as run_backtest_prodlike.py).
Do not print dotenv values.
"""

from __future__ import annotations

import argparse
import json
import queue
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytz
from ibapi.client import EClient  # type: ignore
from ibapi.contract import Contract  # type: ignore
from ibapi.wrapper import EWrapper  # type: ignore

LUMIBOT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOTENV = Path.home() / "Documents/Development/botspot_node/.env-local"
DEFAULT_CONIDS_JSON = LUMIBOT_ROOT / "data/ibkr_tws_backfill_cache_dev_v2/ibkr/conids.json"


@dataclass(frozen=True)
class BaselineSpec:
    name: str
    baseline_prefix: Path
    root_symbol: str
    exchange: str
    asset_type: str = "cont_future"  # "cont_future" | "future"
    expiration_yyyymmdd: str | None = None


BASELINES: tuple[BaselineSpec, ...] = (
    BaselineSpec(
        name="MESFlipStrategy",
        baseline_prefix=Path(
            "/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/logs/"
            "MESFlipStrategy_2025-11-25_23-19_dJE7Kl"
        ),
        root_symbol="MES",
        exchange="CME",
    ),
    BaselineSpec(
        name="GoldFlipMinuteStrategy",
        baseline_prefix=Path(
            "/Users/robertgrzesik/Documents/Development/Strategy Library/logs/"
            "GoldFlipMinuteStrategy_2025-11-12_01-58_ObSl6b"
        ),
        root_symbol="GC",
        exchange="COMEX",
    ),
    BaselineSpec(
        name="MESMomentumSMA9",
        baseline_prefix=Path(
            "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/logs/"
            "MESMomentumSMA9_2025-10-15_12-52_88xWTg"
        ),
        root_symbol="MES",
        exchange="CME",
    ),
    BaselineSpec(
        name="IbkrMesFuturesAcceptance",
        baseline_prefix=Path(
            "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/tests/backtest/"
            "_acceptance_runs/ibkr_mes_futures_acceptance_20260114_235532_0893794e/logs/"
            "IbkrMesFuturesAcceptance_2026-01-14_23-55_Ugrl9o"
        ),
        root_symbol="MES",
        exchange="CME",
        asset_type="future",
        expiration_yyyymmdd="20251219",
    ),
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8", errors="replace"))


def _load_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k] = v
    return out


def _parse_dt(value: Any) -> datetime:
    # Baseline settings typically store tz-aware strings.
    ts = pd.to_datetime(value, utc=True, errors="raise")
    if isinstance(ts, pd.Timestamp):
        return ts.to_pydatetime()
    raise ValueError(f"Unable to parse datetime: {value!r}")


def _baseline_window(prefix: Path) -> tuple[datetime, datetime]:
    settings = _read_json(prefix.with_name(prefix.name + "_settings.json"))
    start = _parse_dt(settings.get("backtesting_start"))
    end = _parse_dt(settings.get("backtesting_end"))
    return start, end


_CONTRACT_RE = re.compile(r"^([A-Z]+)([FGHJKMNQUVXZ])(\d{1,2})$")


def _contract_symbol_to_year_month(contract_symbol: str, *, reference_year: int) -> tuple[int, int]:
    from lumibot.tools import futures_roll

    m = _CONTRACT_RE.match(contract_symbol.strip().upper())
    if not m:
        raise ValueError(f"Unexpected contract symbol format: {contract_symbol}")
    _root, month_code, year_digits = m.group(1), m.group(2), m.group(3)

    reverse = {v: k for k, v in getattr(futures_roll, "_FUTURES_MONTH_CODES", {}).items()}
    month = reverse.get(month_code)
    if not month:
        raise ValueError(f"Unknown futures month code {month_code} in {contract_symbol}")

    if len(year_digits) == 1:
        decade = (reference_year // 10) * 10
        year = decade + int(year_digits)
    else:
        century = (reference_year // 100) * 100
        year = century + int(year_digits)
    return year, month


def _expiration_yyyymmdd_for(root_symbol: str, *, year: int, month: int) -> str:
    from lumibot.tools import futures_roll

    rule = futures_roll.ROLL_RULES.get(str(root_symbol).upper())
    if rule and rule.anchor == "third_last_business_day":
        expiry = futures_roll._third_last_business_day(year, month)
    else:
        expiry = futures_roll._third_friday(year, month)
    return expiry.strftime("%Y%m%d")


def _conid_from_registry(
    *,
    mapping: dict[str, int],
    root_symbol: str,
    exchange: str,
    expiration_yyyymmdd: str,
) -> int:
    # Match the two key variants persisted by the TWS backfill job.
    k1 = "|".join(["future", root_symbol, "", exchange, expiration_yyyymmdd])
    k2 = "|".join(["future", root_symbol, "USD", exchange, expiration_yyyymmdd])
    for k in (k1, k2):
        v = mapping.get(k)
        if isinstance(v, int) and v > 0:
            return int(v)
    raise KeyError(f"Missing conid for {root_symbol} {exchange} {expiration_yyyymmdd}")


class _HistApp(EWrapper, EClient):
    def __init__(self) -> None:
        EClient.__init__(self, wrapper=self)
        self._q: queue.Queue[tuple[str, Any]] = queue.Queue()
        self._next_id: int | None = None

    def nextValidId(self, orderId: int) -> None:  # noqa: N802
        self._next_id = int(orderId)
        self._q.put(("nextValidId", self._next_id))

    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson="") -> None:  # noqa: N802
        self._q.put(("error", {"reqId": int(reqId), "code": int(errorCode), "message": str(errorString)}))

    def historicalData(self, reqId: int, bar) -> None:  # noqa: N802
        self._q.put(
            (
                "bar",
                {
                    "reqId": int(reqId),
                    "date": str(bar.date),
                    "open": float(bar.open),
                    "high": float(bar.high),
                    "low": float(bar.low),
                    "close": float(bar.close),
                    "volume": float(bar.volume),
                },
            )
        )

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:  # noqa: N802
        self._q.put(("end", {"reqId": int(reqId), "start": str(start), "end": str(end)}))


def _tws_connect(*, host: str, port: int, client_id: int, timeout_s: float = 10.0) -> _HistApp:
    app = _HistApp()
    app.connect(host, int(port), int(client_id))
    import threading

    threading.Thread(target=app.run, daemon=True).start()

    deadline = time.time() + float(timeout_s)
    while time.time() < deadline:
        try:
            kind, payload = app._q.get(timeout=0.5)
        except queue.Empty:
            continue
        if kind == "nextValidId":
            return app
        if kind == "error":
            # Ignore noisy global farm status messages.
            code = int(payload.get("code") or 0)
            if code not in (2104, 2106, 2158):
                raise RuntimeError(f"TWS error during connect: {payload}")
    raise TimeoutError("Timed out waiting for nextValidId from TWS")


def _tws_contract(*, conid: int, root_symbol: str, exchange: str) -> Contract:
    c = Contract()
    c.conId = int(conid)
    c.secType = "FUT"
    c.symbol = str(root_symbol).upper()
    c.exchange = str(exchange).upper()
    c.currency = "USD"
    return c


def _parse_tws_bar_dt(value: str, tz: pytz.BaseTzInfo) -> datetime:
    # Typical bar.date: "20251104  15:59:00" (note double space).
    cleaned = " ".join(str(value).strip().split())
    if re.fullmatch(r"\d{8}", cleaned):
        dt = datetime.strptime(cleaned, "%Y%m%d")
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        dt = datetime.strptime(cleaned, "%Y%m%d %H:%M:%S")
    return tz.localize(dt)


def _fetch_history_tws(
    app: _HistApp,
    *,
    contract: Contract,
    start_utc: datetime,
    end_utc: datetime,
    what_to_show: str,
    bar_size: str,
    duration: str,
    tz: pytz.BaseTzInfo,
    sleep_s: float,
) -> pd.DataFrame:
    start_local = start_utc.astimezone(tz)
    end_local = end_utc.astimezone(tz)
    if start_local >= end_local:
        return pd.DataFrame()

    cursor_end = end_local
    chunks: list[pd.DataFrame] = []

    while cursor_end > start_local:
        req_id = int(app._next_id or 1)
        app._next_id = req_id + 1

        end_str = cursor_end.strftime("%Y%m%d %H:%M:%S")
        app.reqHistoricalData(
            req_id,
            contract,
            endDateTime=end_str,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=0,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[],
        )

        bars: list[dict[str, Any]] = []
        done = False
        no_data = False
        deadline = time.time() + 30.0
        while time.time() < deadline and not done:
            try:
                kind, payload = app._q.get(timeout=1.0)
            except queue.Empty:
                continue
            if kind == "error":
                code = int(payload.get("code") or 0)
                # Ignore farm connection status and other non-fatal warnings.
                if code in (2104, 2105, 2106, 2158, 2174, 2176):
                    continue
                # HMDS returned no bars for this request window.
                if code == 162 and int(payload.get("reqId") or -1) in (-1, req_id):
                    no_data = True
                    done = True
                    continue
                raise RuntimeError(f"TWS historicalData error: {payload}")
            if kind == "bar" and int(payload.get("reqId") or -1) == req_id:
                bars.append(payload)
            if kind == "end" and int(payload.get("reqId") or -1) == req_id:
                done = True

        if no_data and not bars:
            # Try stepping backwards to avoid aborting the whole segment on a single sparse window.
            next_end = cursor_end - timedelta(days=1)
            if next_end >= cursor_end:
                break
            cursor_end = next_end
            if sleep_s and sleep_s > 0:
                time.sleep(float(sleep_s))
            continue

        if not bars:
            break

        df = pd.DataFrame(bars)
        dt_index = pd.to_datetime(
            [ _parse_tws_bar_dt(x, tz) for x in df["date"].tolist() ],
            utc=True,
            errors="coerce",
        ).tz_convert(tz)
        df = df.drop(columns=["reqId", "date"], errors="ignore")
        df.index = dt_index
        df = df[~df.index.isna()].sort_index()
        df["missing"] = False
        chunks.append(df)

        earliest = df.index.min()
        if earliest is None:
            break
        if earliest <= start_local:
            break
        cursor_end = earliest

        if sleep_s and sleep_s > 0:
            time.sleep(float(sleep_s))

    if not chunks:
        return pd.DataFrame()

    merged = pd.concat(chunks, axis=0).sort_index()
    merged = merged[~merged.index.duplicated(keep="last")]
    return merged


def _set_cache_env(
    dotenv: dict[str, str],
    *,
    cache_folder: Path,
    cache_version: str,
    use_dotenv_s3_keys: bool,
) -> None:
    # Only set env vars required for the cache backend. Do not print values.
    for k in [
        "LUMIBOT_CACHE_BACKEND",
        "LUMIBOT_CACHE_MODE",
        "LUMIBOT_CACHE_S3_BUCKET",
        "LUMIBOT_CACHE_S3_PREFIX",
        "LUMIBOT_CACHE_S3_REGION",
    ]:
        if k in dotenv:
            import os

            os.environ[k] = dotenv[k]
    if use_dotenv_s3_keys:
        for k in [
            "LUMIBOT_CACHE_S3_ACCESS_KEY_ID",
            "LUMIBOT_CACHE_S3_SECRET_ACCESS_KEY",
            "LUMIBOT_CACHE_S3_SESSION_TOKEN",
        ]:
            if k in dotenv:
                import os

                os.environ[k] = dotenv[k]
    import os

    os.environ["LUMIBOT_CACHE_FOLDER"] = str(cache_folder)
    os.environ["LUMIBOT_CACHE_S3_VERSION"] = str(cache_version)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dotenv", type=str, default=str(DEFAULT_DOTENV), help="Path to botspot_node/.env-local")
    parser.add_argument("--conids-json", type=str, default=str(DEFAULT_CONIDS_JSON), help="Path to conids.json from TWS backfill")
    parser.add_argument("--cache-version", type=str, default="v2", help="S3 cache version (e.g. v2)")
    parser.add_argument("--cache-folder", type=str, default=str(LUMIBOT_ROOT / "data/ibkr_tws_bars_cache"), help="Local cache folder")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7497)
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep between TWS historical requests")
    parser.add_argument(
        "--buffer-days",
        type=int,
        default=7,
        help="Extend each baseline window by +/- this many days to cover indicator lookbacks and roll boundary edges.",
    )
    parser.add_argument(
        "--use-dotenv-s3-keys",
        action="store_true",
        help="Use LUMIBOT_CACHE_S3_ACCESS_KEY_ID/SECRET from dotenv instead of the host AWS credential chain.",
    )
    parser.add_argument(
        "--baselines",
        type=str,
        nargs="*",
        default=[b.name for b in BASELINES],
        help="Subset of baselines to backfill (default: all)",
    )
    args = parser.parse_args()

    dotenv_path = Path(args.dotenv).expanduser().resolve()
    conids_path = Path(args.conids_json).expanduser().resolve()
    cache_folder = Path(args.cache_folder).expanduser().resolve()
    cache_folder.mkdir(parents=True, exist_ok=True)

    dotenv = _load_dotenv(dotenv_path)
    _set_cache_env(
        dotenv,
        cache_folder=cache_folder,
        cache_version=str(args.cache_version),
        use_dotenv_s3_keys=bool(args.use_dotenv_s3_keys),
    )

    conids = _read_json(conids_path)
    selected = [b for b in BASELINES if b.name in set(args.baselines)]
    if not selected:
        raise SystemExit("No baselines selected")

    from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
    from lumibot.entities.asset import Asset
    from lumibot.tools import futures_roll, ibkr_helper
    from lumibot.tools.parquet_series_cache import ParquetSeriesCache

    tws = _tws_connect(host=args.host, port=args.port, client_id=int(time.time()) % 100000)

    try:
        for baseline in selected:
            start_utc, end_utc = _baseline_window(baseline.baseline_prefix)
            buffer = timedelta(days=int(args.buffer_days))
            start_utc = start_utc - buffer
            end_utc = end_utc + buffer
            print(f"[baseline] {baseline.name} root={baseline.root_symbol} exch={baseline.exchange} window={start_utc.isoformat()} -> {end_utc.isoformat()}", flush=True)

            segments: list[tuple[str, datetime, datetime, str, int]] = []
            if baseline.asset_type == "future":
                if not baseline.expiration_yyyymmdd:
                    raise ValueError(f"{baseline.name}: asset_type='future' requires expiration_yyyymmdd")
                expiry_yyyymmdd = str(baseline.expiration_yyyymmdd)
                conid = _conid_from_registry(
                    mapping=conids,
                    root_symbol=baseline.root_symbol,
                    exchange=baseline.exchange,
                    expiration_yyyymmdd=expiry_yyyymmdd,
                )
                segments.append((f"{baseline.root_symbol}_{expiry_yyyymmdd}", start_utc, end_utc, expiry_yyyymmdd, conid))
            else:
                cont_asset = Asset(baseline.root_symbol, asset_type="cont_future")
                schedule = futures_roll.build_roll_schedule(cont_asset, start_utc, end_utc, year_digits=1)
                for contract_symbol, seg_start, seg_end in schedule:
                    year, month = _contract_symbol_to_year_month(contract_symbol, reference_year=seg_start.year)
                    expiry_yyyymmdd = _expiration_yyyymmdd_for(baseline.root_symbol, year=year, month=month)
                    conid = _conid_from_registry(
                        mapping=conids,
                        root_symbol=baseline.root_symbol,
                        exchange=baseline.exchange,
                        expiration_yyyymmdd=expiry_yyyymmdd,
                    )
                    segments.append((contract_symbol, seg_start, seg_end, expiry_yyyymmdd, conid))

            print(f"[baseline] {baseline.name} segments={len(segments)}", flush=True)

            for contract_symbol, seg_start, seg_end, expiry_yyyymmdd, conid in segments:
                print(
                    f"[segment] {baseline.name} {contract_symbol} expiry={expiry_yyyymmdd} conid={conid} "
                    f"seg={seg_start.isoformat()} -> {seg_end.isoformat()}",
                    flush=True,
                )

                expiry_dt = pd.Timestamp(expiry_yyyymmdd, tz=LUMIBOT_DEFAULT_PYTZ).to_pydatetime()
                fut_asset = Asset(baseline.root_symbol, asset_type="future", expiration=expiry_dt)

                contract = _tws_contract(conid=conid, root_symbol=baseline.root_symbol, exchange=baseline.exchange)

                for timestep, bar_size, duration in [
                    ("minute", "1 min", "1 D"),
                    ("hour", "1 hour", "1 W"),
                    ("day", "1 day", "1 Y"),
                ]:
                    print(f"[fetch] {baseline.name} {contract_symbol} timestep={timestep} bar={bar_size} duration={duration}", flush=True)
                    df = _fetch_history_tws(
                        tws,
                        contract=contract,
                        start_utc=seg_start,
                        end_utc=seg_end,
                        what_to_show="TRADES",
                        bar_size=bar_size,
                        duration=duration,
                        tz=LUMIBOT_DEFAULT_PYTZ,
                        sleep_s=float(args.sleep),
                    )
                    if df is None or df.empty:
                        print(f"[fetch] {baseline.name} {contract_symbol} timestep={timestep} -> empty", flush=True)
                        continue

                    cache_path = ibkr_helper._cache_file_for(
                        asset=fut_asset,
                        quote=None,
                        timestep=timestep,
                        exchange=baseline.exchange,
                        source="Trades",
                    )
                    payload = ibkr_helper._remote_payload(
                        fut_asset,
                        None,
                        timestep,
                        baseline.exchange,
                        "Trades",
                    )
                    cache = ParquetSeriesCache(cache_path, remote_payload=payload)
                    cache.hydrate_remote()
                    existing = cache.read()
                    merged = ParquetSeriesCache.merge(existing, df)
                    cache.write(merged)
                    print(f"[cache] wrote {len(df)} rows -> {cache_path}", flush=True)
    finally:
        try:
            tws.disconnect()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
