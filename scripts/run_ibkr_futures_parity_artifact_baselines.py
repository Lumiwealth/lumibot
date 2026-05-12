#!/usr/bin/env python3
"""
Run IBKR futures backtests and compare against stored baseline artifacts.

This is the "no live DataBento key" parity path:
- DataBento side is treated as already-run artifacts on disk (the approved baselines).
- IBKR side is rerun locally using the prod-like runner and TRADES/OHLC parity mode.

Secrets
-------
Reads secrets from `botspot_node/.env-local` and injects them into subprocess env WITHOUT
printing them. Do not add prints of env values.

Outputs
-------
Creates a run root under `tests/backtest/_parity_runs/` containing:
- per-strategy run folders (cold / warm / yappi)
- `summary.json` + `summary.md`
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

import pandas as pd

LUMIBOT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOTENV = Path.home() / "Documents/Development/botspot_node/.env-local"


@dataclasses.dataclass(frozen=True)
class BaselineSpec:
    name: str
    baseline_prefix: Path
    strategy_file: Path
    tick: float
    ibkr_futures_exchange: str = "CME"


def _now_id() -> str:
    return time.strftime("%Y%m%d_%H%M%S")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8", errors="replace"))


def _baseline_settings(prefix: Path) -> dict[str, Any]:
    return _read_json(prefix.with_name(prefix.name + "_settings.json"))


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _read_dotenv(path: Path) -> dict[str, str]:
    payload: dict[str, str] = {}
    try:
        for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            payload[k.strip()] = v.strip().strip("'").strip('"')
    except Exception:
        return {}
    return payload


def _ensure_conids_seeded(*, cache_root: Path, dotenv: Path, cache_version: str | None) -> None:
    """
    Ensure the IBKR conid registry exists under the per-run cache folder.

    Why this exists
    ---------------
    IBKR Client Portal cannot reliably discover expired futures contracts (conids). LumiBot
    therefore maintains a local registry at `<cache_root>/ibkr/conids.json`, populated via a
    one-time TWS/Gateway backfill.

    The parity harness runs each baseline in an isolated cache folder to keep runs
    reproducible. That means the backtest subprocess may not have access to the shared
    conids registry unless we seed it here.
    """
    dst = cache_root / "ibkr" / "conids.json"
    if dst.exists():
        return

    seed = LUMIBOT_ROOT / "data" / "ibkr_tws_backfill_cache_dev_v2" / "ibkr" / "conids.json"
    if not seed.exists():
        raise SystemExit(
            "Missing required IBKR conid registry for parity runs.\n"
            f"- expected seed file: {seed}\n"
            f"- expected cache path: {dst}\n"
            "Run the one-time TWS/Gateway conid backfill to populate ibkr/conids.json, or "
            "restore the repo seed under data/ibkr_tws_backfill_cache_dev_v2/."
        )

    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copy2(seed, dst)
    except Exception as e:
        raise SystemExit(f"Failed to seed conids.json into parity cache folder: {e}") from e

    # If the remote cache backend is enabled (S3), `ensure_local_file()` will delete-and-redownload
    # files without a matching `.s3key` marker. Seed the marker so the subprocess can reuse this
    # conid registry without making a remote call (and without overwriting it with a partial file).
    dotenv_payload = _read_dotenv(dotenv)
    prefix = (dotenv_payload.get("LUMIBOT_CACHE_S3_PREFIX") or "").strip().strip("/")
    version = (cache_version or dotenv_payload.get("LUMIBOT_CACHE_S3_VERSION") or "").strip().strip("/")
    if prefix and version:
        remote_key = f"{prefix}/{version}/ibkr/conids.json"
        marker = dst.with_suffix(dst.suffix + ".s3key")
        try:
            marker.write_text(remote_key, encoding="utf-8")
        except Exception:
            pass


def _newest_prefix(log_dir: Path) -> str | None:
    candidates = sorted(log_dir.glob("*_settings.json"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        return None
    latest = candidates[-1].name
    return latest.removesuffix("_settings.json")


def _load_price_line_from_indicators(path: Path, *, symbol: str) -> pd.Series:
    df = pd.read_csv(path)
    if "datetime" not in df.columns:
        raise ValueError(f"indicators.csv missing datetime column: {path}")
    if "name" not in df.columns or "value" not in df.columns:
        raise ValueError(f"indicators.csv missing name/value columns: {path}")

    df = df[df["name"] == symbol].copy()
    if df.empty:
        raise ValueError(f"indicators.csv has no rows for symbol={symbol}: {path}")

    dt = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    values = pd.to_numeric(df["value"], errors="coerce")
    series = pd.Series(values.values, index=dt)
    series = series[~series.index.isna()]
    series = series.dropna().sort_index()
    # Remove duplicates if any (keep last).
    series = series[~series.index.duplicated(keep="last")]
    return series


def _load_fills(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "status" in df.columns:
        df = df[df["status"] == "fill"].copy()
    if "time" not in df.columns:
        raise ValueError(f"trades.csv missing time column: {path}")
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df[~df["time"].isna()].sort_values("time").reset_index(drop=True)
    return df


def _compare_indicators(
    baseline_path: Path,
    ibkr_path: Path,
    *,
    symbol: str,
    tick: float,
) -> dict[str, Any]:
    base = _load_price_line_from_indicators(baseline_path, symbol=symbol)
    run = _load_price_line_from_indicators(ibkr_path, symbol=symbol)

    joined = pd.DataFrame({"baseline": base, "ibkr": run}).dropna()
    overlap = int(len(joined))
    out: dict[str, Any] = {
        "baseline_points": int(len(base)),
        "ibkr_points": int(len(run)),
        "overlap_points": overlap,
        "overlap_ratio_baseline": float(overlap / len(base)) if len(base) else 0.0,
        "max_abs_diff": None,
        "first_bad_timestamp": None,
    }
    if joined.empty:
        return out

    diff = (joined["baseline"] - joined["ibkr"]).abs()
    out["max_abs_diff"] = float(diff.max())
    bad = diff[diff > (tick + 1e-12)]
    if not bad.empty:
        out["first_bad_timestamp"] = bad.index.min().isoformat()
    return out


def _compare_trades(
    baseline_path: Path,
    ibkr_path: Path,
    *,
    tick: float,
) -> dict[str, Any]:
    base = _load_fills(baseline_path)
    run = _load_fills(ibkr_path)

    # Canonical compare columns (best-effort; tolerate extra columns).
    cols = [c for c in ["time", "side", "type", "symbol", "price", "filled_quantity"] if c in base.columns and c in run.columns]
    out: dict[str, Any] = {
        "baseline_fills": int(len(base)),
        "ibkr_fills": int(len(run)),
        "compare_cols": cols,
        "first_mismatch_index": None,
        "first_mismatch": None,
    }

    n = min(len(base), len(run))
    for i in range(n):
        b = base.iloc[i]
        r = run.iloc[i]

        # Time: require exact timestamp equality at the bar index resolution after UTC normalization.
        if pd.Timestamp(b["time"]) != pd.Timestamp(r["time"]):
            out["first_mismatch_index"] = int(i)
            out["first_mismatch"] = {
                "field": "time",
                "baseline": str(b["time"]),
                "ibkr": str(r["time"]),
            }
            return out

        for field in ["side", "type", "symbol"]:
            if field in cols:
                if str(b[field]) != str(r[field]):
                    out["first_mismatch_index"] = int(i)
                    out["first_mismatch"] = {
                        "field": field,
                        "baseline": str(b[field]),
                        "ibkr": str(r[field]),
                    }
                    return out

        if "price" in cols:
            bp = float(b["price"]) if pd.notna(b["price"]) else None
            rp = float(r["price"]) if pd.notna(r["price"]) else None
            if bp is None or rp is None:
                if bp != rp:
                    out["first_mismatch_index"] = int(i)
                    out["first_mismatch"] = {"field": "price", "baseline": bp, "ibkr": rp}
                    return out
            else:
                if abs(bp - rp) > (tick + 1e-12):
                    out["first_mismatch_index"] = int(i)
                    out["first_mismatch"] = {"field": "price", "baseline": bp, "ibkr": rp, "abs_diff": abs(bp - rp)}
                    return out

    if len(base) != len(run):
        out["first_mismatch_index"] = int(n)
        out["first_mismatch"] = {"field": "length", "baseline": int(len(base)), "ibkr": int(len(run))}
    return out


def _compare_stats(
    baseline_path: Path,
    ibkr_path: Path,
) -> dict[str, Any]:
    base = pd.read_csv(baseline_path)
    run = pd.read_csv(ibkr_path)
    if "datetime" not in base.columns or "datetime" not in run.columns:
        return {"error": "missing datetime column"}
    if "portfolio_value" not in base.columns or "portfolio_value" not in run.columns:
        return {"error": "missing portfolio_value column"}

    base["datetime"] = pd.to_datetime(base["datetime"], utc=True, errors="coerce")
    run["datetime"] = pd.to_datetime(run["datetime"], utc=True, errors="coerce")
    base = base.dropna(subset=["datetime"]).set_index("datetime").sort_index()
    run = run.dropna(subset=["datetime"]).set_index("datetime").sort_index()

    # Some IBKR runs can emit duplicate timestamps around session boundaries (e.g. 18:00 NY
    # reopen). For parity comparisons, keep the last value for each timestamp so we can
    # compute overlap and diffs deterministically.
    if base.index.has_duplicates:
        base = base[~base.index.duplicated(keep="last")]
    if run.index.has_duplicates:
        run = run[~run.index.duplicated(keep="last")]

    joined = pd.DataFrame(
        {
            "baseline": pd.to_numeric(base["portfolio_value"], errors="coerce"),
            "ibkr": pd.to_numeric(run["portfolio_value"], errors="coerce"),
        }
    ).dropna()
    if joined.empty:
        return {
            "baseline_points": int(len(base)),
            "ibkr_points": int(len(run)),
            "overlap_points": 0,
            "max_abs_diff": None,
            "first_bad_timestamp": None,
        }

    diff = (joined["baseline"] - joined["ibkr"]).abs()
    return {
        "baseline_points": int(len(base)),
        "ibkr_points": int(len(run)),
        "overlap_points": int(len(joined)),
        "max_abs_diff": float(diff.max()),
        "first_bad_timestamp": diff[diff > 1e-6].index.min().isoformat() if (diff > 1e-6).any() else None,
    }


def _run_prodlike(
    *,
    name: str,
    strategy_file: Path,
    start: str,
    end: str,
    workdir: Path,
    cache_folder: Path,
    dotenv: Path,
    cache_version: str | None,
    ibkr_futures_exchange: str,
    profile: str | None,
    timeout_s: int,
) -> dict[str, Any]:
    _ensure_dir(workdir)
    _ensure_dir(cache_folder)

    cmd = [
        os.environ.get("PYTHON", "python3"),
        str(LUMIBOT_ROOT / "scripts" / "run_backtest_prodlike.py"),
        "--main",
        str(strategy_file),
        "--start",
        start,
        "--end",
        end,
        "--data-source",
        "ibkr",
        "--ibkr-history-source",
        "Trades",
        "--ibkr-futures-exchange",
        ibkr_futures_exchange,
        "--dotenv",
        str(dotenv),
        "--workdir",
        str(workdir),
        "--cache-folder",
        str(cache_folder),
        "--subprocess-log",
        str(workdir / "subprocess.log"),
    ]
    if cache_version:
        cmd += ["--cache-version", str(cache_version)]
    if profile:
        cmd += ["--profile", profile]

    started = time.time()
    try:
        proc = subprocess.run(cmd, check=False, timeout=timeout_s)
        exit_code = proc.returncode
    except subprocess.TimeoutExpired:
        exit_code = 124
    elapsed_s = time.time() - started

    prefix = _newest_prefix(workdir / "logs")
    artifacts: dict[str, str | None] = {}
    if prefix:
        for suffix in [
            "_settings.json",
            "_trades.csv",
            "_stats.csv",
            "_indicators.csv",
            "_tearsheet.csv",
            "_tearsheet.html",
            "_trade_events.csv",
            "_profile_yappi.csv",
        ]:
            p = (workdir / "logs" / f"{prefix}{suffix}")
            artifacts[suffix.lstrip("_").replace(".", "_")] = str(p) if p.exists() else None

    metrics_path = workdir / "metrics.json"
    metrics = None
    if metrics_path.exists():
        try:
            metrics = _read_json(metrics_path)
        except Exception:
            metrics = None

    return {
        "name": name,
        "workdir": str(workdir),
        "cache_folder": str(cache_folder),
        "window": {"start": start, "end": end},
        "exit_code": exit_code,
        "elapsed_s": float(elapsed_s),
        "prefix": prefix,
        "artifacts": artifacts,
        "runner_metrics": metrics,
    }


def _analyze_yappi(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        proc = subprocess.run(
            [os.environ.get("PYTHON", "python3"), str(LUMIBOT_ROOT / "scripts" / "analyze_yappi_csv.py"), str(path), "--json"],
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if proc.returncode != 0:
            return None
        payload = json.loads(proc.stdout)
        return payload.get(str(path))
    except Exception:
        return None


def _load_existing_run(*, name: str, workdir: Path, cache_folder: Path, start: str, end: str) -> dict[str, Any]:
    prefix = _newest_prefix(workdir / "logs")
    artifacts: dict[str, str | None] = {}
    if prefix:
        for suffix in [
            "_settings.json",
            "_trades.csv",
            "_stats.csv",
            "_indicators.csv",
            "_tearsheet.csv",
            "_tearsheet.html",
            "_trade_events.csv",
            "_profile_yappi.csv",
        ]:
            p = (workdir / "logs" / f"{prefix}{suffix}")
            artifacts[suffix.lstrip("_").replace(".", "_")] = str(p) if p.exists() else None

    metrics_path = workdir / "metrics.json"
    metrics = None
    if metrics_path.exists():
        try:
            metrics = _read_json(metrics_path)
        except Exception:
            metrics = None

    return {
        "name": name,
        "workdir": str(workdir),
        "cache_folder": str(cache_folder),
        "window": {"start": start, "end": end},
        "exit_code": 0 if prefix else 1,
        "elapsed_s": None,
        "prefix": prefix,
        "artifacts": artifacts,
        "runner_metrics": metrics,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run IBKR futures parity against stored baseline artifacts.")
    parser.add_argument("--dotenv", type=str, default=str(DEFAULT_DOTENV), help="Path to botspot_node/.env-local")
    parser.add_argument(
        "--out-root",
        type=str,
        default=None,
        help="Optional output root (defaults under tests/backtest/_parity_runs/)",
    )
    parser.add_argument(
        "--timeout-s",
        type=int,
        default=4 * 60 * 60,
        help="Per-run timeout in seconds (covers cold runs).",
    )
    parser.add_argument(
        "--cache-version",
        type=str,
        default=None,
        help="Override LUMIBOT_CACHE_S3_VERSION for prod-like runs (use v2 for staged conid backfill).",
    )
    parser.add_argument("--only", type=str, default=None, help="Run only a single baseline by name (e.g., MESFlipStrategy).")
    parser.add_argument(
        "--compare-only",
        action="store_true",
        help="Skip running backtests; only compute parity from artifacts under --out-root.",
    )
    parser.add_argument(
        "--include-non-cme",
        action="store_true",
        help="Also attempt parity runs for non-CME exchanges (e.g., COMEX). Defaults to CME-only.",
    )
    args = parser.parse_args()

    if args.compare_only and not args.out_root:
        raise SystemExit("--compare-only requires --out-root pointing at an existing parity run folder.")

    out_root = Path(args.out_root).resolve() if args.out_root else (LUMIBOT_ROOT / "tests" / "backtest" / "_parity_runs" / f"ibkr_vs_artifact_baselines_{_now_id()}")
    _ensure_dir(out_root)

    baselines = [
        BaselineSpec(
            name="MESFlipStrategy",
            baseline_prefix=Path("/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/logs/MESFlipStrategy_2025-11-25_23-19_dJE7Kl"),
            strategy_file=Path("/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/MES Flip.py"),
            tick=0.25,
            ibkr_futures_exchange="CME",
        ),
        BaselineSpec(
            name="GoldFlipMinuteStrategy",
            baseline_prefix=Path("/Users/robertgrzesik/Documents/Development/Strategy Library/logs/GoldFlipMinuteStrategy_2025-11-12_01-58_ObSl6b"),
            strategy_file=Path("/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/Gold Flip Minute.py"),
            tick=0.1,
            ibkr_futures_exchange="COMEX",
        ),
        BaselineSpec(
            name="MESMomentumSMA9",
            baseline_prefix=Path("/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/logs/MESMomentumSMA9_2025-10-15_12-52_88xWTg"),
            strategy_file=Path("/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/MES Momentum SMA 9.py"),
            tick=0.25,
            ibkr_futures_exchange="CME",
        ),
    ]

    if args.only:
        baselines = [b for b in baselines if b.name == args.only]
        if not baselines:
            raise SystemExit(f"Unknown --only baseline: {args.only}")

    dotenv = Path(args.dotenv).expanduser().resolve()
    if not dotenv.exists():
        raise SystemExit(f"dotenv file not found: {dotenv}")

    # Snapshot strategy files used for reproducibility (no edits to originals).
    snapshot_dir = out_root / "strategy_snapshots"
    _ensure_dir(snapshot_dir)
    for b in baselines:
        if b.strategy_file.exists():
            dst = snapshot_dir / b.strategy_file.name
            try:
                shutil.copy2(b.strategy_file, dst)
            except Exception:
                pass

    all_results: dict[str, Any] = {}
    for b in baselines:
        if not args.include_non_cme and str(b.ibkr_futures_exchange).strip().upper() != "CME":
            all_results[b.name] = {
                "baseline": {"name": b.name, "prefix": str(b.baseline_prefix), "exchange": b.ibkr_futures_exchange},
                "skipped": True,
                "skip_reason": f"Skipped non-CME exchange={b.ibkr_futures_exchange} (CME-only run).",
            }
            continue

        settings = _baseline_settings(b.baseline_prefix)
        start = str(settings.get("backtesting_start"))
        end = str(settings.get("backtesting_end"))

        # Symbol for canonical price-line extraction: infer from baseline fills.
        baseline_trades = b.baseline_prefix.with_name(b.baseline_prefix.name + "_trades.csv")
        symbol = None
        try:
            tdf = pd.read_csv(baseline_trades)
            if "symbol" in tdf.columns:
                vc = tdf["symbol"].dropna().astype(str).value_counts()
                symbol = str(vc.index[0]) if not vc.empty else None
        except Exception:
            symbol = None
        if not symbol:
            raise SystemExit(f"Unable to infer symbol from baseline trades: {baseline_trades}")

        cache_folder = out_root / "cache" / b.name
        _ensure_conids_seeded(cache_root=cache_folder, dotenv=dotenv, cache_version=args.cache_version)
        cold_dir = out_root / b.name / "ibkr_cold"
        warm_dir = out_root / b.name / "ibkr_warm"
        yappi_dir = out_root / b.name / "ibkr_yappi"

        if args.compare_only:
            cold = _load_existing_run(name=f"{b.name}:cold", workdir=cold_dir, cache_folder=cache_folder, start=start, end=end)
            warm = _load_existing_run(name=f"{b.name}:warm", workdir=warm_dir, cache_folder=cache_folder, start=start, end=end)
            yappi = _load_existing_run(name=f"{b.name}:yappi", workdir=yappi_dir, cache_folder=cache_folder, start=start, end=end)
        else:
            cold = _run_prodlike(
                name=f"{b.name}:cold",
                strategy_file=b.strategy_file,
                start=start,
                end=end,
                workdir=cold_dir,
                cache_folder=cache_folder,
                dotenv=dotenv,
                cache_version=args.cache_version,
                ibkr_futures_exchange=b.ibkr_futures_exchange,
                profile=None,
                timeout_s=args.timeout_s,
            )
            warm = _run_prodlike(
                name=f"{b.name}:warm",
                strategy_file=b.strategy_file,
                start=start,
                end=end,
                workdir=warm_dir,
                cache_folder=cache_folder,
                dotenv=dotenv,
                cache_version=args.cache_version,
                ibkr_futures_exchange=b.ibkr_futures_exchange,
                profile=None,
                timeout_s=max(1800, int(args.timeout_s / 2)),
            )
            yappi = _run_prodlike(
                name=f"{b.name}:yappi",
                strategy_file=b.strategy_file,
                start=start,
                end=end,
                workdir=yappi_dir,
                cache_folder=cache_folder,
                dotenv=dotenv,
                cache_version=args.cache_version,
                ibkr_futures_exchange=b.ibkr_futures_exchange,
                profile="yappi",
                timeout_s=max(1800, int(args.timeout_s / 2)),
            )

        # Parity comparisons use the warm run (deterministic, cache hit).
        run_prefix = warm.get("prefix")
        parity: dict[str, Any] = {"pass": False, "strict_pass": False}
        if run_prefix:
            base_ind = b.baseline_prefix.with_name(b.baseline_prefix.name + "_indicators.csv")
            base_trades = b.baseline_prefix.with_name(b.baseline_prefix.name + "_trades.csv")
            base_stats = b.baseline_prefix.with_name(b.baseline_prefix.name + "_stats.csv")

            run_ind = Path(warm_dir) / "logs" / f"{run_prefix}_indicators.csv"
            run_trades = Path(warm_dir) / "logs" / f"{run_prefix}_trades.csv"
            run_stats = Path(warm_dir) / "logs" / f"{run_prefix}_stats.csv"

            indicators_cmp = _compare_indicators(base_ind, run_ind, symbol=symbol, tick=b.tick)
            trades_cmp = _compare_trades(base_trades, run_trades, tick=b.tick)
            stats_cmp = _compare_stats(base_stats, run_stats)

            parity["symbol"] = symbol
            parity["tick"] = b.tick
            parity["indicators"] = indicators_cmp
            parity["trades"] = trades_cmp
            parity["stats"] = stats_cmp

            indicators_ok = (indicators_cmp.get("first_bad_timestamp") is None) and (indicators_cmp.get("overlap_ratio_baseline", 0.0) >= 0.99)
            trades_ok = trades_cmp.get("first_mismatch_index") is None
            stats_ok = (stats_cmp.get("error") is None) and (stats_cmp.get("first_bad_timestamp") is None)
            parity["indicators_pass"] = bool(indicators_ok)
            parity["trades_pass"] = bool(trades_ok)
            parity["stats_pass"] = bool(stats_ok)

            # "Pass" is the core correctness signal (fills + price series). Portfolio value can
            # drift by a few ticks when vendors disagree by a tick on a handful of bars; treat
            # stats diffs as informational unless they are wildly off.
            parity["pass"] = bool(indicators_ok and trades_ok)
            parity["strict_pass"] = bool(indicators_ok and trades_ok and stats_ok)

        # YAPPI analysis (best-effort).
        yappi_csv = None
        if yappi.get("prefix"):
            candidate = Path(yappi_dir) / "logs" / f"{yappi['prefix']}_profile_yappi.csv"
            if candidate.exists():
                yappi_csv = candidate
        yappi_analysis = _analyze_yappi(yappi_csv) if yappi_csv else None

        all_results[b.name] = {
            "baseline": {
                "name": b.name,
                "prefix": str(b.baseline_prefix),
                "start": start,
                "end": end,
                "tick": b.tick,
                "symbol": symbol,
                "strategy_file": str(b.strategy_file),
            },
            "runs": {"cold": cold, "warm": warm, "yappi": yappi},
            "parity": parity,
            "yappi": {"csv": str(yappi_csv) if yappi_csv else None, "analysis": yappi_analysis},
        }

    summary_path = out_root / "summary.json"
    summary_path.write_text(json.dumps(all_results, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    # Human summary
    lines = [f"# IBKR vs baseline artifacts parity: {out_root}\n"]
    for name, payload in all_results.items():
        parity = payload.get("parity") or {}
        ok = "PASS" if parity.get("pass") else "FAIL"
        strict = "PASS" if parity.get("strict_pass") else "FAIL"
        lines.append(f"## {name}: {ok} (strict={strict})")
        if "indicators" in parity:
            ind = parity["indicators"]
            lines.append(f"- indicators overlap={ind.get('overlap_ratio_baseline'):.4f} max_abs_diff={ind.get('max_abs_diff')} first_bad={ind.get('first_bad_timestamp')}")
        if "trades" in parity:
            tr = parity["trades"]
            lines.append(f"- trades fills baseline={tr.get('baseline_fills')} ibkr={tr.get('ibkr_fills')} first_mismatch={tr.get('first_mismatch')}")
        if "stats" in parity:
            st = parity["stats"]
            lines.append(f"- stats max_abs_diff={st.get('max_abs_diff')} first_bad={st.get('first_bad_timestamp')}")
        y = payload.get("yappi") or {}
        if y.get("csv"):
            lines.append(f"- yappi csv={y.get('csv')}")
        lines.append("")

    (out_root / "summary.md").write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    print(str(out_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
