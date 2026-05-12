from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from lumibot.backtesting.databento_backtesting_pandas import DataBentoDataBacktestingPandas
from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from lumibot.tools import databento_helper, ibkr_helper
from tests.backtest.parity_strategies.mes_order_matrix_parity import MesOrderMatrixParity, MesParityConfig


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime


def _utc(s: str) -> datetime:
    # Accept YYYY-MM-DD or YYYY-MM-DDTHH:MM.
    raw = s.strip()
    if "T" in raw:
        dt = datetime.fromisoformat(raw)
    else:
        dt = datetime.fromisoformat(raw + "T00:00:00")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _require_env(keys: Iterable[str]) -> None:
    missing = [k for k in keys if not (os.environ.get(k) or "").strip()]
    if missing:
        raise SystemExit(f"Missing required env vars: {missing}")


def _import_strategy_class(path: Path) -> type[Strategy]:
    spec = importlib.util.spec_from_file_location(path.stem.replace(" ", "_"), path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module.__name__] = module
    spec.loader.exec_module(module)  # type: ignore[call-arg]

    candidates: list[type[Strategy]] = []
    for obj in module.__dict__.values():
        if isinstance(obj, type) and issubclass(obj, Strategy) and obj is not Strategy:
            candidates.append(obj)
    if not candidates:
        raise RuntimeError(f"No Strategy subclass found in {path}")

    # Prefer the first class defined in the file order.
    return candidates[0]


def _find_single(glob_iter: Iterable[Path], description: str) -> Path:
    paths = sorted(glob_iter)
    if len(paths) != 1:
        raise RuntimeError(f"Expected exactly 1 {description}, got {len(paths)}: {[p.name for p in paths]}")
    return paths[0]


def _read_tearsheet_metrics(tearsheet_csv: Path) -> dict[str, float]:
    df = pd.read_csv(tearsheet_csv)
    out = {}
    for metric in ("Total Return", "CAGR% (Annual Return)", "Max Drawdown"):
        row = df.loc[df["Metric"] == metric]
        if row.empty:
            continue
        text = str(row["Strategy"].iloc[0]).strip().replace(",", "")
        out[metric] = float(text.rstrip("%")) / 100.0
    return out


def _run_backtest(
    *,
    strategy_cls: type[Strategy],
    datasource_cls: type,
    window: Window,
    run_dir: Path,
    parameters: dict,
    budget: float = 50_000.0,
) -> dict:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "cache").mkdir(parents=True, exist_ok=True)

    env = os.environ
    env["LUMIBOT_CACHE_FOLDER"] = str(run_dir / "cache")
    env["SHOW_TEARSHEET"] = "False"
    env["SHOW_INDICATORS"] = "True"
    env["SAVE_LOGFILE"] = "true"

    prev_cwd = Path.cwd()
    try:
        os.chdir(run_dir)
        t0 = time.time()
        strategy_cls.run_backtest(
            datasource_class=datasource_cls,
            backtesting_start=window.start,
            backtesting_end=window.end,
            market="us_futures",
            analyze_backtest=False,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=True,
            show_indicators=True,
            quiet_logs=True,
            name=strategy_cls.__name__,
            budget=budget,
            parameters=parameters,
            logfile=str(run_dir / "run.log"),
        )
        wall_s = time.time() - t0
    finally:
        os.chdir(prev_cwd)

    logs_dir = run_dir / "logs"
    tearsheet_csv = _find_single(logs_dir.glob(f"{strategy_cls.__name__}_*_tearsheet.csv"), "tearsheet.csv")
    settings_json = _find_single(logs_dir.glob(f"{strategy_cls.__name__}_*_settings.json"), "settings.json")
    trades_csv = _find_single(logs_dir.glob(f"{strategy_cls.__name__}_*_trades.csv"), "trades.csv")

    metrics = _read_tearsheet_metrics(tearsheet_csv)
    trades = pd.read_csv(trades_csv)
    trade_count = int(len(trades.index))

    return {
        "run_dir": str(run_dir),
        "wall_time_seconds": float(wall_s),
        "metrics": metrics,
        "trade_count": trade_count,
        "settings_json": str(settings_json),
        "tearsheet_csv": str(tearsheet_csv),
        "trades_csv": str(trades_csv),
    }


def _bar_close_series_for_provider(
    *,
    provider: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    databento_key: str | None,
) -> pd.Series:
    if provider == "ibkr":
        df = ibkr_helper.get_price_data(
            asset=asset,
            quote=None,
            timestep="minute",
            start_dt=start,
            end_dt=end,
            exchange=None,
            include_after_hours=True,
            source="Trades",
        )
    elif provider == "databento":
        assert databento_key
        df = databento_helper.get_price_data_from_databento(
            api_key=databento_key,
            asset=asset,
            start=start,
            end=end,
            timestep="minute",
        )
    else:
        raise ValueError(provider)

    if df is None or df.empty:
        return pd.Series(dtype=float)
    return pd.to_numeric(df["close"], errors="coerce").dropna()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run IBKR vs DataBento futures parity suite (CME equity index futures).")
    parser.add_argument(
        "--demos-dir",
        default="/Users/robertgrzesik/Documents/Development/Strategy Library/Demos",
        help="Path to Strategy Library demos folder",
    )
    parser.add_argument("--out-dir", default="tests/backtest/_parity_runs", help="Output directory (workspace-relative)")
    parser.add_argument(
        "--windows",
        nargs="*",
        default=[
            "2025-10-06:2025-10-24",
            "2025-11-03:2025-11-21",
            "2025-12-01:2025-12-18",
        ],
        help="Windows as START:END (UTC, END exclusive). Example: 2025-11-03:2025-11-21",
    )
    parser.add_argument(
        "--strategies",
        nargs="*",
        default=[
            "ES Futures Trend.py",
            "MES Momentum SMA 9.py",
            "FuturesThreeToOneRRWithEMA.py",
        ],
        help="Demo strategy filenames to include (from demos-dir)",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    demos_dir = Path(args.demos_dir)
    out_root = (repo_root / args.out_dir).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    _require_env(["DATADOWNLOADER_BASE_URL", "DATADOWNLOADER_API_KEY", "IBKR_FUTURES_EXCHANGE"])
    _require_env(["DATABENTO_API_KEY"])
    databento_key = os.environ["DATABENTO_API_KEY"].strip()

    windows: list[Window] = []
    for w in args.windows:
        start_s, end_s = w.split(":")
        windows.append(Window(start=_utc(start_s), end=_utc(end_s)))

    class _IbkrTradesBacktesting(InteractiveBrokersRESTBacktesting):
        def __init__(self, *a, **kw):
            kw.setdefault("history_source", "Trades")
            super().__init__(*a, **kw)

    providers: list[tuple[str, type]] = [
        ("ibkr", _IbkrTradesBacktesting),
        ("databento", DataBentoDataBacktestingPandas),
    ]

    # Explicit contract baseline for bar parity (deterministic).
    explicit_mes = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=datetime(2025, 12, 19).date())

    suite_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    suite_dir = out_root / f"ibkr_vs_databento_{suite_id}"
    suite_dir.mkdir(parents=True, exist_ok=True)

    results: dict[str, dict] = {"suite_id": suite_id, "runs": {}}

    # 1) Deterministic order-matrix strategy, explicit + continuous.
    parity_variants: list[tuple[str, dict]] = [
        ("mes_order_matrix_explicit", {"cfg": MesParityConfig(asset_type="future", expiration=explicit_mes.expiration)}),
        ("mes_order_matrix_cont", {"cfg": MesParityConfig(asset_type="cont_future")}),
    ]

    for window in windows:
        window_slug = f"{window.start.date().isoformat()}_{window.end.date().isoformat()}"
        for variant_slug, params in parity_variants:
            for provider_slug, datasource_cls in providers:
                run_dir = suite_dir / provider_slug / variant_slug / window_slug
                key = f"{provider_slug}:{variant_slug}:{window_slug}"
                results["runs"][key] = _run_backtest(
                    strategy_cls=MesOrderMatrixParity,
                    datasource_cls=datasource_cls,
                    window=window,
                    run_dir=run_dir,
                    parameters=params,
                )

    # 2) Demo strategies (continuous futures)
    for filename in args.strategies:
        demo_path = demos_dir / filename
        if not demo_path.exists():
            raise SystemExit(f"Missing demo strategy file: {demo_path}")
        strategy_cls = _import_strategy_class(demo_path)
        demo_slug = demo_path.stem.replace(" ", "_")

        for window in windows:
            window_slug = f"{window.start.date().isoformat()}_{window.end.date().isoformat()}"
            for provider_slug, datasource_cls in providers:
                run_dir = suite_dir / provider_slug / f"demo_{demo_slug}" / window_slug
                key = f"{provider_slug}:demo_{demo_slug}:{window_slug}"
                results["runs"][key] = _run_backtest(
                    strategy_cls=strategy_cls,
                    datasource_cls=datasource_cls,
                    window=window,
                    run_dir=run_dir,
                    parameters={},
                    budget=100_000.0,
                )

    # 3) Bar close parity for deterministic explicit MES (per window)
    bar_parity = {}
    for window in windows:
        s = window.start
        e = window.end
        ibkr_close = _bar_close_series_for_provider(provider="ibkr", asset=explicit_mes, start=s, end=e, databento_key=databento_key)
        db_close = _bar_close_series_for_provider(provider="databento", asset=explicit_mes, start=s, end=e, databento_key=databento_key)
        common = ibkr_close.index.intersection(db_close.index)
        if len(common) < 100:
            bar_parity[str(s.date())] = {"overlap_bars": int(len(common)), "max_abs_diff": None}
            continue
        diffs = (ibkr_close.loc[common] - db_close.loc[common]).abs()
        bar_parity[str(s.date())] = {"overlap_bars": int(len(common)), "max_abs_diff": float(diffs.max())}
    results["bar_parity_explicit_mes"] = bar_parity

    out_json = suite_dir / "summary.json"
    out_json.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    print(f"Wrote parity suite summary: {out_json}")


if __name__ == "__main__":
    main()
