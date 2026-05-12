from __future__ import annotations

import os
from datetime import UTC, datetime

import pandas as pd
import pytest

from lumibot.backtesting.databento_backtesting_pandas import DataBentoDataBacktestingPandas
from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.tools import databento_helper, ibkr_helper
from tests.backtest.parity_strategies.mes_order_matrix_parity import MesOrderMatrixParity, MesParityConfig

pytestmark = pytest.mark.apitest


def _require_env(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        pytest.skip(f"Missing {name} for parity apitest")
    return value


def _parse_tearsheet_metrics(logs_dir: str, strategy_name: str) -> dict[str, float]:
    candidates = sorted(
        [p for p in os.listdir(logs_dir) if p.startswith(strategy_name + "_") and p.endswith("_tearsheet.csv")]
    )
    assert len(candidates) == 1, f"Expected 1 tearsheet.csv for {strategy_name}, got {candidates}"
    path = os.path.join(logs_dir, candidates[0])
    df = pd.read_csv(path)
    out = {}
    for metric in ("Total Return", "CAGR% (Annual Return)", "Max Drawdown"):
        row = df.loc[df["Metric"] == metric]
        assert not row.empty
        text = str(row["Strategy"].iloc[0]).strip().replace(",", "")
        out[metric] = float(text.rstrip("%")) / 100.0
    return out


def test_ibkr_vs_databento_mes_minute_bars_and_backtest_are_close(tmp_path, monkeypatch):
    # Provider credentials
    _require_env("DATADOWNLOADER_BASE_URL")
    _require_env("DATADOWNLOADER_API_KEY")
    databento_key = _require_env("DATABENTO_API_KEY")

    monkeypatch.setenv("IBKR_FUTURES_EXCHANGE", "CME")

    # Explicit contract for deterministic parity (Dec 2025 MES).
    fut = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=datetime(2025, 12, 19).date())

    start = datetime(2025, 12, 8, 14, 0, tzinfo=UTC)
    end = datetime(2025, 12, 8, 20, 0, tzinfo=UTC)

    # --- Bar parity (1m close series) ---
    ibkr_df = ibkr_helper.get_price_data(
        asset=fut,
        quote=None,
        timestep="minute",
        start_dt=start,
        end_dt=end,
        exchange=None,
        include_after_hours=True,
        source="Trades",  # OHLC-only parity mode
    )
    assert ibkr_df is not None and not ibkr_df.empty

    db_df = databento_helper.get_price_data_from_databento(
        api_key=databento_key,
        asset=fut,
        start=start,
        end=end,
        timestep="minute",
    )
    assert db_df is not None and not db_df.empty

    # Normalize to a common timezone and compare close series on intersecting timestamps.
    ibkr_close = pd.to_numeric(ibkr_df["close"], errors="coerce").dropna()
    db_close = pd.to_numeric(db_df["close"], errors="coerce").dropna()

    common = ibkr_close.index.intersection(db_close.index)
    assert len(common) > 50, f"Not enough overlapping bars for parity check: {len(common)}"

    ibkr_common = ibkr_close.loc[common]
    db_common = db_close.loc[common]

    # We expect these feeds to be extremely close; assert tight numeric tolerance.
    diffs = (ibkr_common - db_common).abs()
    assert float(diffs.max()) <= 0.25, f"Max close diff exceeded 1 tick: max={float(diffs.max())}"

    # --- Backtest parity (same strategy, two providers) ---
    run_root = tmp_path / "runs"
    run_root.mkdir(parents=True, exist_ok=True)

    class _IbkrTradesBacktesting(InteractiveBrokersRESTBacktesting):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("history_source", "Trades")
            super().__init__(*args, **kwargs)

    cases = [
        ("ibkr", _IbkrTradesBacktesting),
        ("databento_pandas", DataBentoDataBacktestingPandas),
    ]

    metrics = {}
    for slug, datasource in cases:
        run_dir = run_root / slug
        run_dir.mkdir(parents=True, exist_ok=True)
        logs_dir = run_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        # Isolate cache folder per provider so we can compare cold/warm runs later in a full suite.
        monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(run_dir / "cache"))
        monkeypatch.setenv("SHOW_TEARSHEET", "False")
        monkeypatch.setenv("SHOW_INDICATORS", "True")
        monkeypatch.setenv("SAVE_LOGFILE", "true")

        prev_cwd = os.getcwd()
        try:
            os.chdir(run_dir)
            MesOrderMatrixParity.run_backtest(
                datasource_class=datasource,
                backtesting_start=start,
                backtesting_end=end,
                market="us_futures",
                analyze_backtest=False,
                show_plot=False,
                show_tearsheet=False,
                save_tearsheet=True,
                show_indicators=True,
                quiet_logs=True,
                name=f"MES_PARITY_{slug}",
                budget=50_000.0,
                parameters={"cfg": MesParityConfig(asset_type="future", expiration=fut.expiration)},
                logfile=str(run_dir / "run.log"),
            )
        finally:
            os.chdir(prev_cwd)

        m = _parse_tearsheet_metrics(str(logs_dir), "MesOrderMatrixParity")
        metrics[slug] = m

    # Tight parity expectations: headline metrics should be very close.
    tr_ibkr = metrics["ibkr"]["Total Return"]
    tr_db = metrics["databento_pandas"]["Total Return"]
    assert abs(tr_ibkr - tr_db) <= 0.005, f"Total Return parity too far apart: ibkr={tr_ibkr} databento={tr_db}"
