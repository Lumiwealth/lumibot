import datetime as dt
import os
from pathlib import Path

import pandas as pd
import pytest
from dotenv import load_dotenv


DEFAULT_ENV_PATH = Path.home() / "Documents/Development/Strategy Library/Demos/.env"
LOG_DIR = Path(__file__).resolve().parent / "logs"

# Load env and set data source before importing LumiBot/Theta modules so the downloader is used instead of local ThetaTerminal.
env_path = Path(os.environ.get("LUMIBOT_DEMOS_ENV", DEFAULT_ENV_PATH))
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()
os.environ.setdefault("BACKTESTING_DATA_SOURCE", "ThetaData")

from lumibot.backtesting import ThetaDataBacktesting
from tests.backtest.strategies.tqqq_200_day_ma import TQQQ200DayMAStrategy
from tests.backtest.strategies.meli_drawdown_recovery import MELIDrawdownRecovery
from tests.backtest.strategies.pltr_bull_spreads_strategy import BullCallSpreadStrategy
from tests.backtest.strategies.iron_condor_0dte import IronCondor0DTE


def _ensure_env_loaded() -> None:
    env_path_local = Path(os.environ.get("LUMIBOT_DEMOS_ENV", DEFAULT_ENV_PATH))
    if env_path_local.exists():
        load_dotenv(env_path_local)
    required = [
        "DATADOWNLOADER_BASE_URL",
        "DATADOWNLOADER_API_KEY",
    ]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        pytest.fail(f"Missing required env vars for ThetaData backtests: {missing}")

    # Use ThetaData downloader-backed source
    os.environ.setdefault("BACKTESTING_DATA_SOURCE", "ThetaData")


def _ensure_log_dir() -> Path:
    LOG_DIR.mkdir(exist_ok=True)
    return LOG_DIR


def _trade_log_df(strategy_obj) -> pd.DataFrame:
    log = getattr(strategy_obj.broker, "_trade_event_log_df", None)
    if log is None or getattr(log, "empty", True):
        pytest.fail("No trade event log found.")
    return log


def test_tqqq_theta_integration():
    _ensure_env_loaded()
    backtesting_start = dt.datetime(2020, 10, 1)
    backtesting_end = dt.datetime(2025, 11, 4)

    results, strat_obj = TQQQ200DayMAStrategy.run_backtest(
        ThetaDataBacktesting,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        benchmark_asset=None,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        quiet_logs=False,
    )

    assert results is not None
    trades = _trade_log_df(strat_obj)
    fills = trades[trades["status"] == "fill"]
    assert len(fills) > 0
    assert fills["price"].notnull().all()


def test_meli_theta_integration(tmp_path_factory):
    _ensure_env_loaded()
    backtesting_start = dt.datetime(2020, 10, 1)
    backtesting_end = dt.datetime(2025, 11, 4)

    results, strat_obj = MELIDrawdownRecovery.run_backtest(
        ThetaDataBacktesting,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        benchmark_asset=None,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        quiet_logs=True,
    )

    assert results is not None
    trades = _trade_log_df(strat_obj)
    buys = trades[trades["side"].str.contains("buy")]
    assert not buys.empty
    fills = trades[trades["status"] == "fill"]
    assert len(fills) > 0
    assert fills["price"].notnull().all()

    # Persist detailed trade log for manual inspection (ignored by git)
    log_dir = _ensure_log_dir()
    log_path = log_dir / "meli_trades.csv"
    trades.to_csv(log_path, index=False)


def test_pltr_minute_theta_integration():
    _ensure_env_loaded()
    # Recent, short window to keep minute/options runtime reasonable
    backtesting_start = dt.datetime(2025, 9, 16, 13, 30)
    backtesting_end = dt.datetime(2025, 9, 16, 14, 30)

    results, strat_obj = BullCallSpreadStrategy.run_backtest(
        ThetaDataBacktesting,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        benchmark_asset=None,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        quiet_logs=True,
        parameters={
            "symbols": ["PLTR"],
            "max_symbols_per_iteration": 1,
            "max_symbols_per_day": 1,
            "trade_only_top_slope": True,
            "sleeptime": "30M",
        },
    )

    assert results is not None
    trades = _trade_log_df(strat_obj)
    assert len(trades) > 0
    assert trades["price"].notnull().all()


def test_iron_condor_minute_theta_integration():
    _ensure_env_loaded()
    # Recent, shorter window to keep minute/options runtime reasonable
    backtesting_start = dt.datetime(2025, 9, 8)
    backtesting_end = dt.datetime(2025, 10, 10)

    results, strat_obj = IronCondor0DTE.run_backtest(
        ThetaDataBacktesting,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        benchmark_asset=None,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        quiet_logs=True,
    )

    assert results is not None
    trades = _trade_log_df(strat_obj)
    assert len(trades) > 0
    assert trades["price"].notnull().all()
