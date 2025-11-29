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


def _trade_log_df(strategy_obj, require_trades: bool = True) -> pd.DataFrame:
    """Get trade log from strategy. If require_trades=False, returns empty DF if no trades."""
    log = getattr(strategy_obj.broker, "_trade_event_log_df", None)
    if log is None or getattr(log, "empty", True):
        if require_trades:
            pytest.fail("No trade event log found.")
        return pd.DataFrame()
    return log


def test_tqqq_theta_integration():
    _ensure_env_loaded()
    # Use 2 weeks instead of 5 years to keep CI fast (~30min target)
    backtesting_start = dt.datetime(2024, 10, 1)
    backtesting_end = dt.datetime(2024, 10, 14)

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
    # Use 2 weeks instead of 5 years to keep CI fast (~30min target)
    # Purpose: verify ThetaData stock data works, not that strategy trades
    backtesting_start = dt.datetime(2024, 10, 1)
    backtesting_end = dt.datetime(2024, 10, 14)

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

    # Verify backtest completed successfully (ThetaData integration works)
    assert results is not None
    assert strat_obj.portfolio_value > 0  # Strategy ran without errors

    # Trades may or may not happen depending on market conditions
    trades = _trade_log_df(strat_obj, require_trades=False)
    if not trades.empty:
        fills = trades[trades["status"] == "fill"]
        if len(fills) > 0:
            assert fills["price"].notnull().all()
        # Persist detailed trade log for manual inspection (ignored by git)
        log_dir = _ensure_log_dir()
        log_path = log_dir / "meli_trades.csv"
        trades.to_csv(log_path, index=False)


def test_pltr_minute_theta_integration():
    _ensure_env_loaded()
    # Short window to keep minute/options runtime reasonable
    # Purpose: verify ThetaData minute-level options data works
    backtesting_start = dt.datetime(2024, 9, 16, 13, 30)
    backtesting_end = dt.datetime(2024, 9, 16, 14, 30)

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

    # Verify backtest completed successfully (ThetaData integration works)
    assert results is not None
    assert strat_obj.portfolio_value > 0  # Strategy ran without errors

    # Trades may or may not happen depending on market conditions
    trades = _trade_log_df(strat_obj, require_trades=False)
    if not trades.empty:
        assert trades["price"].notnull().all()


def test_iron_condor_minute_theta_integration():
    _ensure_env_loaded()
    # Use 3 trading days for minute-level options (much faster than 1 month)
    # Purpose: verify ThetaData SPX index + options data works
    backtesting_start = dt.datetime(2024, 9, 9)
    backtesting_end = dt.datetime(2024, 9, 11)

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

    # Verify backtest completed successfully (ThetaData integration works)
    assert results is not None
    assert strat_obj.portfolio_value > 0  # Strategy ran without errors

    # Trades may or may not happen (0DTE needs same-day expiration)
    trades = _trade_log_df(strat_obj, require_trades=False)
    if not trades.empty:
        assert trades["price"].notnull().all()
