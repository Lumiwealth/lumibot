"""Execute the FundamentalValueStrategy either in backtest or live mode.

Usage (backtest):
    python examples/run_fundamental_value.py --backtest

Usage (live):
    python examples/run_fundamental_value.py --live

Optional arguments:
    --config PATH            Custom YAML listing tickers (defaults to bundled config)
    --start YYYY-MM-DD       Backtest start date
    --end YYYY-MM-DD         Backtest end date
    --margin FLOAT           Margin of safety (0-1)
    --allocation FLOAT       Target portfolio allocation per symbol (0-1)

Example commands:
    # Equal-weight backtest for 2024 using default tickers
    python examples/run_fundamental_value.py --backtest --start 2024-01-01 --end 2024-12-31

    # Backtest with bundled watch list and 20% margin of safety
    python examples/run_fundamental_value.py \
        --backtest \
        --config lumibot/example_strategies/config/fundamental_value.yaml \
        --margin 0.2

    # Live / paper trading with Alpaca using the bundled watch list and custom allocation
    python examples/run_fundamental_value.py \
        --live \
        --allocation 0.4 \
        --config lumibot/example_strategies/config/fundamental_value.yaml

"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.credentials import ALPACA_CONFIG
from lumibot.example_strategies.fundamental_value import FundamentalValueStrategy


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Lumibot Fundamental Value strategy")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--backtest", action="store_true", help="Run a Yahoo Finance backtest (default)")
    mode.add_argument("--live", action="store_true", help="Run live/paper trading via Alpaca")

    parser.add_argument("--config", type=str, help="YAML file with ticker list")
    parser.add_argument("--start", type=str, help="Backtest start date, e.g. 2023-01-01")
    parser.add_argument("--end", type=str, help="Backtest end date, e.g. 2023-12-31")
    parser.add_argument("--margin", type=float, help="Margin of safety between 0.0 and 1.0")
    parser.add_argument("--allocation", type=float, help="Target allocation per symbol between 0.0 and 1.0")

    return parser.parse_args()


def build_parameters(args: argparse.Namespace) -> dict:
    parameters: dict = {}
    if args.config:
        parameters["config_path"] = str(Path(args.config).expanduser())
    if args.margin is not None:
        parameters["margin_of_safety"] = float(args.margin)
    if args.allocation is not None:
        parameters["target_allocation"] = float(args.allocation)
    return parameters


def run_backtest(args: argparse.Namespace) -> None:
    start_dt = datetime.strptime(args.start, "%Y-%m-%d") if args.start else datetime(2023, 1, 1)
    end_dt = datetime.strptime(args.end, "%Y-%m-%d") if args.end else datetime(2023, 12, 31)

    print(f"Running backtest from {start_dt.date()} to {end_dt.date()} using Yahoo data")
    results = FundamentalValueStrategy.backtest(
        YahooDataBacktesting,
        start_dt,
        end_dt,
        benchmark_asset="SPY",
        parameters=build_parameters(args),
    )
    print(results)


def run_live(args: argparse.Namespace) -> None:
    if not ALPACA_CONFIG:
        raise RuntimeError("ALPACA_CONFIG is not set; please configure your Alpaca credentials.")

    broker = Alpaca(ALPACA_CONFIG)
    strategy = FundamentalValueStrategy(
        broker=broker,
        parameters=build_parameters(args),
    )

    print("Starting live/paper trading. Press Ctrl+C to stop.")
    strategy.run_live()


def main() -> None:
    args = parse_args()

    if args.live:
        run_live(args)
    else:
        run_backtest(args)


if __name__ == "__main__":
    main()
