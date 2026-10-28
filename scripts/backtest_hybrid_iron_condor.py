"""Run the hybrid example with real historical data and explicit ChatGPT plan inference.

Supply existing Alpaca/FRED credentials in the environment. Outputs and usage are
private run artifacts; no credentials are read from or written into the project.
"""

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path

import pytz

from lumibot.backtesting import AlpacaBacktesting
from lumibot.entities import TradingFee
from lumibot.example_strategies.ai_iron_condor import AIIronCondorStrategy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="Exchange-local ISO datetime")
    parser.add_argument("--end", required=True, help="Exchange-local ISO datetime")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    os.chdir(output)  # LumiBot's normal logs/ and trace writers stay inside this run.
    os.environ["BACKTESTING_DATA_SOURCE"] = "none"
    os.environ["LUMIBOT_AGENT_MODEL_REQUEST_TIMEOUT_SECONDS"] = "0"
    os.environ["LUMIBOT_AGENT_RUN_TIMEOUT_SECONDS"] = "0"
    timezone = pytz.timezone("America/New_York")
    start, end = (timezone.localize(datetime.fromisoformat(value)) for value in (args.start, args.end))
    fee = TradingFee(per_contract_fee=.65)
    began = time.perf_counter()
    result, strategy = AIIronCondorStrategy.run_backtest(
        AlpacaBacktesting, start, end, name="hybrid-iron-condor-chatgpt-luna", budget=10000,
        parameters={"model": "chatgpt/gpt-6-luna"}, full_window=True, timestep="minute", auto_adjust=False,
        buy_trading_fees=[fee], sell_trading_fees=[fee],
        show_plot=False, show_tearsheet=False, show_indicators=False,
        show_progress_bar=False, quiet_logs=False, save_logfile=True,
    )
    usage = list(strategy.vars.ai_usage)
    (output / "ai-usage.json").write_text(json.dumps(usage, indent=2, default=str))
    summary = {
        "model": "chatgpt/gpt-6-luna", "funding": "Login with ChatGPT", "service_tier": "default",
        "api_billed_usd": 0, "chatgpt_credit_debit": None,
        "credit_debit_note": "The inference response reports tokens, not account credit debits.",
        "completed_planner_turns": len(usage),
        "completed_inference_responses_with_usage": sum(len(row["completed_requests"]) for row in usage),
        "parameters_and_usage_counters": {k: v for k, v in strategy.parameters.items()
                                          if k.startswith("agent_") or k == "model"},
        "start": start.isoformat(), "end": end.isoformat(),
        "wall_seconds": time.perf_counter() - began,
        "portfolio_value": strategy.portfolio_value, "result": result,
        "ending_positions": [{"asset": str(p.asset), "quantity": str(p.quantity)}
                             for p in strategy.get_positions() if p.quantity and p.asset.asset_type != "forex"],
        "active_orders": [o.identifier for o in strategy.get_orders() if o.is_active()],
        "price_limitation": "Alpaca historical option trade bars are not historical NBBO quotes.",
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps({k: v for k, v in summary.items() if k not in ("result", "parameters_and_usage_counters")}, default=str))


if __name__ == "__main__":
    main()
