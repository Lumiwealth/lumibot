"""A finished simulator is insufficient evidence of a filled lifecycle."""
import importlib.util
from pathlib import Path

import pandas as pd


spec = importlib.util.spec_from_file_location(
    "hybrid_runner", Path(__file__).parents[2] / "scripts/backtest_hybrid_iron_condor.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def package(time, expiration, opening=True, status="fill"):
    # Same event columns as the broker's trade-event log; neutral synthetic prices.
    return [{"time": time, "asset.expiration": expiration, "asset.right": right,
             "asset.strike": strike, "side": ("buy" if (sign if opening else -sign) > 0 else "sell") + (
                 "_to_open" if opening else "_to_close"), "filled_quantity": 2,
             "asset.asset_type": "option", "status": status}
            for strike, right, sign in [(99, "PUT", 1), (100, "PUT", -1),
                                        (110, "CALL", -1), (111, "CALL", 1)]]


def test_report_requires_filled_exit_before_later_complete_entry():
    first = package("2026-01-05 15:46", "2026-01-06")
    close = package("2026-01-06 11:30", "2026-01-06", opening=False)
    later = package("2026-01-06 15:46", "2026-01-07")
    assert runner.lifecycle_evidence(pd.DataFrame(first + close + later))["entry_exit_reentry"]
    assert not runner.lifecycle_evidence(pd.DataFrame(first + later))["entry_exit_reentry"]
    assert not runner.lifecycle_evidence(pd.DataFrame(first + close + later[:3]))["entry_exit_reentry"]


def test_new_orders_and_automatic_settlement_do_not_prove_a_strategy_exit():
    first = package("2026-01-05 15:46", "2026-01-06")
    settlement = package("2026-01-06 16:00", "2026-01-06", opening=False, status="cash_settled")
    later = package("2026-01-07 15:46", "2026-01-08")
    assert not runner.lifecycle_evidence(pd.DataFrame(first + settlement + later))["entry_exit_reentry"]
    assert not runner.lifecycle_evidence(pd.DataFrame(package("2026-01-05", "2026-01-06", status="new")))["entry_exit_reentry"]
    assert not runner.lifecycle_evidence(pd.DataFrame())["entry_exit_reentry"]
