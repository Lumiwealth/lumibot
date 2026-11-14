"""Crude oil contract months must resolve to IBKR's real last trade dates.

Regression (2026-11-13): a crude oil backtest asked IBKR for the June 2026
contract expiring 2026-05-20, but CLM26 last traded 2026-05-19. The rule
"3 business days before the 25th of the prior month" was counted on the Globex
session calendar, which opens on Memorial Day (2026-05-25), instead of CME
business days. The same slip hit Thanksgiving, Juneteenth, MLK and Presidents
Day months. A same-month registry fallback that found two May 2026 crude
entries (one stale delivery-month entry) then gave up.

The fixture holds the last trade dates IBKR itself reported for every CL and
MCL contract in the shared contract registry.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from lumibot.tools import futures_roll

RECORDED = json.loads(
    (Path(__file__).parent / "fixtures" / "ibkr_futures_calendar" / "nymex_crude_last_trade_dates.json").read_text()
)


def _delivery_month(last_trade: str) -> tuple[int, int]:
    year, month = int(last_trade[:4]), int(last_trade[4:6]) + 1
    return (year + 1, 1) if month == 13 else (year, month)


@pytest.mark.parametrize("root,rule", [("CL", futures_roll._cl_last_trade_date),
                                       ("MCL", futures_roll._mcl_last_trade_date)])
def test_computed_last_trade_dates_match_ibkr(root, rule):
    mismatches = []
    for recorded in RECORDED[root]:
        computed = rule(*_delivery_month(recorded)).strftime("%Y%m%d")
        if computed != recorded:
            mismatches.append((recorded, computed))
    assert mismatches == []


def test_reported_june_2026_contracts():
    assert futures_roll._cl_last_trade_date(2026, 6).date() == date(2026, 5, 19)
    assert futures_roll._mcl_last_trade_date(2026, 6).date() == date(2026, 5, 18)


def test_same_month_registry_entries_resolve_to_the_requested_contract_month():
    """A stale delivery-month entry for CLK26 must not block CLM26."""
    from lumibot.tools.ibkr_helper import IbkrConidKey, _lookup_same_month_future_conid_from_mapping

    mapping = {
        # CLM26 (June delivery) under its real last trade date.
        IbkrConidKey("future", "CL", "USD", "NYMEX", "20260519").to_key(): 1001,
        # CLK26 (May delivery, last traded 2026-04-21) keyed by its delivery month.
        IbkrConidKey("future", "CL", "USD", "NYMEX", "20260515").to_key(): 1002,
    }
    requested = IbkrConidKey("future", "CL", "USD", "NYMEX", "20260520")

    assert _lookup_same_month_future_conid_from_mapping(mapping=mapping, key=requested) == (1001, "20260519")


def test_truly_ambiguous_same_month_entries_still_refuse():
    from lumibot.tools.ibkr_helper import IbkrConidKey, _lookup_same_month_future_conid_from_mapping

    mapping = {
        IbkrConidKey("future", "CL", "USD", "NYMEX", "20260514").to_key(): 1001,
        IbkrConidKey("future", "CL", "USD", "NYMEX", "20260515").to_key(): 1002,
    }
    requested = IbkrConidKey("future", "CL", "USD", "NYMEX", "20260520")

    assert _lookup_same_month_future_conid_from_mapping(mapping=mapping, key=requested) is None
