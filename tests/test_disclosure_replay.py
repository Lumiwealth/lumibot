import json
from pathlib import Path

from lumibot.example_strategies.disclosure_replay import (
    replay_congress_disclosures,
    write_replay_artifacts,
)

DISCLOSURE = {
    "id": "clock-report-date",
    "Politician": "Clock Test Member",
    "Ticker": "NVDA",
    "Transaction": "Purchase",
    "TransactionDate": "2026-01-05",
    "ReportDate": "2026-02-14T15:00:00+00:00",
    "Amount": "$100,001 - $250,000",
    "source": "clock_test_not_a_filing",
    "fetched_at": "2026-02-14T15:00:01+00:00",
}


def test_congress_replay_never_orders_before_publication_and_orders_only_from_risk_role():
    market = {"NVDA": {"price": 100.0, "average_daily_volume": 2_000_000}}

    before = replay_congress_disclosures(
        [DISCLOSURE],
        market,
        as_of="2026-02-14T14:59:59+00:00",
        initial_cash=100_000,
    )
    after = replay_congress_disclosures(
        [DISCLOSURE],
        market,
        as_of="2026-02-14T15:00:00+00:00",
        initial_cash=100_000,
        max_position_pct=5,
        max_total_exposure_pct=20,
        minimum_average_dollar_volume=1_000_000,
    )

    assert before["orders"] == []
    assert len(after["orders"]) == 1
    order = after["orders"][0]
    assert order["origin_role"] == "trading_risk_manager"
    assert order["submitted_at"] == DISCLOSURE["ReportDate"]
    assert order["side"] == "buy"
    assert order["quantity"] == 50
    assert order["notional"] == 5_000
    assert after["trace"][0]["role"] == "disclosure_researcher"
    assert after["trace"][1]["role"] == "trading_risk_manager"
    assert all(event.get("order_id") is None for event in after["trace"] if event["role"] != "trading_risk_manager")


def test_congress_replay_enforces_liquidity_staleness_and_total_exposure():
    low_liquidity = replay_congress_disclosures(
        [DISCLOSURE],
        {"NVDA": {"price": 100.0, "average_daily_volume": 100}},
        as_of="2026-02-14T15:00:00+00:00",
        initial_cash=100_000,
        minimum_average_dollar_volume=1_000_000,
    )
    stale = replay_congress_disclosures(
        [DISCLOSURE],
        {"NVDA": {"price": 100.0, "average_daily_volume": 2_000_000}},
        as_of="2026-06-01T15:00:00+00:00",
        initial_cash=100_000,
        max_disclosure_age_days=45,
    )

    assert low_liquidity["orders"] == []
    assert low_liquidity["decisions"][0]["reason"] == "liquidity_below_minimum"
    assert stale["orders"] == []
    assert stale["decisions"][0]["reason"] == "stale_disclosure"


def test_disclosure_replay_writes_trace_trade_and_tearsheet_artifacts(tmp_path):
    result = replay_congress_disclosures(
        [DISCLOSURE],
        {"NVDA": {"price": 100.0, "average_daily_volume": 2_000_000}},
        as_of="2026-02-14T15:00:00+00:00",
        initial_cash=100_000,
    )

    receipt = write_replay_artifacts(result, tmp_path)

    assert set(receipt) == {"summary", "trace", "trades", "tearsheet"}
    for artifact in receipt.values():
        assert Path(artifact["path"]).is_file()
        assert artifact["sha256"]
    trace = json.loads(Path(receipt["trace"]["path"]).read_text(encoding="utf-8"))
    assert [event["role"] for event in trace] == ["disclosure_researcher", "trading_risk_manager"]
    assert "45-day" in Path(receipt["tearsheet"]["path"]).read_text(encoding="utf-8")
