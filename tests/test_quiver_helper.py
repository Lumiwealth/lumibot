from datetime import datetime
from types import SimpleNamespace

import pytest


def test_quiver_component_loads_without_credentials_and_filters_by_report_date(monkeypatch, tmp_path):
    monkeypatch.delenv("QUIVER_API_KEY", raising=False)
    from lumibot.components.quiver_helper import QuiverHelper

    helper = QuiverHelper(SimpleNamespace(log_message=lambda *args, **kwargs: None), cache_path=tmp_path / "quiver.csv")
    records = [
        {
            "Ticker": "NVDA",
            "Transaction": "Purchase",
            "TransactionDate": "2026-01-05",
            "ReportDate": "2026-02-14",
            "Amount": "1000",
        }
    ]

    assert helper.filter_disclosures_as_of(records, datetime(2026, 2, 13).date()) == []
    assert helper.filter_disclosures_as_of(records, datetime(2026, 2, 14).date()) == records
    with pytest.raises(ValueError, match="QUIVER_API_KEY"):
        helper.fetch_congress_trading_data("P000197")
