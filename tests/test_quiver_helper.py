from __future__ import annotations

import datetime
import importlib
import sys
from typing import Any


class _Strategy:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def log_message(self, message: str, *args: Any, **kwargs: Any) -> None:
        self.messages.append(message)


def _load_quiver(monkeypatch, tmp_path):
    monkeypatch.setenv("QUIVER_API_KEY", "test-token")
    monkeypatch.chdir(tmp_path)
    sys.modules.pop("lumibot.components.quiver_helper", None)
    return importlib.import_module("lumibot.components.quiver_helper")


def test_calculate_portfolio_filters_sales_and_future_transactions(monkeypatch, tmp_path) -> None:
    quiver = _load_quiver(monkeypatch, tmp_path)
    helper = quiver.QuiverHelper(_Strategy())

    portfolio = helper.calculate_portfolio(
        [
            {"Ticker": "NVDA", "TransactionDate": "2024-01-02", "Transaction": "Purchase", "Amount": "100"},
            {"Ticker": "NVDA", "TransactionDate": "2024-01-03", "Transaction": "Sale", "Amount": "25"},
            {"Ticker": "AAPL", "TransactionDate": "2024-02-01", "Transaction": "Purchase", "Amount": "50"},
        ],
        datetime.date(2024, 1, 31),
    )

    assert portfolio == {"NVDA": 75.0}


def test_fetch_congress_trading_data_normalizes_response(monkeypatch, tmp_path) -> None:
    quiver = _load_quiver(monkeypatch, tmp_path)
    helper = quiver.QuiverHelper(_Strategy())

    class _Response:
        status_code = 200

        def json(self) -> object:
            return [
                {"Ticker": "MSFT", "TransactionDate": "2024-01-05", "Transaction": "Purchase", "Amount": "10"},
                "not-a-record",
            ]

    monkeypatch.setattr(quiver.requests, "get", lambda *args, **kwargs: _Response())

    records = helper.fetch_congress_trading_data("P000197")

    assert records == [
        {"Ticker": "MSFT", "TransactionDate": "2024-01-05", "Transaction": "Purchase", "Amount": "10"}
    ]
