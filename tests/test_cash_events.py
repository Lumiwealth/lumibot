from __future__ import annotations

import json
import logging
from collections import deque
from datetime import datetime
from types import SimpleNamespace

import requests

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.brokers.alpaca import Alpaca
from lumibot.brokers.tradier import Tradier
from lumibot.entities import Asset, CashEvent
from lumibot.strategies._strategy import _Strategy


def test_cash_event_builds_stable_serialized_payload():
    event = CashEvent(
        broker_name="alpaca",
        broker_event_id="evt-123",
        event_type="deposit",
        raw_type="CSD",
        amount="250.50",
        currency="usd",
        occurred_at="2026-03-25",
        description="Account deposit",
        is_external_cash_flow=True,
    )

    assert event.event_id == "alpaca:evt-123"
    assert event.amount == 250.5
    assert event.direction == "in"
    assert event.to_dict() == {
        "event_id": "alpaca:evt-123",
        "broker_event_id": "evt-123",
        "broker_name": "alpaca",
        "event_type": "deposit",
        "raw_type": "CSD",
        "amount": 250.5,
        "currency": "USD",
        "occurred_at": "2026-03-25T00:00:00+00:00",
        "description": "Account deposit",
        "direction": "in",
        "is_external_cash_flow": True,
    }


def test_backtesting_broker_default_cash_events_is_noop():
    data_source = PandasDataBacktesting(
        datetime_start=datetime(2025, 1, 2),
        datetime_end=datetime(2025, 1, 20),
    )
    broker = BacktestingBroker(data_source=data_source)
    assert broker.get_cash_events() == []


def test_alpaca_activity_normalization_maps_external_and_tax_events():
    deposit_event = Alpaca._normalize_activity_to_cash_event(
        {
            "id": "evt-deposit",
            "activity_type": "CSD",
            "date": "2026-03-25",
            "net_amount": "500.00",
            "description": "ACH deposit",
            "status": "executed",
        }
    )
    tax_event = Alpaca._normalize_activity_to_cash_event(
        {
            "id": "evt-tax",
            "activity_type": "DIVWH",
            "date": "2026-03-26",
            "net_amount": "-12.34",
            "description": "Dividend withholding",
            "status": "executed",
        }
    )

    assert deposit_event is not None
    assert deposit_event.event_type == "deposit"
    assert deposit_event.is_external_cash_flow is True
    assert deposit_event.direction == "in"

    assert tax_event is not None
    assert tax_event.event_type == "tax"
    assert tax_event.is_external_cash_flow is False
    assert tax_event.direction == "out"


def test_alpaca_get_cash_events_fetches_unfiltered_activity_page_and_normalizes():
    requested = {}

    class _DummyAPI:
        def get(self, path, params):
            requested["path"] = path
            requested["params"] = dict(params)
            return [
                {
                    "id": "evt-deposit",
                    "activity_type": "CSD",
                    "date": "2026-03-25",
                    "net_amount": "500.00",
                    "description": "ACH deposit",
                    "status": "executed",
                },
                {
                    "id": "evt-fill",
                    "activity_type": "FILL",
                    "date": "2026-03-25",
                    "net_amount": "-50.00",
                    "description": "Trade fill",
                    "status": "executed",
                },
            ]

    broker = Alpaca.__new__(Alpaca)
    broker.api = _DummyAPI()

    events = broker.get_cash_events(limit=10)

    assert requested["path"] == "/account/activities"
    assert requested["params"]["page_size"] == 100
    assert requested["params"]["direction"] == "desc"
    assert "activity_types" not in requested["params"]
    assert len(events) == 1
    assert events[0].event_type == "deposit"


def test_alpaca_get_cash_events_paginates_until_it_finds_older_cash_events():
    requests = []

    class _DummyAPI:
        def get(self, path, params):
            requests.append(dict(params))
            if "page_token" not in params:
                return [
                    {
                        "id": "evt-fill-1",
                        "activity_type": "FILL",
                        "date": "2026-03-26",
                        "net_amount": "-50.00",
                        "description": "Trade fill",
                        "status": "executed",
                    },
                    {
                        "id": "evt-fill-2",
                        "activity_type": "FILL",
                        "date": "2026-03-25",
                        "net_amount": "-25.00",
                        "description": "Trade fill",
                        "status": "executed",
                    },
                ]
            return [
                {
                    "id": "evt-deposit",
                    "activity_type": "CSD",
                    "date": "2026-03-24",
                    "net_amount": "500.00",
                    "description": "ACH deposit",
                    "status": "executed",
                }
            ]

    broker = Alpaca.__new__(Alpaca)
    broker.api = _DummyAPI()

    events = broker.get_cash_events(limit=1)

    assert len(requests) == 2
    assert requests[1]["page_token"] == "evt-fill-2"
    assert len(events) == 1
    assert events[0].event_type == "deposit"


def test_alpaca_trade_like_cash_activity_rows_are_skipped():
    fx_event = Alpaca._normalize_activity_to_cash_event(
        {
            "id": "evt-fx",
            "activity_type": "FXTRD",
            "date": "2026-03-24",
            "net_amount": "15.00",
            "description": "FX trade cash movement",
        }
    )
    option_trade_event = Alpaca._normalize_activity_to_cash_event(
        {
            "id": "evt-opt",
            "activity_type": "OPTRD",
            "date": "2026-03-24",
            "net_amount": "-15.00",
            "description": "Option trade cash movement",
        }
    )

    assert fx_event is None
    assert option_trade_event is None


def test_tradier_history_normalization_maps_deposit_and_fee_events():
    deposit_event = Tradier._normalize_history_row_to_cash_event(
        {
            "id": "tradier-deposit",
            "type": "ach",
            "date": "2026-03-24",
            "amount": "1000.00",
            "description": "Bank ACH",
        }
    )
    fee_event = Tradier._normalize_history_row_to_cash_event(
        {
            "event_id": "tradier-fee",
            "type": "fee",
            "date": "2026-03-24",
            "amount": "-1.25",
            "description": "Regulatory fee",
        }
    )

    assert deposit_event is not None
    assert deposit_event.event_type == "deposit"
    assert deposit_event.is_external_cash_flow is True
    assert deposit_event.direction == "in"

    assert fee_event is not None
    assert fee_event.event_type == "fee"
    assert fee_event.is_external_cash_flow is False
    assert fee_event.direction == "out"


def test_tradier_history_normalization_extracts_nested_fields_and_overrides_transfer_fee():
    fee_event = Tradier._normalize_history_row_to_cash_event(
        {
            "type": "transfer",
            "date": "2026-02-20",
            "amount": "-30.00",
            "transfer.description": "Annual IRA Fee",
            "transfer.quantity": "0",
            "transfer.symbol": "",
        }
    )

    assert fee_event is not None
    assert fee_event.event_type == "fee"
    assert fee_event.is_external_cash_flow is False
    assert fee_event.description == "Annual IRA Fee"
    assert fee_event.direction == "out"


def test_tradier_history_normalization_skips_zero_amount_transfers():
    event = Tradier._normalize_history_row_to_cash_event(
        {
            "id": "tradier-zero-transfer",
            "type": "transfer",
            "date": "2026-03-24",
            "amount": "0.00",
            "description": "Internal transfer placeholder",
        }
    )

    assert event is None


def test_tradier_history_normalization_uses_nested_fields_for_stable_synthetic_ids():
    base_row = {
        "type": "adjustment",
        "date": "2026-10-27",
        "amount": "-13681.66",
        "adjustment.description": "TFR TO TYPE 1",
        "adjustment.quantity": "1",
        "adjustment.symbol": "ABC",
    }

    event_one = Tradier._normalize_history_row_to_cash_event(base_row)
    event_two = Tradier._normalize_history_row_to_cash_event(dict(base_row))
    event_three = Tradier._normalize_history_row_to_cash_event(
        {**base_row, "adjustment.symbol": "XYZ"}
    )

    assert event_one is not None
    assert event_two is not None
    assert event_three is not None
    assert event_one.event_id == event_two.event_id
    assert event_one.event_id != event_three.event_id
    assert event_one.description == "TFR TO TYPE 1"


def test_tradier_get_cash_events_paginates_per_activity_type():
    requests = []

    class _DummyAccount:
        def get_history(self, **kwargs):
            requests.append(dict(kwargs))
            if kwargs["activity_type"] != "ach":
                return None

            import pandas as pd

            if kwargs.get("page") == 1:
                return pd.DataFrame(
                    [
                        {
                            "id": f"tradier-deposit-{index}",
                            "type": "ach",
                            "date": "2026-03-24",
                            "amount": "1000.00",
                            "description": "Bank ACH",
                        }
                        for index in range(1000)
                    ]
                )

            if kwargs.get("page") == 2:
                return pd.DataFrame(
                    [
                        {
                            "id": "tradier-deposit-last",
                            "type": "ach",
                            "date": "2026-03-23",
                            "amount": "500.00",
                            "description": "Bank ACH",
                        }
                    ]
                )

            return pd.DataFrame()

    broker = Tradier.__new__(Tradier)
    broker.tradier = SimpleNamespace(account=_DummyAccount())

    events = broker.get_cash_events(limit=1500)

    ach_requests = [req for req in requests if req["activity_type"] == "ach"]
    assert ach_requests[0]["limit"] == 1000
    assert ach_requests[0]["page"] == 1
    assert ach_requests[1]["page"] == 2
    assert len(events) == 1001
    assert events[0].event_type == "deposit"


def _cloud_update_dummy(get_cash_events):
    return SimpleNamespace(
        is_backtesting=False,
        lumiwealth_api_key="test_key_123",
        _logged_missing_lumiwealth_api_key=False,
        _name="DummyStrategy",
        broker=SimpleNamespace(name="DummyBroker", get_cash_events=get_cash_events),
        logger=logging.getLogger("tests.cash_events.cloud"),
        _cash_event_poll_lookback_days=7,
        _cash_event_poll_interval_seconds=0,
        _cash_event_cloud_emit_limit=50,
        _cash_event_fetch_limit=100,
        _cash_event_dedupe_capacity=1000,
        _cash_event_last_poll_at=None,
        _cash_event_pending_for_cloud=[],
        _cash_event_sent_ids=set(),
        _cash_event_sent_id_order=deque(),
        get_portfolio_value=lambda: 100.0,
        get_cash=lambda: 80.0,
        get_positions=lambda: [],
        get_orders=lambda: [],
    )


class _Response:
    def __init__(self, status_code: int = 200, text: str = "ok"):
        self.status_code = status_code
        self.text = text
        self.headers = {}


def test_send_update_to_cloud_includes_cash_events_and_dedupes_on_success(monkeypatch):
    event = CashEvent(
        broker_name="alpaca",
        broker_event_id="evt-1",
        event_type="deposit",
        raw_type="CSD",
        amount=250.0,
        occurred_at="2026-03-25",
        description="Deposit",
        is_external_cash_flow=True,
    )
    payloads: list[dict] = []

    def capture_post(*_args, **kwargs):
        payloads.append(json.loads(kwargs["data"]))
        return _Response(200)

    dummy = _cloud_update_dummy(lambda **_kwargs: [event])
    monkeypatch.setattr(requests, "post", capture_post)

    assert _Strategy.send_update_to_cloud(dummy) is True
    assert payloads[0]["cash_events"][0]["event_id"] == "alpaca:evt-1"

    assert _Strategy.send_update_to_cloud(dummy) is True
    assert payloads[1]["cash_events"] == []


def test_send_update_to_cloud_retries_pending_cash_events_after_failure(monkeypatch):
    event = CashEvent(
        broker_name="alpaca",
        broker_event_id="evt-2",
        event_type="withdrawal",
        raw_type="CSW",
        amount=-100.0,
        occurred_at="2026-03-25",
        description="Withdrawal",
        is_external_cash_flow=True,
    )
    payloads: list[dict] = []
    attempts = {"count": 0}

    def flaky_post(*_args, **kwargs):
        payloads.append(json.loads(kwargs["data"]))
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise requests.exceptions.ConnectionError("boom")
        return _Response(200)

    dummy = _cloud_update_dummy(lambda **_kwargs: [event])
    monkeypatch.setattr(requests, "post", flaky_post)

    assert _Strategy.send_update_to_cloud(dummy) is False
    assert payloads[0]["cash_events"][0]["event_id"] == "alpaca:evt-2"

    assert _Strategy.send_update_to_cloud(dummy) is True
    assert payloads[1]["cash_events"][0]["event_id"] == "alpaca:evt-2"
    assert dummy._cash_event_pending_for_cloud == []
