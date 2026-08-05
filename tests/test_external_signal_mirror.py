import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from lumibot.components.external_signal_mirror import (
    ExternalSignalMirror,
    ExternalSignalMirrorError,
)
from lumibot.entities import Asset, Order


class FakeStrategy:
    def __init__(self, positions=None):
        self.positions = positions or []
        self.orders = []
        self.messages = []
        self.logger = None
        self.broker = SimpleNamespace(data_source=SimpleNamespace(option_quote_fallback_allowed=False))

    def log_message(self, message, **_kwargs):
        self.messages.append(message)

    def get_datetime(self):
        return datetime(2026, 8, 5, 19, 30, tzinfo=timezone.utc)

    def get_positions(self, **_kwargs):
        return self.positions

    def get_quote(self, _asset):
        return SimpleNamespace(bid=4.8, ask=5.2, price=5.0)

    def create_order(self, asset, quantity, side, **kwargs):
        order = SimpleNamespace(
            asset=asset,
            quantity=quantity,
            side=side,
            order_type=kwargs.get("order_type"),
            smart_limit=kwargs.get("smart_limit"),
            limit_price=5.15,
            identifier=f"order-{len(self.orders) + 1}",
            status="new",
        )
        self.orders.append(order)
        return order

    def submit_order(self, order):
        order.status = "submitted"
        return order


def batch(records):
    return {
        "batchId": "batch-123",
        "contentSha256": "a" * 64,
        "receivedAt": "2026-08-05T19:20:00Z",
        "records": records,
    }


def option_record(action, symbol="QQQ", quantity=2):
    return {
        "rowNumber": 2,
        "action": action,
        "symbol": symbol,
        "expiration": "2026-08-07",
        "optionType": "CALL",
        "strike": "708",
        "quantity": quantity,
        "referenceOptionPrice": "5.99",
        "actualFillPrice": "5.10",
        "orderType": "SMART_LIMIT",
    }


def test_executes_exact_buy_and_sell_and_keeps_hold(tmp_path):
    sell_asset = Asset(
        "SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=datetime(2026, 8, 7).date(),
        strike=708,
        right=Asset.OptionRight.CALL,
    )
    position = SimpleNamespace(asset=sell_asset, quantity=3)
    strategy = FakeStrategy([position])
    records = [
        option_record("BUY"),
        option_record("SELL", symbol="SPY", quantity=1),
        {"rowNumber": 4, "action": "HOLD", "symbol": "MSFT"},
    ]

    mirror = ExternalSignalMirror(strategy, batch=batch(records), audit_path=tmp_path / "audit.jsonl")
    audits = mirror.execute_batch()

    assert len(strategy.orders) == 2
    assert strategy.orders[0].side == Order.OrderSide.BUY_TO_OPEN
    assert strategy.orders[1].side == Order.OrderSide.SELL_TO_CLOSE
    assert strategy.orders[0].quantity == 2
    assert strategy.orders[0].order_type == Order.OrderType.SMART_LIMIT
    assert audits[0]["intendedInitialLimit"] == 5.2
    assert audits[0]["submittedLimit"] == 5.15
    assert audits[1]["positionQuantityBefore"] == "3"
    assert audits[2]["outcome"] == "retained"
    assert audits[2]["orderSubmitted"] is False
    assert len((tmp_path / "audit.jsonl").read_text().splitlines()) == 3


def test_rejects_sell_larger_than_exact_position_without_submitting(tmp_path):
    asset = Asset(
        "QQQ",
        asset_type=Asset.AssetType.OPTION,
        expiration=datetime(2026, 8, 7).date(),
        strike=708,
        right=Asset.OptionRight.CALL,
    )
    strategy = FakeStrategy([SimpleNamespace(asset=asset, quantity=1)])
    mirror = ExternalSignalMirror(
        strategy,
        batch=batch([option_record("SELL", quantity=2)]),
        audit_path=tmp_path / "audit.jsonl",
    )

    audits = mirror.execute_batch()

    assert strategy.orders == []
    assert audits[0]["outcome"] == "rejected_insufficient_position"


def test_loads_only_hash_verified_batch_from_private_s3(monkeypatch, tmp_path):
    payload = json.dumps(batch([option_record("BUY")]), sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(payload).hexdigest()
    s3 = SimpleNamespace(get_object=lambda **_kwargs: {"Body": io.BytesIO(payload)})
    monkeypatch.setenv("BOTSPOT_EXTERNAL_SIGNAL_BATCH_S3_BUCKET", "private-bucket")
    monkeypatch.setenv("BOTSPOT_EXTERNAL_SIGNAL_BATCH_S3_KEY", "external-signal-runs/bot/run.json")
    monkeypatch.setenv("BOTSPOT_EXTERNAL_SIGNAL_BATCH_SHA256", digest)
    mirror = ExternalSignalMirror(FakeStrategy(), s3_client=s3, audit_path=tmp_path / "audit.jsonl")

    assert mirror.load_batch()["batchId"] == "batch-123"

    monkeypatch.setenv("BOTSPOT_EXTERNAL_SIGNAL_BATCH_SHA256", "0" * 64)
    other = ExternalSignalMirror(FakeStrategy(), s3_client=s3, audit_path=tmp_path / "audit2.jsonl")
    with pytest.raises(ExternalSignalMirrorError, match="SHA-256"):
        other.load_batch()


def test_rejects_malformed_runtime_record_before_any_order(tmp_path):
    strategy = FakeStrategy()
    mirror = ExternalSignalMirror(
        strategy,
        batch=batch([{"rowNumber": 2, "action": "BUY", "symbol": "QQQ"}]),
        audit_path=Path(tmp_path) / "audit.jsonl",
    )
    with pytest.raises(ExternalSignalMirrorError, match="contract is incomplete"):
        mirror.execute_batch()
    assert strategy.orders == []
