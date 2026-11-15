"""Trade artifacts preserve authoritative derivative units without fetching metadata."""

import csv
import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.backtesting import BacktestingBroker
from lumibot.brokers.broker import TRADE_EVENT_LOG_COLUMNS
from lumibot.entities import Asset, CashEvent, ContractSpec, Order


LEGACY_COLUMNS = (
    "time", "strategy", "exchange", "identifier", "symbol", "side", "type", "status",
    "price", "filled_quantity", "multiplier", "trade_cost", "trade_slippage", "time_in_force",
    "asset.right", "asset.strike", "asset.multiplier", "asset.expiration", "asset.asset_type",
    "price_source", "event_kind", "event_id", "cash_event_type", "cash_event_amount",
    "cash_event_currency", "cash_event_description", "cash_event_direction", "cash_event_reason",
    "is_external_cash_flow", "cash_event_raw_type", "cash_event_raw_subtype",
    "cash_event_broker_name", "cash_event_broker_event_id",
)
CONTRACT_COLUMN = "asset.contract_spec"
NOW = datetime(2026, 3, 3, 12, tzinfo=timezone.utc)


def _broker(audit):
    broker = BacktestingBroker.__new__(BacktestingBroker)
    broker.name = "backtesting"
    broker.logger = Mock()
    broker.data_source = SimpleNamespace(_datetime=NOW)
    broker._backtest_audit_enabled = audit
    broker._trade_event_log_enabled = True
    broker._trade_event_log_rows = []
    broker._trade_event_log_columns = TRADE_EVENT_LOG_COLUMNS
    broker._trade_event_log_df_cache = None
    return broker


def _order(asset):
    order = Order("neutral", asset, 3, "buy")
    order.identifier = "neutral-order"
    order.status = "fill"
    order._price_source = "recorded"
    return order


def _csv_rows(broker, tmp_path):
    path = tmp_path / "trade_events.csv"
    broker.export_trade_events_to_csv(str(path))
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return reader.fieldnames, rows


@pytest.mark.parametrize("audit", [False, True], ids=["compact", "audit"])
@pytest.mark.parametrize(
    "spec,quantity,price,quote_exposure",
    [
        (ContractSpec("ABC", "USD", "ABC", Decimal("100"), True), 3, 20000, Decimal("300")),
        (ContractSpec("ABC", "USDT", "USDT", Decimal("0.01")), 3, 20000, Decimal("600")),
        (ContractSpec("ABC", "USDT", "USDT", Decimal("1"), quantity_unit="base"),
         Decimal("0.03"), 20000, Decimal("600")),
    ],
    ids=["inverse-contracts", "linear-contracts", "legacy-base-units"],
)
def test_trade_event_csv_preserves_contract_economics(
    tmp_path, audit, spec, quantity, price, quote_exposure
):
    broker = _broker(audit)
    asset = Asset("ABC-PERP", asset_type="crypto_future", contract_spec=spec)
    order = _order(asset)
    order.quantity = quantity
    broker._record_trade_event_row(order, price, quantity, multiplier=asset.multiplier)

    columns, rows = _csv_rows(broker, tmp_path)
    assert tuple(columns) == LEGACY_COLUMNS + (CONTRACT_COLUMN,)
    assert len(rows) == 1
    restored = ContractSpec.from_dict(json.loads(rows[0][CONTRACT_COLUMN]))
    assert restored == spec
    assert restored.exposure(Decimal(rows[0]["filled_quantity"]), rows[0]["price"])["quote"] == quote_exposure
    assert rows[0]["status"] == "fill"
    assert order.asset is asset and order.quantity == quantity and order.status == "fill"


@pytest.mark.parametrize("audit", [False, True], ids=["compact", "audit"])
def test_trade_event_csv_noncontract_rows_keep_schema_and_values(tmp_path, audit):
    broker = _broker(audit)
    asset = Asset("EQT", asset_type="stock")
    broker._record_trade_event_row(_order(asset), 25, 3)
    broker.record_cash_event(CashEvent(
        broker_name="recorded", event_type="interest", amount=-2, occurred_at=NOW,
        event_id="interest-1", currency="USD", direction="out",
    ), strategy="neutral")
    broker.record_corporate_action_event(
        action_type="stock_split", asset=asset, strategy="neutral", ratio=2,
        quantity_before=3, quantity_after=6, occurred_at=NOW,
    )

    columns, rows = _csv_rows(broker, tmp_path)
    assert tuple(columns) == LEGACY_COLUMNS + (CONTRACT_COLUMN,)
    assert [row["event_kind"] for row in rows] == ["trade", "cash_event", "corporate_action"]
    assert all(row[CONTRACT_COLUMN] == "" for row in rows)
    assert rows[0]["price"] == "25.0" and rows[0]["filled_quantity"] == "3.0"
    assert rows[1]["cash_event_amount"] == "-2.0" and rows[1]["cash_event_currency"] == "USD"
    assert rows[2]["status"] == "stock_split" and rows[2]["filled_quantity"] == "6.0"


@pytest.mark.parametrize("audit", [False, True], ids=["compact", "audit"])
def test_resolved_contract_terms_are_retained_without_logging_fetch(tmp_path, audit):
    broker = _broker(audit)
    asset = Asset("ABC-INVERSE", asset_type="crypto_future")
    spec = ContractSpec("ABC", "USD", "ABC", Decimal("100"), True)
    getter = Mock(return_value=spec)
    broker.data_source.get_contract_spec = getter
    original_hash = hash(asset)

    assert broker.get_contract_spec(asset) is spec
    assert asset.contract_spec is None
    assert asset._resolved_contract_spec is spec
    assert hash(asset) == original_hash
    broker._record_trade_event_row(_order(asset), 20000, 3)
    _, rows = _csv_rows(broker, tmp_path)
    assert ContractSpec.from_dict(json.loads(rows[0][CONTRACT_COLUMN])) == spec
    getter.assert_called_once_with(asset, None)
    # Retained provenance must not cache the first quote or change resolver behavior.
    other_quote = Asset("USD", asset_type="forex")
    assert broker.get_contract_spec(asset, other_quote) is spec
    assert getter.call_count == 2
    getter.assert_called_with(asset, other_quote)


@pytest.mark.parametrize("audit", [False, True], ids=["compact", "audit"])
def test_unresolved_contract_terms_stay_unknown_without_logging_fetch(tmp_path, audit):
    broker = _broker(audit)
    broker.data_source.get_contract_spec = Mock(side_effect=AssertionError("logging must not fetch"))
    asset = Asset("ABC-UNRESOLVED", asset_type="crypto_future")
    broker._record_trade_event_row(_order(asset), 20000, 3)
    _, rows = _csv_rows(broker, tmp_path)
    assert rows[0][CONTRACT_COLUMN] == ""
    broker.data_source.get_contract_spec.assert_not_called()


def test_empty_trade_event_csv_includes_optional_contract_column(tmp_path):
    columns, rows = _csv_rows(_broker(False), tmp_path)
    assert tuple(columns) == LEGACY_COLUMNS + (CONTRACT_COLUMN,)
    assert rows == []
