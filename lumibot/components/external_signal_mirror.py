"""Execute validated external option signals through a LumiBot strategy."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset


class ExternalSignalMirrorError(ValueError):
    """Raised when a normalized signal batch violates the runtime contract."""


class ExternalSignalMirror:
    """Mirror an immutable batch without interpreting prose or changing intent.

    The control plane validates email and CSV input. This component performs a
    second structural validation, retrieves broker state through the owning
    LumiBot strategy, and submits exact option contracts with SMART_LIMIT.
    """

    def __init__(
        self,
        strategy,
        *,
        batch: dict[str, Any] | None = None,
        s3_client=None,
        smart_limit_config: SmartLimitConfig | None = None,
        max_spread_pct: float | None = None,
        audit_path: str | Path | None = None,
    ) -> None:
        self.strategy = strategy
        self.s3_client = s3_client
        self.smart_limit_config = smart_limit_config or SmartLimitConfig(
            preset=SmartLimitPreset.NORMAL,
            final_price_pct=1.0,
        )
        self.max_spread_pct = max_spread_pct
        self.options = OptionsHelper(strategy)
        self.batch = batch
        self.audit_path = Path(audit_path) if audit_path else self._default_audit_path()
        self.order_context: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _default_audit_path() -> Path:
        directory = Path(os.environ.get("BOTSPOT_ARTIFACT_LOCAL_DIR") or "/app/.lumibot-cache/agent_runtime")
        return directory / "external_signal_audit.jsonl"

    def _load_batch_from_s3(self) -> dict[str, Any]:
        bucket = str(os.environ.get("BOTSPOT_EXTERNAL_SIGNAL_BATCH_S3_BUCKET") or "").strip()
        key = str(os.environ.get("BOTSPOT_EXTERNAL_SIGNAL_BATCH_S3_KEY") or "").strip()
        expected_sha = str(os.environ.get("BOTSPOT_EXTERNAL_SIGNAL_BATCH_SHA256") or "").strip().lower()
        if not bucket or not key or not expected_sha:
            raise ExternalSignalMirrorError("External signal batch location is not configured")
        if self.s3_client is None:
            import boto3

            self.s3_client = boto3.client("s3")
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        body = response.get("Body")
        raw = body.read() if hasattr(body, "read") else body
        if not isinstance(raw, bytes):
            raw = bytes(raw or b"")
        actual_sha = hashlib.sha256(raw).hexdigest()
        if actual_sha != expected_sha:
            raise ExternalSignalMirrorError("External signal batch SHA-256 does not match")
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ExternalSignalMirrorError("External signal batch is not valid JSON") from exc

    def load_batch(self) -> dict[str, Any]:
        batch = self.batch if self.batch is not None else self._load_batch_from_s3()
        if not isinstance(batch, dict):
            raise ExternalSignalMirrorError("External signal batch must be an object")
        if not batch.get("batchId") or not batch.get("contentSha256"):
            raise ExternalSignalMirrorError("External signal batch identity is incomplete")
        if not isinstance(batch.get("records"), list) or not batch["records"]:
            raise ExternalSignalMirrorError("External signal batch has no records")
        self.batch = batch
        return batch

    @staticmethod
    def _option_asset(record: dict[str, Any]) -> Asset:
        try:
            expiration = date.fromisoformat(str(record["expiration"]))
            strike = Decimal(str(record["strike"]))
        except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
            raise ExternalSignalMirrorError("Signal option contract is incomplete") from exc
        right = str(record.get("optionType") or "").upper()
        if right not in {"CALL", "PUT"}:
            raise ExternalSignalMirrorError("Signal optionType must be CALL or PUT")
        symbol = str(record.get("symbol") or "").strip().upper()
        if not symbol:
            raise ExternalSignalMirrorError("Signal symbol is required")
        return Asset(
            symbol=symbol,
            asset_type=Asset.AssetType.OPTION,
            expiration=expiration,
            strike=float(strike),
            right=Asset.OptionRight.CALL if right == "CALL" else Asset.OptionRight.PUT,
        )

    @staticmethod
    def _quantity(record: dict[str, Any]) -> int:
        quantity = record.get("quantity")
        if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
            raise ExternalSignalMirrorError("Signal quantity must be a positive whole number")
        return quantity

    @staticmethod
    def _order_identifier(order: Any) -> str | None:
        for field in ("identifier", "id", "broker_order_id"):
            value = getattr(order, field, None)
            if value:
                return str(value)
        return None

    def _record_audit(self, payload: dict[str, Any]) -> dict[str, Any]:
        event = {
            "batchId": self.batch.get("batchId") if self.batch else None,
            "contentSha256": self.batch.get("contentSha256") if self.batch else None,
            **payload,
        }
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        with self.audit_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True, default=str, separators=(",", ":")) + "\n")
        self.strategy.log_message(f"external_signal_audit {json.dumps(event, sort_keys=True, default=str)}")
        return event

    def execute_batch(self) -> list[dict[str, Any]]:
        batch = self.load_batch()
        positions = self.strategy.get_positions(broker_refresh=True)
        position_by_asset = {position.asset: position for position in positions}
        audits: list[dict[str, Any]] = []

        for record in batch["records"]:
            if not isinstance(record, dict):
                raise ExternalSignalMirrorError("Signal record must be an object")
            action = str(record.get("action") or "").upper()
            symbol = str(record.get("symbol") or "").upper()
            if action == "HOLD":
                audits.append(self._record_audit({
                    "rowNumber": record.get("rowNumber"),
                    "action": action,
                    "symbol": symbol,
                    "outcome": "retained",
                    "orderSubmitted": False,
                }))
                continue
            if action not in {"BUY", "SELL"}:
                raise ExternalSignalMirrorError("Signal action must be BUY, SELL, or HOLD")

            asset = self._option_asset(record)
            quantity = self._quantity(record)
            position = position_by_asset.get(asset)
            position_quantity = Decimal(str(getattr(position, "quantity", 0))) if position else Decimal("0")
            if action == "SELL" and position_quantity < quantity:
                audits.append(self._record_audit({
                    "rowNumber": record.get("rowNumber"),
                    "action": action,
                    "symbol": symbol,
                    "contract": str(asset),
                    "intendedQuantity": quantity,
                    "positionQuantityBefore": str(position_quantity),
                    "outcome": "rejected_insufficient_position",
                    "orderSubmitted": False,
                }))
                continue

            market = self.options.evaluate_option_market(asset, max_spread_pct=self.max_spread_pct)
            intended_limit = market.buy_price if action == "BUY" else market.sell_price
            if intended_limit is None or market.spread_too_wide:
                audits.append(self._record_audit({
                    "rowNumber": record.get("rowNumber"),
                    "action": action,
                    "symbol": symbol,
                    "contract": str(asset),
                    "intendedQuantity": quantity,
                    "quote": asdict(market),
                    "outcome": "rejected_unactionable_quote",
                    "orderSubmitted": False,
                }))
                continue

            side = Order.OrderSide.BUY_TO_OPEN if action == "BUY" else Order.OrderSide.SELL_TO_CLOSE
            order = self.strategy.create_order(
                asset,
                quantity,
                side,
                order_type=Order.OrderType.SMART_LIMIT,
                smart_limit=self.smart_limit_config,
            )
            try:
                submitted = self.strategy.submit_order(order)
                submitted_order = submitted[0] if isinstance(submitted, list) and submitted else submitted
                order_id = self._order_identifier(submitted_order or order)
                submitted_limit = getattr(submitted_order or order, "limit_price", None)
                if submitted_limit is None:
                    submitted_limit = intended_limit
                context = {
                    "rowNumber": record.get("rowNumber"),
                    "action": action,
                    "symbol": symbol,
                    "contract": str(asset),
                    "intendedQuantity": quantity,
                    "positionQuantityBefore": str(position_quantity),
                    "quote": asdict(market),
                    "intendedInitialLimit": intended_limit,
                    "submittedLimit": submitted_limit,
                    "smartLimit": self.smart_limit_config.to_dict(),
                    "reportedFillPrice": record.get("actualFillPrice"),
                    "referenceOptionPrice": record.get("referenceOptionPrice"),
                    "brokerOrderId": order_id,
                    "brokerStatus": str(getattr(submitted_order or order, "status", "submitted")),
                    "outcome": "submitted",
                    "orderSubmitted": True,
                }
                if order_id:
                    self.order_context[order_id] = context
                audits.append(self._record_audit(context))
            except Exception as exc:
                audits.append(self._record_audit({
                    "rowNumber": record.get("rowNumber"),
                    "action": action,
                    "symbol": symbol,
                    "contract": str(asset),
                    "intendedQuantity": quantity,
                    "intendedInitialLimit": intended_limit,
                    "submittedLimit": intended_limit,
                    "outcome": "broker_rejected",
                    "orderSubmitted": False,
                    "brokerError": str(exc)[:1000],
                }))
        return audits

    def record_order_event(
        self,
        order: Any,
        *,
        status: str,
        fill_price: Any = None,
        filled_quantity: Any = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        order_id = self._order_identifier(order)
        context = dict(self.order_context.get(order_id or "", {}))
        reported = context.get("reportedFillPrice")
        difference = None
        if fill_price is not None and reported is not None:
            try:
                difference = float(fill_price) - float(reported)
            except (TypeError, ValueError):
                difference = None
        return self._record_audit({
            **context,
            "brokerOrderId": order_id,
            "event": "order_status",
            "brokerStatus": status,
            "fillPrice": fill_price,
            "filledQuantity": filled_quantity,
            "reportedFillDifference": difference,
            "brokerError": error,
        })
