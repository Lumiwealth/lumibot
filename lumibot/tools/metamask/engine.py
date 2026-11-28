"""Native execution state machine: reserve, sign, submit and reconcile once."""

from __future__ import annotations

import hashlib
import time
from dataclasses import replace
from decimal import ROUND_CEILING, Context, Decimal, localcontext
from uuid import uuid4

from .journal import Journal
from .models import Binding, PerpIntent, decimal, positive_integer
from .provider import HYPERLIQUID_SIGNING_CHAIN, METAMASK_WALLET_ORIGIN, MetaMaskError, MetaMaskProvider, json_object
from .venue import HyperliquidVenue, action_typed_data, signature_parts, typed_data_hash

_TERMINAL = {"filled", "canceled", "rejected"}
_CANCELED_VENUE = {
    "canceled",
    "marginCanceled",
    "vaultWithdrawalCanceled",
    "openInterestCapCanceled",
    "selfTradeCanceled",
    "reduceOnlyCanceled",
    "siblingFilledCanceled",
    "delistedCanceled",
    "liquidatedCanceled",
    "scheduledCancel",
}
_REJECTED_VENUE = {
    "rejected",
    "tickRejected",
    "minTradeNtlRejected",
    "perpMarginRejected",
    "reduceOnlyRejected",
    "badAloPxRejected",
    "iocCancelRejected",
    "badTriggerPxRejected",
    "marketOrderNoLiquidityRejected",
    "positionIncreaseAtOpenInterestCapRejected",
    "positionFlipAtOpenInterestCapRejected",
    "tooAggressiveAtOpenInterestCapRejected",
    "openInterestIncreaseRejected",
    "insufficientSpotBalanceRejected",
    "oracleRejected",
    "perpMaxPositionRejected",
}


class ExecutionEngine:
    def __init__(self, journal: Journal, provider: MetaMaskProvider, venue: HyperliquidVenue, agent_id: str):
        self.journal, self.provider, self.venue, self.agent_id = journal, provider, venue, agent_id
        self.epoch = str(uuid4())
        self._execution = journal.execution_lock(str(journal.binding(agent_id)["wallet"]))

    def _authority(self, expected=None, *, cancellation=False) -> tuple[Binding, dict[str, object]]:
        record = self.journal.binding(self.agent_id)
        binding = Binding.parse(record, validate_registry=not cancellation)
        if record.get("projectId") is not None and record["projectId"] != self.provider.credentials().project_id:
            raise MetaMaskError("PROJECT_MISMATCH", "Journal belongs to a different provider project")
        if binding.network != self.venue.network:
            raise MetaMaskError("NETWORK_MISMATCH", "Journal network differs from the configured execution venue")
        if not cancellation and (not binding.enabled or binding.expires_at <= int(time.time() * 1000)):
            raise MetaMaskError("REVOKED", "Agent authority expired or was revoked")
        if expected is not None and expected != record:
            raise MetaMaskError("POLICY_CHANGED", "Agent authority changed after execution preparation")
        self.provider.verify_wallet(binding.wallet)
        policy = self.provider.policy(binding.wallet)
        if policy.digest != binding.policy_digest:
            raise MetaMaskError("POLICY_CHANGED", "Provider policy changed; review the binding before signing")
        return binding, record

    def _operation(self, operation_id: str) -> dict[str, object]:
        value = self.journal.get(self.agent_id, operation_id)
        if value is None:
            raise MetaMaskError("NOT_FOUND", "Requested operation does not belong to this agent")
        return value

    def _find_operation(self, target_id: str) -> tuple[dict[str, object], dict[str, object] | None]:
        direct = self.journal.get(self.agent_id, target_id)
        if direct is not None:
            return direct, None
        for row in self.journal.operations(self.agent_id):
            children = row.get("childOperations")
            if isinstance(children, list):
                for child in children:
                    if isinstance(child, dict) and child.get("id") == target_id:
                        return row, child
        raise MetaMaskError("NOT_FOUND", "Requested operation does not belong to this agent")

    @staticmethod
    def _has_live_children(operation: dict[str, object]) -> bool:
        children = operation.get("childOperations")
        if not isinstance(children, list) or not children:
            return False
        return any(
            not isinstance(child, dict)
            or child.get("state") not in _TERMINAL
            or bool(child.get("reconciliationErrorCode"))
            for child in children
        )

    @staticmethod
    def _has_reconcilable_children(operation: dict[str, object]) -> bool:
        children = operation.get("childOperations")
        if not isinstance(children, list) or not children:
            return False
        return any(
            not isinstance(child, dict)
            or child.get("state") not in {"filled", "rejected"}
            or (child.get("state") == "rejected" and child.get("providerOrderId") is not None)
            or bool(child.get("reconciliationErrorCode"))
            for child in children
        )

    @staticmethod
    def _mark_children(operation: dict[str, object], state: str, **extra) -> list[dict[str, object]] | None:
        children = operation.get("childOperations")
        if not isinstance(children, list) or not children:
            return None
        updated = []
        for child in children:
            item = dict(json_object(child, "child operation"))
            if item.get("state") not in _TERMINAL:
                item.update(state=state, **extra)
            updated.append(item)
        return updated

    @staticmethod
    def _child_view(operation: dict[str, object], child: dict[str, object]) -> dict[str, object]:
        request = json_object(operation.get("request"), "parent request")
        return {
            **child,
            "symbol": request.get("symbol"),
            "parentId": operation["id"],
            "createdAt": operation.get("createdAt"),
            "updatedAt": operation.get("updatedAt"),
            "orderClass": request.get("orderClass", "simple"),
            "request": {
                "kind": "perp",
                "id": child["id"],
                "symbol": request.get("symbol"),
                "side": child["side"],
                "quantity": child["quantity"],
                "orderType": child["orderType"],
                "tpsl": child.get("tpsl"),
                "reduceOnly": True,
                **({"limitPrice": child["limitPrice"]} if child.get("limitPrice") is not None else {}),
                **({"stopPrice": child["stopPrice"]} if child.get("stopPrice") is not None else {}),
            },
        }

    def _change(self, operation: dict[str, object], **values) -> dict[str, object]:
        updated = {**operation, **values, "updatedAt": int(time.time() * 1000)}
        self.journal.save(updated)
        return updated

    def _cloid(self, binding: Binding, operation_id: str) -> str:
        scope = "\0".join(
            (self.provider.credentials().project_id, binding.wallet, binding.network, binding.agent_id, operation_id)
        )
        return "0x" + hashlib.sha256(scope.encode()).hexdigest()[:32]

    def _initial_child_records(self, binding: Binding, intent: PerpIntent) -> list[dict[str, object]]:
        records = []
        for child in intent.children:
            records.append(
                {
                    "id": child.identifier,
                    "parentId": intent.identifier,
                    "clientOrderId": self._cloid(binding, child.identifier),
                    "side": child.side,
                    "quantity": format(child.quantity, "f"),
                    "orderType": child.kind,
                    "tpsl": child.tpsl,
                    "reduceOnly": True,
                    "limitPrice": format(child.limit_price, "f") if child.limit_price is not None else None,
                    "stopPrice": format(child.stop_price, "f") if child.stop_price is not None else None,
                    "state": "prepared",
                    "venueStatus": None,
                    "providerOrderId": None,
                    "filledQuantity": "0",
                    "averagePrice": None,
                    "fees": [],
                    "reconciliationErrorCode": None,
                    "reconciliationMessage": None,
                }
            )
        return records

    @staticmethod
    def _child_fill_step(child_id: str) -> str:
        direct = f"fills-{child_id}"
        if len(direct) <= 128 and not child_id.startswith("h-"):
            return direct
        return "fills-h-" + hashlib.sha256(child_id.encode()).hexdigest()[:40]

    def _check_id_collisions(self, intent: PerpIntent) -> None:
        candidate_ids = {intent.identifier, *(child.identifier for child in intent.children)}
        for existing_op in self.journal.operations(self.agent_id):
            if str(existing_op["id"]) in candidate_ids:
                raise MetaMaskError("ID_CONFLICT", "Order ID already belongs to different intent")
            existing_children = existing_op.get("childOperations")
            if isinstance(existing_children, list):
                for existing_child in existing_children:
                    if isinstance(existing_child, dict) and existing_child.get("id") in candidate_ids:
                        raise MetaMaskError("ID_CONFLICT", "Order ID already belongs to different intent")
            req_children = json_object(existing_op.get("request", {}), "existing request").get("children")
            if isinstance(req_children, list):
                for req_child in req_children:
                    if isinstance(req_child, dict) and req_child.get("id") in candidate_ids:
                        raise MetaMaskError("ID_CONFLICT", "Order ID already belongs to different intent")

    def submit(self, request: dict[str, object]) -> dict[str, object]:
        intent = PerpIntent.parse(request)
        canonical = intent.record()
        prior = self.journal.get(self.agent_id, intent.identifier)
        if prior is not None:
            stored_symbol = json_object(prior["request"]).get("symbol")
            if isinstance(stored_symbol, str) and stored_symbol.casefold() == intent.symbol.casefold():
                canonical = replace(intent, symbol=stored_symbol).record()
            if prior.get("request") != canonical:
                raise MetaMaskError("ID_CONFLICT", "Order ID already belongs to different intent")
            return self.order(intent.identifier)
        if not self._execution.acquire(blocking=False):
            raise MetaMaskError("EXECUTION_IN_PROGRESS", "Another wallet execution is already in progress")
        try:
            # Re-check parent idempotency and full parent/child ID uniqueness under the wallet lock.
            prior = self.journal.get(self.agent_id, intent.identifier)
            if prior is not None:
                stored_symbol = json_object(prior["request"]).get("symbol")
                if isinstance(stored_symbol, str) and stored_symbol.casefold() == intent.symbol.casefold():
                    canonical = replace(intent, symbol=stored_symbol).record()
                if prior.get("request") != canonical:
                    raise MetaMaskError("ID_CONFLICT", "Order ID already belongs to different intent")
                return self._advance(prior)
            self._check_id_collisions(intent)
            binding, snapshot = self._authority()
            self.venue.account(binding.wallet)
            if ":" in intent.symbol:
                raise MetaMaskError(
                    "DEX_EXECUTION_UNQUALIFIED", "HIP3 collateral and effective fee metadata need native qualification"
                )
            if (
                intent.symbol.casefold() not in {name.casefold() for name in binding.markets}
                or intent.leverage > binding.max_leverage
            ):
                raise MetaMaskError("MARKET_DENIED", "Market or leverage is outside the agent binding")
            market = self.venue.market(intent.symbol)
            if market.symbol.casefold() != intent.symbol.casefold():
                raise MetaMaskError("UNKNOWN_MARKET", "Venue market differs from the requested symbol")
            intent = replace(intent, symbol=market.symbol)
            canonical = intent.record()
            if intent.leverage > market.max_leverage:
                raise MetaMaskError("LEVERAGE_LIMIT", "Leverage exceeds the venue market limit")
            client_id = self._cloid(binding, intent.identifier)
            plan_legs: list[dict[str, object]] | None = None
            child_records: list[dict[str, object]] | None = None
            if intent.order_class == "simple":
                action = self.venue.order_action(
                    intent.symbol,
                    intent.side,
                    intent.quantity,
                    intent.kind,
                    intent.limit_price,
                    intent.tif,
                    intent.reduce_only,
                    client_id,
                    slippage_bps=binding.max_slippage_bps,
                    stop_price=intent.stop_price,
                    tpsl=intent.tpsl,
                )
                orders = action.get("orders")
                if not isinstance(orders, list) or len(orders) != 1:
                    raise MetaMaskError("INVALID_INTENT", "Expected one normalized venue order")
                execution_price = decimal(json_object(orders[0]).get("p"), positive=True)
                reservation_anchor = max(execution_price, intent.stop_price or Decimal(0))
            else:
                grouping = "positionTpsl" if intent.order_class == "oco" else "normalTpsl"
                venue_legs: list[dict[str, object]] = []
                plan_legs = []
                if grouping == "normalTpsl":
                    venue_legs.append(
                        {
                            "side": intent.side,
                            "quantity": intent.quantity,
                            "kind": intent.kind,
                            "limit": intent.limit_price,
                            "stop_price": None,
                            "tif": intent.tif,
                            "reduce_only": intent.reduce_only,
                            "client_id": client_id,
                            "tpsl": None,
                            "as_trigger": False,
                        }
                    )
                    plan_legs.append(
                        {
                            "role": "entry",
                            "id": intent.identifier,
                            "clientOrderId": client_id,
                            "side": intent.side,
                            "quantity": format(intent.quantity, "f"),
                            "orderType": intent.kind,
                            "tif": intent.tif,
                            "reduceOnly": intent.reduce_only,
                            "tpsl": None,
                        }
                    )
                for child in intent.children:
                    child_cloid = self._cloid(binding, child.identifier)
                    venue_legs.append(
                        {
                            "side": child.side,
                            "quantity": child.quantity,
                            "kind": child.kind,
                            "limit": child.limit_price,
                            "stop_price": child.stop_price,
                            "tif": "gtc",
                            "reduce_only": True,
                            "client_id": child_cloid,
                            "tpsl": child.tpsl,
                            "as_trigger": True,
                        }
                    )
                    plan_legs.append(
                        {
                            "role": "child",
                            "id": child.identifier,
                            "clientOrderId": child_cloid,
                            "side": child.side,
                            "quantity": format(child.quantity, "f"),
                            "orderType": child.kind,
                            "tif": "gtc",
                            "reduceOnly": True,
                            "tpsl": child.tpsl,
                        }
                    )
                action = self.venue.grouped_order_action(
                    intent.symbol, venue_legs, grouping, slippage_bps=binding.max_slippage_bps
                )
                orders = action.get("orders")
                if not isinstance(orders, list) or len(orders) != len(venue_legs):
                    raise MetaMaskError("INVALID_INTENT", "Normalized grouped venue orders count mismatch")
                if grouping == "normalTpsl":
                    execution_price = decimal(json_object(orders[0]).get("p"), positive=True)
                    reservation_anchor = execution_price
                else:
                    leg_prices = [decimal(json_object(row).get("p"), positive=True) for row in orders]
                    stop_prices = [c.stop_price for c in intent.children if c.stop_price is not None]
                    execution_price = max(leg_prices)
                    reservation_anchor = max([execution_price, *stop_prices])
                child_records = self._initial_child_records(binding, intent)
            fee_rate = self.venue.fee_rate(binding.wallet)
            with localcontext(Context(prec=160, rounding=ROUND_CEILING)):
                mark_ceiling = self.venue.price(intent.symbol) * (1 + Decimal(binding.max_slippage_bps) / 10000)
                valuation = intent.quantity * max(reservation_anchor, mark_ceiling) * (1 + fee_rate)
            operation, created = self.journal.reserve(
                self.agent_id,
                canonical,
                format(valuation, "f"),
                expected_binding=snapshot,
            )
            if not created:
                return operation
            plan_payload: dict[str, object] = {
                "action": action,
                "clientOrderId": client_id,
                "ownerEpoch": self.epoch,
                "binding": snapshot,
                "wallet": binding.wallet,
                "network": binding.network,
                "projectId": self.provider.credentials().project_id,
                "marketIndex": market.index,
                "isCross": market.is_cross,
                "origin": "lumibot",
                "feeRate": format(fee_rate, "f"),
            }
            if plan_legs is not None:
                plan_payload["legs"] = plan_legs
            self.journal.save_effect(operation, "plan", plan_payload)
            updates: dict[str, object] = {
                "executionLimitPrice": format(execution_price, "f"),
                "reservationAnchor": format(reservation_anchor, "f"),
            }
            if child_records is not None:
                updates["childOperations"] = child_records
            operation = self._change(operation, **updates)
            return self._advance(operation)
        finally:
            self._execution.release()

    def _advance(self, operation: dict[str, object], *, observation=None) -> dict[str, object]:
        try:
            return self._advance_inner(operation, observation=observation)
        except MetaMaskError as error:
            current = self._operation(str(operation["id"]))
            return self._change(current, state="unknown", lastErrorCode=error.code, message=str(error))

    def _advance_inner(self, operation: dict[str, object], *, observation=None) -> dict[str, object]:
        venue_id = operation.get("providerOrderId")
        if venue_id is not None and (not isinstance(venue_id, str) or not venue_id.isdecimal() or int(venue_id) <= 0):
            return self._change(
                operation,
                state=operation["state"] if operation["state"] in _TERMINAL else "unknown",
                reconciliationErrorCode="RECONCILIATION_REQUIRED",
                reconciliationMessage="Recorded venue order identity is malformed",
            )
        if operation["state"] == "filled" and not self._has_reconcilable_children(operation):
            return operation
        operation_id = str(operation["id"])
        plan = self.journal.effect(self.agent_id, operation_id, "plan")
        order_effect = self.journal.effect(self.agent_id, operation_id, "order")
        if venue_id is not None:
            # Accepted identity dominates an incomplete effect history on every entrypoint.
            return self._reconcile_order(operation, plan or {"clientOrderId": None}, observation=observation)
        if operation["state"] in _TERMINAL:
            leverage_effect = self.journal.effect(self.agent_id, operation_id, "leverage")
            if (
                operation["state"] == "canceled"
                and "orderRelease" not in operation
                and not (
                    leverage_effect
                    and leverage_effect.get("phase") == "venue"
                    and leverage_effect.get("state") != "confirmed"
                )
                and self._unreleased_native_plan(operation, plan, order_effect)
            ):
                # Older native cancels retain positive checkpoints but lack the newer public provenance field.
                return self._change(
                    operation,
                    orderRelease={
                        "state": "not_released",
                        "source": "native_plan_local_cancel",
                        "clientOrderId": plan["clientOrderId"],
                    },
                )
            if (
                order_effect is not None
                and order_effect.get("phase") == "venue"
                and order_effect.get("state") in {"released", "confirmed"}
            ):
                # Closed venue orders (or grouped orders with live exit legs) refresh only;
                # never resume signing or release an earlier mutation, including after restart.
                return self._reconcile_order(operation, plan or {"clientOrderId": None}, observation=observation)
            return operation
        if order_effect is not None and order_effect.get("phase") == "venue":
            return self._reconcile_order(operation, plan or {"clientOrderId": None}, observation=observation)
        fills = self.journal.effect(self.agent_id, operation_id, "fills")
        if (
            operation.get("reconciliationErrorCode")
            or decimal(operation["filledQuantity"]) > 0
            or (fills is not None and fills.get("items"))
            or operation["state"] in {"submitted", "open", "partial_fill"}
        ):
            # Missing identity cannot turn retained execution or an unresolved hold
            # into permission to sign an earlier prepared entry again.
            return self._change(
                operation,
                reconciliationErrorCode="RECONCILIATION_REQUIRED",
                reconciliationMessage="Retained execution history requires reconciliation before further actions",
            )
        if plan is None:
            return self._reconcile_legacy(operation, observation=observation)
        if plan.get("ownerEpoch") != self.epoch:
            leverage_effect = self.journal.effect(self.agent_id, operation_id, "leverage")
            if (
                leverage_effect
                and leverage_effect.get("phase") == "venue"
                and leverage_effect.get("state") != "confirmed"
            ):
                return self._reconcile_leverage(operation, plan, leverage_effect)
            # An old pending signature cannot create a new execution after restart.
            for step in ("leverage", "order"):
                effect = self.journal.effect(self.agent_id, operation_id, step)
                if effect and effect.get("phase") == "provider":
                    try:
                        job = self.provider.signature(
                            str(effect["requestId"]),
                            str(plan["wallet"]),
                            str(plan["network"]),
                            origin=str(effect["origin"]),
                        )
                    except MetaMaskError:
                        return operation
                    if not job.terminal:
                        return operation
            updates: dict[str, object] = {
                "state": "rejected",
                "message": (
                    "Interrupted signing was not released; no order replay. Earlier leverage changes may have applied."
                ),
            }
            marked_children = self._mark_children(operation, "rejected")
            if marked_children is not None:
                updates["childOperations"] = marked_children
            return self._change(operation, **updates)
        intent = PerpIntent.parse(json_object(operation["request"], "order intent"))
        if not intent.reduce_only:
            leverage = {
                "type": "updateLeverage",
                "asset": plan["marketIndex"],
                "isCross": plan["isCross"],
                "leverage": intent.leverage,
            }
            operation, ready = self._step(operation, plan, "leverage", leverage)
            if not ready:
                return operation
        operation, ready = self._step(operation, plan, "order", json_object(plan["action"], "prepared action"))
        if not ready:
            return operation
        return self._reconcile_order(operation, plan)

    def _reconcile_leverage(self, operation, plan, effect):
        expiry = positive_integer(effect.get("expiresAfter"), "leverage expiry")
        if int(time.time() * 1000) <= expiry:
            return operation
        intent = PerpIntent.parse(json_object(operation["request"]))
        observed = self.venue.leverage(str(plan["wallet"]), intent.symbol)
        # Expiry rules out a late mutation; current leverage does not prove which request set it.
        self.journal.save_effect(
            operation,
            "leverage",
            {**effect, "state": "reconciled", "observedLeverage": observed, "reconciledAt": int(time.time() * 1000)},
        )
        updates: dict[str, object] = {
            "state": "rejected",
            "message": "Expired leverage submission reconciled; no order sent. Submit a new intent to trade.",
            "observedLeverage": observed,
            "lastErrorCode": None,
        }
        marked_children = self._mark_children(operation, "rejected")
        if marked_children is not None:
            updates["childOperations"] = marked_children
        return self._change(operation, **updates)

    def _step(self, operation, plan, step, action):
        cancellation = step.startswith("cancel-")

        def change(current, **values):
            if not cancellation:
                if values.get("state") in {"rejected", "awaiting_approval"} and "childOperations" not in values:
                    marked = self._mark_children(current, str(values["state"]))
                    if marked is not None:
                        values["childOperations"] = marked
                return self._change(current, **values)
            # Never write cancellation lifecycle into the original order state.
            return self._change(
                current,
                cancelState=values.get("state"),
                cancelMessage=values.get("message", ""),
                cancelErrorCode=values.get("lastErrorCode"),
                cancelRejectionReason=values.get("rejectionReason"),
            )

        operation_id = str(operation["id"])
        effect = self.journal.effect(self.agent_id, operation_id, step)
        if effect is not None and effect.get("state") == "confirmed":
            return operation, True
        if effect is not None and effect.get("phase") == "venue":
            if step == "order":
                reconciled = self._reconcile_order(operation, plan)
                return reconciled, reconciled["state"] not in {"unknown", "prepared", "awaiting_approval"}
            if step == "leverage":
                return self._reconcile_leverage(operation, plan, effect), False
            return operation, False
        try:
            binding, _ = self._authority(plan["binding"], cancellation=step.startswith("cancel-"))
            if effect is None:
                nonce = self.journal.next_nonce()
                expiry = nonce + 300_000 if step.startswith("cancel-") else min(binding.expires_at, nonce + 300_000)
                data = action_typed_data(action, nonce, binding.network, expiry)
                effect = {
                    "requestId": str(uuid4()),
                    "wallet": binding.wallet,
                    "projectId": self.provider.credentials().project_id,
                    "network": binding.network,
                    "origin": METAMASK_WALLET_ORIGIN,
                    "clientOrigin": "lumibot",
                    "method": "eth_signTypedData_v4",
                    "chainId": HYPERLIQUID_SIGNING_CHAIN,
                    "action": action,
                    "protocolPayload": action,
                    "nonce": nonce,
                    "expiresAfter": expiry,
                    "typedData": data,
                    "typedDataHash": typed_data_hash(data),
                    "ownerEpoch": self.epoch,
                    "state": "prepared",
                    "phase": "provider",
                }
                self.journal.save_effect(operation, step, effect)
                operation = change(
                    operation, state="unknown", message="Managed signature request prepared; order not yet submitted"
                )
                effect = {**effect, "state": "requested"}
                self.journal.save_effect(operation, step, effect)
                job = self.provider.submit_signature(
                    str(effect["requestId"]), binding.wallet, binding.network, data, action
                )
            else:
                job = self.provider.signature(
                    str(effect["requestId"]), binding.wallet, binding.network, origin=str(effect["origin"])
                )
        except MetaMaskError as error:
            if error.code == "REQUEST_REJECTED":
                if effect is not None:
                    self.journal.save_effect(
                        operation, step, {**effect, "state": "rejected", "rejectionCode": error.code}
                    )
                return change(operation, state="rejected", message=str(error)), False
            if effect is None:
                return change(operation, state="rejected", message=str(error)), False
            return change(operation, state="unknown", message=str(error)), False
        if job.state in {"DENIED", "EXPIRED", "FAILED"}:
            self.journal.save_effect(
                operation,
                step,
                {**effect, "state": "rejected", "providerState": job.state, "failureCode": job.failure_code},
            )
            reason = f" ({job.failure_code})" if job.failure_code else ""
            return change(
                operation, state="rejected", message=f"Managed signing {job.state.lower()}{reason}; no venue order sent"
            ), False
        if job.state != "SIGNED":
            self.journal.save_effect(operation, step, {**effect, "state": "pending", "providerState": job.state})
            return change(
                operation,
                state="awaiting_approval" if job.state == "AWAITING_MFA" else "unknown",
                approvalId=job.request_id,
                signatureOnly=True,
                message=f"Managed signing {job.state.lower()}; no venue order sent",
            ), False
        if effect.get("ownerEpoch") != self.epoch:
            return change(
                operation, state="rejected", message="Interrupted signature will not be released after restart"
            ), False
        try:
            binding, _ = self._authority(plan["binding"], cancellation=step.startswith("cancel-"))
            expiry = positive_integer(effect.get("expiresAfter"), "execution expiry")
            if int(time.time() * 1000) >= expiry:
                raise MetaMaskError("EXECUTION_EXPIRED", "Frozen execution authority expired before submission")
            data = json_object(effect["typedData"], "persisted typed data")
            rebuilt = action_typed_data(action, int(effect["nonce"]), binding.network, expiry)
            if (
                effect["action"] != action
                or effect["protocolPayload"] != action
                or rebuilt != data
                or typed_data_hash(data) != effect["typedDataHash"]
            ):
                raise MetaMaskError("INTENT_CHANGED", "Persisted signing payload differs from the frozen action")
            if not step.startswith("cancel-"):
                # Account abstraction can change while managed approval is pending.
                self.venue.account(binding.wallet)
                intent = PerpIntent.parse(json_object(operation["request"]))
                execution_limit = decimal(operation["executionLimitPrice"], positive=True)
                stored_anchor = operation.get("reservationAnchor")
                if stored_anchor is not None:
                    reservation_anchor = decimal(stored_anchor, positive=True)
                else:
                    # Legacy pre-anchor operations: reconstruct the same reservation basis from
                    # the persisted intent and executionLimitPrice (triggers/OCO include stops).
                    stop_prices = [
                        price
                        for price in (
                            intent.stop_price if intent.order_class == "simple" else None,
                            *(c.stop_price for c in intent.children if intent.order_class == "oco"),
                        )
                        if price is not None
                    ]
                    reservation_anchor = max([execution_limit, *stop_prices])
                with localcontext(Context(prec=160, rounding=ROUND_CEILING)):
                    fresh_usd = (
                        intent.quantity
                        * max(self.venue.price(intent.symbol), reservation_anchor)
                        * (1 + max(decimal(plan["feeRate"]), self.venue.fee_rate(binding.wallet)))
                    )
                if fresh_usd > decimal(operation["reservedUsd"]):
                    raise MetaMaskError(
                        "REQUOTE_REQUIRED", "Current pre-trade valuation exceeds the original reservation"
                    )
            if job.signature is None:
                raise MetaMaskError("INVALID_SIGNATURE", "Managed signing completed without a usable signature")
            signature = signature_parts(data, job.signature, binding.wallet)
            self._authority(plan["binding"], cancellation=cancellation)
            with self.journal.submission(self.agent_id, plan["binding"], cancellation=cancellation):
                if int(time.time() * 1000) >= expiry:
                    raise MetaMaskError("EXECUTION_EXPIRED", "Frozen execution authority expired before submission")
                effect = {**effect, "phase": "venue", "state": "released", "releasedAt": int(time.time() * 1000)}
                self.journal.save_effect(operation, step, effect)
                operation = change(
                    operation,
                    state="unknown",
                    signatureOnly=False,
                    message="Signature released; reconciling venue acceptance before another execution",
                )
                try:
                    response = self.venue.submit_action(
                        action, int(effect["nonce"]), signature, int(effect["expiresAfter"])
                    )
                except (MetaMaskError, ValueError, TypeError):
                    return operation, False
        except MetaMaskError as error:
            self.journal.save_effect(operation, step, {**effect, "state": "rejected", "failureCode": error.code})
            return change(operation, state="rejected", message=str(error)), False

        def reject(reason):
            if not isinstance(reason, str) or not 0 < len(reason) <= 2000:
                raise MetaMaskError("INVALID_RESPONSE", "Venue rejection reason is malformed")
            self.journal.save_effect(operation, step, {**effect, "state": "rejected", "response": response})
            return change(
                operation,
                state="rejected",
                lastErrorCode="VENUE_REJECTED",
                rejectionReason=reason,
                message=f"Venue rejected {action['type']}: {reason}",
            ), False

        if response.get("status") == "err":
            reason = response.get("response")
            return reject(reason if isinstance(reason, str) else "Venue explicitly rejected the action")
        if response.get("status") != "ok":
            return operation, False
        body = response.get("response")
        if step == "leverage":
            if not isinstance(body, dict) or body.get("type") != "default":
                return operation, False
        elif step == "order":
            if not isinstance(body, dict) or body.get("type") != "order":
                return operation, False
            data_body = json_object(json_object(body).get("data"), "order action result")
            statuses = data_body.get("statuses")
            expected_orders = json_object(plan.get("action"), "prepared action").get("orders")
            expected_count = len(expected_orders) if isinstance(expected_orders, list) else 1
            if (
                isinstance(statuses, list)
                and len(statuses) == 1
                and isinstance(statuses[0], dict)
                and "error" in statuses[0]
            ):
                return reject(statuses[0]["error"])
            if not isinstance(statuses, list) or len(statuses) != expected_count:
                return operation, False
            if expected_count == 1:
                raw_status = statuses[0]
                if raw_status == "waitingForTrigger":
                    operation = change(
                        operation,
                        state="submitted",
                        message="Venue accepted trigger order; reconciling order ID",
                    )
                else:
                    status = json_object(raw_status)
                    if "error" in status:
                        return reject(status["error"])
                    accepted = status.get("resting") or status.get("filled")
                    if not isinstance(accepted, dict):
                        return operation, False
                    venue_id = positive_integer(accepted.get("oid"), "venue order ID")
                    operation = change(
                        operation,
                        state="submitted",
                        providerOrderId=str(venue_id),
                        message="Venue accepted order; fills require reconciliation",
                    )
            else:
                plan_legs = plan.get("legs")
                if not isinstance(plan_legs, list) or len(plan_legs) != expected_count:
                    return operation, False
                grouping = json_object(plan.get("action")).get("grouping")
                existing_children = {
                    str(c["id"]): dict(json_object(c, "child operation"))
                    for c in (operation.get("childOperations") or [])
                    if isinstance(c, dict) and c.get("id")
                }
                entry_oid: str | None = None
                entry_error: str | None = None
                any_accepted = False
                first_error: str | None = None
                for leg_meta, raw_status in zip(plan_legs, statuses):
                    leg_info = json_object(leg_meta, "plan leg")
                    role = leg_info.get("role")
                    leg_id = str(leg_info["id"])
                    if raw_status == "waitingForFill":
                        any_accepted = True
                        if role == "child" and leg_id in existing_children:
                            existing_children[leg_id]["state"] = "waiting_for_fill"
                    elif raw_status == "waitingForTrigger":
                        any_accepted = True
                        if role == "child" and leg_id in existing_children:
                            existing_children[leg_id]["state"] = "submitted"
                    elif isinstance(raw_status, dict) and "error" in raw_status:
                        err_msg = str(raw_status["error"])
                        if first_error is None:
                            first_error = err_msg
                        if role == "entry":
                            entry_error = err_msg
                        elif leg_id in existing_children:
                            existing_children[leg_id]["state"] = "rejected"
                            existing_children[leg_id]["rejectionReason"] = err_msg
                    elif isinstance(raw_status, dict):
                        accepted = raw_status.get("resting") or raw_status.get("filled")
                        if not isinstance(accepted, dict):
                            return operation, False
                        oid = str(positive_integer(accepted.get("oid"), "venue order ID"))
                        any_accepted = True
                        if role == "entry":
                            entry_oid = oid
                        elif leg_id in existing_children:
                            existing_children[leg_id]["state"] = "submitted"
                            existing_children[leg_id]["providerOrderId"] = oid
                    else:
                        return operation, False
                if (grouping == "normalTpsl" and entry_error is not None and entry_oid is None) or (
                    not any_accepted and first_error is not None
                ):
                    return reject(entry_error or first_error or "Venue rejected grouped order")
                ordered_children = [
                    existing_children[str(c["id"])]
                    for c in (operation.get("childOperations") or [])
                    if isinstance(c, dict) and str(c["id"]) in existing_children
                ]
                updates: dict[str, object] = {
                    "state": "submitted",
                    "childOperations": ordered_children,
                    "message": "Venue accepted grouped order; fills require reconciliation",
                }
                if entry_oid is not None:
                    updates["providerOrderId"] = entry_oid
                operation = change(operation, **updates)
        else:
            if not isinstance(body, dict) or body.get("type") != "cancel":
                return operation, False
            data_body = json_object(json_object(body).get("data"), "cancel action result")
            cancels = action.get("cancels")
            expected_cancels = len(cancels) if isinstance(cancels, list) else 1
            statuses = data_body.get("statuses")
            if statuses != ["success"] * expected_cancels:
                if (
                    isinstance(statuses, list)
                    and len(statuses) == expected_cancels
                    and all(isinstance(s, dict) and "error" in s for s in statuses)
                ):
                    return reject(statuses[0]["error"])
                if not (
                    isinstance(statuses, list)
                    and len(statuses) == expected_cancels
                    and any(s == "success" for s in statuses)
                    and all(s == "success" or (isinstance(s, dict) and "error" in s) for s in statuses)
                ):
                    return operation, False
        self.journal.save_effect(operation, step, {**effect, "state": "confirmed", "response": response})
        return operation, True

    def _reconcile_order(self, operation, plan, *, observation=None):
        try:
            with localcontext(Context(prec=160)):
                return self._read_order(operation, plan, observation=observation)
        except MetaMaskError as error:
            return self._change(
                operation,
                state=operation["state"] if operation["state"] in _TERMINAL else "unknown",
                lastErrorCode=error.code,
                reconciliationErrorCode=error.code,
                reconciliationMessage=str(error),
                message=str(error),
            )

    def _reconcile_leg(
        self,
        binding: Binding,
        operation: dict[str, object],
        plan: dict[str, object],
        *,
        symbol: str,
        side: str,
        orig_quantity: Decimal,
        reduce_only: bool,
        leg_kind: str,
        leg_tif: str,
        cloid: str | None,
        provider_order_id: str | None,
        current_state: str,
        current_filled: Decimal,
        fill_step: str,
        wire_index: int | None,
        observation: dict[str, object] | None,
        allow_waiting_for_fill: bool = False,
        parent_canceled_unfilled: bool = False,
        resized_orig_quantity: Decimal | None = None,
    ) -> dict[str, object]:
        reference: int | str | None = (
            int(provider_order_id) if isinstance(provider_order_id, str) and provider_order_id.isdecimal() else cloid
        )
        result = self.venue.order_status(binding.wallet, reference)
        if result.get("status") != "order":
            if parent_canceled_unfilled and current_filled == 0:
                return {
                    "state": "canceled",
                    "venueStatus": "parentCanceled",
                    "providerOrderId": provider_order_id,
                    "filledQuantity": "0",
                    "averagePrice": None,
                    "fees": [],
                    "reconciliationErrorCode": None,
                    "reconciliationMessage": None,
                }
            if allow_waiting_for_fill and provider_order_id is None and current_filled == 0:
                return {
                    "state": "waiting_for_fill",
                    "venueStatus": "waitingForFill",
                    "providerOrderId": None,
                    "filledQuantity": "0",
                    "averagePrice": None,
                    "fees": [],
                    "reconciliationErrorCode": None,
                    "reconciliationMessage": None,
                }
            raise MetaMaskError("ORDER_UNRESOLVED", "Venue has not resolved the original order; no retransmission")
        status = json_object(result.get("order"), "venue order status")
        order = json_object(status.get("order"), "venue order")
        venue_id = positive_integer(order.get("oid"), "venue order ID")
        venue_orig_sz = decimal(order.get("origSz"), positive=True)
        expected_orig_qty = (
            resized_orig_quantity
            if resized_orig_quantity is not None and venue_orig_sz == resized_orig_quantity
            else orig_quantity
        )
        if (
            (isinstance(reference, int) and venue_id != reference)
            or order.get("coin") != symbol
            or order.get("side") != ("B" if side == "buy" else "A")
            or venue_orig_sz != expected_orig_qty
            or (cloid is not None and order.get("cloid") != cloid)
            or order.get("reduceOnly") is not reduce_only
        ):
            raise MetaMaskError("ORDER_IDENTITY_MISMATCH", "Venue order differs from the persisted wallet intent")
        raw_state = status.get("status")
        if raw_state in _CANCELED_VENUE:
            state = "canceled"
        elif raw_state in _REJECTED_VENUE:
            state = "rejected"
        elif raw_state == "waitingForTrigger":
            state = "open"
        elif raw_state == "triggered":
            state = "triggered"
        elif raw_state == "waitingForFill" and allow_waiting_for_fill:
            return {
                "state": "canceled" if parent_canceled_unfilled else "waiting_for_fill",
                "venueStatus": raw_state,
                "providerOrderId": str(venue_id),
                "filledQuantity": "0",
                "averagePrice": None,
                "fees": [],
                "reconciliationErrorCode": None,
                "reconciliationMessage": None,
            }
        else:
            state = raw_state
        if state not in {"open", "triggered", "filled", "canceled", "rejected"}:
            raise MetaMaskError("ORDER_STATUS_UNSUPPORTED", "Venue terminal status requires explicit reconciliation")
        in_transit_trigger = False
        if state in {"open", "triggered"}:
            # orderStatus retains the size from its last lifecycle transition. Actual
            # partial-fill remainders come from the current live order snapshot.
            live_rows = observation["openOrders"] if observation is not None else None
            if live_rows is None:
                live_rows = self.venue.frontend_open_orders(binding.wallet)
            matches = [row for row in live_rows if row.get("oid") == venue_id]
            if len(matches) == 1:
                live = matches[0]
                live_orig_sz = decimal(live.get("origSz"), positive=True)
                if (
                    positive_integer(live.get("oid"), "live venue order ID") != venue_id
                    or live.get("coin") != symbol
                    or live.get("side") != order.get("side")
                    or live_orig_sz != expected_orig_qty
                    or (cloid is not None and live.get("cloid") is not None and live.get("cloid") != cloid)
                    or live.get("reduceOnly") is not reduce_only
                ):
                    raise MetaMaskError(
                        "ORDER_IDENTITY_MISMATCH", "Live order differs from the persisted wallet intent"
                    )
                remaining = decimal(live.get("sz"), positive=True)
                state = "open"
            elif state == "triggered" and not matches:
                # A triggered market/IOC stop can leave frontendOpenOrders before orderStatus settles.
                in_transit_trigger = True
                remaining = Decimal(0)
            else:
                raise MetaMaskError("OPEN_ORDER_UNRESOLVED", "Live order snapshot is absent or ambiguous")
        else:
            remaining = Decimal(0) if state == "filled" else decimal(order.get("sz"))
        if not in_transit_trigger and not 0 <= remaining <= expected_orig_qty:
            raise MetaMaskError("ORDER_SIZE_MISMATCH", "Venue remaining size is outside the original order quantity")
        prior_fills = self.journal.effect(self.agent_id, str(operation["id"]), fill_step)
        cached = prior_fills.get("items", []) if prior_fills else []
        if not isinstance(cached, list):
            raise MetaMaskError("INVALID_STATE", "Persisted fill ledger is malformed")
        rows = {json_object(row)["tid"]: json_object(row) for row in cached}
        start = max(0, int(operation["createdAt"]) - 5000)
        observed_fills = (
            observation["fills"]
            if observation is not None
            else self.venue.fills(binding.wallet, start, int(time.time() * 1000))
        )
        for item in observed_fills:
            if item.get("oid") != venue_id:
                continue
            if item.get("coin") != symbol or item.get("side") != ("B" if side == "buy" else "A"):
                raise MetaMaskError("FILL_IDENTITY_MISMATCH", "Venue fill differs from the original order")
            identity = item["tid"]
            if identity in rows and rows[identity] != item:
                raise MetaMaskError("FILL_REGRESSION", "Venue changed an already recorded fill")
            rows[identity] = item
        quantity, value = Decimal(0), Decimal(0)
        fees: dict[str, Decimal] = {}
        for item in rows.values():
            size, price = decimal(item.get("sz"), positive=True), decimal(item.get("px"), positive=True)
            quantity += size
            value += size * price
            token = item.get("feeToken")
            if not isinstance(token, str) or not token:
                raise MetaMaskError("INVALID_RESPONSE", "Fill fee currency is missing")
            fees[token] = fees.get(token, Decimal(0)) + decimal(item.get("fee"))
        if in_transit_trigger:
            if quantity > expected_orig_qty or quantity < current_filled:
                raise MetaMaskError(
                    "FILL_HISTORY_INCOMPLETE", "Venue order and retained fill history disagree; preserving known fills"
                )
            self.journal.save_effect(
                operation,
                fill_step,
                {"wallet": binding.wallet, "network": binding.network, "items": list(rows.values())},
            )
            out: dict[str, object] = {
                "state": "partial_fill" if quantity > 0 else "submitted",
                "venueStatus": raw_state,
                "providerOrderId": str(venue_id),
                "filledQuantity": format(quantity, "f"),
                "fees": [{"asset": token, "amount": format(amount, "f")} for token, amount in sorted(fees.items())],
                "reconciliationErrorCode": None,
                "reconciliationMessage": None,
            }
            if quantity > 0:
                out["averagePrice"] = format(value / quantity, "f")
            return out
        if (
            raw_state == "filled"
            and 0 < quantity < expected_orig_qty
            and (leg_tif == "ioc" or leg_kind == "market")
            and order.get("tif") == "Ioc"
            and wire_index is not None
        ):
            orders = json_object(plan.get("action"), "persisted order action").get("orders")
            if isinstance(orders, list) and 0 <= wire_index < len(orders):
                wire_type = json_object(
                    json_object(orders[wire_index], "persisted order").get("t"), "persisted order type"
                )
                terminal_remaining = decimal(order.get("sz"))
                if (
                    wire_type.get("limit") == {"tif": "Ioc"}
                    and 0 < terminal_remaining < expected_orig_qty
                    and quantity + terminal_remaining == expected_orig_qty
                ):
                    # IOC reports terminal "filled" even when its unfilled remainder was canceled.
                    state, remaining = "canceled", terminal_remaining
        if current_state in _TERMINAL and state != current_state:
            raise MetaMaskError("TERMINAL_STATUS_CONFLICT", "A later venue snapshot cannot change the closed lifecycle")
        self.journal.save_effect(
            operation, fill_step, {"wallet": binding.wallet, "network": binding.network, "items": list(rows.values())}
        )
        expected = expected_orig_qty - remaining
        if quantity != expected or quantity > expected_orig_qty or quantity < current_filled:
            raise MetaMaskError(
                "FILL_HISTORY_INCOMPLETE", "Venue order and retained fill history disagree; preserving known fills"
            )
        next_state = "partial_fill" if state == "open" and quantity > 0 else state
        out = {
            "state": next_state,
            "venueStatus": raw_state,
            "providerOrderId": str(venue_id),
            "filledQuantity": format(quantity, "f"),
            "fees": [{"asset": token, "amount": format(amount, "f")} for token, amount in sorted(fees.items())],
            "reconciliationErrorCode": None,
            "reconciliationMessage": None,
        }
        if quantity > 0:
            out["averagePrice"] = format(value / quantity, "f")
        return out

    def _read_order(self, operation, plan, *, observation=None):
        binding = Binding.parse(self.journal.binding(self.agent_id), validate_registry=False)
        if observation is not None and observation["wallet"] != binding.wallet:
            observation = None
        intent = PerpIntent.parse(json_object(operation["request"]))
        if intent.order_class == "simple":
            leg = self._reconcile_leg(
                binding,
                operation,
                plan,
                symbol=intent.symbol,
                side=intent.side,
                orig_quantity=intent.quantity,
                reduce_only=intent.reduce_only,
                leg_kind=intent.kind,
                leg_tif=intent.tif,
                cloid=plan.get("clientOrderId"),
                provider_order_id=operation.get("providerOrderId"),
                current_state=str(operation["state"]),
                current_filled=decimal(operation["filledQuantity"]),
                fill_step="fills",
                wire_index=0,
                observation=observation,
            )
            after = {
                **operation,
                "state": leg["state"],
                "venueStatus": leg["venueStatus"],
                "providerOrderId": leg["providerOrderId"],
                "filledQuantity": leg["filledQuantity"],
                "fees": leg["fees"],
                "updatedAt": int(time.time() * 1000),
                "lastErrorCode": None,
                "reconciliationErrorCode": None,
                "reconciliationMessage": None,
                "message": "Venue order and cumulative fills reconciled",
            }
            if "averagePrice" in leg:
                after["averagePrice"] = leg["averagePrice"]
            return self.journal.replace_if_unchanged(operation, after)

        # Grouped orders (bracket / oto / oco)
        children_raw = operation.get("childOperations")
        if not isinstance(children_raw, list) or len(children_raw) != len(intent.children):
            raise MetaMaskError("INVALID_STATE", "Persisted grouped order children are missing or malformed")
        plan_legs = plan.get("legs")
        cloid_by_id = {}
        if isinstance(plan_legs, list):
            for entry in plan_legs:
                if isinstance(entry, dict) and entry.get("id") and entry.get("clientOrderId"):
                    cloid_by_id[str(entry["id"])] = str(entry["clientOrderId"])

        # Share one venue fill/open-order fetch across parent + all children when not already provided.
        if observation is None:
            start = max(0, int(operation["createdAt"]) - 5000)
            observation = {
                "wallet": binding.wallet,
                "openOrders": self.venue.frontend_open_orders(binding.wallet),
                "fills": self.venue.fills(binding.wallet, start, int(time.time() * 1000)),
            }

        after = dict(operation)
        if intent.order_class in ("bracket", "oto"):
            # Reconcile entry leg unless it is already cleanly terminal filled.
            entry_failed = False
            if operation["state"] != "filled" or operation.get("reconciliationErrorCode"):
                try:
                    entry = self._reconcile_leg(
                        binding,
                        operation,
                        plan,
                        symbol=intent.symbol,
                        side=intent.side,
                        orig_quantity=intent.quantity,
                        reduce_only=intent.reduce_only,
                        leg_kind=intent.kind,
                        leg_tif=intent.tif,
                        cloid=cloid_by_id.get(intent.identifier, plan.get("clientOrderId")),
                        provider_order_id=operation.get("providerOrderId"),
                        current_state=str(operation["state"]),
                        current_filled=decimal(operation["filledQuantity"]),
                        fill_step="fills",
                        wire_index=0,
                        observation=observation,
                    )
                    after.update(
                        state=entry["state"],
                        venueStatus=entry["venueStatus"],
                        providerOrderId=entry["providerOrderId"],
                        filledQuantity=entry["filledQuantity"],
                        fees=entry["fees"],
                        lastErrorCode=None,
                        reconciliationErrorCode=None,
                        reconciliationMessage=None,
                        message="Venue order and cumulative fills reconciled",
                    )
                    if "averagePrice" in entry:
                        after["averagePrice"] = entry["averagePrice"]
                except MetaMaskError as error:
                    entry_failed = True
                    after.update(
                        state=operation["state"] if operation["state"] in _TERMINAL else "unknown",
                        reconciliationErrorCode=error.code,
                        reconciliationMessage=str(error),
                    )
            parent_state = str(after["state"])
            parent_filled = decimal(after["filledQuantity"])
            if entry_failed:
                # Never infer child cancellation or resizing from a failed entry lookup; still
                # reconcile exits whose own venue identity and size are verifiable.
                parent_unfilled_closed = False
                allow_waiting = False
                resized_child_qty = None
            else:
                parent_unfilled_closed = parent_state in {"canceled", "rejected"} and parent_filled == 0
                allow_waiting = parent_state in {"submitted", "open", "partial_fill"} or parent_unfilled_closed
                resized_child_qty = (
                    parent_filled
                    if parent_state == "canceled" and Decimal(0) < parent_filled < intent.quantity
                    else None
                )
            updated_children = []
            for idx, (child_intent, child_raw) in enumerate(zip(intent.children, children_raw)):
                child_rec = dict(json_object(child_raw, "child operation"))
                if child_rec.get("state") == "filled" and not child_rec.get("reconciliationErrorCode"):
                    updated_children.append(child_rec)
                    continue
                if child_rec.get("state") == "rejected" and child_rec.get("providerOrderId") is None:
                    updated_children.append(child_rec)
                    continue
                try:
                    c_res = self._reconcile_leg(
                        binding,
                        operation,
                        plan,
                        symbol=intent.symbol,
                        side=child_intent.side,
                        orig_quantity=child_intent.quantity,
                        reduce_only=True,
                        leg_kind=child_intent.kind,
                        leg_tif="gtc",
                        cloid=cloid_by_id.get(child_intent.identifier, child_rec.get("clientOrderId")),
                        provider_order_id=child_rec.get("providerOrderId"),
                        current_state=str(child_rec.get("state", "prepared")),
                        current_filled=decimal(child_rec.get("filledQuantity", "0")),
                        fill_step=self._child_fill_step(child_intent.identifier),
                        wire_index=idx + 1,
                        observation=observation,
                        allow_waiting_for_fill=allow_waiting,
                        parent_canceled_unfilled=parent_unfilled_closed,
                        resized_orig_quantity=resized_child_qty,
                    )
                    child_rec.update(
                        state=c_res["state"],
                        venueStatus=c_res["venueStatus"],
                        providerOrderId=c_res["providerOrderId"],
                        filledQuantity=c_res["filledQuantity"],
                        fees=c_res["fees"],
                        reconciliationErrorCode=None,
                        reconciliationMessage=None,
                    )
                    if "averagePrice" in c_res:
                        child_rec["averagePrice"] = c_res["averagePrice"]
                except MetaMaskError as error:
                    child_rec["state"] = child_rec["state"] if child_rec.get("state") in _TERMINAL else "unknown"
                    child_rec["reconciliationErrorCode"] = error.code
                    child_rec["reconciliationMessage"] = str(error)
                updated_children.append(child_rec)
            after["childOperations"] = updated_children
            after["updatedAt"] = int(time.time() * 1000)
            return self.journal.replace_if_unchanged(operation, after)

        # OCO container (grouping == "positionTpsl"): reconcile both child legs and aggregate container state.
        updated_children = []
        child_errors: list[tuple[str, str]] = []
        for idx, (child_intent, child_raw) in enumerate(zip(intent.children, children_raw)):
            child_rec = dict(json_object(child_raw, "child operation"))
            if child_rec.get("state") == "filled" and not child_rec.get("reconciliationErrorCode"):
                updated_children.append(child_rec)
                continue
            if child_rec.get("state") == "rejected" and child_rec.get("providerOrderId") is None:
                updated_children.append(child_rec)
                continue
            try:
                c_res = self._reconcile_leg(
                    binding,
                    operation,
                    plan,
                    symbol=intent.symbol,
                    side=child_intent.side,
                    orig_quantity=child_intent.quantity,
                    reduce_only=True,
                    leg_kind=child_intent.kind,
                    leg_tif="gtc",
                    cloid=cloid_by_id.get(child_intent.identifier, child_rec.get("clientOrderId")),
                    provider_order_id=child_rec.get("providerOrderId"),
                    current_state=str(child_rec.get("state", "prepared")),
                    current_filled=decimal(child_rec.get("filledQuantity", "0")),
                    fill_step=self._child_fill_step(child_intent.identifier),
                    wire_index=idx,
                    observation=observation,
                )
                child_rec.update(
                    state=c_res["state"],
                    venueStatus=c_res["venueStatus"],
                    providerOrderId=c_res["providerOrderId"],
                    filledQuantity=c_res["filledQuantity"],
                    fees=c_res["fees"],
                    reconciliationErrorCode=None,
                    reconciliationMessage=None,
                )
                if "averagePrice" in c_res:
                    child_rec["averagePrice"] = c_res["averagePrice"]
            except MetaMaskError as error:
                child_rec["state"] = child_rec["state"] if child_rec.get("state") in _TERMINAL else "unknown"
                child_rec["reconciliationErrorCode"] = error.code
                child_rec["reconciliationMessage"] = str(error)
                child_errors.append((error.code, str(error)))
            updated_children.append(child_rec)

        states = [str(c.get("state")) for c in updated_children]
        if child_errors:
            agg_state = operation["state"] if operation["state"] in _TERMINAL else "unknown"
            err_code, err_msg = child_errors[0]
        elif "filled" in states:
            agg_state, err_code, err_msg = "filled", None, None
        elif "partial_fill" in states:
            agg_state, err_code, err_msg = "partial_fill", None, None
        elif any(s in {"open", "submitted"} for s in states):
            agg_state, err_code, err_msg = "open", None, None
        elif all(s == "rejected" for s in states):
            agg_state, err_code, err_msg = "rejected", None, None
        elif all(s in {"canceled", "rejected"} for s in states):
            agg_state, err_code, err_msg = "canceled", None, None
        else:
            agg_state, err_code, err_msg = "unknown", None, None

        after.update(
            state=agg_state,
            childOperations=updated_children,
            updatedAt=int(time.time() * 1000),
            lastErrorCode=err_code,
            reconciliationErrorCode=err_code,
            reconciliationMessage=err_msg,
            message=err_msg or "OCO child legs reconciled",
        )
        return self.journal.replace_if_unchanged(operation, after)

    def _reconcile_legacy(self, operation, *, observation=None):
        binding = Binding.parse(self.journal.binding(self.agent_id), validate_registry=False)
        if operation.get("signatureOnly") and isinstance(operation.get("approvalId"), str):
            job = self.provider.signature(
                str(operation["approvalId"]), binding.wallet, binding.network, origin="metamask"
            )
            if job.terminal:
                return self._change(
                    operation,
                    state="rejected",
                    message="Legacy interrupted signature resolved without release; no order replay",
                )
        elif operation.get("providerOrderId") is not None and json_object(operation["request"]).get("kind") == "perp":
            return self._reconcile_order(operation, {"clientOrderId": None}, observation=observation)
        return operation

    @staticmethod
    def _wire_matches_leg(
        wire: dict[str, object],
        *,
        market_index: int,
        cloid: str,
        side: str,
        quantity: Decimal,
        reduce_only: bool,
        kind: str,
        tif: str,
        tpsl: str | None,
        as_trigger: bool,
    ) -> bool:
        if (
            wire.get("c") != cloid
            or type(wire.get("a")) is not int
            or wire["a"] != market_index
            or wire.get("b") is not (side == "buy")
            or wire.get("r") is not reduce_only
            or decimal(wire.get("s"), positive=True) != quantity
        ):
            return False
        wire_t = wire.get("t")
        if not isinstance(wire_t, dict):
            return False
        if as_trigger or kind in {"stop", "stop_limit"}:
            trig = wire_t.get("trigger")
            expected_role = tpsl if tpsl is not None else ("tp" if kind == "limit" else "sl")
            expected_market = kind == "stop"
            return (
                set(wire_t) == {"trigger"}
                and isinstance(trig, dict)
                and set(trig) == {"isMarket", "triggerPx", "tpsl"}
                and trig.get("isMarket") is expected_market
                and trig.get("tpsl") == expected_role
                and decimal(trig.get("triggerPx"), positive=True) > 0
            )
        expected_tif = "Ioc" if kind == "market" else {"gtc": "Gtc", "ioc": "Ioc", "alo": "Alo"}[tif]
        return wire_t == {"limit": {"tif": expected_tif}}

    def _unreleased_native_plan(self, operation, plan, effect):
        """Positive native checkpoints prove no order handoff; missing history does not."""
        try:
            if not isinstance(plan, dict) or plan.get("origin") != "lumibot":
                return False
            binding = Binding.parse(json_object(plan["binding"]), validate_registry=False)
            current = Binding.parse(self.journal.binding(self.agent_id), validate_registry=False)
            intent = PerpIntent.parse(json_object(operation["request"]))
            action = json_object(plan["action"])
            orders = action.get("orders")
            if not isinstance(orders, list) or not orders:
                return False
            fills = self.journal.effect(self.agent_id, str(operation["id"]), "fills")
            if (
                binding.agent_id != self.agent_id
                or current.agent_id != self.agent_id
                or (binding.wallet, binding.network) != (current.wallet, current.network)
                or (plan.get("wallet"), plan.get("network")) != (binding.wallet, binding.network)
                or plan.get("projectId") != self.provider.credentials().project_id
                or not isinstance(plan.get("ownerEpoch"), str)
                or not plan["ownerEpoch"]
                or intent.identifier != operation["id"]
                or intent.record() != operation["request"]
                or plan.get("clientOrderId") != self._cloid(binding, intent.identifier)
                or action.get("type") != "order"
                or type(plan.get("marketIndex")) is not int
                or plan["marketIndex"] < 0
                or operation.get("providerOrderId") is not None
                or decimal(operation["filledQuantity"]) != 0
                or operation.get("fees")
                or (fills is not None and fills.get("items") != [])
            ):
                return False
            market_index = plan["marketIndex"]
            if intent.order_class == "simple":
                if action.get("grouping") != "na" or len(orders) != 1:
                    return False
                wire = json_object(orders[0])
                if decimal(wire.get("p"), positive=True) != decimal(operation["executionLimitPrice"], positive=True):
                    return False
                if not self._wire_matches_leg(
                    wire,
                    market_index=market_index,
                    cloid=plan["clientOrderId"],
                    side=intent.side,
                    quantity=intent.quantity,
                    reduce_only=intent.reduce_only,
                    kind=intent.kind,
                    tif=intent.tif,
                    tpsl=intent.tpsl,
                    as_trigger=False,
                ):
                    return False
            else:
                expected_grouping = "positionTpsl" if intent.order_class == "oco" else "normalTpsl"
                expected_len = len(intent.children) + (1 if expected_grouping == "normalTpsl" else 0)
                if action.get("grouping") != expected_grouping or len(orders) != expected_len:
                    return False
                child_ops = operation.get("childOperations")
                if not isinstance(child_ops, list) or len(child_ops) != len(intent.children):
                    return False
                wire_idx = 0
                if expected_grouping == "normalTpsl":
                    entry_wire = json_object(orders[0])
                    if decimal(entry_wire.get("p"), positive=True) != decimal(
                        operation["executionLimitPrice"], positive=True
                    ):
                        return False
                    if not self._wire_matches_leg(
                        entry_wire,
                        market_index=market_index,
                        cloid=plan["clientOrderId"],
                        side=intent.side,
                        quantity=intent.quantity,
                        reduce_only=intent.reduce_only,
                        kind=intent.kind,
                        tif=intent.tif,
                        tpsl=None,
                        as_trigger=False,
                    ):
                        return False
                    wire_idx = 1
                else:
                    max_p = max(decimal(json_object(w).get("p"), positive=True) for w in orders)
                    if max_p != decimal(operation["executionLimitPrice"], positive=True):
                        return False
                for child_intent, child_rec in zip(intent.children, child_ops):
                    c_wire = json_object(orders[wire_idx])
                    wire_idx += 1
                    c_obj = json_object(child_rec)
                    c_fills = self.journal.effect(
                        self.agent_id, str(operation["id"]), self._child_fill_step(child_intent.identifier)
                    )
                    if (
                        c_obj.get("id") != child_intent.identifier
                        or c_obj.get("providerOrderId") is not None
                        or decimal(c_obj.get("filledQuantity", "0")) != 0
                        or c_obj.get("fees")
                        or (c_fills is not None and c_fills.get("items") != [])
                    ):
                        return False
                    if not self._wire_matches_leg(
                        c_wire,
                        market_index=market_index,
                        cloid=self._cloid(binding, child_intent.identifier),
                        side=child_intent.side,
                        quantity=child_intent.quantity,
                        reduce_only=True,
                        kind=child_intent.kind,
                        tif="gtc",
                        tpsl=child_intent.tpsl,
                        as_trigger=True,
                    ):
                        return False
            if effect is not None:
                if (
                    effect.get("phase") != "provider"
                    or effect.get("state") not in {"prepared", "requested", "pending", "rejected"}
                    or effect.get("clientOrigin") != "lumibot"
                    or effect.get("origin") != METAMASK_WALLET_ORIGIN
                    or effect.get("ownerEpoch") != plan["ownerEpoch"]
                    or (effect.get("wallet"), effect.get("network")) != (binding.wallet, binding.network)
                    or effect.get("action") != action
                    or effect.get("protocolPayload") != action
                ):
                    return False
                data = action_typed_data(
                    action,
                    positive_integer(effect.get("nonce"), "nonce"),
                    binding.network,
                    positive_integer(effect.get("expiresAfter"), "expiry"),
                )
                if effect.get("typedData") != data or effect.get("typedDataHash") != typed_data_hash(data):
                    return False
            return True
        except (MetaMaskError, ValueError, TypeError, KeyError):
            return False

    def _run_cancel_action(
        self,
        operation: dict[str, object],
        native_plan: dict[str, object] | None,
        step_prefix: str,
        cancels: list[dict[str, object]],
        *,
        target_still_live: bool,
    ) -> dict[str, object]:
        operation_id = str(operation["id"])
        binding, snapshot = self._authority(cancellation=True)
        cancel_plan = {"binding": snapshot, "wallet": binding.wallet, "network": binding.network}
        action = {"type": "cancel", "cancels": cancels}
        attempts = [
            (name, effect)
            for name, effect in self.journal.effects(self.agent_id, operation_id).items()
            if name.startswith(step_prefix)
        ]
        step, effect = attempts[-1] if attempts else (step_prefix + "1", None)
        if (
            effect is not None
            and effect.get("phase") == "venue"
            and effect.get("state") == "released"
            and target_still_live
            and int(time.time() * 1000) >= positive_integer(effect.get("expiresAfter"), "cancellation expiry")
        ):
            # The old signature cannot arrive later, and the original order is still live.
            effect = {**effect, "state": "expired"}
            self.journal.save_effect(operation, step, effect)
        if effect is not None and effect.get("state") in {"rejected", "expired"}:
            step, effect = step_prefix + str(len(attempts) + 1), None
        if effect is not None and effect.get("ownerEpoch") != self.epoch:
            # Read only after restart. An unused old cancellation signature is never released.
            if effect.get("phase") == "provider":
                job = self.provider.signature(
                    str(effect["requestId"]), binding.wallet, binding.network, origin=str(effect["origin"])
                )
                if job.terminal:
                    self.journal.save_effect(
                        operation, step, {**effect, "state": "rejected", "providerState": job.state}
                    )
            return self._reconcile_order(operation, native_plan or {"clientOrderId": None})
        operation = self._cancel_step(operation, cancel_plan, step, action)
        native_plan = self.journal.effect(self.agent_id, operation_id, "plan")
        return self._reconcile_order(operation, native_plan or {"clientOrderId": None})

    def cancel(self, operation_id: str) -> dict[str, object]:
        with self._execution:
            operation, target_child = self._find_operation(operation_id)
            parent_id = str(operation["id"])
            if target_child is None and operation["state"] in _TERMINAL and not self._has_live_children(operation):
                return operation
            if target_child is not None and target_child.get("state") in _TERMINAL:
                return self._child_view(operation, target_child)
            venue_id = operation.get("providerOrderId")
            if venue_id is not None and (
                not isinstance(venue_id, str) or not venue_id.isdecimal() or int(venue_id) <= 0
            ):
                self._change(
                    operation,
                    reconciliationErrorCode="RECONCILIATION_REQUIRED",
                    reconciliationMessage="Recorded venue order identity is malformed",
                )
                raise MetaMaskError("RECONCILIATION_REQUIRED", "Resolve original order identity before cancellation")
            order_effect = self.journal.effect(self.agent_id, parent_id, "order")
            leverage_effect = self.journal.effect(self.agent_id, parent_id, "leverage")
            any_child_oid = any(
                isinstance(c, dict) and c.get("providerOrderId") is not None
                for c in (operation.get("childOperations") or [])
            )
            if (
                (order_effect is None or order_effect.get("phase") != "venue")
                and operation.get("providerOrderId") is None
                and not any_child_oid
            ):
                native_plan = self.journal.effect(self.agent_id, parent_id, "plan")
                if not self._unreleased_native_plan(operation, native_plan, order_effect):
                    self._change(
                        operation,
                        reconciliationErrorCode="RECONCILIATION_REQUIRED",
                        reconciliationMessage=(
                            "Order release history is incomplete; cancellation cannot prove no handoff"
                        ),
                    )
                    raise MetaMaskError("RECONCILIATION_REQUIRED", "Resolve order release history before cancellation")
                if (
                    leverage_effect
                    and leverage_effect.get("phase") == "venue"
                    and leverage_effect.get("state") != "confirmed"
                ):
                    operation = self._reconcile_leverage(operation, native_plan, leverage_effect)
                    if operation["state"] in _TERMINAL:
                        updated = self._change(operation, reconciliationErrorCode=None, reconciliationMessage=None)
                        if target_child is not None:
                            refreshed = next(c for c in updated["childOperations"] if c["id"] == operation_id)
                            return self._child_view(updated, refreshed)
                        return updated
                    raise MetaMaskError(
                        "RECONCILIATION_REQUIRED", "Resolve uncertain leverage before canceling the prepared intent"
                    )
                if target_child is not None:
                    raise MetaMaskError(
                        "RECONCILIATION_REQUIRED",
                        "Cannot cancel an individual child of an unreleased grouped order; "
                        "cancel the parent order explicitly",
                    )
                local_updates: dict[str, object] = {
                    "state": "canceled",
                    "message": "Unreleased intent canceled locally; no venue order sent",
                    "reconciliationErrorCode": None,
                    "reconciliationMessage": None,
                    "orderRelease": {
                        "state": "not_released",
                        "source": "native_plan_local_cancel",
                        "clientOrderId": native_plan["clientOrderId"],
                    },
                }
                marked_children = self._mark_children(operation, "canceled")
                if marked_children is not None:
                    local_updates["childOperations"] = marked_children
                return self._change(operation, **local_updates)

            native_plan = self.journal.effect(self.agent_id, parent_id, "plan")
            # Cancellation may observe the entry, but must never resume its signing/release.
            operation = self._reconcile_order(operation, native_plan or {"clientOrderId": None})
            intent = PerpIntent.parse(json_object(operation["request"]))
            market = self.venue.market(intent.symbol)

            # Case 1: Cancel an individual child leg by child ID.
            if target_child is not None:
                children = operation.get("childOperations") or []
                refreshed = next((c for c in children if isinstance(c, dict) and c.get("id") == operation_id), None)
                if refreshed is None:
                    raise MetaMaskError("NOT_FOUND", "Child order disappeared from persisted operation")
                if refreshed.get("state") in _TERMINAL:
                    return self._child_view(operation, refreshed)
                if refreshed.get("state") == "waiting_for_fill":
                    raise MetaMaskError(
                        "RECONCILIATION_REQUIRED",
                        "Child exit leg is waiting for parent entry fill and has no live venue order yet",
                    )
                if refreshed.get("state") not in {"open", "partial_fill"} or refreshed.get("reconciliationErrorCode"):
                    raise MetaMaskError(
                        "RECONCILIATION_REQUIRED", "Verify the matching live child order before cancellation"
                    )
                child_oid = refreshed.get("providerOrderId")
                if not isinstance(child_oid, str) or not child_oid.isdecimal() or int(child_oid) <= 0:
                    raise MetaMaskError(
                        "RECONCILIATION_REQUIRED", "Resolve original child order identity before cancellation"
                    )
                operation = self._run_cancel_action(
                    operation,
                    native_plan,
                    f"cancel-{child_oid}-",
                    [{"a": market.index, "o": int(child_oid)}],
                    target_still_live=True,
                )
                refreshed = next(c for c in (operation.get("childOperations") or []) if c["id"] == operation_id)
                return self._child_view(operation, refreshed)

            # Case 2: Cancel an OCO container -> cancel all live child legs together.
            if intent.order_class == "oco":
                if operation["state"] in _TERMINAL and not self._has_live_children(operation):
                    return operation
                if operation.get("reconciliationErrorCode"):
                    raise MetaMaskError(
                        "RECONCILIATION_REQUIRED", "Verify the matching live OCO child orders before cancellation"
                    )
                live_children = [
                    c
                    for c in (operation.get("childOperations") or [])
                    if isinstance(c, dict) and c.get("state") in {"open", "partial_fill"}
                ]
                if not live_children:
                    if operation["state"] in _TERMINAL:
                        return operation
                    raise MetaMaskError("RECONCILIATION_REQUIRED", "Verify the matching live order before cancellation")
                cancels = []
                oid_parts = []
                for child in live_children:
                    c_oid = child.get("providerOrderId")
                    if not isinstance(c_oid, str) or not c_oid.isdecimal() or int(c_oid) <= 0:
                        raise MetaMaskError(
                            "RECONCILIATION_REQUIRED", "Resolve original child order identity before cancellation"
                        )
                    cancels.append({"a": market.index, "o": int(c_oid)})
                    oid_parts.append(c_oid)
                prefix = "cancel-oco-" + "-".join(sorted(oid_parts)) + "-"
                return self._run_cancel_action(operation, native_plan, prefix, cancels, target_still_live=True)

            # Case 3: Cancel simple order or bracket/OTO entry order.
            if operation["state"] in _TERMINAL:
                return operation
            if operation["state"] not in {"open", "partial_fill"} or operation.get("reconciliationErrorCode"):
                raise MetaMaskError("RECONCILIATION_REQUIRED", "Verify the matching live order before cancellation")
            venue_id = operation.get("providerOrderId")
            if not isinstance(venue_id, str) or not venue_id.isdecimal() or int(venue_id) <= 0:
                raise MetaMaskError("RECONCILIATION_REQUIRED", "Resolve original order identity before cancellation")
            return self._run_cancel_action(
                operation,
                native_plan,
                f"cancel-{venue_id}-",
                [{"a": market.index, "o": int(venue_id)}],
                target_still_live=operation["state"] in {"open", "partial_fill"},
            )

    @staticmethod
    def _clean_live_oids(operation: dict[str, object]) -> set[int]:
        if operation.get("reconciliationErrorCode"):
            return set()
        clean_live_oids: set[int] = set()
        if (
            operation.get("state") in {"open", "partial_fill"}
            and isinstance(operation.get("providerOrderId"), str)
            and operation["providerOrderId"].isdecimal()
            and int(operation["providerOrderId"]) > 0
        ):
            clean_live_oids.add(int(operation["providerOrderId"]))
        for c in operation.get("childOperations") or []:
            if (
                isinstance(c, dict)
                and c.get("state") in {"open", "partial_fill"}
                and not c.get("reconciliationErrorCode")
                and isinstance(c.get("providerOrderId"), str)
                and c["providerOrderId"].isdecimal()
                and int(c["providerOrderId"]) > 0
            ):
                clean_live_oids.add(int(c["providerOrderId"]))
        return clean_live_oids

    def _ledger_matches_snapshot(
        self,
        operation_id: str,
        leg: dict[str, object],
        fill_step: str,
        observed_fills: list[dict[str, object]],
    ) -> bool:
        ledger = self.journal.effect(self.agent_id, operation_id, fill_step)
        items = ledger.get("items", []) if ledger else []
        if not isinstance(items, list) or not all(
            isinstance(item, dict) and isinstance(item.get("tid"), int) for item in items
        ):
            return False
        cached = {item["tid"]: item for item in items}
        if len(cached) != len(items) or not all(cached.get(fill["tid"]) == fill for fill in observed_fills):
            return False
        try:
            ledger_qty = Decimal(0)
            ledger_fees: dict[str, Decimal] = {}
            for item in items:
                ledger_qty += decimal(item.get("sz"), positive=True)
                token = item.get("feeToken")
                if not isinstance(token, str) or not token:
                    return False
                ledger_fees[token] = ledger_fees.get(token, Decimal(0)) + decimal(item.get("fee"))
            snapshot_qty = decimal(leg.get("filledQuantity", "0"))
            if snapshot_qty != ledger_qty:
                return False
            if ledger_qty > 0 and leg.get("averagePrice") is None:
                return False
            expected_fees = [
                {"asset": token, "amount": format(amount, "f")} for token, amount in sorted(ledger_fees.items())
            ]
            snapshot_fees = leg.get("fees") if isinstance(leg.get("fees"), list) else []
            if snapshot_fees and snapshot_fees != expected_fees:
                return False
        except (MetaMaskError, ValueError, TypeError):
            return False
        return True

    def _cancel_step(self, operation, plan, step, action):
        try:
            operation, _ = self._step(operation, plan, step, action)
            return operation
        except MetaMaskError as error:
            current = self._operation(str(operation["id"]))
            return self._change(current, cancelState="unknown", cancelErrorCode=error.code, cancelMessage=str(error))

    def order(
        self, operation_id: str, *, _observation=None, _skipped_out: list[str] | None = None
    ) -> dict[str, object]:
        if not self._execution.acquire(blocking=False):
            if _skipped_out is not None:
                _skipped_out.append(operation_id)
            operation, child = self._find_operation(operation_id)
            return self._child_view(operation, child) if child is not None else operation
        try:
            operation, target_child = self._find_operation(operation_id)
            parent_id = str(operation["id"])
            operation = self._advance(operation, observation=_observation)
            for step, effect in self.journal.effects(self.agent_id, parent_id).items():
                if (
                    step.startswith("cancel-")
                    and effect.get("phase") == "provider"
                    and effect.get("state") != "rejected"
                    and effect.get("ownerEpoch") == self.epoch
                ):
                    clean_live_oids = self._clean_live_oids(operation)
                    action = json_object(effect["action"])
                    cancels = action.get("cancels")
                    target_oids = (
                        [item.get("o") for item in cancels if isinstance(item, dict)]
                        if isinstance(cancels, list) and cancels
                        else []
                    )
                    if not target_oids or not all(type(oid) is int and oid in clean_live_oids for oid in target_oids):
                        operation = self._change(
                            operation,
                            cancelState="unknown",
                            cancelErrorCode="RECONCILIATION_REQUIRED",
                            cancelMessage="Cancel target is no longer cleanly live; verification required",
                        )
                        continue
                    binding, snapshot = self._authority(cancellation=True)
                    plan = {"binding": snapshot, "wallet": binding.wallet, "network": binding.network}
                    operation = self._cancel_step(operation, plan, step, action)
                    native_plan = self.journal.effect(self.agent_id, parent_id, "plan")
                    operation = self._reconcile_order(operation, native_plan or {"clientOrderId": None})
            if target_child is not None:
                refreshed = next(
                    c
                    for c in (operation.get("childOperations") or [])
                    if isinstance(c, dict) and c.get("id") == operation_id
                )
                return self._child_view(operation, refreshed)
            return operation
        finally:
            self._execution.release()

    def _group_ledgers_match_snapshot(self, operation: dict[str, object]) -> bool:
        identifier = str(operation["id"])
        children = operation.get("childOperations")
        if not isinstance(children, list) or not children:
            return False
        legs_to_check: list[tuple[dict[str, object], str]] = [
            (operation, "fills"),
            *[(c, self._child_fill_step(str(c["id"]))) for c in children if isinstance(c, dict) and c.get("id")],
        ]
        return all(
            self._ledger_matches_snapshot(identifier, leg_rec, leg_step, []) for leg_rec, leg_step in legs_to_check
        )

    @staticmethod
    def _is_fully_exited_group(operation: dict[str, object]) -> bool:
        children = operation.get("childOperations")
        if not isinstance(children, list) or not children:
            return False
        if operation.get("state") != "filled" or operation.get("reconciliationErrorCode"):
            return False
        if any(
            not isinstance(c, dict) or c.get("state") not in _TERMINAL or c.get("reconciliationErrorCode")
            for c in children
        ):
            return False
        try:
            parent_filled = decimal(operation.get("filledQuantity", "0"))
            exited_qty = sum(
                (decimal(c.get("filledQuantity", "0")) for c in children if c.get("state") == "filled"),
                Decimal(0),
            )
            return (
                parent_filled > 0
                and exited_qty >= parent_filled
                and all(c.get("state") != "canceled" or decimal(c.get("filledQuantity", "0")) == 0 for c in children)
            )
        except (MetaMaskError, ValueError, TypeError):
            return False

    def orders(self) -> list[dict[str, object]]:
        # Swaps share the journal but belong exclusively to EVM reconciliation.
        # Other legacy request shapes retain their existing single-order dispatch.
        operations = [row for row in self.journal.operations(self.agent_id) if row["request"].get("kind") != "swap"]
        cursor_key = f"closed_fill_cursor:{self.agent_id}"
        cursor_raw = self.journal.metadata(cursor_key)
        cursor_ms = int(cursor_raw) if isinstance(cursor_raw, str) and cursor_raw.isdecimal() else None
        overlap_ms = 60_000
        existing = {}
        settled_closed_groups: set[str] = set()
        skipped_settled: set[str] = set()
        for row in operations:
            identifier = str(row["id"])
            reconcilable_children = self._has_reconcilable_children(row)
            venue_id = row.get("providerOrderId")
            has_valid_oid = isinstance(venue_id, str) and venue_id.isdecimal() and int(venue_id) > 0
            plan = self.journal.effect(self.agent_id, identifier, "plan")
            effect = self.journal.effect(self.agent_id, identifier, "order")
            released = effect is not None and effect.get("phase") == "venue"
            if row["state"] == "filled" and not reconcilable_children:
                continue
            if not has_valid_oid and not (released and row.get("childOperations")):
                continue
            if (
                row.get("childOperations")
                and row["state"] in _TERMINAL
                and not row.get("reconciliationErrorCode")
                and not self._has_live_children(row)
                and self._group_ledgers_match_snapshot(row)
            ):
                updated_at = int(row.get("updatedAt", row["createdAt"]))
                if self._is_fully_exited_group(row) and cursor_ms is not None and cursor_ms >= updated_at + overlap_ms:
                    # Parent and exit child filled the full quantity and the post-close overlap window was scanned.
                    skipped_settled.add(identifier)
                    continue
                settled_closed_groups.add(identifier)
            if row["state"] in _TERMINAL or plan is None or released:
                existing[identifier] = row
        if not existing:
            return [row if str(row["id"]) in skipped_settled else self.order(str(row["id"])) for row in operations]
        closed = {
            identifier: row
            for identifier, row in existing.items()
            if row["state"] in _TERMINAL and not self._has_live_children(row)
        }
        try:
            wallet = Binding.parse(self.journal.binding(self.agent_id), validate_registry=False).wallet
            live = self.venue.frontend_open_orders(wallet) if len(existing) > len(closed) else None
            poll_end = int(time.time() * 1000)
            anchors = []
            for identifier, row in existing.items():
                base_start = max(0, int(row["createdAt"]) - 5000)
                if identifier in settled_closed_groups and cursor_ms is not None and cursor_ms - overlap_ms <= poll_end:
                    anchors.append(max(base_start, cursor_ms - overlap_ms))
                else:
                    anchors.append(base_start)
            start = min(anchors)
            if start > poll_end:
                start = max(0, poll_end - overlap_ms)
            recent = self.venue.fills(wallet, start, poll_end)
        except MetaMaskError:
            # Preserve ordinary durable reconciliation errors if discovery is unavailable.
            return [row if str(row["id"]) in skipped_settled else self.order(str(row["id"])) for row in operations]
        observation = {"wallet": wallet, "openOrders": live, "fills": recent}
        by_order = {}
        for fill in recent:
            by_order.setdefault(fill.get("oid"), []).append(fill)
        results = []
        skipped_reconciliation: list[str] = []
        for operation in operations:
            identifier = str(operation["id"])
            if identifier in skipped_settled:
                results.append(operation)
                continue
            if identifier in closed and not operation.get("reconciliationErrorCode"):
                current = self._operation(identifier)
                if (
                    current["state"] in _TERMINAL
                    and not current.get("reconciliationErrorCode")
                    and not self._has_live_children(current)
                ):
                    children = current.get("childOperations")
                    if not children:
                        observed = by_order.get(int(current["providerOrderId"]), [])
                        if self._ledger_matches_snapshot(identifier, current, "fills", observed):
                            # Missing old rows cannot erase the retained ledger. Only a new or
                            # changed receipt (or a ledger ahead of the snapshot) needs reconciliation.
                            results.append(current)
                            continue
                    elif isinstance(children, list):
                        legs_to_check: list[tuple[dict[str, object], str]] = [
                            (current, "fills"),
                            *[
                                (c, self._child_fill_step(str(c["id"])))
                                for c in children
                                if isinstance(c, dict) and c.get("id")
                            ],
                        ]
                        all_clean = True
                        for leg_rec, leg_step in legs_to_check:
                            leg_oid = leg_rec.get("providerOrderId")
                            observed = (
                                by_order.get(int(leg_oid), [])
                                if isinstance(leg_oid, str) and leg_oid.isdecimal() and int(leg_oid) > 0
                                else []
                            )
                            if not self._ledger_matches_snapshot(identifier, leg_rec, leg_step, observed):
                                all_clean = False
                                break
                        if all_clean:
                            results.append(current)
                            continue
            results.append(
                self.order(
                    identifier,
                    _observation=observation if identifier in existing else None,
                    _skipped_out=skipped_reconciliation,
                )
            )
        if not skipped_reconciliation and not any(
            row.get("reconciliationErrorCode")
            or any(isinstance(c, dict) and c.get("reconciliationErrorCode") for c in (row.get("childOperations") or []))
            for row in results
        ):
            if cursor_ms is None or poll_end > cursor_ms:
                self.journal.set_metadata(cursor_key, str(poll_end))
        return results
