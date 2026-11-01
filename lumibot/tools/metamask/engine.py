"""Native execution state machine: reserve, sign, submit and reconcile once."""

from __future__ import annotations

import hashlib
import time
from decimal import ROUND_CEILING, Context, Decimal, localcontext
from uuid import uuid4

from .journal import Journal
from .models import Binding, PerpIntent, decimal, positive_integer
from .provider import HYPERLIQUID_SIGNING_CHAIN, MetaMaskError, MetaMaskProvider, json_object
from .venue import HyperliquidVenue, action_typed_data, signature_parts, typed_data_hash

_TERMINAL = {"filled", "canceled", "rejected"}


class ExecutionEngine:
    def __init__(self, journal: Journal, provider: MetaMaskProvider, venue: HyperliquidVenue, agent_id: str):
        self.journal, self.provider, self.venue, self.agent_id = journal, provider, venue, agent_id
        self.epoch = str(uuid4())
        self._execution = journal.execution_lock(str(journal.binding(agent_id)["wallet"]))

    def _authority(self, expected=None, *, cancellation=False) -> tuple[Binding, dict[str, object]]:
        record = self.journal.binding(self.agent_id)
        binding = Binding.parse(record)
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

    def _change(self, operation: dict[str, object], **values) -> dict[str, object]:
        updated = {**operation, **values, "updatedAt": int(time.time() * 1000)}
        self.journal.save(updated)
        return updated

    def _cloid(self, binding: Binding, operation_id: str) -> str:
        scope = "\0".join(
            (self.provider.credentials().project_id, binding.wallet, binding.network, binding.agent_id, operation_id)
        )
        return "0x" + hashlib.sha256(scope.encode()).hexdigest()[:32]

    def submit(self, request: dict[str, object]) -> dict[str, object]:
        intent = PerpIntent.parse(request)
        canonical = intent.record()
        prior = self.journal.get(self.agent_id, intent.identifier)
        if prior is not None:
            if prior.get("request") != canonical:
                raise MetaMaskError("ID_CONFLICT", "Order ID already belongs to different intent")
            return self.order(intent.identifier)
        if not self._execution.acquire(blocking=False):
            raise MetaMaskError("EXECUTION_IN_PROGRESS", "Another wallet execution is already in progress")
        try:
            binding, snapshot = self._authority()
            if ":" in intent.symbol:
                raise MetaMaskError(
                    "DEX_EXECUTION_UNQUALIFIED", "HIP3 collateral and effective fee metadata need native qualification"
                )
            if intent.symbol not in binding.markets or intent.leverage > binding.max_leverage:
                raise MetaMaskError("MARKET_DENIED", "Market or leverage is outside the agent binding")
            market = self.venue.market(intent.symbol)
            if intent.leverage > market.max_leverage:
                raise MetaMaskError("LEVERAGE_LIMIT", "Leverage exceeds the venue market limit")
            client_id = self._cloid(binding, intent.identifier)
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
            )
            orders = action.get("orders")
            if not isinstance(orders, list) or len(orders) != 1:
                raise MetaMaskError("INVALID_INTENT", "Expected one normalized venue order")
            execution_price = decimal(json_object(orders[0]).get("p"), positive=True)
            fee_rate = self.venue.fee_rate(binding.wallet)
            with localcontext(Context(prec=160, rounding=ROUND_CEILING)):
                valuation = intent.quantity * max(execution_price, self.venue.price(intent.symbol)) * (1 + fee_rate)
            operation, created = self.journal.reserve(
                self.agent_id,
                canonical,
                format(valuation, "f"),
                expected_binding=snapshot,
            )
            if not created:
                return operation
            self.journal.save_effect(
                operation,
                "plan",
                {
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
                },
            )
            operation = self._change(operation, executionLimitPrice=format(execution_price, "f"))
            return self._advance(operation)
        finally:
            self._execution.release()

    def _advance(self, operation: dict[str, object]) -> dict[str, object]:
        try:
            return self._advance_inner(operation)
        except MetaMaskError as error:
            current = self._operation(str(operation["id"]))
            return self._change(current, state="unknown", lastErrorCode=error.code, message=str(error))

    def _advance_inner(self, operation: dict[str, object]) -> dict[str, object]:
        if operation["state"] in _TERMINAL:
            return operation
        operation_id = str(operation["id"])
        plan = self.journal.effect(self.agent_id, operation_id, "plan")
        if plan is None:
            return self._reconcile_legacy(operation)
        order_effect = self.journal.effect(self.agent_id, operation_id, "order")
        if plan.get("ownerEpoch") != self.epoch:
            if order_effect is not None and order_effect.get("phase") == "venue":
                return self._reconcile_order(operation, plan)
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
                            str(effect["requestId"]), str(plan["wallet"]), str(plan["network"])
                        )
                    except MetaMaskError:
                        return operation
                    if not job.terminal:
                        return operation
            return self._change(
                operation,
                state="rejected",
                message=(
                    "Interrupted signing was not released; no order replay. Earlier leverage changes may have applied."
                ),
            )
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
        return self._change(
            operation,
            state="rejected",
            message="Expired leverage submission reconciled; no order sent. Submit a new intent to trade.",
            observedLeverage=observed,
            lastErrorCode=None,
        )

    def _step(self, operation, plan, step, action):
        cancellation = step.startswith("cancel-")

        def change(current, **values):
            if not cancellation:
                return self._change(current, **values)
            # Never write cancellation lifecycle into the original order state.
            return self._change(current, cancelState=values.get("state"), cancelMessage=values.get("message", ""))

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
                    "origin": "lumibot",
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
                job = self.provider.signature(str(effect["requestId"]), binding.wallet, binding.network)
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
                intent = PerpIntent.parse(json_object(operation["request"]))
                with localcontext(Context(prec=160, rounding=ROUND_CEILING)):
                    fresh_usd = (
                        intent.quantity
                        * max(self.venue.price(intent.symbol), decimal(operation["executionLimitPrice"]))
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
        if response.get("status") == "err":
            self.journal.save_effect(operation, step, {**effect, "state": "rejected", "response": response})
            return change(operation, state="rejected", message="Venue explicitly rejected the action"), False
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
            if not isinstance(statuses, list) or len(statuses) != 1:
                return operation, False
            status = json_object(statuses[0])
            if "error" in status:
                return change(operation, state="rejected", message="Venue explicitly rejected the order"), False
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
            if not isinstance(body, dict) or body.get("type") != "cancel":
                return operation, False
            data_body = json_object(json_object(body).get("data"), "cancel action result")
            if data_body.get("statuses") != ["success"]:
                statuses = data_body.get("statuses")
                if (
                    isinstance(statuses, list)
                    and len(statuses) == 1
                    and isinstance(statuses[0], dict)
                    and "error" in statuses[0]
                ):
                    self.journal.save_effect(operation, step, {**effect, "state": "rejected", "response": response})
                    return change(
                        operation,
                        state="rejected",
                        message="Venue rejected cancellation; original order requires reconciliation",
                    ), False
                return operation, False
        self.journal.save_effect(operation, step, {**effect, "state": "confirmed", "response": response})
        return operation, True

    def _reconcile_order(self, operation, plan):
        try:
            with localcontext(Context(prec=160)):
                return self._read_order(operation, plan)
        except MetaMaskError as error:
            return self._change(operation, state="unknown", lastErrorCode=error.code, message=str(error))

    def _read_order(self, operation, plan):
        binding = Binding.parse(self.journal.binding(self.agent_id))
        reference = operation.get("providerOrderId")
        reference = int(reference) if isinstance(reference, str) and reference.isdecimal() else plan["clientOrderId"]
        result = self.venue.order_status(binding.wallet, reference)
        if result.get("status") != "order":
            return self._change(
                operation, state="unknown", message="Venue has not resolved the original order; no retransmission"
            )
        status = json_object(result.get("order"), "venue order status")
        order = json_object(status.get("order"), "venue order")
        intent = PerpIntent.parse(json_object(operation["request"]))
        venue_id = positive_integer(order.get("oid"), "venue order ID")
        if (
            order.get("coin") != intent.symbol
            or order.get("side") != ("B" if intent.side == "buy" else "A")
            or decimal(order.get("origSz"), positive=True) != intent.quantity
            or (plan.get("clientOrderId") is not None and order.get("cloid") != plan["clientOrderId"])
            or order.get("reduceOnly") is not intent.reduce_only
        ):
            raise MetaMaskError("ORDER_IDENTITY_MISMATCH", "Venue order differs from the persisted wallet intent")
        raw_state = status.get("status")
        canceled = {
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
        rejected = {
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
        state = "canceled" if raw_state in canceled else "rejected" if raw_state in rejected else raw_state
        if state not in {"open", "filled", "canceled", "rejected"}:
            return self._change(
                operation, state="unknown", message="Venue terminal status requires explicit reconciliation"
            )
        prior_fills = self.journal.effect(self.agent_id, intent.identifier, "fills")
        cached = prior_fills.get("items", []) if prior_fills else []
        if not isinstance(cached, list):
            raise MetaMaskError("INVALID_STATE", "Persisted fill ledger is malformed")
        rows = {json_object(row)["tid"]: json_object(row) for row in cached}
        start = max(0, int(operation["createdAt"]) - 5000)
        for item in self.venue.fills(binding.wallet, start, int(time.time() * 1000)):
            if item.get("oid") != venue_id:
                continue
            if item.get("coin") != intent.symbol or item.get("side") != ("B" if intent.side == "buy" else "A"):
                raise MetaMaskError("FILL_IDENTITY_MISMATCH", "Venue fill differs from the original order")
            identity = item["tid"]
            if identity in rows and rows[identity] != item:
                raise MetaMaskError("FILL_REGRESSION", "Venue changed an already recorded fill")
            rows[identity] = item
        self.journal.save_effect(
            operation, "fills", {"wallet": binding.wallet, "network": binding.network, "items": list(rows.values())}
        )
        quantity, value = Decimal(0), Decimal(0)
        fees = {}
        for item in rows.values():
            size, price = decimal(item.get("sz"), positive=True), decimal(item.get("px"), positive=True)
            quantity += size
            value += size * price
            token = item.get("feeToken")
            if not isinstance(token, str) or not token:
                raise MetaMaskError("INVALID_RESPONSE", "Fill fee currency is missing")
            fees[token] = fees.get(token, Decimal(0)) + decimal(item.get("fee"))
        expected = intent.quantity if state == "filled" else intent.quantity - decimal(order.get("sz"))
        if quantity != expected or quantity > intent.quantity or quantity < decimal(operation["filledQuantity"]):
            return self._change(
                operation,
                state="unknown",
                message="Venue order and retained fill history disagree; preserving known fills",
            )
        next_state = "partial_fill" if state == "open" and quantity > 0 else state
        after = {
            **operation,
            "state": next_state,
            "venueStatus": raw_state,
            "providerOrderId": str(venue_id),
            "filledQuantity": format(quantity, "f"),
            "fees": [{"asset": token, "amount": format(amount, "f")} for token, amount in sorted(fees.items())],
            "updatedAt": int(time.time() * 1000),
            "lastErrorCode": None,
            "message": "Venue order and cumulative fills reconciled",
        }
        if quantity > 0:
            after["averagePrice"] = format(value / quantity, "f")
        return self.journal.replace_if_unchanged(operation, after)

    def _reconcile_legacy(self, operation):
        binding = Binding.parse(self.journal.binding(self.agent_id))
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
            return self._reconcile_order(operation, {"clientOrderId": None})
        return operation

    def cancel(self, operation_id: str) -> dict[str, object]:
        with self._execution:
            operation = self._operation(operation_id)
            if operation["state"] in _TERMINAL:
                return operation
            order_effect = self.journal.effect(self.agent_id, operation_id, "order")
            leverage_effect = self.journal.effect(self.agent_id, operation_id, "leverage")
            if order_effect is None or order_effect.get("phase") != "venue":
                if (
                    leverage_effect
                    and leverage_effect.get("phase") == "venue"
                    and leverage_effect.get("state") != "confirmed"
                ):
                    operation = self._advance(operation)
                    if operation["state"] in _TERMINAL:
                        return operation
                    raise MetaMaskError(
                        "RECONCILIATION_REQUIRED", "Resolve uncertain leverage before canceling the prepared intent"
                    )
                if operation.get("providerOrderId") is None:
                    return self._change(
                        operation, state="canceled", message="Unreleased intent canceled locally; no venue order sent"
                    )
            operation = self._advance(operation)
            if operation["state"] in _TERMINAL:
                return operation
            venue_id = operation.get("providerOrderId")
            if not isinstance(venue_id, str) or not venue_id.isdecimal():
                raise MetaMaskError("RECONCILIATION_REQUIRED", "Resolve original order identity before cancellation")
            binding, snapshot = self._authority(cancellation=True)
            intent = PerpIntent.parse(json_object(operation["request"]))
            market = self.venue.market(intent.symbol)
            plan = {"binding": snapshot, "wallet": binding.wallet, "network": binding.network}
            action = {"type": "cancel", "cancels": [{"a": market.index, "o": int(venue_id)}]}
            attempts = [
                (name, effect)
                for name, effect in self.journal.effects(self.agent_id, operation_id).items()
                if name.startswith("cancel-" + venue_id + "-")
            ]
            step, effect = attempts[-1] if attempts else ("cancel-" + venue_id + "-1", None)
            if (
                effect is not None
                and effect.get("phase") == "venue"
                and effect.get("state") == "released"
                and operation["state"] in {"open", "partial_fill"}
                and int(time.time() * 1000) >= positive_integer(effect.get("expiresAfter"), "cancellation expiry")
            ):
                # The old signature cannot arrive later, and the original order is still live.
                effect = {**effect, "state": "expired"}
                self.journal.save_effect(operation, step, effect)
            if effect is not None and effect.get("state") in {"rejected", "expired"}:
                step, effect = "cancel-" + venue_id + "-" + str(len(attempts) + 1), None
            if effect is not None and effect.get("ownerEpoch") != self.epoch:
                # Read only after restart. An unused old cancellation signature is never released.
                if effect.get("phase") == "provider":
                    job = self.provider.signature(str(effect["requestId"]), binding.wallet, binding.network)
                    if job.terminal:
                        self.journal.save_effect(
                            operation, step, {**effect, "state": "rejected", "providerState": job.state}
                        )
                return self._advance(operation)
            operation = self._cancel_step(operation, plan, step, action)
            native_plan = self.journal.effect(self.agent_id, operation_id, "plan")
            return self._reconcile_order(operation, native_plan or {"clientOrderId": None})

    def _cancel_step(self, operation, plan, step, action):
        try:
            operation, _ = self._step(operation, plan, step, action)
            return operation
        except MetaMaskError as error:
            current = self._operation(str(operation["id"]))
            return self._change(current, cancelState="unknown", cancelErrorCode=error.code, cancelMessage=str(error))

    def order(self, operation_id: str) -> dict[str, object]:
        if not self._execution.acquire(blocking=False):
            return self._operation(operation_id)
        try:
            operation = self._advance(self._operation(operation_id))
            if operation["state"] not in _TERMINAL:
                for step, effect in self.journal.effects(self.agent_id, operation_id).items():
                    if (
                        step.startswith("cancel-")
                        and effect.get("phase") == "provider"
                        and effect.get("state") != "rejected"
                        and effect.get("ownerEpoch") == self.epoch
                    ):
                        binding, snapshot = self._authority(cancellation=True)
                        plan = {"binding": snapshot, "wallet": binding.wallet, "network": binding.network}
                        operation = self._cancel_step(operation, plan, step, json_object(effect["action"]))
                        native_plan = self.journal.effect(self.agent_id, operation_id, "plan")
                        operation = self._reconcile_order(operation, native_plan or {"clientOrderId": None})
            return operation
        finally:
            self._execution.release()

    def orders(self) -> list[dict[str, object]]:
        return [self.order(str(row["id"])) for row in self.journal.operations(self.agent_id)]
