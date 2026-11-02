"""Native execution state machine: reserve, sign, submit and reconcile once."""

from __future__ import annotations

import hashlib
import time
from decimal import ROUND_CEILING, Context, Decimal, localcontext
from uuid import uuid4

from .journal import Journal
from .models import Binding, PerpIntent, decimal, positive_integer
from .provider import HYPERLIQUID_SIGNING_CHAIN, METAMASK_WALLET_ORIGIN, MetaMaskError, MetaMaskProvider, json_object
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
                mark_ceiling = self.venue.price(intent.symbol) * (1 + Decimal(binding.max_slippage_bps) / 10000)
                valuation = intent.quantity * max(execution_price, mark_ceiling) * (1 + fee_rate)
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
        if operation["state"] == "filled":
            return operation
        operation_id = str(operation["id"])
        plan = self.journal.effect(self.agent_id, operation_id, "plan")
        order_effect = self.journal.effect(self.agent_id, operation_id, "order")
        if venue_id is not None:
            # Accepted identity dominates an incomplete effect history on every entrypoint.
            return self._reconcile_order(operation, plan or {"clientOrderId": None}, observation=observation)
        if operation["state"] in _TERMINAL:
            if (
                order_effect is not None
                and order_effect.get("phase") == "venue"
                and order_effect.get("state") in {"released", "confirmed"}
            ):
                # Closed venue orders can acquire delayed fill receipts. Refresh only;
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
            if not isinstance(statuses, list) or len(statuses) != 1:
                return operation, False
            status = json_object(statuses[0])
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
                    return reject(statuses[0]["error"])
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

    def _read_order(self, operation, plan, *, observation=None):
        binding = Binding.parse(self.journal.binding(self.agent_id))
        if observation is not None and observation["wallet"] != binding.wallet:
            observation = None
        reference = operation.get("providerOrderId")
        reference = int(reference) if isinstance(reference, str) and reference.isdecimal() else plan["clientOrderId"]
        result = self.venue.order_status(binding.wallet, reference)
        if result.get("status") != "order":
            raise MetaMaskError("ORDER_UNRESOLVED", "Venue has not resolved the original order; no retransmission")
        status = json_object(result.get("order"), "venue order status")
        order = json_object(status.get("order"), "venue order")
        intent = PerpIntent.parse(json_object(operation["request"]))
        venue_id = positive_integer(order.get("oid"), "venue order ID")
        if (
            (isinstance(reference, int) and venue_id != reference)
            or order.get("coin") != intent.symbol
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
            raise MetaMaskError("ORDER_STATUS_UNSUPPORTED", "Venue terminal status requires explicit reconciliation")
        if state == "open":
            # orderStatus retains the size from its last lifecycle transition. Actual
            # partial-fill remainders come from the current live order snapshot.
            live_rows = observation["openOrders"] if observation is not None else None
            if live_rows is None:
                live_rows = self.venue.frontend_open_orders(binding.wallet)
            matches = [row for row in live_rows if row.get("oid") == venue_id]
            if len(matches) != 1:
                raise MetaMaskError("OPEN_ORDER_UNRESOLVED", "Live order snapshot is absent or ambiguous")
            live = matches[0]
            if (
                positive_integer(live.get("oid"), "live venue order ID") != venue_id
                or live.get("coin") != intent.symbol
                or live.get("side") != order.get("side")
                or decimal(live.get("origSz"), positive=True) != intent.quantity
                or (plan.get("clientOrderId") is not None and live.get("cloid") != plan["clientOrderId"])
                or live.get("reduceOnly") is not intent.reduce_only
            ):
                raise MetaMaskError("ORDER_IDENTITY_MISMATCH", "Live order differs from the persisted wallet intent")
            remaining = decimal(live.get("sz"), positive=True)
        else:
            remaining = Decimal(0) if state == "filled" else decimal(order.get("sz"))
        if not 0 <= remaining <= intent.quantity:
            raise MetaMaskError("ORDER_SIZE_MISMATCH", "Venue remaining size is outside the original order quantity")
        prior_fills = self.journal.effect(self.agent_id, intent.identifier, "fills")
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
            if item.get("coin") != intent.symbol or item.get("side") != ("B" if intent.side == "buy" else "A"):
                raise MetaMaskError("FILL_IDENTITY_MISMATCH", "Venue fill differs from the original order")
            identity = item["tid"]
            if identity in rows and rows[identity] != item:
                raise MetaMaskError("FILL_REGRESSION", "Venue changed an already recorded fill")
            rows[identity] = item
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
        if (
            raw_state == "filled"
            and 0 < quantity < intent.quantity
            and (intent.tif == "ioc" or intent.kind == "market")
            and order.get("tif") == "Ioc"
        ):
            orders = json_object(plan.get("action"), "persisted order action").get("orders")
            if isinstance(orders, list) and len(orders) == 1:
                wire_type = json_object(json_object(orders[0], "persisted order").get("t"), "persisted order type")
                terminal_remaining = decimal(order.get("sz"))
                if (
                    wire_type.get("limit") == {"tif": "Ioc"}
                    and 0 < terminal_remaining < intent.quantity
                    and quantity + terminal_remaining == intent.quantity
                ):
                    # IOC reports terminal "filled" even when its unfilled remainder was canceled.
                    state, remaining = "canceled", terminal_remaining
        if operation["state"] in _TERMINAL and state != operation["state"]:
            raise MetaMaskError("TERMINAL_STATUS_CONFLICT", "A later venue snapshot cannot change the closed lifecycle")
        self.journal.save_effect(
            operation, "fills", {"wallet": binding.wallet, "network": binding.network, "items": list(rows.values())}
        )
        expected = intent.quantity - remaining
        if quantity != expected or quantity > intent.quantity or quantity < decimal(operation["filledQuantity"]):
            raise MetaMaskError(
                "FILL_HISTORY_INCOMPLETE", "Venue order and retained fill history disagree; preserving known fills"
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
            "reconciliationErrorCode": None,
            "reconciliationMessage": None,
            "message": "Venue order and cumulative fills reconciled",
        }
        if quantity > 0:
            after["averagePrice"] = format(value / quantity, "f")
        return self.journal.replace_if_unchanged(operation, after)

    def _reconcile_legacy(self, operation, *, observation=None):
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
            return self._reconcile_order(operation, {"clientOrderId": None}, observation=observation)
        return operation

    def _unreleased_native_plan(self, operation, plan, effect):
        """Positive native checkpoints prove no order handoff; missing history does not."""
        try:
            if not isinstance(plan, dict) or plan.get("origin") != "lumibot":
                return False
            binding = Binding.parse(json_object(plan["binding"]))
            current = Binding.parse(self.journal.binding(self.agent_id))
            intent = PerpIntent.parse(json_object(operation["request"]))
            action = json_object(plan["action"])
            orders = action.get("orders")
            if not isinstance(orders, list) or len(orders) != 1:
                return False
            wire = json_object(orders[0])
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
                or action.get("grouping") != "na"
                or wire.get("c") != plan["clientOrderId"]
                or type(plan.get("marketIndex")) is not int
                or plan["marketIndex"] < 0
                or type(wire.get("a")) is not int
                or wire["a"] != plan["marketIndex"]
                or wire.get("b") is not (intent.side == "buy")
                or wire.get("r") is not intent.reduce_only
                or decimal(wire.get("s"), positive=True) != intent.quantity
                or decimal(wire.get("p"), positive=True) != decimal(operation["executionLimitPrice"], positive=True)
                or wire.get("t")
                != {
                    "limit": {
                        "tif": "Ioc"
                        if intent.kind == "market"
                        else {"gtc": "Gtc", "ioc": "Ioc", "alo": "Alo"}[intent.tif]
                    }
                }
                or operation.get("providerOrderId") is not None
                or decimal(operation["filledQuantity"]) != 0
                or operation.get("fees")
                or (fills is not None and fills.get("items") != [])
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

    def cancel(self, operation_id: str) -> dict[str, object]:
        with self._execution:
            operation = self._operation(operation_id)
            if operation["state"] in _TERMINAL:
                return operation
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
            order_effect = self.journal.effect(self.agent_id, operation_id, "order")
            leverage_effect = self.journal.effect(self.agent_id, operation_id, "leverage")
            if (order_effect is None or order_effect.get("phase") != "venue") and operation.get(
                "providerOrderId"
            ) is None:
                native_plan = self.journal.effect(self.agent_id, operation_id, "plan")
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
                        return self._change(operation, reconciliationErrorCode=None, reconciliationMessage=None)
                    raise MetaMaskError(
                        "RECONCILIATION_REQUIRED", "Resolve uncertain leverage before canceling the prepared intent"
                    )
                if operation.get("providerOrderId") is None:
                    return self._change(
                        operation,
                        state="canceled",
                        message="Unreleased intent canceled locally; no venue order sent",
                        reconciliationErrorCode=None,
                        reconciliationMessage=None,
                        orderRelease={
                            "state": "not_released",
                            "source": "native_plan_local_cancel",
                            "clientOrderId": native_plan["clientOrderId"],
                        },
                    )
            native_plan = self.journal.effect(self.agent_id, operation_id, "plan")
            # Cancellation may observe the entry, but must never resume its signing/release.
            operation = self._reconcile_order(operation, native_plan or {"clientOrderId": None})
            if operation["state"] in _TERMINAL:
                return operation
            if operation["state"] not in {"open", "partial_fill"} or operation.get("reconciliationErrorCode"):
                raise MetaMaskError("RECONCILIATION_REQUIRED", "Verify the matching live order before cancellation")
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
                    job = self.provider.signature(
                        str(effect["requestId"]), binding.wallet, binding.network, origin=str(effect["origin"])
                    )
                    if job.terminal:
                        self.journal.save_effect(
                            operation, step, {**effect, "state": "rejected", "providerState": job.state}
                        )
                return self._reconcile_order(operation, native_plan or {"clientOrderId": None})
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

    def order(self, operation_id: str, *, _observation=None) -> dict[str, object]:
        if not self._execution.acquire(blocking=False):
            return self._operation(operation_id)
        try:
            operation = self._advance(self._operation(operation_id), observation=_observation)
            if operation["state"] in {"open", "partial_fill"} and not operation.get("reconciliationErrorCode"):
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
        # Swaps share the journal but belong exclusively to EVM reconciliation.
        # Other legacy request shapes retain their existing single-order dispatch.
        operations = [row for row in self.journal.operations(self.agent_id) if row["request"].get("kind") != "swap"]
        existing = {}
        for row in operations:
            venue_id = row.get("providerOrderId")
            if (
                row["state"] == "filled"
                or not isinstance(venue_id, str)
                or not venue_id.isdecimal()
                or int(venue_id) <= 0
            ):
                continue
            identifier = str(row["id"])
            plan = self.journal.effect(self.agent_id, identifier, "plan")
            effect = self.journal.effect(self.agent_id, identifier, "order")
            if row["state"] in _TERMINAL or plan is None or (effect is not None and effect.get("phase") == "venue"):
                existing[identifier] = row
        if not existing:
            return [self.order(str(row["id"])) for row in operations]
        closed = {identifier: row for identifier, row in existing.items() if row["state"] in _TERMINAL}
        try:
            wallet = Binding.parse(self.journal.binding(self.agent_id)).wallet
            live = self.venue.frontend_open_orders(wallet) if len(existing) > len(closed) else None
            start = max(0, min(int(row["createdAt"]) for row in existing.values()) - 5000)
            recent = self.venue.fills(wallet, start, int(time.time() * 1000))
        except MetaMaskError:
            # Preserve ordinary durable reconciliation errors if discovery is unavailable.
            return [self.order(str(row["id"])) for row in operations]
        observation = {"wallet": wallet, "openOrders": live, "fills": recent}
        by_order = {}
        for fill in recent:
            by_order.setdefault(fill.get("oid"), []).append(fill)
        results = []
        for operation in operations:
            identifier = str(operation["id"])
            if identifier in closed and not operation.get("reconciliationErrorCode"):
                ledger = self.journal.effect(self.agent_id, identifier, "fills")
                items = ledger.get("items", []) if ledger else []
                if isinstance(items, list) and all(
                    isinstance(item, dict) and isinstance(item.get("tid"), int) for item in items
                ):
                    cached = {item["tid"]: item for item in items}
                    observed = by_order.get(int(operation["providerOrderId"]), [])
                    if len(cached) == len(items) and all(cached.get(fill["tid"]) == fill for fill in observed):
                        # Missing old rows cannot erase the retained ledger. Only a new or
                        # changed receipt needs an individual terminal status reconciliation.
                        results.append(self._operation(identifier))
                        continue
            results.append(self.order(identifier, _observation=observation if identifier in existing else None))
        return results
