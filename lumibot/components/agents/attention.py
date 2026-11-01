"""Persistent, read-only attention conditions selected by an agent.

Polling uses the strategy clock and existing data methods. It never calls a
model or an order method. An application chooses which team to run for a packet;
the team retains all trading decisions. State lives in the ordinary Vars backup.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import math
import json
import threading
from typing import Any, Callable, Iterator

from .asset_resolution import resolve_asset_and_quote


def _datetime(value: Any) -> datetime:
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)


def observation_key(metric: str, asset: dict[str, Any]) -> str:
    return ":".join([metric, str(asset["symbol"]).upper(), str(asset.get("asset_type") or "stock"),
                     str(asset.get("expiration") or ""), str(asset.get("strike") or ""),
                     str(asset.get("right") or "").lower(), str(asset.get("quote_symbol") or "")])


def _order_status(value: Any) -> str:
    from lumibot.entities.order import STATUS_ALIAS_MAP

    status = str(value).lower()
    return STATUS_ALIAS_MAP.get(status, status)


class AttentionMonitor:
    """Edge-triggered attention with durable pending events and local single-flight decisions."""

    def __init__(self, strategy: Any, lock: Any = None):
        self.strategy = strategy
        self._lock = lock or threading.RLock()
        self._deciding = False
        self._worker: threading.Thread | None = None
        self._worker_error: BaseException | None = None

    def _state(self) -> dict[str, Any]:
        state = self.strategy.vars.get("_agent_attention", None)
        if state is None:
            state = {"version": 1, "sequence": 0, "watches": {}, "pending": {}}
            self.strategy.vars.set("_agent_attention", state)
        if state.get("version") != 1:
            raise ValueError("Unsupported attention state version; preserve the saved state")
        # Vars.all() is a shallow snapshot used by the ordinary backup owner.
        # Publish complete replacement states so its nested dictionaries never
        # change while another thread serializes a previously captured state.
        return deepcopy(state)

    def _save(self, state: dict[str, Any]) -> None:
        self.strategy.vars.set("_agent_attention", state)

    @staticmethod
    def _remove(state: dict[str, Any], owner: str, watch_id: str) -> None:
        state["watches"].pop(json.dumps([owner, watch_id], separators=(",", ":")), None)
        state["pending"] = {key: event for key, event in state["pending"].items()
                            if event["owner"] != owner or event["watch_id"] != watch_id}

    def watch(self, *, owner: str, watch_id: str, operation_id: str, metric: str,
              comparison: str, value: Any, reason: str, asset: dict[str, Any] | None = None,
              order_ids: list[str] | None = None, legs: list[dict[str, Any]] | None = None,
              review_scope: str = "management") -> dict[str, Any]:
        """Create/update one condition. Identical registration preserves its episode."""
        if not owner or not watch_id or not operation_id or not reason.strip():
            raise ValueError("Owner, watch, operation and reason are required")
        if review_scope not in {"setup", "research", "management"}:
            raise ValueError("Review scope must be setup, research or management")
        if metric not in {"price", "position_quantity", "basket_value", "time", "order_status"}:
            raise ValueError("Use price, position_quantity, basket_value, time or order_status")
        if comparison not in {"gte", "lte", "eq", "ne", "in"}:
            raise ValueError("Use gte, lte, eq, ne or in")
        if metric in {"price", "position_quantity", "basket_value"}:
            if metric != "basket_value" and (not asset or not asset.get("symbol")):
                raise ValueError("An exact asset is required")
            if comparison == "in" or isinstance(value, bool) or not math.isfinite(float(value)):
                raise ValueError("A finite numeric threshold is required")
            if metric == "basket_value":
                if not legs:
                    raise ValueError("Basket observations need exact legs, signed quantities and multipliers")
                for leg in legs:
                    if not leg.get("asset", {}).get("symbol") or any(
                        isinstance(leg.get(field), bool) or not math.isfinite(float(leg[field]))
                        for field in ("quantity", "multiplier")
                    ):
                        raise ValueError("Basket legs need finite signed quantities and multipliers")
        elif metric == "time":
            if comparison not in {"gte", "lte"}:
                raise ValueError("Time comparison must be gte or lte")
            _datetime(value)
        elif comparison != "in" or not isinstance(value, list) or not value or not order_ids:
            raise ValueError("Order-status watches require explicit order_ids and a nonempty status list")
        if metric == "order_status":
            value = [_order_status(status) for status in value]
        definition = {"owner": owner, "watch_id": watch_id, "operation_id": operation_id,
                      "metric": metric, "comparison": comparison, "value": value, "reason": reason,
                      "asset": asset, "order_ids": list(order_ids or []), "review_scope": review_scope}
        if legs is not None:
            definition["legs"] = deepcopy(legs)
        with self._lock:
            state = self._state()
            key = json.dumps([owner, watch_id], separators=(",", ":"))
            previous = state["watches"].get(key)
            def condition(record):
                return {**{k: v for k, v in record.items() if k != "reason" and
                           (k != "order_ids" or record["metric"] == "order_status")},
                        "review_scope": record.get("review_scope", "management")}
            if previous is None or condition(previous["definition"]) != condition(definition):
                self._remove(state, owner, watch_id)
                state["watches"][key] = {"definition": deepcopy(definition), "active": False,
                                          "unavailable": False}
            else:
                previous["definition"] = deepcopy(definition)
            self._save(state)
            return deepcopy(definition)

    def remove(self, *, owner: str, watch_id: str) -> None:
        with self._lock:
            state = self._state()
            self._remove(state, owner, watch_id)
            self._save(state)

    def pending(self) -> list[dict[str, Any]]:
        with self._lock:
            return deepcopy(list(self._state()["pending"].values()))

    def snapshot(self, *, owner: str) -> dict[str, Any]:
        with self._lock:
            return deepcopy({"watches": [w["definition"] for w in self._state()["watches"].values()
                                          if w["definition"]["owner"] == owner],
                             "pending": [e for e in self.pending() if e["owner"] == owner]})

    def acknowledge(self, event_ids: list[str]) -> None:
        with self._lock:
            state = self._state()
            for event_id in event_ids:
                state["pending"].pop(event_id, None)
            self._save(state)

    def _observe(self, definition: dict[str, Any], cache: dict[str, Any]) -> Any:
        metric = definition["metric"]
        if metric == "time":
            return self.strategy.get_datetime().isoformat()
        if metric == "order_status":
            if "orders" not in cache:
                cache["orders"] = {str(o.identifier): _order_status(o.status) for o in self.strategy.get_orders()}
            statuses = cache["orders"]
            return [statuses.get(identifier) for identifier in definition["order_ids"]]
        if metric == "basket_value":
            return sum(float(leg["quantity"]) * float(leg["multiplier"]) * float(
                self._observe({"metric": "price", "asset": leg["asset"]}, cache))
                for leg in definition["legs"])
        asset_args = definition["asset"]
        key = observation_key(metric, asset_args)
        if key not in cache:
            asset, quote = resolve_asset_and_quote(self.strategy, **asset_args)
            if metric == "price":
                cache[key] = self.strategy.get_last_price(asset, **({"quote": quote} if quote else {}))
            else:
                if "positions" not in cache:
                    cache["positions"] = self.strategy.get_positions()
                cache[key] = sum(float(p.quantity) for p in cache["positions"] if p.asset == asset)
        return cache[key]

    def poll(self, *, observations: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Evaluate saved conditions once. An unavailable observation never clears a breach.

        ``observations`` optionally reuses an application's existing minute
        snapshot (keys from observation_key). Omit it to use normal data methods.
        Results include all still-pending events, not only this poll's new events.
        """
        with self._lock:
            state = self._state()
            cache: dict[str, Any] = {}
            for watch in state["watches"].values():
                definition = watch["definition"]
                metric, comparison = definition["metric"], definition["comparison"]
                observed, error = None, None
                try:
                    if observations is not None and metric == "basket_value":
                        observed = sum(float(leg["quantity"]) * float(leg["multiplier"]) * float(
                            observations.get(observation_key("price", leg["asset"]))) for leg in definition["legs"])
                    elif observations is not None and metric not in {"time", "order_status"}:
                        observed = observations.get(observation_key(metric, definition["asset"]))
                    else:
                        observed = self._observe(definition, cache)
                    if metric == "order_status":
                        if any(status is None for status in observed):
                            raise ValueError("Tracked order status is unavailable; reconcile exact identifiers")
                        met = any(status in definition["value"] for status in observed)
                    else:
                        actual, target = ((_datetime(observed), _datetime(definition["value"])) if metric == "time"
                                          else (float(observed), float(definition["value"])))
                        if metric != "time" and not math.isfinite(actual):
                            raise ValueError("Observation is not finite")
                        met = {"gte": actual >= target, "lte": actual <= target, "eq": actual == target,
                               "ne": actual != target}[comparison]
                except Exception as exc:
                    error = type(exc).__name__
                if error:
                    # One data-gap notification per continuous gap, even when
                    # the provider changes its exception or missing-value shape.
                    if watch["unavailable"]:
                        continue
                    watch["unavailable"] = True
                    kind = "observation_unavailable"
                    observed = None
                else:
                    watch["unavailable"] = False
                    if not met:
                        watch["active"] = False
                        continue
                    changed_status = metric == "order_status" and observed != watch.get("trigger_statuses")
                    if watch["active"] and not changed_status:
                        continue
                    watch["active"] = True
                    if metric == "order_status":
                        watch["trigger_statuses"] = deepcopy(observed)
                    kind = "condition_met"
                state["sequence"] += 1
                event_id = str(state["sequence"])
                state["pending"][event_id] = {**deepcopy(definition), "id": event_id,
                    "kind": kind, "observed": observed, "error_type": error,
                    "observed_at": self.strategy.get_datetime().isoformat()}
            self._save(state)
            return deepcopy(list(state["pending"].values()))

    @contextmanager
    def decision(self, packet: list[dict[str, Any]] | None = None) -> Iterator[list[dict[str, Any]] | None]:
        """One team decision at a time; acknowledge only its packet on success.

        Exceptions leave the exact packet pending for recovery. Warnings arriving
        during the cycle remain pending. A restart reconstructs a free local
        single-flight lock and replays unacknowledged evidence, never an order.
        """
        with self._lock:
            selected = None if self._deciding else (deepcopy(packet) if packet is not None else self.pending())
            if selected:
                self._deciding = True
        if not selected:
            yield None
            return
        try:
            yield selected
        except BaseException:
            raise
        else:
            self.acknowledge([event["id"] for event in selected])
        finally:
            with self._lock:
                self._deciding = False

    def dispatch(self, callback: Callable[[list[dict[str, Any]]], Any]) -> bool:
        """Deliver pending attention once; live inference leaves the lifecycle free.

        Historical callbacks run synchronously so simulated time cannot advance
        underneath a decision. Live work runs in one owned worker; polling and
        broker callbacks remain available. The callable must raise on an
        incomplete decision. Errors are surfaced by join/dispatch, retain the
        original packet and are never silently converted to a successful hold.
        Call join from the strategy's termination lifecycle before closing it.
        """
        with self._lock:
            if self._worker_error is not None:
                raise self._worker_error
            if self._deciding:
                return False
            selected = self.pending()
            if not selected:
                return False
            self._deciding = True

        live = not getattr(self.strategy, "is_backtesting", True)

        def execute() -> None:
            try:
                callback(selected)
                self.acknowledge([event["id"] for event in selected])
            except BaseException as exc:
                if live:
                    with self._lock:
                        self._worker_error = exc
                raise
            finally:
                with self._lock:
                    self._deciding = False

        if not live:
            execute()
        else:
            def background() -> None:
                try:
                    execute()
                except BaseException:
                    pass  # join/dispatch surfaces the saved failure to the lifecycle
            self._worker = threading.Thread(target=background, name="agent-attention-decision")
            try:
                self._worker.start()
            except BaseException:
                self._deciding = False
                raise
        return True

    def dispatch_team(self, *, decision_agent: str, research_agents: list[str],
                      context: dict[str, Any] | None = None) -> bool:
        """Run the AI-selected review scope through the existing dispatcher.

        Setup/research events refresh the declared research roles. Management
        events reuse their unchanged dated answers. One decision role then acts
        under its own policy and tools. Watch names have no routing meaning.
        This method selects no instruments, quantities, prices or orders.
        """
        role_names = tuple(research_agents)
        requested_context = deepcopy(context or {})
        def completed(result: Any, role: str) -> None:
            if not (result.payload or {}).get("execution_outcome", {}).get("decision_completed"):
                raise RuntimeError(f"Incomplete decision from {role}; preserve attention for recovery")

        def answers() -> dict[str, Any]:
            with self._lock:
                return deepcopy(self._state().get("research_answers", {}))

        def review(packet: list[dict[str, Any]]) -> None:
            decision = self.strategy.agents[decision_agent]
            common = {"attention": packet, "team_context": requested_context}
            if any(event.get("review_scope", "management") in {"setup", "research"} for event in packet):
                researchers = [(name, self.strategy.agents[name]) for name in role_names]
                for name, handle in researchers:
                    result = handle.run(
                        task_prompt="Review this attention evidence under your research policy. Return the actual evidence and limitations; do not trade.",
                        context={**common, "dated_research": answers()})
                    completed(result, name)
                    with self._lock:
                        state = self._state()
                        state.setdefault("research_answers", {})[name] = {
                            "at": self.strategy.get_datetime().isoformat(), "answer": result.summary}
                        self._save(state)
            result = decision.run(
                task_prompt=("Review the attention evidence and dated research under your policy. "
                    "You own every decision and action through your tools. Leave the next needed attention conditions. "
                    "Use review_scope=setup for an eligible new-setup review, research when fresh research is needed, "
                    "and management for a decision using still-relevant dated research. Setup/research scopes run the "
                    "declared researchers before your next decision; watch names do not select roles. "
                    "If an action requires missing research, defer that action and request a research review; "
                    "the subsequent coordinator cycle supplies it. Do not claim future research or unobserved fills."),
                context={**common, "dated_research": answers()})
            completed(result, decision_agent)

        return self.dispatch(review)

    def join(self) -> None:
        """Finish this monitor's worker and surface its actual error, without a deadline."""
        if self._worker is not None:
            self._worker.join()
        if self._worker_error is not None:
            raise self._worker_error
