from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import Callable, Dict, Optional, Any, List, TYPE_CHECKING
from .runner import AgentRunner, apply_actions_on_strategy, build_snapshot


def _parse_cadence_to_timedelta(cadence: str) -> timedelta:
    """Parse flexible cadence inputs into a timedelta.

    Supported examples (case-insensitive, spaces ok):
    - "5s", "5 sec", "5 seconds"
    - "2m", "2 min", "2 minutes"
    - "1h", "1 hr", "1 hour"
    - "1d", "1 day", "2 days"
    - plain integer -> minutes
    """
    if cadence is None:
        raise ValueError("cadence cannot be None")
    raw = cadence.strip().lower()
    # direct cron: if looks like a 5-field cron, leave to cron scheduler elsewhere
    if raw.count(" ") >= 4 and all(part for part in raw.split(" ")):
        # Let caller handle as cron
        raise ValueError("cadence looks like cron; not a duration")

    parts = raw.split()
    if len(parts) == 1:
        token = parts[0]
        # suffix-based
        suffix_map = {
            "s": "seconds",
            "sec": "seconds",
            "secs": "seconds",
            "second": "seconds",
            "seconds": "seconds",
            "m": "minutes",
            "min": "minutes",
            "mins": "minutes",
            "minute": "minutes",
            "minutes": "minutes",
            "h": "hours",
            "hr": "hours",
            "hrs": "hours",
            "hour": "hours",
            "hours": "hours",
            "d": "days",
            "day": "days",
            "days": "days",
        }
        # find the first alpha run as unit
        num_str = ''.join(ch for ch in token if (ch.isdigit()))
        unit_str = token[len(num_str):]
        if num_str:
            value = int(num_str)
            unit = suffix_map.get(unit_str or 'm', 'minutes')
            return timedelta(**{unit: value})
        # plain text like 'hour' is not allowed
        raise ValueError(f"Unrecognized cadence token: {cadence}")
    else:
        # forms like '5 min', '10 seconds'
        try:
            value = int(parts[0])
        except Exception as e:
            raise ValueError(f"Invalid cadence number: {cadence}") from e
        unit_word = parts[1]
        unit_alias = {
            "s": "seconds",
            "sec": "seconds",
            "secs": "seconds",
            "second": "seconds",
            "seconds": "seconds",
            "m": "minutes",
            "min": "minutes",
            "mins": "minutes",
            "minute": "minutes",
            "minutes": "minutes",
            "h": "hours",
            "hr": "hours",
            "hrs": "hours",
            "hour": "hours",
            "hours": "hours",
            "d": "days",
            "day": "days",
            "days": "days",
        }
        unit = unit_alias.get(unit_word, None)
        if unit is None:
            raise ValueError(f"Invalid cadence unit: {cadence}")
        return timedelta(**{unit: value})


class AgentHandle:
    def __init__(self, name: str, prompt: str, cadence: str, allow_trading: bool, symbols: Optional[List[str]] = None,
                 provider: Optional[str] = None, model: Optional[str] = None, search: bool = True):
        self.name = name
        self.prompt = prompt
        self.cadence = cadence
        self.allow_trading = allow_trading
        self.symbols = symbols
        self.paused = False

        # LLM config overrides
        self.provider = provider
        self.model = model
        self.search = search

        # Backtest timing support
        self._next_run_at: Optional[datetime] = None


class AgentManager:
    """Per-strategy AI agent manager.

    Live mode: schedules APScheduler cron/interval callbacks via Strategy.register_cron_callback.
    Backtest mode: ticks are emulated via on_iteration() with per-agent next_run_at.
    Decisions are accumulated thread-safely and drained on the strategy thread.
    """

    def __init__(self, strategy: Any, runner_factory: Optional[Callable[[Any], AgentRunner]] = None):
        self.strategy = strategy
        self._handles: Dict[str, AgentHandle] = {}
        self._jobs: Dict[str, str] = {}
        self._pending_decisions_lock = threading.Lock()
        self._pending_decisions: List[Dict[str, Any]] = []
        self._runner_factory = runner_factory or (lambda strat: AgentRunner(strat))
        self._runner = self._runner_factory(self.strategy)

    # Public API
    def create(self, name: str, prompt: str, cadence: str = "5m", allow_trading: bool = False, symbols: Optional[List[str]] = None,
               provider: Optional[str] = None, model: Optional[str] = None, search: bool = True) -> AgentHandle:
        if name in self._handles:
            raise ValueError(f"Agent '{name}' already exists")

        handle = AgentHandle(name=name, prompt=prompt, cadence=cadence, allow_trading=allow_trading, symbols=symbols,
                             provider=provider, model=model, search=search)
        self._handles[name] = handle

        if self.strategy.is_backtesting:
            # Initialize next run for backtests to now so it runs on the next iteration
            handle._next_run_at = self.strategy.get_datetime()
        else:
            # Live: schedule via APScheduler using Strategy.register_cron_callback
            # Interpret simple cadence like "5m" as cron: every 5 minutes
            job_id = self._schedule_live_job(handle)
            self._jobs[name] = job_id

        return handle

    def pause(self, name: str) -> None:
        handle = self._require(name)
        handle.paused = True

    def resume(self, name: str) -> None:
        handle = self._require(name)
        handle.paused = False

    def destroy(self, name: str) -> None:
        if name not in self._handles:
            return
        self._handles.pop(name)
        # Unschedule job in live: Strategy has no unschedule API in v1; rely on shutdown.
        if name in self._jobs:
            self._jobs.pop(name)

    # Live scheduling
    def _schedule_live_job(self, handle: AgentHandle) -> str:
        raw = handle.cadence.strip().lower()
        # If appears to be cron expression (5 fields), use cron
        is_cron = raw.count(" ") >= 4 and all(part for part in raw.split(" "))
        def callback():
            # Build decision off-thread (APScheduler thread) and store for later execution
            try:
                # Build snapshot for visibility/logging
                snapshot = build_snapshot(self.strategy, handle.symbols)
                # Lifecycle hook: on_ai_tick (before)
                try:
                    if hasattr(self.strategy, "on_ai_tick"):
                        self.strategy.on_ai_tick(handle.name, {
                            "phase": "before",
                            "handle": {"name": handle.name, "cadence": handle.cadence, "allow_trading": handle.allow_trading, "symbols": handle.symbols},
                            "snapshot": snapshot,
                        })
                except Exception:
                    pass

                decision_raw = self._runner.tick(handle)
                # Normalize decision so we always emit an "after" tick and logs
                decision = (
                    decision_raw if isinstance(decision_raw, dict)
                    else {"actions": [], "notes": "no decision"}
                )
                # Default logging on unless strategy disables
                if getattr(self.strategy, "ai_log_enabled", True):
                    try:
                        actions = decision.get("actions", []) if isinstance(decision, dict) else []
                        diag = decision.get("_diagnostics", {}) if isinstance(decision, dict) else {}
                        status = diag.get("status")
                        reason = diag.get("reason")
                        latency = diag.get("latency_ms")
                        tin = diag.get("tokens_in")
                        tout = diag.get("tokens_out")
                        tt = diag.get("tokens_total")
                        cit = diag.get("citations_count")
                        self.strategy.log_message(
                            f"[AI:{handle.name}] status={status} reason={reason} actions={len(actions)} notes={decision.get('notes', '')} "
                            f"latency={latency}ms tokens={tin}/{tout}/{tt} citations={cit}"
                        )
                    except Exception:
                        pass
                # Lifecycle hook: on_ai_tick (after)
                try:
                    if hasattr(self.strategy, "on_ai_tick"):
                        self.strategy.on_ai_tick(handle.name, {
                            "phase": "after",
                            "handle": {"name": handle.name, "cadence": handle.cadence, "allow_trading": handle.allow_trading, "symbols": handle.symbols},
                            "snapshot": snapshot,
                            "decision": decision,
                            "diagnostics": decision.get("_diagnostics", {}),
                        })
                except Exception:
                    pass
                # Only enqueue if actions exist
                if isinstance(decision, dict) and decision.get("actions"):
                    with self._pending_decisions_lock:
                        self._pending_decisions.append({
                            "agent_name": handle.name,
                            "decision": decision,
                            "allow_trading": handle.allow_trading,
                        })
            except Exception as e:
                # Defer to strategy hook on error on strategy thread at next drain
                with self._pending_decisions_lock:
                    self._pending_decisions.append({
                        "agent_name": handle.name,
                        "error": e,
                        "allow_trading": False,
                    })
        if is_cron:
            cron = raw
            job_id = self.strategy.register_cron_callback(cron, callback)
            return job_id
        # Determine duration
        td = _parse_cadence_to_timedelta(raw)
        # If seconds-level cadence, use interval job directly for live
        total_seconds = int(td.total_seconds())
        if total_seconds < 60:
            # schedule interval job with APScheduler directly
            scheduler = getattr(self.strategy, "_executor", None)
            if scheduler is None or getattr(scheduler, "scheduler", None) is None:
                # Fallback to minute cron if scheduler not ready
                cron = "* * * * *"
                return self.strategy.register_cron_callback(cron, callback)
            aps = scheduler.scheduler
            job_id = f"ai_agent_{id(self.strategy)}_{handle.name}"
            try:
                aps.add_job(callback, trigger="interval", seconds=max(1, total_seconds), id=job_id, jobstore="default", replace_existing=True)
            except Exception:
                # if add_job fails (e.g., duplicate), still return id
                pass
            return job_id
        # minutes or larger: build cron equivalent
        if total_seconds % 86400 == 0:
            days = total_seconds // 86400
            cron = f"0 0 */{max(1, days)} * *"
        elif total_seconds % 3600 == 0:
            hours = total_seconds // 3600
            cron = f"0 */{max(1, hours)} * * *"
        else:
            minutes = max(1, total_seconds // 60)
            cron = f"*/{minutes} * * * *"
        job_id = self.strategy.register_cron_callback(cron, callback)
        return job_id

    # Backtest emulation: call each iteration from strategy thread
    def on_iteration(self) -> None:
        if not self.strategy.is_backtesting:
            return
        now = self.strategy.get_datetime()
        for handle in self._handles.values():
            if handle.paused:
                continue
            if handle._next_run_at is None or now >= handle._next_run_at:
                # Tick synchronously (still no broker calls inside tick)
                try:
                    snapshot = build_snapshot(self.strategy, handle.symbols)
                    try:
                        if hasattr(self.strategy, "on_ai_tick"):
                            self.strategy.on_ai_tick(handle.name, {
                                "phase": "before",
                                "handle": {"name": handle.name, "cadence": handle.cadence, "allow_trading": handle.allow_trading, "symbols": handle.symbols},
                                "snapshot": snapshot,
                            })
                    except Exception:
                        pass
                    decision_raw = self._runner.tick(handle)
                    decision = (
                        decision_raw if isinstance(decision_raw, dict)
                        else {"actions": [], "notes": "no decision"}
                    )
                    if getattr(self.strategy, "ai_log_enabled", True):
                        try:
                            actions = decision.get("actions", []) if isinstance(decision, dict) else []
                            diag = decision.get("_diagnostics", {}) if isinstance(decision, dict) else {}
                            status = diag.get("status")
                            reason = diag.get("reason")
                            latency = diag.get("latency_ms")
                            tin = diag.get("tokens_in")
                            tout = diag.get("tokens_out")
                            tt = diag.get("tokens_total")
                            cit = diag.get("citations_count")
                            self.strategy.log_message(
                                f"[AI:{handle.name}] status={status} reason={reason} actions={len(actions)} notes={decision.get('notes', '')} "
                                f"latency={latency}ms tokens={tin}/{tout}/{tt} citations={cit}"
                            )
                        except Exception:
                            pass
                    try:
                        if hasattr(self.strategy, "on_ai_tick"):
                            self.strategy.on_ai_tick(handle.name, {
                                "phase": "after",
                                "handle": {"name": handle.name, "cadence": handle.cadence, "allow_trading": handle.allow_trading, "symbols": handle.symbols},
                                "snapshot": snapshot,
                                "decision": decision,
                                "diagnostics": decision.get("_diagnostics", {}),
                            })
                    except Exception:
                        pass
                    if isinstance(decision, dict) and decision.get("actions"):
                        with self._pending_decisions_lock:
                            self._pending_decisions.append({
                                "agent_name": handle.name,
                                "decision": decision,
                                "allow_trading": handle.allow_trading,
                            })
                except Exception as e:
                    with self._pending_decisions_lock:
                        self._pending_decisions.append({
                            "agent_name": handle.name,
                            "error": e,
                            "allow_trading": False,
                        })
                # schedule next
                delta = _parse_cadence_to_timedelta(handle.cadence)
                handle._next_run_at = now + delta

    # Strategy thread drain: to be called at start/end of on_trading_iteration
    def drain_pending(self) -> None:
        batch: List[Dict[str, Any]] = []
        with self._pending_decisions_lock:
            if self._pending_decisions:
                batch = self._pending_decisions
                self._pending_decisions = []

        for item in batch:
            agent_name = item.get("agent_name")
            error = item.get("error")
            if error is not None:
                try:
                    if hasattr(self.strategy, "on_ai_error"):
                        self.strategy.on_ai_error(agent_name, error, {})
                except Exception:
                    pass
                continue

            decision = item.get("decision") or {}
            # Hook: allow user to patch decision
            try:
                if hasattr(self.strategy, "on_ai_decision"):
                    patch = self.strategy.on_ai_decision(agent_name, decision)
                    if isinstance(patch, dict):
                        # shallow merge for top-level keys
                        decision.update(patch)
            except Exception:
                pass

            if item.get("allow_trading") and decision.get("actions"):
                # Execute actions on strategy thread
                try:
                    result = apply_actions_on_strategy(self.strategy, decision.get("actions", []))
                    try:
                        if hasattr(self.strategy, "on_ai_executed"):
                            self.strategy.on_ai_executed(agent_name, decision, result)
                    except Exception:
                        pass
                except Exception as exc:
                    try:
                        if hasattr(self.strategy, "on_ai_error"):
                            self.strategy.on_ai_error(agent_name, exc, {"decision": decision})
                    except Exception:
                        pass

    # Utilities
    def _require(self, name: str) -> AgentHandle:
        if name not in self._handles:
            raise KeyError(f"Unknown agent '{name}'")
        return self._handles[name]
