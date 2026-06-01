import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path


class ScheduledRunTiming:
    """Timing recorder for exact one-shot scheduled trading iterations."""

    def __init__(self, logger=None):
        self.logger = logger
        self._timing = {}

    @staticmethod
    def truthy(value):
        return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}

    def exact_enabled(self):
        return self.truthy(os.environ.get("LUMIBOT_SCHEDULED_EXECUTION")) and bool(
            os.environ.get("LUMIBOT_SCHEDULED_TARGET_RUN_AT")
        )

    @staticmethod
    def parse_datetime(raw_value):
        if raw_value in (None, ""):
            return None
        value = str(raw_value).strip()
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def now_utc():
        return datetime.now(timezone.utc)

    @staticmethod
    def iso(value):
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    @staticmethod
    def int_env(name, default):
        try:
            return int(os.environ.get(name, default))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def timing_path():
        raw_path = os.environ.get("LUMIBOT_SCHEDULED_TIMING_FILE")
        return Path(raw_path) if raw_path else None

    def record(self, **fields):
        if not self.exact_enabled():
            return
        payload = {key: value for key, value in fields.items() if value is not None}
        self._timing.update(payload)
        self.write()

    def write(self):
        path = self.timing_path()
        if not path:
            return
        payload = {
            "target_run_at": os.environ.get("LUMIBOT_SCHEDULED_TARGET_RUN_AT"),
            "pre_start_at": os.environ.get("LUMIBOT_SCHEDULED_PRE_START_AT"),
            "max_target_drift_ms": self.int_env("LUMIBOT_SCHEDULED_MAX_TARGET_DRIFT_MS", 1000),
            "post_iteration_seconds": self.int_env("LUMIBOT_SCHEDULED_POST_ITERATION_SECONDS", 0),
            **self._timing,
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = path.with_suffix(path.suffix + ".tmp")
            tmp_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
            tmp_path.replace(path)
        except Exception as exc:
            if self.logger:
                self.logger.warning("Failed to write scheduled timing JSON: %s", exc)

    def wait_until_target(self, now_utc=None, monotonic=None, sleep=None, log_message=None):
        if not self.exact_enabled():
            return True

        now_utc = now_utc or self.now_utc
        monotonic = monotonic or time.monotonic
        sleep = sleep or time.sleep

        target = self.parse_datetime(os.environ.get("LUMIBOT_SCHEDULED_TARGET_RUN_AT"))
        if target is None:
            return True

        max_drift_ms = self.int_env("LUMIBOT_SCHEDULED_MAX_TARGET_DRIFT_MS", 1000)
        wait_started = now_utc()
        monotonic_started = monotonic()
        self.record(
            wait_started_at=self.iso(wait_started),
            status="waiting_for_target",
            exact_timing_verified=False,
        )

        while True:
            now = now_utc()
            remaining_seconds = (target - now).total_seconds()
            if remaining_seconds <= 0:
                break
            sleep_seconds = min(1.0, remaining_seconds)
            if remaining_seconds > 0.05:
                sleep_seconds = min(sleep_seconds, max(remaining_seconds - 0.05, 0.001))
            sleep(max(sleep_seconds, 0.0))

        wait_finished = now_utc()
        drift_ms = int(round((wait_finished - target).total_seconds() * 1000))
        local_wait_seconds = max(monotonic() - monotonic_started, 0)
        if drift_ms > max_drift_ms:
            if log_message:
                log_message(f"Scheduled target missed by {drift_ms}ms; skipping trading iteration", color="red")
            self.record(
                wait_finished_at=self.iso(wait_finished),
                local_wait_seconds=local_wait_seconds,
                target_drift_ms=drift_ms,
                status="missed_target",
                exact_timing_verified=False,
            )
            return False

        self.record(
            wait_finished_at=self.iso(wait_finished),
            local_wait_seconds=local_wait_seconds,
            target_drift_ms=drift_ms,
            status="target_reached",
            exact_timing_verified=True,
        )
        return True

    def drain_after_iteration(self, stop_event, process_queue, now_utc=None, monotonic=None, sleep=None):
        if not self.truthy(os.environ.get("LUMIBOT_SCHEDULED_EXECUTION")):
            return

        post_iteration_seconds = self.int_env("LUMIBOT_SCHEDULED_POST_ITERATION_SECONDS", 0)
        if post_iteration_seconds <= 0:
            return

        now_utc = now_utc or self.now_utc
        monotonic = monotonic or time.monotonic
        sleep = sleep or time.sleep

        drain_started = now_utc()
        deadline = monotonic() + post_iteration_seconds
        self.record(
            drain_started_at=self.iso(drain_started),
            status="draining",
        )
        while not stop_event.is_set():
            process_queue()
            remaining_seconds = deadline - monotonic()
            if remaining_seconds <= 0:
                break
            sleep(min(0.5, remaining_seconds))
        process_queue()
        self.record(
            drain_finished_at=self.iso(now_utc()),
            status="drained",
        )
