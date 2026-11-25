"""Opt-in saved memory for live runs: keep ``self.vars`` in a local JSON file.

Why this exists: a live bot that restarts (new machine, crash, redeploy) loses
everything in ``self.vars``. The database backup (``DB_CONNECTION_STR``) can
persist it, but it reloads ``self.vars`` from the database before every
iteration and saves only after one, so values set in ``on_filled_order`` or
``before_market_opens`` can be overwritten. This module is a simpler, separate
path with different rules:

* Off unless ``LUMIBOT_LIVE_VARS_FILE`` names a file. Unset means no file is
  read or written and behaviour is unchanged.
* Never active for backtests or scheduled one-shot runs (those have their own
  state file).
* Read ONCE, right after ``initialize()`` and before any other lifecycle
  method, so saved values win over defaults set in ``initialize()``. Never read
  again during the run: in-memory ``self.vars`` is the only truth while running.
* Written atomically (temporary file, fsync, rename) only when the content
  changed: after an iteration once ``LUMIBOT_LIVE_VARS_SAVE_INTERVAL_SECONDS``
  (default 600) has passed, right after fill / partial fill / cancel events,
  on graceful exit, and on SIGTERM when the handler could be installed.
* Only JSON values (plus the Asset / datetime / date / tuple envelopes the
  database backup already uses) are saved. A key that cannot be saved is
  skipped with a warning naming the key; the bot never crashes for it.
* Does not touch the database backup, the scheduled state file or Discord.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
import signal
import threading
import time
from typing import Callable, Optional

ENV_FILE = "LUMIBOT_LIVE_VARS_FILE"
ENV_INTERVAL = "LUMIBOT_LIVE_VARS_SAVE_INTERVAL_SECONDS"
DEFAULT_SAVE_INTERVAL_SECONDS = 600
FORMAT_KEY = "lumibot_live_vars"
FORMAT_VERSION = 1

# Module-level clock so tests can move time without patching ``time`` globally.
_monotonic = time.monotonic


def _strategy_codec():
    # Imported lazily: _strategy imports the executor, which imports this module.
    from lumibot.strategies._strategy import _Strategy

    return _Strategy


def _parse_interval(raw) -> float:
    if raw is None or str(raw).strip() == "":
        return float(DEFAULT_SAVE_INTERVAL_SECONDS)
    try:
        value = float(str(raw).strip())
    except ValueError:
        return float(DEFAULT_SAVE_INTERVAL_SECONDS)
    if value != value or value < 0:  # NaN or negative
        return float(DEFAULT_SAVE_INTERVAL_SECONDS)
    return value


class LiveVarsFile:
    """Saves and restores one strategy's ``self.vars`` to one local file."""

    def __init__(self, strategy, path: str, interval_seconds: float = DEFAULT_SAVE_INTERVAL_SECONDS):
        self.strategy = strategy
        self.path = os.path.abspath(path)
        self.interval_seconds = float(interval_seconds)
        self._loaded = False
        self._last_hash: Optional[str] = None
        self._last_save_at: Optional[float] = None
        self._warned_keys: set = set()
        self._lock = threading.RLock()

    # ------------------------------------------------------------ construction

    @classmethod
    def from_environment(cls, strategy, run_once: bool = False) -> Optional["LiveVarsFile"]:
        path = str(os.environ.get(ENV_FILE, "") or "").strip()
        if not path:
            return None
        if getattr(strategy, "is_backtesting", False):
            return None
        if run_once:
            # Scheduled one-shot runs keep their own state file; never mix the two.
            return None
        return cls(strategy, path, _parse_interval(os.environ.get(ENV_INTERVAL)))

    @property
    def _logger(self):
        return self.strategy.logger

    # ------------------------------------------------------------ loading

    def load_once(self) -> bool:
        """Restore saved values into ``self.vars``. Only the first call reads the file."""
        with self._lock:
            if self._loaded:
                return False
            self._loaded = True
            try:
                with open(self.path, "r", encoding="utf-8") as handle:
                    raw = handle.read()
            except FileNotFoundError:
                self._logger.info("Saved strategy memory: no file yet, starting with empty self.vars")
                return False
            except OSError as exc:
                self._logger.warning(f"Saved strategy memory could not be read ({exc}); starting with empty self.vars")
                return False

            try:
                payload = json.loads(raw)
                saved = payload.get("vars") if isinstance(payload, dict) and FORMAT_KEY in payload else payload
                if not isinstance(saved, dict):
                    raise ValueError("expected a JSON object")
            except (ValueError, AttributeError) as exc:
                self._logger.warning(
                    f"Saved strategy memory could not be read ({exc}); starting with empty self.vars"
                )
                self._set_aside_corrupt_file()
                return False

            codec = _strategy_codec()
            restored = 0
            current = self.strategy.vars.all()
            for key, encoded in saved.items():
                try:
                    value = codec._decode_variable_from_backup(encoded)
                    value = codec._restore_legacy_assets(value, current.get(key))
                except Exception as exc:
                    self._logger.warning(f"Saved strategy memory: key {key!r} could not be restored ({exc}); skipped")
                    continue
                self.strategy.vars.set(key, value)
                restored += 1

            # What is on disk now matches memory for the restored keys; the first
            # save writes only if something differs.
            self._last_hash = self._hash(self._encode_all()[0])
            self._logger.info(f"Saved strategy memory restored: {restored} variable(s)")
            return True

    def _set_aside_corrupt_file(self):
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        try:
            os.replace(self.path, f"{self.path}.corrupt-{stamp}")
        except OSError:
            pass

    # ------------------------------------------------------------ saving

    def _encode_all(self):
        codec = _strategy_codec()
        encoded = {}
        skipped = []
        for key, value in self.strategy.vars.all().items():
            try:
                item = codec._encode_variable_for_backup(value)
                # Strict check: no fallback encoder, so unknown objects fail here.
                json.dumps(item)
            except (TypeError, ValueError, OverflowError, RecursionError) as exc:
                skipped.append(key)
                if key not in self._warned_keys:
                    self._warned_keys.add(key)
                    self._logger.warning(
                        f"Saved strategy memory: self.vars key {key!r} is not JSON-serializable "
                        f"({type(value).__name__}: {exc}); it is not saved"
                    )
                continue
            encoded[key] = item
        return encoded, skipped

    @staticmethod
    def _hash(encoded) -> str:
        text = json.dumps(encoded, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def maybe_save_after_iteration(self) -> bool:
        with self._lock:
            if not self._loaded:
                return False
            if self._last_save_at is not None and _monotonic() - self._last_save_at < self.interval_seconds:
                return False
            return self.save("interval")

    def save(self, reason: str, force: bool = False) -> bool:
        """Write the file if its content changed. Never raises. Returns True when written.

        ``force`` is accepted for readability at call sites (order events, exit):
        those ignore the interval, which only ``maybe_save_after_iteration`` applies.
        """
        with self._lock:
            if not self._loaded:
                # Never write before the saved file was read: that would replace the
                # saved memory with whatever the strategy holds before restoring it.
                return False
            try:
                encoded, _ = self._encode_all()
                digest = self._hash(encoded)
                if digest == self._last_hash:
                    self._last_save_at = _monotonic()
                    return False
                self._write_atomic(encoded)
            except Exception as exc:
                self._logger.error(f"Saved strategy memory could not be written ({reason}): {exc}")
                return False
            self._last_hash = digest
            self._last_save_at = _monotonic()
            self._logger.debug(f"Saved strategy memory written ({reason})")
            return True

    def _write_atomic(self, encoded):
        directory = os.path.dirname(self.path) or "."
        os.makedirs(directory, exist_ok=True)
        payload = {
            FORMAT_KEY: FORMAT_VERSION,
            "saved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "vars": encoded,
        }
        data = json.dumps(payload, sort_keys=True).encode("utf-8")
        tmp_path = f"{self.path}.{os.getpid()}.{threading.get_ident()}.tmp"
        try:
            fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                view = memoryview(data)
                while view:
                    written = os.write(fd, view)
                    view = view[written:]
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp_path, self.path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        # Make the rename itself durable where the platform allows it.
        try:
            dir_fd = os.open(directory, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)


def _make_sigterm_handler(store, previous) -> Callable:
    """Save on SIGTERM, then hand the signal to whatever handled it before.

    ``store`` is a ``LiveVarsFile`` or a zero-argument callable that saves.
    """

    def handler(signum, frame):
        try:
            if hasattr(store, "save"):
                store.save("sigterm", force=True)
            else:
                store()
        except Exception:
            pass
        if callable(previous):
            previous(signum, frame)
            return
        if previous == signal.SIG_IGN:
            return
        # Default action: terminate exactly as if no handler had been installed.
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)

    return handler


def install_sigterm_save(store) -> bool:
    """Install the SIGTERM save hook. Only possible from the main thread; otherwise a no-op."""
    sigterm = getattr(signal, "SIGTERM", None)
    if sigterm is None or threading.current_thread() is not threading.main_thread():
        return False
    try:
        previous = signal.getsignal(sigterm)
        signal.signal(sigterm, _make_sigterm_handler(store, previous))
    except (ValueError, OSError):
        return False
    return True
