"""Speed-ups for the end-of-backtest reports that never change a report number.

Two pieces of work used to sit on the critical path after every backtest:

1. Importing the report stack (QuantStats, scipy.stats, matplotlib). It only started once
   the simulation had finished. ``prewarm_report_imports()`` starts it in a background
   thread when a backtest that will write a tearsheet begins, so it overlaps with data
   setup and the simulation. ``wait_for_report_imports()`` joins it before the report is
   built. The modules are the same modules, so the output is identical.

2. Recomputing the drawdown table of the same drawdown series. QuantStats asks for
   ``stats.drawdown_details`` 13 times per tearsheet (metrics for the HTML, metrics again
   for the JSON, the worst-drawdowns table, the drawdown plot) for only a few distinct
   series. ``report_computation_cache()`` remembers the table per exact input (dates,
   values, dtype, name/columns) for the duration of one report and hands every caller its
   own copy, so callers cannot affect each other.

3. Downloading the risk-free rate (^IRX) after the simulation. ``prefetch_risk_free_rate()``
   starts it when the backtest starts (see the section below).

Switches (all on by default):
- ``LUMIBOT_PREFETCH_RISK_FREE_RATE=0`` turns the risk-free rate prefetch off.
- ``LUMIBOT_PREWARM_REPORT_IMPORTS=0`` turns the background import off.
- ``LUMIBOT_REPORT_DRAWDOWN_CACHE=0`` turns the table cache (drawdowns and period aggregations) off.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib
import os
import threading

_REPORT_MODULES = (
    "quantstats_lumi",
    "quantstats_lumi.stats",
    "quantstats_lumi.reports",
    "quantstats_lumi.plots",
)

_prewarm_lock = threading.Lock()
_prewarm_thread: threading.Thread | None = None


def _env_enabled(name: str) -> bool:
    value = os.environ.get(name)
    if value is None:
        return True
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _import_report_modules() -> None:
    for module_name in _REPORT_MODULES:
        try:
            importlib.import_module(module_name)
        except Exception:
            # The report path imports these again and reports any real error there.
            return


def prewarm_report_imports() -> threading.Thread | None:
    """Start importing the report stack in a daemon thread (once per process)."""
    global _prewarm_thread
    if not _env_enabled("LUMIBOT_PREWARM_REPORT_IMPORTS"):
        return None
    with _prewarm_lock:
        if _prewarm_thread is None:
            _prewarm_thread = threading.Thread(
                target=_import_report_modules,
                name="lumibot-report-import-prewarm",
                daemon=True,
            )
            _prewarm_thread.start()
        return _prewarm_thread


_prewarm_requested = False


def request_report_prewarm() -> None:
    """Start the background report import now, at backtest start.

    WHY: 4.6.99 delayed this thread to the first simulation step to reach the first step
    about 3 s sooner. A short backtest then reached its report while the thread was still
    importing matplotlib and read a half-imported module, so the tearsheet was replaced by
    a placeholder (intermittent). Starting here, as before 4.6.99, lets the import finish
    during data setup. Do not move it later without making every report path wait for it.
    """
    global _prewarm_requested
    _prewarm_requested = False
    prewarm_report_imports()


def clear_report_prewarm_request() -> None:
    """Drop a request left by an earlier backtest in this process that never took a step.

    Called at the start of every backtest so only that backtest's own settings decide
    whether the report import starts.
    """
    global _prewarm_requested
    _prewarm_requested = False


def start_requested_report_prewarm() -> None:
    """Start the requested background import (no-op when none was requested)."""
    global _prewarm_requested
    if not _prewarm_requested:
        return
    _prewarm_requested = False
    prewarm_report_imports()


def wait_for_report_imports() -> None:
    """Wait for a started background import so the report never races it."""
    thread = _prewarm_thread
    if thread is not None and thread is not threading.current_thread():
        thread.join()


# ---- Risk-free rate prefetch -------------------------------------------------------------
# The backtest statistics and tearsheet read the 13-week Treasury rate (^IRX) for the last
# simulated day. On a fresh machine that is a Yahoo download (two deliberate 2-5 s
# rate-limit sleeps plus the request: about 7-10 s) that used to start only after the
# simulation ended. The prefetch downloads the same series through the same YahooHelper
# cache path in a background thread when the backtest starts; the rate lookup joins it
# first, then reads the cached series exactly as before. Only YahooHelper (static, its own
# pickle cache and the thread-safe remote cache manager) is touched: no broker, router or
# data-source state.

_risk_free_lock = threading.Lock()
_risk_free_threads: dict[object, threading.Thread] = {}


def _prefetch_risk_free_series(last_needed_datetime) -> None:
    try:
        from lumibot.tools.yahoo_helper import YahooHelper

        YahooHelper.get_symbol_data("^IRX", last_needed_datetime=last_needed_datetime)
    except Exception:
        # The real lookup at report time fetches again and handles any error as before.
        return


def prefetch_risk_free_rate(last_needed_datetime) -> threading.Thread | None:
    """Start downloading the risk-free series needed up to ``last_needed_datetime``."""
    if last_needed_datetime is None or not _env_enabled("LUMIBOT_PREFETCH_RISK_FREE_RATE"):
        return None
    key = getattr(last_needed_datetime, "date", lambda: last_needed_datetime)()
    with _risk_free_lock:
        thread = _risk_free_threads.get(key)
        if thread is None:
            thread = threading.Thread(
                target=_prefetch_risk_free_series,
                args=(last_needed_datetime,),
                name="lumibot-risk-free-prefetch",
                daemon=True,
            )
            _risk_free_threads[key] = thread
            thread.start()
        return thread


def wait_for_risk_free_prefetch() -> None:
    """Wait for any started risk-free download so a lookup never downloads it twice."""
    with _risk_free_lock:
        threads = list(_risk_free_threads.values())
    current = threading.current_thread()
    for thread in threads:
        if thread is not current:
            thread.join()


def drawdown_cache_enabled() -> bool:
    return _env_enabled("LUMIBOT_REPORT_DRAWDOWN_CACHE")


def _fingerprint(obj) -> str | None:
    """Exact content key for a pandas Series/DataFrame, or None when not cacheable."""
    import numpy as np
    import pandas as pd

    if not isinstance(obj, (pd.Series, pd.DataFrame)):
        return None
    digest = hashlib.sha1()
    index = obj.index
    digest.update(type(obj).__name__.encode())
    digest.update(repr(obj.shape).encode())
    digest.update(str(index.dtype).encode())
    digest.update(str(getattr(index, "tz", None)).encode())
    digest.update(repr(index.name).encode())
    if hasattr(index, "asi8"):
        digest.update(np.ascontiguousarray(index.asi8).tobytes())
    else:
        digest.update(repr(index.tolist()).encode())
    if isinstance(obj, pd.Series):
        columns = [(obj.name, obj)]
    else:
        digest.update(repr(list(obj.columns)).encode())
        columns = [(label, obj.iloc[:, position]) for position, label in enumerate(obj.columns)]
    for label, column in columns:
        digest.update(repr(label).encode())
        digest.update(str(column.dtype).encode())
        values = column.to_numpy()
        if values.dtype == object:
            digest.update(repr(values.tolist()).encode())
        else:
            digest.update(np.ascontiguousarray(values).tobytes())
    return digest.hexdigest()


def _drawdown_details_key(args, kwargs):
    if kwargs or len(args) != 1:
        return None
    return _fingerprint(args[0])


def _aggregate_returns_key(args, kwargs):
    # aggregate_returns(returns, period=None, compounded=True). With no period (or a daily
    # one) it hands back the caller's own object; that identity is kept by not caching.
    params = dict(zip(("returns", "period", "compounded"), args))
    if len(args) > 3:
        return None
    params.update(kwargs)
    if set(params) - {"returns", "period", "compounded"}:
        return None
    period = params.get("period")
    if not isinstance(period, str) or "day" in period:
        return None
    compounded = params.get("compounded", True)
    if not isinstance(compounded, bool):
        return None
    returns_key = _fingerprint(params.get("returns"))
    if returns_key is None:
        return None
    return f"{returns_key}|{period}|{compounded}"


# (module, function, key builder). Each function is pure: the same input gives the same
# table. Callers receive a deep copy, so nothing one caller does reaches another.
_CACHED_FUNCTIONS = (
    ("quantstats_lumi.stats", "drawdown_details", _drawdown_details_key),
    ("quantstats_lumi.utils", "aggregate_returns", _aggregate_returns_key),
)


@contextlib.contextmanager
def report_computation_cache():
    """Cache pure QuantStats tables per exact input for the duration of one report."""
    if not drawdown_cache_enabled():
        yield
        return

    installed = []

    def _restore():
        # Restore only our own wrappers; never clobber a replacement made by someone else.
        for module, function_name, original, cached in reversed(installed):
            if getattr(module, function_name) is cached:
                setattr(module, function_name, original)

    try:
        _install_cached_functions(installed)
    except Exception:
        # Setup must never change the report outcome: undo any partial install and
        # build the report without the cache (the report keeps its own error handling).
        _restore()
        yield
        return
    try:
        yield
    finally:
        _restore()


def _install_cached_functions(installed):
    for module_name, function_name, key_builder in _CACHED_FUNCTIONS:
        module = importlib.import_module(module_name)
        original = getattr(module, function_name)
        cache: dict[str, object] = {}

        def cached(*args, _original=original, _cache=cache, _key_builder=key_builder, **kwargs):
            try:
                key = _key_builder(args, kwargs)
            except Exception:
                key = None
            if key is None:
                return _original(*args, **kwargs)
            if key not in _cache:
                _cache[key] = _original(*args, **kwargs)
            result = _cache[key]
            return result.copy(deep=True) if hasattr(result, "copy") else result

        setattr(module, function_name, cached)
        installed.append((module, function_name, original, cached))
