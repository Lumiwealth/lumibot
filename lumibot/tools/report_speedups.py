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

Switches (both on by default):
- ``LUMIBOT_PREWARM_REPORT_IMPORTS=0`` turns the background import off.
- ``LUMIBOT_REPORT_DRAWDOWN_CACHE=0`` turns the drawdown-table cache off.
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


def wait_for_report_imports() -> None:
    """Wait for a started background import so the report never races it."""
    thread = _prewarm_thread
    if thread is not None and thread is not threading.current_thread():
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


@contextlib.contextmanager
def report_computation_cache():
    """Cache QuantStats drawdown tables per exact input for the duration of one report."""
    if not drawdown_cache_enabled():
        yield
        return

    import quantstats_lumi.stats as qs_stats

    original = qs_stats.drawdown_details
    cache: dict[str, object] = {}

    def cached_drawdown_details(drawdown):
        try:
            key = _fingerprint(drawdown)
        except Exception:
            key = None
        if key is None:
            return original(drawdown)
        if key not in cache:
            cache[key] = original(drawdown)
        result = cache[key]
        return result.copy(deep=True) if hasattr(result, "copy") else result

    qs_stats.drawdown_details = cached_drawdown_details
    try:
        yield
    finally:
        # Restore only our own wrapper; never clobber a replacement made by someone else.
        if qs_stats.drawdown_details is cached_drawdown_details:
            qs_stats.drawdown_details = original
