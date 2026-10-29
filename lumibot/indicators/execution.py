"""How indicators run: one small, replaceable hook.

By default an indicator runs in the same Python process as the code that
uses it. That is all most people need, and indicator authors never have to
think about this module.

A program that hosts indicators for other people (a chart server, a notebook
service, a platform that runs code it did not write) may want indicator code
to run somewhere else: another process, a container, a remote worker. It
registers an :class:`IndicatorExecutor`. Every place LumiBot creates an
indicator instance to run (:class:`~lumibot.indicators.IndicatorRunner` and
``self.indicators.get(...)`` inside strategies) hands the new instance to the
active executor's :meth:`~IndicatorExecutor.prepare` and uses what it
returns.

The returned object must behave like the indicator for these members:
``key``, ``version``, ``inputs``, ``outputs``, ``update(bar, closed)``,
``reset(inputs)`` and ``describe(inputs)``. Anything ``prepare`` returns that
forwards those calls to another process works. Results must match the
in-process numbers exactly; LumiBot's own tests check a process-backed
executor against in-process runs.

::

    from lumibot.indicators import IndicatorExecutor, use_indicator_executor

    class LoggingExecutor(IndicatorExecutor):
        name = "logging"

        def prepare(self, indicator):
            print("running", indicator.key, indicator.inputs)
            return indicator            # still in-process

    with use_indicator_executor(LoggingExecutor()):
        runner.add("rsi", length=14)
"""

from __future__ import annotations

import threading
from contextlib import contextmanager


class IndicatorExecutor:
    """Decides where an indicator instance runs. Subclass and override
    :meth:`prepare`."""

    #: Short name for logs and diagnostics.
    name = "custom"

    def prepare(self, indicator):
        """Return the object callers will use for ``indicator``.

        Called once per indicator instance, before any bar is processed.
        The default returns ``indicator`` itself (in-process)."""
        return indicator

    def close(self) -> None:
        """Release anything the executor holds (processes, connections)."""


class InProcessExecutor(IndicatorExecutor):
    """The default: indicators run in the caller's process."""

    name = "in_process"


_DEFAULT = InProcessExecutor()
_lock = threading.Lock()
_active: IndicatorExecutor = _DEFAULT


def get_indicator_executor() -> IndicatorExecutor:
    """The executor currently used for new indicator instances."""
    return _active


def set_indicator_executor(executor: IndicatorExecutor | None) -> IndicatorExecutor:
    """Use ``executor`` for indicator instances created from now on.

    ``None`` restores the in-process default. Returns the previous executor
    so it can be put back. Instances created earlier keep the executor they
    were prepared with. The setting is process-wide."""
    global _active
    if executor is None:
        executor = _DEFAULT
    if not callable(getattr(executor, "prepare", None)):
        raise TypeError(f"an indicator executor needs a prepare(indicator) method, got {executor!r}")
    with _lock:
        previous, _active = _active, executor
    return previous


@contextmanager
def use_indicator_executor(executor: IndicatorExecutor | None):
    """Temporarily use ``executor``; the previous one is restored on exit,
    including when an error is raised."""
    previous = set_indicator_executor(executor)
    try:
        yield executor
    finally:
        set_indicator_executor(previous)


def prepare_indicator(indicator, executor: IndicatorExecutor | None = None):
    """Hand ``indicator`` to ``executor`` (or the active one) and return the
    object to run. Used by LumiBot's own indicator call sites."""
    return (executor or _active).prepare(indicator)


__all__ = [
    "InProcessExecutor",
    "IndicatorExecutor",
    "get_indicator_executor",
    "prepare_indicator",
    "set_indicator_executor",
    "use_indicator_executor",
]
