from __future__ import annotations

import sys
from collections.abc import Callable, Iterable
from functools import wraps
from types import FrameType
from typing import Any, ParamSpec, TypeVar, cast

P = ParamSpec("P")
R = TypeVar("R")


def staticdecorator(func: Any) -> Any:
    """Makes a staticmethod-decorated function executable as a decorator."""
    return func.__get__("")


def call_function_get_frame(  # noqa: UP047 - keep Python 3.11 parser compatibility.
    func: Callable[P, R], *args: P.args, **kwargs: P.kwargs
) -> tuple[FrameType | None, R]:
    """Call ``func`` and return the wrapped function's final local frame plus result.

    The hot path uses ``sys.setprofile`` so we capture only call/return events.
    When a debugger trace is attached, keep the existing trace function active
    and fall back to call-frame capture.
    """

    target_code = getattr(func, "__code__", None)
    previous_trace = sys.gettrace()
    previous_profile = sys.getprofile()
    frame: FrameType | None = None

    if previous_trace is None and previous_profile is None and target_code is not None:

        def capture_return(profile_frame: FrameType, event: str, _arg: Any) -> None:
            nonlocal frame
            if event == "return" and profile_frame.f_code is target_code:
                frame = profile_frame

        sys.setprofile(capture_return)
        try:
            result = func(*args, **kwargs)
        finally:
            sys.setprofile(previous_profile)
        return frame, result

    def capture_call(trace_frame: FrameType, event: str, arg: Any) -> Any:
        nonlocal frame
        if frame is None and event == "call" and (target_code is None or trace_frame.f_code is target_code):
            frame = trace_frame
        if previous_trace is not None:
            previous_trace(trace_frame, event, arg)
        return capture_call

    sys.settrace(capture_call)
    try:
        result = func(*args, **kwargs)
    finally:
        sys.settrace(previous_trace)

    return frame, result


def snatch_locals(store: dict[str, Any]) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator factory that stores the wrapped function's latest locals."""

    def wrapper(func_input: Callable[P, R]) -> Callable[P, R]:
        @wraps(func_input)
        def func_output(*args: P.args, **kwargs: P.kwargs) -> R:
            frame, result = call_function_get_frame(func_input, *args, **kwargs)
            store.clear()
            if frame is not None:
                store.update(frame.f_locals)
            return result

        return func_output

    return wrapper


def append_locals(func_input: Callable[P, R]) -> Callable[P, R]:  # noqa: UP047
    """Attach the wrapped function's latest locals to ``func_output.locals``."""

    @wraps(func_input)
    def func_output(*args: P.args, **kwargs: P.kwargs) -> R:
        frame, result = call_function_get_frame(func_input, *args, **kwargs)
        cast(Any, func_output).locals = dict(frame.f_locals) if frame is not None else None
        return result

    cast(Any, func_output).locals = None
    return func_output


def execute_after(actions: Iterable[Callable[[], Any]]) -> Callable[[Callable[P, Any]], Callable[P, None]]:
    def decorator_func(input_func: Callable[P, Any]) -> Callable[P, None]:
        @wraps(input_func)
        def output_func(*args: P.args, **kwargs: P.kwargs) -> None:
            input_func(*args, **kwargs)
            for action in actions:
                action()

        return output_func

    return decorator_func
