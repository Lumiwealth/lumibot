import sys
from functools import wraps
from types import SimpleNamespace


def staticdecorator(func):
    """Makes a function decorated with staticmethod executable"""
    return func.__get__("")


def call_function_get_frame(func, *args, **kwargs):
    """
    Calls func and returns a stable snapshot of its local frame and result.

    Python 3.13 exposes optimized-frame locals through a live proxy whose
    contents are no longer reliable after the frame returns. Capture a plain
    dictionary at the return event so callers retain the lifecycle method's
    actual locals instead of the wrapper's locals or an expired proxy.
    """
    target = getattr(func, "__func__", func)
    target_code = target.__code__
    previous_profile = sys.getprofile()
    locals_snapshot = None

    def capture_locals(frame, event, arg):
        nonlocal locals_snapshot
        if event == "return" and frame.f_code is target_code:
            locals_snapshot = dict(frame.f_locals)
        if previous_profile is not None:
            previous_profile(frame, event, arg)

    sys.setprofile(capture_locals)
    try:
        result = func(*args, **kwargs)
    finally:
        sys.setprofile(previous_profile)

    frame_snapshot = None if locals_snapshot is None else SimpleNamespace(f_locals=locals_snapshot)
    return frame_snapshot, result


def snatch_locals(store):
    """Snatch a function local variables
    and store them in store variable"""

    def wrapper(func_input):
        @wraps(func_input)
        def func_output(*args, **kwargs):
            global store
            frame, result = call_function_get_frame(func_input, *args, **kwargs)
            store = frame.f_locals
            return result

        return func_output

    return wrapper


def append_locals(func_input):
    """Snatch a function local variables
    and store them in store variable"""

    @wraps(func_input)
    def func_output(*args, **kwargs):
        frame, result = call_function_get_frame(func_input, *args, **kwargs)
        if frame is not None:
            func_output.locals = frame.f_locals
        else:
            func_output.locals = None
        return result

    return func_output


def execute_after(actions):
    def decorator_func(input_func):
        @wraps(input_func)
        def output_func(*args, **kwargs):
            input_func(*args, **kwargs)
            for action in actions:
                action()

        return output_func

    return decorator_func
