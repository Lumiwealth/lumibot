import sys
from copy import deepcopy
from functools import wraps


def call_function_get_frame(func, *args, **kwargs):
    """
    Calls the function *func* with the specified arguments and keyword
    arguments and snatches its local frame before it actually executes.
    """

    frame = None
    trace = sys.gettrace()

    def snatch_locals(_frame, name, arg):
        nonlocal frame
        if frame is None and name == "call":
            frame = _frame
            sys.settrace(trace)
        return trace

    sys.settrace(snatch_locals)
    try:
        result = func(*args, **kwargs)
    finally:
        sys.settrace(trace)
    return frame, result


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


def snatch_method_locals(name, snapshot_before=False, copy_method=deepcopy):
    """snatch a class method locals and
    stores in an instance variable"""

    if not isinstance(name, str):
        raise ValueError(
            "snatch_method_locals must receive a string as input, received %r instead"
            % name
        )

    def wrapper(func_input):
        @wraps(func_input)
        def func_output(*args, **kwargs):
            instance_copy = None
            if snapshot_before and hasattr(func_input, "__self__"):
                instance_copy = copy_method(func_input.__self__.__dict__)

            frame, result = call_function_get_frame(func_input, *args, **kwargs)
            store = frame.f_locals
            if instance_copy:
                store["snapshot_before"] = instance_copy
            instance = store.pop("self")
            instance.__dict__[name] = store
            return result

        return func_output

    return wrapper


def execute_after(actions):
    def decorator_func(input_func):
        @wraps(input_func)
        def output_func(*args, **kwargs):
            input_func(*args, **kwargs)
            for action in actions:
                action()

        return output_func

    return decorator_func
