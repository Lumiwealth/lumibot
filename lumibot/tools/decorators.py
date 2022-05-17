import sys
from asyncio.log import logger
from copy import deepcopy
from functools import wraps


def staticdecorator(func):
    """Makes a function decorated with staticmethod executable"""
    return func.__get__("")


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
        return trace

    if trace is None:
        sys.settrace(snatch_locals)
        try:
            result = func(*args, **kwargs)
        finally:
            sys.settrace(trace)
    else:
        result = func(*args, **kwargs)

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
