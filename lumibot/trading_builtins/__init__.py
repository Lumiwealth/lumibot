"""Trading builtins package exports without eager stream imports."""

_NAME_TO_MODULE = {
    "CustomStream": "custom_stream",
    "PollingStream": "custom_stream",
    "SafeList": "safe_list",
    "SafeOrderDict": "safe_list",
}

__all__ = sorted(_NAME_TO_MODULE)


def __getattr__(name):
    module_name = _NAME_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from importlib import import_module

    module = import_module(f"{__name__}.{module_name}")
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
