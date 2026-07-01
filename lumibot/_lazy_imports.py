"""Small lazy import helpers for startup-sensitive modules."""

from importlib import import_module
import os
import sys
from types import ModuleType


class LazyModule(ModuleType):
    """Module-like proxy that imports the target module on first real use."""

    def __init__(self, module_name: str):
        super().__init__(module_name)
        super().__setattr__("_module_name", module_name)
        super().__setattr__("_module", None)

    def _load(self):
        module = super().__getattribute__("_module")
        if module is None:
            module = import_module(super().__getattribute__("_module_name"))
            super().__setattr__("_module", module)
        return module

    def __getattr__(self, name):
        return getattr(self._load(), name)

    def __setattr__(self, name, value):
        if name in {"_module_name", "_module"}:
            super().__setattr__(name, value)
            return
        setattr(self._load(), name, value)

    def __delattr__(self, name):
        if name in {"_module_name", "_module"}:
            super().__delattr__(name)
            return
        delattr(self._load(), name)

    def __dir__(self):
        return dir(self._load())


class LazyClassMeta(type):
    """Class-like proxy that imports a target class on first construction/use."""

    def __new__(mcls, name, bases, namespace, **kwargs):
        resolved_bases = []
        changed = False
        for base in bases:
            if isinstance(base, LazyClassMeta) and "_module_name" in base.__dict__:
                resolved_bases.append(base._load())
                changed = True
            else:
                resolved_bases.append(base)

        if changed:
            return type(name, tuple(resolved_bases), dict(namespace))
        return super().__new__(mcls, name, bases, namespace, **kwargs)

    def _load(cls):
        target_class = getattr(cls, "_target_class", None)
        if target_class is None:
            module = import_module(cls._module_name)
            target_class = getattr(module, cls._class_name)
            setattr(cls, "_target_class", target_class)
        return target_class

    def __call__(cls, *args, **kwargs):
        return cls._load()(*args, **kwargs)

    @property
    def __signature__(cls):
        import inspect

        return inspect.signature(cls._load())

    def __getattr__(cls, name):
        return getattr(cls._load(), name)

    def __instancecheck__(cls, instance):
        return isinstance(instance, cls._load())

    def __subclasscheck__(cls, subclass):
        return issubclass(subclass, cls._load())

    def __dir__(cls):
        return dir(cls._load())

    def __repr__(cls):
        target_class = getattr(cls, "_target_class", None)
        if target_class is not None:
            return repr(target_class)
        return f"<lazy class {cls._module_name}.{cls._class_name}>"


def lazy_class(module_name: str, class_name: str):
    """Return a class-like lazy proxy for a target class."""

    return LazyClassMeta(
        class_name,
        (),
        {
            "__module__": module_name,
            "__doc__": f"Lazy proxy for {module_name}.{class_name}.",
            "_module_name": module_name,
            "_class_name": class_name,
            "_target_class": None,
        },
    )


class LazyTypingName:
    """Proxy for typing names used by stringified annotations."""

    __slots__ = ("_name", "_value")

    def __init__(self, name: str):
        self._name = name
        self._value = None

    def _load(self):
        value = self._value
        if value is None:
            import typing

            value = getattr(typing, self._name)
            self._value = value
        return value

    def __getitem__(self, item):
        return self._load()[item]

    def __getattr__(self, name):
        return getattr(self._load(), name)

    def __call__(self, *args, **kwargs):
        return self._load()(*args, **kwargs)

    def __or__(self, other):
        return self._load() | other

    def __ror__(self, other):
        return other | self._load()

    def __repr__(self):
        return f"<lazy typing.{self._name}>"


def lazy_typing(name: str):
    """Return a lazy proxy for a typing module export."""

    return LazyTypingName(name)


_LOG_LEVELS = {
    "CRITICAL": 50,
    "FATAL": 50,
    "ERROR": 40,
    "WARNING": 30,
    "WARN": 30,
    "INFO": 20,
    "DEBUG": 10,
    "NOTSET": 0,
}


def _configured_log_level() -> int:
    return _LOG_LEVELS.get(os.environ.get("LUMIBOT_LOG_LEVEL", "INFO").upper(), 20)


class LazyLogger:
    """Logger proxy that imports LumiBot logging only when a message should emit."""

    __slots__ = ("_name", "_logger", "__dict__")

    def __init__(self, name: str):
        self._name = name
        self._logger = None

    def _load(self):
        if self._logger is None:
            from lumibot.tools.lumibot_logger import get_logger

            self._logger = get_logger(self._name)
        return self._logger

    def _log(self, level: int, method: str, *args, **kwargs):
        if self._logger is not None or "lumibot.tools.lumibot_logger" in sys.modules or level >= _configured_log_level():
            getattr(self._load(), method)(*args, **kwargs)

    def debug(self, *args, **kwargs):
        self._log(10, "debug", *args, **kwargs)

    def info(self, *args, **kwargs):
        self._log(20, "info", *args, **kwargs)

    def warning(self, *args, **kwargs):
        self._log(30, "warning", *args, **kwargs)

    def error(self, *args, **kwargs):
        self._log(40, "error", *args, **kwargs)

    def exception(self, *args, **kwargs):
        kwargs.setdefault("exc_info", True)
        self.error(*args, **kwargs)

    def isEnabledFor(self, level: int):
        if self._logger is not None or "lumibot.tools.lumibot_logger" in sys.modules:
            return self._load().isEnabledFor(level)
        return level >= _configured_log_level()

    def __getattr__(self, name):
        return getattr(self._load(), name)


class LazyStrategyLogger:
    """Strategy logger proxy that defers LumiBot logger imports until first emitted message."""

    __slots__ = ("_name", "_strategy_name", "_logger", "__dict__")

    def __init__(self, name: str, strategy_name: str):
        self._name = name
        self._strategy_name = strategy_name
        self._logger = None

    def _load(self):
        if self._logger is None:
            from lumibot.tools.lumibot_logger import get_strategy_logger

            self._logger = get_strategy_logger(self._name, self._strategy_name)
        return self._logger

    def _log(self, level: int, method: str, *args, **kwargs):
        if self._logger is not None or "lumibot.tools.lumibot_logger" in sys.modules or level >= _configured_log_level():
            getattr(self._load(), method)(*args, **kwargs)

    def debug(self, *args, **kwargs):
        self._log(10, "debug", *args, **kwargs)

    def info(self, *args, **kwargs):
        self._log(20, "info", *args, **kwargs)

    def warning(self, *args, **kwargs):
        self._log(30, "warning", *args, **kwargs)

    def error(self, *args, **kwargs):
        self._log(40, "error", *args, **kwargs)

    def exception(self, *args, **kwargs):
        kwargs.setdefault("exc_info", True)
        self.error(*args, **kwargs)

    def isEnabledFor(self, level: int):
        if self._logger is not None or "lumibot.tools.lumibot_logger" in sys.modules:
            return self._load().isEnabledFor(level)
        return level >= _configured_log_level()

    def update_strategy_name(self, strategy_name: str):
        self._strategy_name = strategy_name
        if self._logger is not None:
            self._logger.update_strategy_name(strategy_name)

    def __getattr__(self, name):
        return getattr(self._load(), name)


class LazyPytzTimezoneRef:
    """Descriptor/proxy that loads LazyPytzTimezone only when timezone is accessed."""

    __isabstractmethod__ = False
    __slots__ = ("_timezone_name", "_timezone")

    def __init__(self, timezone_name: str):
        self._timezone_name = timezone_name
        self._timezone = None

    def _load(self):
        if self._timezone is None:
            from lumibot._lazy_timezone import LazyPytzTimezone

            self._timezone = LazyPytzTimezone(self._timezone_name)
        return self._timezone

    def __get__(self, instance, owner):
        return self._load()

    def __getattr__(self, name):
        return getattr(self._load(), name)

    def __repr__(self):
        return f"<lazy timezone {self._timezone_name}>"


def __getattr__(name):
    if name == "LazyPytzTimezone":
        from lumibot._lazy_timezone import LazyPytzTimezone

        return LazyPytzTimezone
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
