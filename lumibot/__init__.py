import importlib.util
import logging
import os
import re
import sys
import types
import warnings
from importlib.machinery import ModuleSpec

_SETUP_VERSION_RE = re.compile(r"^\s*version\s*=\s*(['\"])(?P<version>.+?)\1\s*,?\s*$")


def _read_version_from_setup_py() -> str | None:
    """Best-effort: when running from a source checkout via PYTHONPATH, prefer setup.py's version.

    This avoids the common confusion where importlib.metadata returns the *installed* wheel
    version (e.g. 4.4.16) while the runtime is actually importing source (e.g. 4.4.18).
    """

    try:
        current = os.path.dirname(os.path.abspath(__file__))
        while True:
            setup_py = os.path.join(current, "setup.py")
            if os.path.isfile(setup_py):
                with open(setup_py, encoding="utf-8", errors="ignore") as file:
                    for line in file:
                        match = _SETUP_VERSION_RE.match(line)
                        if match:
                            return match.group("version").strip()
            parent = os.path.dirname(current)
            if parent == current:
                break
            current = parent
    except Exception:
        pass
    return None


# Get and display the version
try:
    __version__ = _read_version_from_setup_py()
    if __version__ is None:
        from importlib.metadata import version

        __version__ = version("lumibot")
except ImportError:
    # Fallback for Python < 3.8
    try:
        import pkg_resources
        __version__ = pkg_resources.get_distribution("lumibot").version
    except:
        __version__ = "unknown"
except:
    __version__ = "unknown"


def _log_startup_version() -> None:
    level_name = os.environ.get("LUMIBOT_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger = logging.getLogger("lumibot")
    logger.setLevel(level)

    console_handlers = [
        handler
        for handler in logger.handlers
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler)
    ]
    if not console_handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
        console_handlers = [handler]

    for handler in console_handlers:
        handler.setLevel(level)
        if handler.formatter is None:
            handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

    logger.info(f"LumiBot v{__version__} starting")


_log_startup_version()

# Get the major and minor Python version
major, minor = sys.version_info[:2]

# Check if Python version is less than 3.10
if (major, minor) < (3, 10):
    warnings.warn("Lumibot requires Python 3.10 or higher.", RuntimeWarning)

# SOURCE PATH
LUMIBOT_SOURCE_PATH = os.path.dirname(os.path.abspath(__file__))
LUMIBOT_DEFAULT_TIMEZONE = "America/New_York"
LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL = "USD"
LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE = "forex"


def _default_cache_folder() -> str:
    env_value = os.environ.get("LUMIBOT_CACHE_FOLDER")
    if env_value:
        return env_value
    if sys.platform == "darwin":
        return os.path.expanduser("~/Library/Caches/lumibot/1.0")
    if os.name == "nt":
        root = os.environ.get("LOCALAPPDATA") or os.path.expanduser("~\\AppData\\Local")
        return os.path.join(root, "LumiWealth", "lumibot", "Cache", "1.0")
    root = os.environ.get("XDG_CACHE_HOME") or os.path.expanduser("~/.cache")
    return os.path.join(root, "lumibot", "1.0")


LUMIBOT_CACHE_FOLDER = _default_cache_folder()

# Ensure cache folder exists
if not os.path.exists(LUMIBOT_CACHE_FOLDER):
    try:
        os.makedirs(LUMIBOT_CACHE_FOLDER)
    except Exception as e:
        warnings.warn(
            f"""Could not create cache folder because of the following error:
            {e}. Please fix the issue to use data caching."""
        )

_SUBMODULE_EXPORTS = {
    "strategies",
    "brokers",
    "backtesting",
    "entities",
    "data_sources",
    "traders",
    "tools",
    "components",
    "constants",
    "credentials",
    "trading_builtins",
}


class _EntitiesAliasLoader:
    def create_module(self, spec):
        return None

    def exec_module(self, module):
        load = module.__dict__.get("_load")
        if load is not None:
            load()


_ENTITIES_ALIAS_LOADER = _EntitiesAliasLoader()


class _EntitiesAliasFinder:
    _lumibot_entities_alias_finder = True

    def find_spec(self, fullname, path=None, target=None):
        if fullname in _ENTITY_SUBMODULE_ALIAS_NAMES:
            return ModuleSpec(fullname, _ENTITIES_ALIAS_LOADER, is_package=False)
        if fullname == "entities":
            spec = ModuleSpec(fullname, _ENTITIES_ALIAS_LOADER, is_package=True)
            spec.submodule_search_locations = _EntitiesAlias.__path__
            return spec
        return None


class _EntitiesAlias(types.ModuleType):
    _lumibot_entities_alias = True
    __path__ = [os.path.join(os.path.dirname(os.path.abspath(__file__)), "entities")]
    __file__ = os.path.join(os.path.dirname(os.path.abspath(__file__)), "entities", "__init__.py")
    __package__ = "entities"

    def __init__(self, name: str):
        super().__init__(name)
        self.__spec__ = ModuleSpec(name, _ENTITIES_ALIAS_LOADER, is_package=True)
        self.__spec__.submodule_search_locations = self.__path__

    def __getattr__(self, name):
        import importlib

        entities_module = importlib.import_module("lumibot.entities")
        if name in getattr(entities_module, "__all__", ()):
            value = getattr(entities_module, name)
            setattr(self, name, value)
            return value

        alias = sys.modules.get(f"entities.{name}")
        if alias is not None:
            return alias

        try:
            module = importlib.import_module(f"lumibot.entities.{name}")
            sys.modules[f"entities.{name}"] = module
        except ModuleNotFoundError:
            module = importlib.import_module(f"entities.{name}")
        return module


class _EntitiesSubmoduleAlias(types.ModuleType):
    def __init__(self, alias_name: str, target_name: str):
        super().__init__(alias_name)
        self.__package__ = "entities"
        self.__spec__ = ModuleSpec(alias_name, _ENTITIES_ALIAS_LOADER, is_package=False)
        self._target_name = target_name
        self._target_module = None

    def _load(self):
        import importlib

        module = self._target_module
        if module is None:
            module = importlib.import_module(self._target_name)
            self._target_module = module
        return module

    def __getattr__(self, name):
        return getattr(self._load(), name)

    def __dir__(self):
        return dir(self._load())


_ENTITY_SUBMODULES = (
    "asset",
    "bar",
    "bars",
    "cash_event",
    "chains",
    "data",
    "data_polars",
    "dataline",
    "order",
    "position",
    "quote",
    "trading_fee",
    "trading_slippage",
    "smart_limit",
)
_ENTITY_SUBMODULE_ALIAS_NAMES = {f"entities.{_submodule}" for _submodule in _ENTITY_SUBMODULES}


def _has_entities_alias_finder() -> bool:
    return any(getattr(_finder, "_lumibot_entities_alias_finder", False) for _finder in sys.meta_path)


def _entities_package_exists() -> bool:
    existing = sys.modules.get("entities")
    if existing is not None and not getattr(existing, "_lumibot_entities_alias", False):
        return True
    if _has_entities_alias_finder():
        return False
    try:
        return importlib.util.find_spec("entities") is not None
    except (ImportError, AttributeError, ValueError):
        return False


if not _entities_package_exists():
    if not _has_entities_alias_finder():
        sys.meta_path.insert(0, _EntitiesAliasFinder())

    sys.modules.setdefault("entities", _EntitiesAlias("entities"))
    for _submodule in _ENTITY_SUBMODULES:
        sys.modules.setdefault(
            f"entities.{_submodule}",
            _EntitiesSubmoduleAlias(f"entities.{_submodule}", f"lumibot.entities.{_submodule}"),
        )


def __getattr__(name):
    if name == "LUMIBOT_DEFAULT_PYTZ":
        from .constants import LUMIBOT_DEFAULT_PYTZ

        globals()[name] = LUMIBOT_DEFAULT_PYTZ
        return LUMIBOT_DEFAULT_PYTZ
    if name in _SUBMODULE_EXPORTS:
        import importlib

        module = importlib.import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    if name == "get_logger":
        from lumibot.tools.lumibot_logger import get_logger

        globals()[name] = get_logger
        return get_logger
    if name == "logger":
        logger = __getattr__("get_logger")(__name__)
        globals()[name] = logger
        return logger
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(__all__))

# Export the default timezone constants so they can be imported by other modules
__all__ = [
    '__version__',
    'LUMIBOT_DEFAULT_TIMEZONE',
    'LUMIBOT_DEFAULT_PYTZ',
    'LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL',
    'LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE',
    'LUMIBOT_SOURCE_PATH',
    'LUMIBOT_CACHE_FOLDER',
    'strategies',
    'brokers',
    'backtesting',
    'entities',
    'data_sources',
    'traders',
    'tools',
    'components',
    'constants',
    'credentials',
    'trading_builtins',
    'get_logger',
    'logger',
]
