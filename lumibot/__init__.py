import logging
import os
import sys
import warnings
import appdirs
import pytz
import importlib

# Get the major and minor Python version
major, minor = sys.version_info[:2]

# Check if Python version is less than 3.10
if (major, minor) < (3, 10):
    warnings.warn("Lumibot requires Python 3.10 or higher.", RuntimeWarning)

# SOURCE PATH
LUMIBOT_SOURCE_PATH = os.path.abspath(os.path.dirname(__file__))

# GLOBAL PARAMETERS
LUMIBOT_DEFAULT_TIMEZONE = "America/New_York"
LUMIBOT_DEFAULT_PYTZ = pytz.timezone(LUMIBOT_DEFAULT_TIMEZONE)
LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL = "USD"
LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE = "forex"

# CACHING CONFIGURATIONS
LUMIBOT_CACHE_FOLDER = appdirs.user_cache_dir(appauthor="LumiWealth", appname="lumibot", version="1.0")

if not os.path.exists(LUMIBOT_CACHE_FOLDER):
    try:
        os.makedirs(LUMIBOT_CACHE_FOLDER)
    except Exception as e:
        logging.critical(
            f"""Could not create cache folder because of the following error:
            {e}. Please fix the issue to use data caching."""
        )

# === Backward Compatibility Aliases ===
# Some older code and documentation may still refer to the top-level
# package name `entities` (e.g. `entities.asset`) that existed in
# earlier Lumibot versions.  To avoid breaking those references we
# expose `lumibot.entities` and its sub-modules under the legacy name
# when the library is imported.

# Map the root package alias.
import lumibot.entities as _lb_entities
sys.modules.setdefault("entities", _lb_entities)

# Expose common sub-modules (asset, bars, data, order, position, trading_fee)
# so that e.g. `import entities.asset` keeps working.
for _sub in ("asset", "bars", "data", "order", "position", "trading_fee"):
    _full = f"lumibot.entities.{_sub}"
    _alias = f"entities.{_sub}"
    try:
        _mod = importlib.import_module(_full)
        sys.modules[_alias] = _mod
    except ModuleNotFoundError as e:
        # If a particular sub-module was removed or fails to import (e.g., due to deeper issues like 'fp')
        # log a warning and skip. Sphinx will simply not find this specific alias.
        logging.warning(f"[lumibot/__init__.py] Could not create alias '{_alias}' for '{_full}': {e}")
        # Ensure the problematic alias isn't lingering if it was partially set or if it's a mock
        if _alias in sys.modules and isinstance(sys.modules[_alias], importlib.util.LazyLoader):
            pass # Don't remove if it's a lazy loader that might resolve later or differently
        elif _alias in sys.modules:
            # If it's a real module or a simple mock that failed, best to remove its alias attempt
            # to avoid Sphinx confusion with a potentially broken module object.
            # However, if Sphinx/autodoc is running, it might be safer to leave mocks as they are.
            # For now, we'll log and continue, letting Sphinx handle missing modules.
            pass # Reconsidering removal: could interfere with Sphinx's own mock handling
    except ImportError as e: # Catch other import-related errors
        logging.warning(f"[lumibot/__init__.py] ImportError while creating alias '{_alias}' for '{_full}': {e}")
    except Exception as e: # Catch any other unexpected errors during aliasing
        logging.warning(f"[lumibot/__init__.py] Unexpected error creating alias '{_alias}' for '{_full}': {e}")
