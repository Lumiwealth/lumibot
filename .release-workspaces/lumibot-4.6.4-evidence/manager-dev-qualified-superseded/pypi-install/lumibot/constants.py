"""
Lumibot Constants
=================

This module contains all the constants used throughout Lumibot.
These are defined here to avoid circular import issues.
"""

import os
import pytz
import appdirs

# SOURCE PATH
LUMIBOT_SOURCE_PATH = os.path.abspath(os.path.dirname(__file__))

# GLOBAL PARAMETERS
LUMIBOT_DEFAULT_TIMEZONE = "America/New_York"
LUMIBOT_DEFAULT_PYTZ = pytz.timezone(LUMIBOT_DEFAULT_TIMEZONE)
LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL = "USD"
LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE = "forex"

# CACHING CONFIGURATIONS
# Allow an explicit override so tests/CI can run with a fresh, isolated cache root without
# needing to mutate a developer machine's global cache directory.
#
# NOTE: This is read at import time. Callers must set `LUMIBOT_CACHE_FOLDER` in the environment
# before importing `lumibot` for it to take effect.
LUMIBOT_CACHE_FOLDER = os.environ.get("LUMIBOT_CACHE_FOLDER") or appdirs.user_cache_dir(
    appauthor="LumiWealth", appname="lumibot", version="1.0"
)
