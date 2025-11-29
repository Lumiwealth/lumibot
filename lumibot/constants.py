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
LUMIBOT_CACHE_FOLDER = appdirs.user_cache_dir(appauthor="LumiWealth", appname="lumibot", version="1.0") 