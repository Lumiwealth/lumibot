import logging
import os
import sys
import warnings
import appdirs
import pytz

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
