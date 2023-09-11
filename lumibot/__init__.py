import logging
import os

import appdirs
import pytz

# SOURCE PATH
LUMIBOT_SOURCE_PATH = os.path.abspath(os.path.dirname(__file__))

# GLOBAL PARAMETERS
LUMIBOT_DEFAULT_TIMEZONE = "America/New_York"
LUMIBOT_DEFAULT_PYTZ = pytz.timezone(LUMIBOT_DEFAULT_TIMEZONE)

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
