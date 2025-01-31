# NOTE: 
# This file is not meant to be modified. This file loads the credentials from the ".env" file or secrets and sets them as environment variables.
# If you want to set the environment variables on your computer, you can do so by creating a ".env" file in the root directory of the project
# and adding the variables described in the "Secrets Configuration" section of the README.md file like this (but without the "# " at the front):
# IS_BACKTESTING=True
# POLYGON_API_KEY=p0izKxeskywlLjKi82NLrQPUvSzvlYVT
# etc.

import os
import sys

from .brokers import Alpaca, Ccxt, InteractiveBrokers, InteractiveBrokersREST, Tradier
import logging
from dotenv import load_dotenv
import termcolor
import datetime

# Configure logging
logger = logging.getLogger(__name__)


def find_and_load_dotenv(base_dir) -> bool:
    for root, dirs, files in os.walk(base_dir):
        logger.debug(f"Checking {root} for .env file")
        if '.env' in files:
            dotenv_path = os.path.join(root, '.env')
            load_dotenv(dotenv_path)

            # Create a colored message for the log using termcolor
            colored_message = termcolor.colored(f".env file loaded from: {dotenv_path}", "green")
            logger.info(colored_message)
            return True

    return False


# Get the directory of the original script being run
script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
logger.debug(f"script_dir: {script_dir}")
found_dotenv = find_and_load_dotenv(script_dir)

if not found_dotenv:
    # Get the root directory of the project
    cwd_dir = os.getcwd()
    logger.debug(f"cwd_dir: {cwd_dir}")
    found_dotenv = find_and_load_dotenv(cwd_dir)

# If no .env file was found, print a warning message
if not found_dotenv:
    # Create a colored message for the log using termcolor
    colored_message = termcolor.colored("No .env file found. This is ok if you are using environment variables or secrets (like on Replit, AWS, etc), but if you are not, please create a .env file in the root directory of the project.", "yellow")
    logger.warning(colored_message)

# dotenv.load_dotenv()
broker=None

# Check if we are backtesting or not
is_backtesting = os.environ.get("IS_BACKTESTING")
if not is_backtesting or is_backtesting.lower() == "false":
    IS_BACKTESTING = False
elif is_backtesting.lower() == "true":
    IS_BACKTESTING = True
else:
    # Log a warning if the value is not a boolean
    colored_message = termcolor.colored(f"IS_BACKTESTING must be set to 'true' or 'false'. Got '{is_backtesting}'. Defaulting to False.", "yellow")
    logger.warning(colored_message)
    IS_BACKTESTING = False

# Get the backtesting start and end dates
backtesting_start = os.environ.get("BACKTESTING_START")
backtesting_end = os.environ.get("BACKTESTING_END")

# Check if backtesting is enabled but no start and end dates are provided
if IS_BACKTESTING and (not backtesting_start or not backtesting_end):
    # Warn the user that backtesting is enabled but no start and end dates are provided
    colored_message = termcolor.colored("Backtesting is enabled but no start and end dates are provided. Defaulting to the last year.", "yellow")
    logger.warning(colored_message)

    # Set a default start and end date for backtesting
    BACKTESTING_START = datetime.datetime.now() - datetime.timedelta(days=365)
    BACKTESTING_END = datetime.datetime.now()
else:
    # Convert the start and end dates to datetime objects in a way thats very forgiving
    from dateutil import parser

    # Check if the dates are not None and not empty strings before parsing
    if backtesting_start and backtesting_end:
        BACKTESTING_START = parser.parse(backtesting_start)
        BACKTESTING_END = parser.parse(backtesting_end)

# Check if we should hide trades
hide_trades = os.environ.get("HIDE_TRADES")
if not hide_trades or hide_trades.lower() == "false":
    HIDE_TRADES = False
elif hide_trades.lower() == "true":
    HIDE_TRADES = True
else:
    # Log a warning if the value is not a boolean
    colored_message = termcolor.colored(f"HIDE_TRADES must be set to 'true' or 'false'. Got '{hide_trades}'. Defaulting to False.", "yellow")
    logger.warning(colored_message)
    HIDE_TRADES = False

# Check if we should hide positions
hide_positions = os.environ.get("HIDE_POSITIONS")
if not hide_positions or hide_positions.lower() == "false":
    HIDE_POSITIONS = False
elif hide_positions.lower() == "true":
    HIDE_POSITIONS = True
else:
    # Log a warning if the value is not a boolean
    colored_message = termcolor.colored(f"HIDE_POSITIONS must be set to 'true' or 'false'. Got '{hide_positions}'. Defaulting to False.", "yellow")
    logger.warning(colored_message)
    HIDE_POSITIONS = False

# Name for the strategy to be used in the database
STRATEGY_NAME = os.environ.get("STRATEGY_NAME")

# Market to be traded
MARKET = os.environ.get("MARKET")

# Live trading configuration (if applicable)
LIVE_CONFIG = os.environ.get("LIVE_CONFIG")

# Discord credentials
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

# Get SHOW_PLOT and SHOW_INDICATORS from the environment variables, default to True
SHOW_PLOT = os.environ.get("SHOW_PLOT", "True") == "True"
SHOW_INDICATORS = os.environ.get("SHOW_INDICATORS", "True") == "True"
SHOW_TEARSHEET = os.environ.get("SHOW_TEARSHEET", "True") == "True"

# Set DB_CONNECTION_STR to None by default
DB_CONNECTION_STR = None

# Add a warning if ACCOUNT_HISTORY_DB_CONNECTION_STR is set because it is now replaced by DB_CONNECTION_STR
if os.environ.get("ACCOUNT_HISTORY_DB_CONNECTION_STR"):
    print("ACCOUNT_HISTORY_DB_CONNECTION_STR is deprecated and will be removed in a future version. Please use DB_CONNECTION_STR instead.")
    DB_CONNECTION_STR = os.environ.get("ACCOUNT_HISTORY_DB_CONNECTION_STR")

# Database connection string
if os.environ.get("DB_CONNECTION_STR"):
    DB_CONNECTION_STR = os.environ.get("DB_CONNECTION_STR")

# Name for the strategy to be used in the database
STRATEGY_NAME = os.environ.get("STRATEGY_NAME")

# Set a hard limit on the memory polygon uses
POLYGON_MAX_MEMORY_BYTES = os.environ.get("POLYGON_MAX_MEMORY_BYTES")

POLYGON_CONFIG = {
    # Add POLYGON_API_KEY and POLYGON_IS_PAID_SUBSCRIPTION to your .env file or set them as secrets
    "API_KEY": os.environ.get("POLYGON_API_KEY"),
    "IS_PAID_SUBSCRIPTION": os.environ.get("POLYGON_IS_PAID_SUBSCRIPTION").lower()
    == "true"
    if os.environ.get("POLYGON_IS_PAID_SUBSCRIPTION")
    else False,
}

# Polygon API Key
POLYGON_API_KEY = POLYGON_CONFIG['API_KEY']

# Thetadata Configuration
THETADATA_CONFIG = {
    # Get the ThetaData API key from the .env file or secrets
    "THETADATA_USERNAME": os.environ.get("THETADATA_USERNAME"),
    "THETADATA_PASSWORD": os.environ.get("THETADATA_PASSWORD")
}

# Alpaca Configuration
ALPACA_CONFIG = {  # Paper trading!
    # Add ALPACA_API_KEY, ALPACA_API_SECRET, and ALPACA_IS_PAPER to your .env file or set them as secrets
    "API_KEY": os.environ.get("ALPACA_API_KEY"),
    "API_SECRET": os.environ.get("ALPACA_API_SECRET"),
    "PAPER": os.environ.get("ALPACA_IS_PAPER").lower() == "true" if os.environ.get("ALPACA_IS_PAPER") else True,
}

# Tradier Configuration
TRADIER_CONFIG = {
    # Add TRADIER_ACCESS_TOKEN, TRADIER_ACCOUNT_NUMBER, and TRADIER_IS_PAPER to your .env file or set them as secrets
    "ACCESS_TOKEN": os.environ.get("TRADIER_ACCESS_TOKEN"),
    "ACCOUNT_NUMBER": os.environ.get("TRADIER_ACCOUNT_NUMBER"),
    "PAPER": os.environ.get("TRADIER_IS_PAPER").lower() == "true"
    if os.environ.get("TRADIER_IS_PAPER")
    else True,
}

KRAKEN_CONFIG = {
    # Add KRAKEN_API_KEY and KRAKEN_API_SECRET to your .env file or set them as secrets
    "exchange_id": "kraken",
    "apiKey": os.environ.get("KRAKEN_API_KEY"),
    "secret": os.environ.get("KRAKEN_API_SECRET"),
    "margin": True,
    "sandbox": False,
}

COINBASE_CONFIG = {
    # Add COINBASE_API_KEY and COINBASE_API_SECRET to your .env file or set them as secrets
    "exchange_id": "coinbase",
    "apiKey": os.environ.get("COINBASE_API_KEY"),
    "secret": os.environ.get("COINBASE_API_SECRET"),
    "margin": False,
    "sandbox": False,
}

INTERACTIVE_BROKERS_CONFIG = {
    "SOCKET_PORT": int(os.environ.get("INTERACTIVE_BROKERS_PORT")) if os.environ.get("INTERACTIVE_BROKERS_PORT") else None,
    "CLIENT_ID": int(os.environ.get("INTERACTIVE_BROKERS_CLIENT_ID")) if os.environ.get("INTERACTIVE_BROKERS_CLIENT_ID") else None,
    "IP": os.environ.get("INTERACTIVE_BROKERS_IP", "127.0.0.1"),
    "IB_SUBACCOUNT": os.environ.get("IB_SUBACCOUNT", None)
}

INTERACTIVE_BROKERS_REST_CONFIG = {
    "IB_USERNAME": os.environ.get("IB_USERNAME"),
    "IB_PASSWORD": os.environ.get("IB_PASSWORD"),
    "ACCOUNT_ID": os.environ.get("ACCOUNT_ID"),
    "API_URL": os.environ.get("IB_API_URL"),
    "RUNNING_ON_SERVER": os.environ.get("RUNNING_ON_SERVER")
}

LUMIWEALTH_API_KEY = os.environ.get("LUMIWEALTH_API_KEY")

if IS_BACKTESTING:
    broker = None
else:
    # If using Alpaca as a broker, set that as the broker
    if ALPACA_CONFIG["API_KEY"]:
        broker = Alpaca(ALPACA_CONFIG)

    # If using Tradier as a broker, set that as the broker
    elif TRADIER_CONFIG["ACCESS_TOKEN"]:
        broker = Tradier(TRADIER_CONFIG)

    # If using Coinbase as a broker, set that as the broker
    elif COINBASE_CONFIG["apiKey"]:
        broker = Ccxt(COINBASE_CONFIG)

    # If using Kraken as a broker, set that as the broker
    elif KRAKEN_CONFIG["apiKey"]:
        broker = Ccxt(KRAKEN_CONFIG)

    # If using Interactive Brokers as a broker, set that as the broker
    elif INTERACTIVE_BROKERS_CONFIG["SOCKET_PORT"]:
        broker = InteractiveBrokers(INTERACTIVE_BROKERS_CONFIG)
    
    elif INTERACTIVE_BROKERS_REST_CONFIG["IB_USERNAME"]:
        broker = InteractiveBrokersREST(INTERACTIVE_BROKERS_REST_CONFIG)

BROKER = broker