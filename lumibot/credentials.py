# NOTE: 
# This file is not meant to be modified. This file loads the credentials from the ".env" file or secrets and sets them as environment variables.
# If you want to set the environment variables on your computer, you can do so by creating a ".env" file in the root directory of the project
# and adding the variables described in the "Secrets Configuration" section of the README.md file like this (but without the "# " at the front):
# IS_BACKTESTING=True
# POLYGON_API_KEY=p0izKxeskywlLjKi82NLrQPUvSzvlYVT
# etc.

import os
import sys

from .brokers import Alpaca, Ccxt, InteractiveBrokers, InteractiveBrokersREST, Tradier, Tradeovate, Schwab
import logging
from dotenv import load_dotenv
import termcolor
from dateutil import parser

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

# Check if the dates are not None and not empty strings before parsing
BACKTESTING_START = None
if backtesting_start:
    BACKTESTING_START = parser.parse(backtesting_start)
BACKTESTING_END = None
if backtesting_end:
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

# Flag to determine if backtest progress should be logged to a file (True/False)
LOG_BACKTEST_PROGRESS_TO_FILE = os.environ.get("LOG_BACKTEST_PROGRESS_TO_FILE")

# Determine if backtesting logs should be quiet via env variable (default True)
_btl = os.environ.get("BACKTESTING_QUIET_LOGS", None)
if _btl is not None:
    if _btl.lower() == "true":
        BACKTESTING_QUIET_LOGS = True
    elif _btl.lower() == "false":
        BACKTESTING_QUIET_LOGS = False
    else:
        colored_message = termcolor.colored(f"BACKTESTING_QUIET_LOGS must be set to 'true' or 'false'. Got '{_btl}'. Defaulting to True.", "yellow")
        logger.warning(colored_message)
        BACKTESTING_QUIET_LOGS = True
else:
    BACKTESTING_QUIET_LOGS = None

_btl = os.environ.get("BACKTESTING_SHOW_PROGRESS_BAR", None)
if _btl is not None:
    if _btl.lower() == "true":
        BACKTESTING_SHOW_PROGRESS_BAR = True
    elif _btl.lower() == "false":
        BACKTESTING_SHOW_PROGRESS_BAR = False
    else:
        colored_message = termcolor.colored(f"BACKTESTING_SHOW_PROGRESS_BAR must be set to 'true' or 'false'. Got '{_btl}'. Defaulting to True.", "yellow")
        logger.warning(colored_message)
        BACKTESTING_SHOW_PROGRESS_BAR = True
else:
    BACKTESTING_SHOW_PROGRESS_BAR = None

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
ALPACA_CONFIG = {
    # Add ALPACA_API_KEY, ALPACA_API_SECRET, and ALPACA_IS_PAPER to your .env file or set them as secrets
    "API_KEY": os.environ.get("ALPACA_API_KEY"),
    "API_SECRET": os.environ.get("ALPACA_API_SECRET"),
    "PAPER": os.environ.get("ALPACA_IS_PAPER").lower() == "true" if os.environ.get("ALPACA_IS_PAPER") else True,
}

# Alpaca test configuration for unit tests
ALPACA_TEST_CONFIG = {  # Paper trading!
    # Add ALPACA_TEST_API_KEY, and ALPACA_TEST_API_SECRET to your .env file or set them as secrets
    "API_KEY": os.environ.get("ALPACA_TEST_API_KEY"),
    "API_SECRET": os.environ.get("ALPACA_TEST_API_SECRET"),
    "PAPER": True
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

# Tradier test configuration for unit tests
TRADIER_TEST_CONFIG = {
    # Add TRADIER_TEST_ACCESS_TOKEN and TRADIER_TEST_ACCOUNT_NUMBER to your .env file or set them as secrets
    "ACCESS_TOKEN": os.environ.get("TRADIER_TEST_ACCESS_TOKEN"),
    "ACCOUNT_NUMBER": os.environ.get("TRADIER_TEST_ACCOUNT_NUMBER"),
    "PAPER": True
}

# Kraken Configuration
KRAKEN_CONFIG = {
    # Add KRAKEN_API_KEY and KRAKEN_API_SECRET to your .env file or set them as secrets
    "exchange_id": "kraken",
    "apiKey": os.environ.get("KRAKEN_API_KEY"),
    "secret": os.environ.get("KRAKEN_API_SECRET"),
    "margin": True,
    "sandbox": False,
}

# Coinbase Configuration
COINBASE_CONFIG = {
    # Add COINBASE_API_KEY and COINBASE_API_SECRET to your .env file or set them as secrets
    "exchange_id": "coinbase",
    "apiKey": os.environ.get("COINBASE_API_KEY_NAME"),   # API key name/identifier
    "secret": os.environ.get("COINBASE_PRIVATE_KEY"),      # Your private key goes here
    "password": os.environ.get("COINBASE_API_PASSPHRASE"),   # Passphrase if required
    "margin": False,
    "sandbox": os.environ.get("COINBASE_SANDBOX", "false").lower() == "true",
}

# Interactive Brokers Configuration
INTERACTIVE_BROKERS_CONFIG = {
    "SOCKET_PORT": int(os.environ.get("INTERACTIVE_BROKERS_PORT")) if os.environ.get("INTERACTIVE_BROKERS_PORT") else None,
    "CLIENT_ID": int(os.environ.get("INTERACTIVE_BROKERS_CLIENT_ID")) if os.environ.get("INTERACTIVE_BROKERS_CLIENT_ID") else None,
    "IP": os.environ.get("INTERACTIVE_BROKERS_IP", "127.0.0.1"),
    "IB_SUBACCOUNT": os.environ.get("IB_SUBACCOUNT", None)
}

# Interactive Brokers REST Configuration
INTERACTIVE_BROKERS_REST_CONFIG = {
    "IB_USERNAME": os.environ.get("IB_USERNAME"),
    "IB_PASSWORD": os.environ.get("IB_PASSWORD"),
    "ACCOUNT_ID": os.environ.get("ACCOUNT_ID"),
    "API_URL": os.environ.get("IB_API_URL"),
    "RUNNING_ON_SERVER": os.environ.get("RUNNING_ON_SERVER")
}

# Tradeovate Configuration
TRADEOVATE_CONFIG = {
    "USERNAME": os.environ.get("TRADEOVATE_USERNAME"),
    "DEDICATED_PASSWORD": os.environ.get("TRADEOVATE_DEDICATED_PASSWORD"),
    "APP_ID": os.environ.get("TRADEOVATE_APP_ID", "Lumibot"),
    "APP_VERSION": os.environ.get("TRADEOVATE_APP_VERSION", "1.0"),
    "CID": os.environ.get("TRADEOVATE_CID"),
    "SECRET": os.environ.get("TRADEOVATE_SECRET"),
    "IS_PAPER": os.environ.get("TRADEOVATE_IS_PAPER", "true").lower() == "true",
    "MD_URL": os.environ.get("TRADEOVATE_MD_URL", "https://md.tradovateapi.com/v1"),
}

# Schwab Configuration
SCHWAB_CONFIG = {
    "SCHWAB_API_KEY": os.environ.get("SCHWAB_API_KEY"),
    "SCHWAB_SECRET": os.environ.get("SCHWAB_SECRET"),
    "SCHWAB_ACCOUNT_NUMBER": os.environ.get("SCHWAB_ACCOUNT_NUMBER"),
}

LUMIWEALTH_API_KEY = os.environ.get("LUMIWEALTH_API_KEY")

# Get TRADING_BROKER and DATA_SOURCE from environment variables
trading_broker_name = os.environ.get("TRADING_BROKER")
data_source_name = os.environ.get("DATA_SOURCE")

broker = None
data_source = None

# Check if we are backtesting or not
is_backtesting = os.environ.get("IS_BACKTESTING")
if not is_backtesting or is_backtesting.lower() == "false":
    IS_BACKTESTING = False
    
    # Determine which trading broker to use based on TRADING_BROKER environment variable or available configs
    if trading_broker_name:
        # Create broker instance based on explicitly specified name
        if trading_broker_name.lower() == "alpaca":
            broker = Alpaca(ALPACA_CONFIG)
        elif trading_broker_name.lower() == "tradier":
            broker = Tradier(TRADIER_CONFIG)
        elif trading_broker_name.lower() == "ccxt":
            broker = Ccxt(COINBASE_CONFIG)
        elif trading_broker_name.lower() == "coinbase":
            broker = Ccxt(COINBASE_CONFIG)
        elif trading_broker_name.lower() == "kraken":
            broker = Ccxt(KRAKEN_CONFIG)
        elif trading_broker_name.lower() == "ib" or trading_broker_name.lower() == "interactivebrokers":
            broker = InteractiveBrokers(INTERACTIVE_BROKERS_CONFIG)
        elif trading_broker_name.lower() == "ibrest" or trading_broker_name.lower() == "interactivebrokersrest":
            broker = InteractiveBrokersREST(INTERACTIVE_BROKERS_REST_CONFIG)
        elif trading_broker_name.lower() == "tradeovate":
            broker = Tradeovate(TRADEOVATE_CONFIG)
        elif trading_broker_name.lower() == "schwab":
            broker = Schwab(SCHWAB_CONFIG)
        else:
            colored_message = termcolor.colored(f"Unknown trading broker name: {trading_broker_name}. Please check your environment variables.", "red")
            logger.error(colored_message)
    else:
        # Auto-detect broker based on available credentials if not explicitly specified
        if ALPACA_CONFIG["API_KEY"]:
            broker = Alpaca(ALPACA_CONFIG)
        elif TRADIER_CONFIG["ACCESS_TOKEN"]:
            broker = Tradier(TRADIER_CONFIG)
        elif INTERACTIVE_BROKERS_CONFIG["CLIENT_ID"]:
            broker = InteractiveBrokers(INTERACTIVE_BROKERS_CONFIG)
        elif INTERACTIVE_BROKERS_REST_CONFIG["IB_USERNAME"]:
            broker = InteractiveBrokersREST(INTERACTIVE_BROKERS_REST_CONFIG)
        elif TRADEOVATE_CONFIG["USERNAME"]:
            broker = Tradeovate(TRADEOVATE_CONFIG)
        elif SCHWAB_CONFIG["SCHWAB_API_KEY"]:
            broker = Schwab(SCHWAB_CONFIG)
        elif COINBASE_CONFIG["apiKey"]:
            broker = Ccxt(COINBASE_CONFIG)
        elif KRAKEN_CONFIG["apiKey"]:
            broker = Ccxt(KRAKEN_CONFIG)
    
    # Determine if we should use a custom data source based on DATA_SOURCE environment variable
    if data_source_name:
        try:
            # Import necessary data source classes
            if data_source_name.lower() == "alpaca":
                from .data_sources import AlpacaData
                data_source = AlpacaData(ALPACA_CONFIG)
            elif data_source_name.lower() == "tradier":
                from .data_sources import TradierData
                data_source = TradierData(TRADIER_CONFIG)
            elif data_source_name.lower() == "ccxt":
                from .data_sources import CcxtData
                data_source = CcxtData(COINBASE_CONFIG)
            elif data_source_name.lower() == "coinbase":
                from .data_sources import CcxtData
                data_source = CcxtData(COINBASE_CONFIG)
            elif data_source_name.lower() == "kraken":
                from .data_sources import CcxtData
                data_source = CcxtData(KRAKEN_CONFIG)
            elif data_source_name.lower() == "ib" or data_source_name.lower() == "interactivebrokers":
                from .data_sources import InteractiveBrokersData
                data_source = InteractiveBrokersData(INTERACTIVE_BROKERS_CONFIG)
            elif data_source_name.lower() == "ibrest" or data_source_name.lower() == "interactivebrokersrest":
                from .data_sources import InteractiveBrokersRESTData
                data_source = InteractiveBrokersRESTData(INTERACTIVE_BROKERS_REST_CONFIG)
            elif data_source_name.lower() == "polygon":
                from .data_sources import PolygonData
                data_source = PolygonData(api_key=POLYGON_API_KEY)
            elif data_source_name.lower() == "yahoo":
                from .data_sources import YahooData
                
                # Initialize YahooData without explicitly passing dates
                # The class will handle defaults internally
                data_source = YahooData()
                
                # Only set dates if they're explicitly provided in environment variables
                if BACKTESTING_START and BACKTESTING_END:
                    data_source._update_datetime_limits(BACKTESTING_START, BACKTESTING_END)
            elif data_source_name.lower() == "schwab":
                from .data_sources import SchwabData
                # Create the data source with explicit credentials
                data_source = SchwabData(
                    api_key=SCHWAB_CONFIG["SCHWAB_API_KEY"],
                    secret=SCHWAB_CONFIG["SCHWAB_SECRET"],
                    account_number=SCHWAB_CONFIG["SCHWAB_ACCOUNT_NUMBER"]
                )
                
                # If broker is also Schwab, share the client
                if broker and broker.name.lower() == "schwab" and hasattr(broker, "client"):
                    data_source.set_client(broker.client)
            elif data_source_name.lower() == "thetadata":
                # Check if we have ThetaData configuration
                if THETADATA_CONFIG["THETADATA_USERNAME"] and THETADATA_CONFIG["THETADATA_PASSWORD"]:
                    from .data_sources import ThetaData
                    data_source = ThetaData(
                        username=THETADATA_CONFIG["THETADATA_USERNAME"],
                        password=THETADATA_CONFIG["THETADATA_PASSWORD"]
                    )
                else:
                    colored_message = termcolor.colored("Missing ThetaData credentials. Please set THETADATA_USERNAME and THETADATA_PASSWORD environment variables.", "red")
                    logger.error(colored_message)
            else:
                colored_message = termcolor.colored(f"Unknown data source name: {data_source_name}. Please check your environment variables.", "red")
                logger.error(colored_message)
        except ImportError as e:
            colored_message = termcolor.colored(f"Could not import data source {data_source_name}: {str(e)}", "red")
            logger.error(colored_message)
    
    # If we have both a broker and a custom data source, set the broker's data source
    if broker and data_source:
        logger.info(termcolor.colored(f"Using {data_source_name} as data source for {broker.name} broker", "green"))
        # Store the original data source for reference
        original_broker_data_source = broker.data_source
        
        # Set the custom data source
        broker.data_source = data_source

elif is_backtesting.lower() == "true":
    IS_BACKTESTING = True
else:
    # Log a warning if the value is not a boolean
    colored_message = termcolor.colored(f"IS_BACKTESTING must be set to 'true' or 'false'. Got '{is_backtesting}'. Defaulting to False.", "yellow")
    logger.warning(colored_message)
    IS_BACKTESTING = False

# Export variables for use in strategies
BROKER = broker
DATA_SOURCE = data_source