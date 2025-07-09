# NOTE: 
# This file is not meant to be modified. This file loads the credentials from the ".env" file or secrets and sets them as environment variables.
# If you want to set the environment variables on your computer, you can do so by creating a ".env" file in the root directory of the project
# and adding the variables described in the "Secrets Configuration" section of the README.md file like this (but without the "# " at the front):
# IS_BACKTESTING=True
# POLYGON_API_KEY=p0izKxeskywlLjKi82NLrQPUvSzvlYVT
# etc.

import os
import sys

from .brokers import Alpaca, Ccxt, InteractiveBrokers, InteractiveBrokersREST, Tradier, Tradovate, Schwab, Bitunix, ProjectX
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

# Flag to determine if error logs should be logged to a CSV file (True/False)
_log_errors_to_csv = os.environ.get("LOG_ERRORS_TO_CSV")
if _log_errors_to_csv is None:
    LOG_ERRORS_TO_CSV = False
elif _log_errors_to_csv.lower() in ("true", "1", "yes", "on"):
    LOG_ERRORS_TO_CSV = True
elif _log_errors_to_csv.lower() in ("false", "0", "no", "off"):
    LOG_ERRORS_TO_CSV = False
else:
    LOG_ERRORS_TO_CSV = False

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

# DataBento Configuration
DATABENTO_CONFIG = {
    # Add DATABENTO_API_KEY to your .env file or set them as secrets
    "API_KEY": os.environ.get("DATABENTO_API_KEY"),
    "TIMEOUT": int(os.environ.get("DATABENTO_TIMEOUT", "30")),
    "MAX_RETRIES": int(os.environ.get("DATABENTO_MAX_RETRIES", "3")),
}

# Alpaca Configuration
ALPACA_CONFIG = {
    # Add ALPACA_API_KEY, ALPACA_API_SECRET, ALPACA_OAUTH_TOKEN, and ALPACA_IS_PAPER to your .env file or set them as secrets
    "API_KEY": os.environ.get("ALPACA_API_KEY"),
    "API_SECRET": os.environ.get("ALPACA_API_SECRET"),
    "OAUTH_TOKEN": os.environ.get("ALPACA_OAUTH_TOKEN"),
    "PAPER": os.environ.get("ALPACA_IS_PAPER").lower() == "true" if os.environ.get("ALPACA_IS_PAPER") else True,
}

# Alpaca OAuth Configuration Constants
ALPACA_OAUTH_CONFIG = {
    "CALLBACK_URL": "https://api.botspot.trade/broker_oauth/alpaca",
    "CLIENT_ID": "6625abd29ce3f95285dfa4405934de83",
    "REDIRECT_URL": "https://botspot.trade/oauth/alpaca/success",
}

# Alpaca test configuration for unit tests
ALPACA_TEST_CONFIG = {  # Paper trading!
    # Add ALPACA_TEST_API_KEY, ALPACA_TEST_API_SECRET, ALPACA_TEST_OAUTH_TOKEN to your .env file or set them as secrets
    "API_KEY": os.environ.get("ALPACA_TEST_API_KEY"),
    "API_SECRET": os.environ.get("ALPACA_TEST_API_SECRET"),
    "OAUTH_TOKEN": os.environ.get("ALPACA_TEST_OAUTH_TOKEN"),
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
    "IB_ACCOUNT_ID": os.environ.get("IB_ACCOUNT_ID"),
    "API_URL": os.environ.get("IB_API_URL"),
    "RUNNING_ON_SERVER": os.environ.get("RUNNING_ON_SERVER")
}

# Tradovate Configuration
TRADOVATE_CONFIG = {
    "USERNAME": os.environ.get("TRADOVATE_USERNAME"),
    "DEDICATED_PASSWORD": os.environ.get("TRADOVATE_DEDICATED_PASSWORD"),
    "APP_ID": os.environ.get("TRADOVATE_APP_ID", "Lumibot"),
    "APP_VERSION": os.environ.get("TRADOVATE_APP_VERSION", "1.0"),
    "CID": os.environ.get("TRADOVATE_CID"),
    "SECRET": os.environ.get("TRADOVATE_SECRET"),
    "IS_PAPER": os.environ.get("TRADOVATE_IS_PAPER", "true").lower() == "true",
    "MD_URL": os.environ.get("TRADOVATE_MD_URL", "https://md.tradovateapi.com/v1"),
}

# Schwab Configuration
SCHWAB_CONFIG = {
    # Only these three matter
    "SCHWAB_TOKEN":          os.getenv("SCHWAB_TOKEN"),          # optional
    "SCHWAB_ACCOUNT_NUMBER": os.getenv("SCHWAB_ACCOUNT_NUMBER"), # required
    "SCHWAB_APP_KEY":        os.getenv("SCHWAB_APP_KEY"),        # required, loaded from env
    "SCHWAB_APP_SECRET":     os.getenv("SCHWAB_APP_SECRET"),     # required, loaded from env
    "SCHWAB_BACKEND_CALLBACK_URL": os.getenv("SCHWAB_BACKEND_CALLBACK_URL"), # required for auth flow
}

# Bitunix Configuration
BITUNIX_CONFIG = {
    "API_KEY": os.environ.get("BITUNIX_API_KEY"),
    "API_SECRET": os.environ.get("BITUNIX_API_SECRET"),
    "TRADING_MODE": os.environ.get("BITUNIX_TRADING_MODE", "FUTURES"), # Add TRADING_MODE, default to FUTURES
}

# ProjectX URL mappings - REST API base URLs (v2 gateway URLs preferred)
PROJECTX_BASE_URLS = {
    "topstepx": "https://api.topstepx.com/",
    "topone": "https://gateway-api-toponefutures.s2f.projectx.com/",  # Top One Futures
    "tickticktrader": "https://gateway-api-tickticktrader.s2f.projectx.com/",
    "alphaticks": "https://gateway-api-alphaticks.s2f.projectx.com/",
    "aquafutures": "https://gateway-api-aquafutures.s2f.projectx.com/",
    "blueguardianfutures": "https://gateway-api-blueguardianfutures.s2f.projectx.com/",
    "blusky": "https://gateway-api-blusky.s2f.projectx.com/",
    "bulenox": "https://gateway-api-bulenox.s2f.projectx.com/",
    "e8x": "https://gateway-api-e8x.s2f.projectx.com/",
    "fundingfutures": "https://gateway-api-fundingfutures.s2f.projectx.com/",
    "thefuturesdesk": "https://gateway-api-thefuturesdesk.s2f.projectx.com/",
    "futureselite": "https://gateway-api-futureselite.s2f.projectx.com/",
    "fxifyfutures": "https://gateway-api-fxifyfutures.s2f.projectx.com/",
    "goatfundedfutures": "https://gateway-api-goatfundedfutures.s2f.projectx.com/",
    "holaprime": "https://gateway-api-holaprime.s2f.projectx.com/",
    "nexgen": "https://gateway-api-nexgen.s2f.projectx.com/",
    "tx3funding": "https://gateway-api-tx3funding.s2f.projectx.com/",
    "demo": "https://gateway-api-demo.s2f.projectx.com/",
    "daytraders": "https://gateway-api-daytraders.s2f.projectx.com/",
}

# ProjectX SignalR streaming URL mappings
PROJECTX_STREAMING_URLS = {
    "topstepx": "https://gateway-rtc-topstepx.s2f.projectx.com/",
    "topone": "https://gateway-rtc-demo.s2f.projectx.com/",  # Top One Futures
    "tickticktrader": "https://gateway-rtc-tickticktrader.s2f.projectx.com/",
    "alphaticks": "https://gateway-rtc-alphaticks.s2f.projectx.com/",
    "aquafutures": "https://gateway-rtc-aquafutures.s2f.projectx.com/",
    "blueguardianfutures": "https://gateway-rtc-blueguardianfutures.s2f.projectx.com/",
    "blusky": "https://gateway-rtc-blusky.s2f.projectx.com/",
    "bulenox": "https://gateway-rtc-bulenox.s2f.projectx.com/",
    "e8x": "https://gateway-rtc-e8x.s2f.projectx.com/",
    "fundingfutures": "https://gateway-rtc-fundingfutures.s2f.projectx.com/",
    "thefuturesdesk": "https://gateway-rtc-thefuturesdesk.s2f.projectx.com/",
    "futureselite": "https://gateway-rtc-futureselite.s2f.projectx.com/",
    "fxifyfutures": "https://gateway-rtc-fxifyfutures.s2f.projectx.com/",
    "goatfundedfutures": "https://gateway-rtc-goatfundedfutures.s2f.projectx.com/",
    "holaprime": "https://gateway-rtc-holaprime.s2f.projectx.com/",
    "nexgen": "https://gateway-rtc-nexgen.s2f.projectx.com/",
    "tx3funding": "https://gateway-rtc-tx3funding.s2f.projectx.com/",
    "demo": "https://gateway-rtc-demo.s2f.projectx.com/",
    "daytraders": "https://gateway-rtc-daytraders.s2f.projectx.com/",
}

# ProjectX Configuration - Multi-firm support
def get_projectx_config(firm: str = None) -> dict:
    """Get ProjectX configuration for a specific firm with automatic URL resolution"""
    # If no firm specified, try to get from environment
    if firm is None:
        firm = os.environ.get("PROJECTX_FIRM")
    
    if not firm:
        # Try to auto-detect available firm
        available_firms = get_available_projectx_firms()
        if available_firms:
            firm = available_firms[0]  # Use first available
    
    if not firm:
        return {}
    
    firm_lower = firm.lower()
    firm_upper = firm.upper()
    
    # Get URLs: Environment override OR built-in mapping
    base_url = (os.environ.get(f"PROJECTX_{firm_upper}_BASE_URL") or 
                PROJECTX_BASE_URLS.get(firm_lower))
    
    streaming_url = (os.environ.get(f"PROJECTX_{firm_upper}_STREAMING_BASE_URL") or 
                     PROJECTX_STREAMING_URLS.get(firm_lower))
    
    return {
        "firm": firm_upper,
        "api_key": os.environ.get(f"PROJECTX_{firm_upper}_API_KEY"),
        "username": os.environ.get(f"PROJECTX_{firm_upper}_USERNAME"),
        "base_url": base_url,
        "preferred_account_name": os.environ.get(f"PROJECTX_{firm_upper}_PREFERRED_ACCOUNT_NAME"),
        "streaming_base_url": streaming_url,
    }

def get_available_projectx_firms() -> list:
    """Get list of firms that have ProjectX configuration available"""
    firms = []
    for key in os.environ.keys():
        if key.startswith("PROJECTX_") and key.endswith("_API_KEY"):
            # Extract firm name from PROJECTX_FIRMNAME_API_KEY
            firm_name = key[9:-8]  # Remove "PROJECTX_" and "_API_KEY"
            if firm_name:
                firms.append(firm_name)
    return firms

# Default ProjectX config (for backwards compatibility and auto-detection)
PROJECTX_CONFIG = get_projectx_config()

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
        elif trading_broker_name.lower() == "tradovate":
            broker = Tradovate(TRADOVATE_CONFIG)
        elif trading_broker_name.lower() == "schwab":
            broker = Schwab(SCHWAB_CONFIG)
        elif trading_broker_name.lower() == "bitunix":
            broker = Bitunix(BITUNIX_CONFIG)
        elif trading_broker_name.lower() == "projectx":
            try:
                # Get specified firm or use auto-detection
                firm = os.environ.get("PROJECTX_FIRM")
                config = get_projectx_config(firm)
                
                if not config or not config.get("api_key"):
                    raise ValueError("No valid ProjectX configuration found. Please set environment variables for at least one firm.")
                
                from .data_sources import ProjectXData
                data_source = ProjectXData(config)
                broker = ProjectX(config, data_source=data_source)
            except Exception as e:
                colored_message = termcolor.colored(f"Failed to initialize ProjectX broker: {e}", "red")
                logger.error(colored_message)
        else:
            colored_message = termcolor.colored(f"Unknown trading broker name: {trading_broker_name}. Please check your environment variables.", "red")
            logger.error(colored_message)
    else:
        # Auto-detect broker based on available credentials if not explicitly specified
        if ALPACA_CONFIG["API_KEY"] or ALPACA_CONFIG["OAUTH_TOKEN"]:
            try:
                broker = Alpaca(ALPACA_CONFIG)
            except ValueError as e:
                # If Alpaca initialization fails due to missing credentials, skip it
                if "Either OAuth token or API key/secret must be provided" in str(e):
                    pass
                else:
                    raise e
        elif TRADIER_CONFIG["ACCESS_TOKEN"]:
            broker = Tradier(TRADIER_CONFIG)
        elif INTERACTIVE_BROKERS_CONFIG["CLIENT_ID"]:
            broker = InteractiveBrokers(INTERACTIVE_BROKERS_CONFIG)
        elif INTERACTIVE_BROKERS_REST_CONFIG["IB_USERNAME"]:
            broker = InteractiveBrokersREST(INTERACTIVE_BROKERS_REST_CONFIG)
        elif TRADOVATE_CONFIG["USERNAME"]:
            broker = Tradovate(TRADOVATE_CONFIG)
        # Only check for SCHWAB_ACCOUNT_NUMBER to select Schwab
        elif SCHWAB_CONFIG.get("SCHWAB_ACCOUNT_NUMBER"):
            broker = Schwab(SCHWAB_CONFIG)
        elif COINBASE_CONFIG["apiKey"]:
            broker = Ccxt(COINBASE_CONFIG)
        elif KRAKEN_CONFIG["apiKey"]:
            broker = Ccxt(KRAKEN_CONFIG)
        elif BITUNIX_CONFIG["API_KEY"] and BITUNIX_CONFIG["API_SECRET"]:
            broker = Bitunix(BITUNIX_CONFIG)
        elif get_available_projectx_firms():
            try:
                # Use first available ProjectX firm
                available_firms = get_available_projectx_firms()
                config = get_projectx_config(available_firms[0])
                
                if config.get("api_key") and config.get("username"):
                    from .data_sources import ProjectXData
                    data_source = ProjectXData(config)
                    broker = ProjectX(config, data_source=data_source)
            except Exception as e:
                colored_message = termcolor.colored(f"Failed to initialize ProjectX broker: {e}", "red")
                logger.error(colored_message)
    
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
                # Only pass account_number, never api_key/secret
                data_source = SchwabData(
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
            elif data_source_name.lower() == "databento":
                # Check if we have DataBento configuration
                if DATABENTO_CONFIG["API_KEY"]:
                    from .data_sources import DataBentoData
                    data_source = DataBentoData(
                        api_key=DATABENTO_CONFIG["API_KEY"],
                        timeout=DATABENTO_CONFIG["TIMEOUT"],
                        max_retries=DATABENTO_CONFIG["MAX_RETRIES"]
                    )
                else:
                    colored_message = termcolor.colored("Missing DataBento credentials. Please set DATABENTO_API_KEY environment variable.", "red")
                    logger.error(colored_message)
            elif data_source_name.lower() == "bitunix":
                from .data_sources import BitunixData
                data_source = BitunixData(BITUNIX_CONFIG)
                # If broker is also Bitunix, share the same client instance
                if broker and broker.name.lower() == "bitunix" and hasattr(broker, "api"):
                    data_source.client = broker.api
            elif data_source_name.lower() == "projectx":
                from .data_sources import ProjectXData
                # Get specified firm or use auto-detection
                firm = os.environ.get("PROJECTX_FIRM")
                config = get_projectx_config(firm)
                
                if not config or not config.get("api_key"):
                    colored_message = termcolor.colored("No valid ProjectX configuration found for data source. Please set environment variables for at least one firm.", "red")
                    logger.error(colored_message)
                else:
                    data_source = ProjectXData(config)
                    # If broker is also ProjectX, share the same client instance
                    if broker and broker.name.lower().startswith("projectx") and hasattr(broker, "client"):
                        data_source.client = broker.client
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