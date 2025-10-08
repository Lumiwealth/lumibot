# This file contains helper functions for getting data from Polygon.io
import time
import os
from datetime import date, datetime, timedelta
from pathlib import Path
import pytz
import pandas as pd
import pandas_market_calendars as mcal
import requests
from lumibot import LUMIBOT_CACHE_FOLDER, LUMIBOT_DEFAULT_PYTZ
from lumibot.tools.lumibot_logger import get_logger
from lumibot.entities import Asset
from tqdm import tqdm

logger = get_logger(__name__)

WAIT_TIME = 60
MAX_DAYS = 30
CACHE_SUBFOLDER = "thetadata"
BASE_URL = "http://127.0.0.1:25510"

# Global process tracking for ThetaTerminal
THETA_DATA_PROCESS = None
THETA_DATA_PID = None


def get_price_data(
    username: str,
    password: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    timespan: str = "minute",
    quote_asset: Asset = None,
    dt=None,
    datastyle: str = "ohlc",
    include_after_hours: bool = True
):
    """
    Queries ThetaData for pricing data for the given asset and returns a DataFrame with the data. Data will be
    cached in the LUMIBOT_CACHE_FOLDER/{CACHE_SUBFOLDER} folder so that it can be reused later and we don't have to query
    ThetaData every time we run a backtest.

    Parameters
    ----------
    username : str
        Your ThetaData username
    password : str
        Your ThetaData password
    asset : Asset
        The asset we are getting data for
    start : datetime
        The start date/time for the data we want
    end : datetime
        The end date/time for the data we want
    timespan : str
        The timespan for the data we want. Default is "minute" but can also be "second", "hour", "day", "week",
        "month", "quarter"
    quote_asset : Asset
        The quote asset for the asset we are getting data for. This is only needed for Forex assets.
    datastyle : str
        The style of data to retrieve ("ohlc" or "quote")
    include_after_hours : bool
        Whether to include after-hours trading data (default True)

    Returns
    -------
    pd.DataFrame
        A DataFrame with the pricing data for the asset

    """
    import pytz  # Import at function level to avoid scope issues in nested calls

    # Check if we already have data for this asset in the cache file
    df_all = None
    df_cached = None
    cache_file = build_cache_filename(asset, timespan, datastyle)
    if cache_file.exists():
        logger.info(f"\nLoading '{datastyle}' pricing data for {asset} / {quote_asset} with '{timespan}' timespan from cache file...")
        df_cached = load_cache(cache_file)
        if df_cached is not None and not df_cached.empty:
            df_all = df_cached.copy() # Make a copy so we can check the original later for differences

    # Check if we need to get more data
    missing_dates = get_missing_dates(df_all, asset, start, end)
    if not missing_dates:
        # Filter cached data to requested date range before returning
        if df_all is not None and not df_all.empty:
            # For daily data, use date-based filtering (timestamps vary by provider)
            # For intraday data, use precise datetime filtering
            if timespan == "day":
                # Convert index to dates for comparison
                import pandas as pd
                df_dates = pd.to_datetime(df_all.index).date
                start_date = start.date() if hasattr(start, 'date') else start
                end_date = end.date() if hasattr(end, 'date') else end
                mask = (df_dates >= start_date) & (df_dates <= end_date)
                df_all = df_all[mask]
            else:
                # Intraday: use precise datetime filtering
                import datetime as dt
                # Convert date to datetime if needed
                if isinstance(start, dt.date) and not isinstance(start, dt.datetime):
                    start = dt.datetime.combine(start, dt.time.min)
                if isinstance(end, dt.date) and not isinstance(end, dt.datetime):
                    end = dt.datetime.combine(end, dt.time.max)

                # Handle datetime objects with midnight time (users often pass datetime(YYYY, MM, DD))
                if isinstance(end, dt.datetime) and end.time() == dt.time.min:
                    # Convert end-of-period midnight to end-of-day
                    end = dt.datetime.combine(end.date(), dt.time.max)

                if start.tzinfo is None:
                    start = LUMIBOT_DEFAULT_PYTZ.localize(start).astimezone(pytz.UTC)
                if end.tzinfo is None:
                    end = LUMIBOT_DEFAULT_PYTZ.localize(end).astimezone(pytz.UTC)
                df_all = df_all[(df_all.index >= start) & (df_all.index <= end)]
        return df_all

    start = missing_dates[0]  # Data will start at 8am UTC (4am EST)
    end = missing_dates[-1]  # Data will end at 23:59 UTC (7:59pm EST)

    # Initialize tqdm progress bar
    total_days = (end - start).days + 1
    total_queries = (total_days // MAX_DAYS) + 1
    description = f"\nDownloading '{datastyle}' data for {asset} / {quote_asset} with '{timespan}' from ThetaData..."
    logger.info(description)
    pbar = tqdm(total=1, desc=description, dynamic_ncols=True)

    delta = timedelta(days=MAX_DAYS)

    # For daily bars, use ThetaData's EOD endpoint for official daily OHLC
    # The EOD endpoint includes the 16:00 closing auction and follows SIP sale-condition rules
    # This matches Polygon and Yahoo Finance EXACTLY (zero tolerance)
    if timespan == "day":
        logger.info(f"Daily bars: using EOD endpoint for official close prices")

        # Use EOD endpoint for official daily OHLC
        result_df = get_historical_eod_data(
            asset=asset,
            start_dt=start,
            end_dt=end,
            username=username,
            password=password,
            datastyle=datastyle
        )

        return result_df

    # Map timespan to milliseconds for intraday intervals
    TIMESPAN_TO_MS = {
        "second": 1000,
        "minute": 60000,
        "5minute": 300000,
        "10minute": 600000,
        "15minute": 900000,
        "30minute": 1800000,
        "hour": 3600000,
        "2hour": 7200000,
        "4hour": 14400000,
    }

    interval_ms = TIMESPAN_TO_MS.get(timespan)
    if interval_ms is None:
        raise ValueError(
            f"Unsupported timespan '{timespan}'. "
            f"Supported values: {list(TIMESPAN_TO_MS.keys())} or 'day'"
        )

    while start <= missing_dates[-1]:
        # If we don't have a paid subscription, we need to wait 1 minute between requests because of
        # the rate limit. Wait every other query so that we don't spend too much time waiting.

        if end > start + delta:
            end = start + delta

        result_df = get_historical_data(asset, start, end, interval_ms, username, password, datastyle=datastyle, include_after_hours=include_after_hours)

        if result_df is None or len(result_df) == 0:
            logger.warning(
                f"No data returned for {asset} / {quote_asset} with '{timespan}' timespan between {start} and {end}"
            )

        else:
            df_all = update_df(df_all, result_df)
            pbar.update(1)

        start = end + timedelta(days=1)
        end = start + delta

        if asset.expiration and start > asset.expiration:
            break

    update_cache(cache_file, df_all, df_cached)
    # Close the progress bar when done
    pbar.close()
    return df_all




def get_trading_dates(asset: Asset, start: datetime, end: datetime):
    """
    Get a list of trading days for the asset between the start and end dates
    Parameters
    ----------
    asset : Asset
        Asset we are getting data for
    start : datetime
        Start date for the data requested
    end : datetime
        End date for the data requested

    Returns
    -------

    """
    # Crypto Asset Calendar
    if asset.asset_type == "crypto":
        # Crypto trades every day, 24/7 so we don't need to check the calendar
        return [start.date() + timedelta(days=x) for x in range((end.date() - start.date()).days + 1)]

    # Stock/Option/Index Asset for Backtesting - Assuming NYSE trading days
    elif asset.asset_type == "stock" or asset.asset_type == "option" or asset.asset_type == "index":
        cal = mcal.get_calendar("NYSE")

    # Forex Asset for Backtesting - Forex trades weekdays, 24hrs starting Sunday 5pm EST
    # Calendar: "CME_FX"
    elif asset.asset_type == "forex":
        cal = mcal.get_calendar("CME_FX")

    else:
        raise ValueError(f"Unsupported asset type for thetadata: {asset.asset_type}")

    # Get the trading days between the start and end dates
    start_date = start.date() if hasattr(start, 'date') else start
    end_date = end.date() if hasattr(end, 'date') else end
    df = cal.schedule(start_date=start_date, end_date=end_date)
    trading_days = df.index.date.tolist()
    return trading_days


def build_cache_filename(asset: Asset, timespan: str, datastyle: str = "ohlc"):
    """Helper function to create the cache filename for a given asset and timespan"""

    lumibot_cache_folder = Path(LUMIBOT_CACHE_FOLDER) / CACHE_SUBFOLDER

    # If It's an option then also add the expiration date, strike price and right to the filename
    if asset.asset_type == "option":
        if asset.expiration is None:
            raise ValueError(f"Expiration date is required for option {asset} but it is None")

        # Make asset.expiration datetime into a string like "YYMMDD"
        expiry_string = asset.expiration.strftime("%y%m%d")
        uniq_str = f"{asset.symbol}_{expiry_string}_{asset.strike}_{asset.right}"
    else:
        uniq_str = asset.symbol

    cache_filename = f"{asset.asset_type}_{uniq_str}_{timespan}_{datastyle}.parquet"
    cache_file = lumibot_cache_folder / cache_filename
    return cache_file


def get_missing_dates(df_all, asset, start, end):
    """
    Check if we have data for the full range
    Later Query to Polygon will pad an extra full day to start/end dates so that there should never
    be any gap with intraday data missing.

    Parameters
    ----------
    df_all : pd.DataFrame
        Data loaded from the cache file
    asset : Asset
        Asset we are getting data for
    start : datetime
        Start date for the data requested
    end : datetime
        End date for the data requested

    Returns
    -------
    list[datetime.date]
        A list of dates that we need to get data for
    """
    trading_dates = get_trading_dates(asset, start, end)
    if df_all is None or not len(df_all):
        return trading_dates

    # It is possible to have full day gap in the data if previous queries were far apart
    # Example: Query for 8/1/2023, then 8/31/2023, then 8/7/2023
    # Whole days are easy to check for because we can just check the dates in the index
    dates = pd.Series(df_all.index.date).unique()
    missing_dates = sorted(set(trading_dates) - set(dates))

    # For Options, don't need any dates passed the expiration date
    if asset.asset_type == "option":
        missing_dates = [x for x in missing_dates if x <= asset.expiration]

    return missing_dates


def load_cache(cache_file):
    """Load the data from the cache file and return a DataFrame with a DateTimeIndex"""
    df = pd.read_parquet(cache_file, engine='pyarrow')

    # Set the 'datetime' column as the index of the DataFrame
    df.set_index("datetime", inplace=True)

    df.index = pd.to_datetime(
        df.index
    )  # TODO: Is there some way to speed this up? It takes several times longer than just reading the cache file
    df = df.sort_index()

    # Check if the index is already timezone aware
    if df.index.tzinfo is None:
        # Set the timezone to UTC
        df.index = df.index.tz_localize("UTC")

    return df


def update_cache(cache_file, df_all, df_cached):
    """Update the cache file with the new data"""
    # Check if df_all is different from df_cached (if df_cached exists)
    if df_all is not None and len(df_all) > 0:
        # Check if the dataframes are the same
        if df_all.equals(df_cached):
            return

        # Create the directory if it doesn't exist
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        # Reset the index to convert DatetimeIndex to a regular column
        df_all_reset = df_all.reset_index()

        # Save the data to a parquet file
        df_all_reset.to_parquet(cache_file, engine='pyarrow', compression='snappy')


def update_df(df_all, result):
    """
    Update the DataFrame with the new data from ThetaData

    Parameters
    ----------
    df_all : pd.DataFrame
        A DataFrame with the data we already have
    result : pandas DataFrame
        A List of dictionaries with the new data from Polygon
        Format:
        {
                "close": [2, 3, 4, 5, 6],
                "open": [1, 2, 3, 4, 5],
                "high": [3, 4, 5, 6, 7],
                "low": [1, 2, 3, 4, 5],
                "datetime": [
                    "2023-07-01 09:30:00",
                    "2023-07-01 09:31:00",
                    "2023-07-01 09:32:00",
                    "2023-07-01 09:33:00",
                    "2023-07-01 09:34:00",
                ],
            }
    """
    ny_tz = LUMIBOT_DEFAULT_PYTZ
    df = pd.DataFrame(result)
    if not df.empty:
        if "datetime" not in df.index.names:
            # check if df has a column named "datetime", if not raise key error
            if "datetime" not in df.columns:
                raise KeyError("KeyError: update_df function requires 'result' input with 'datetime' column, but not found")

            # if column "datetime" is not index set it as index
            df = df.set_index("datetime").sort_index()
        else:
            df = df.sort_index()

        if not df.index.tzinfo:
            df.index = df.index.tz_localize(ny_tz).tz_convert(pytz.utc)
        else:
            df.index = df.index.tz_convert(pytz.utc)

        if df_all is not None:
            # set "datetime" column as index of df_all
            if isinstance(df.index, pd.DatetimeIndex) and df.index.name == 'datetime':
                df_all = df_all.sort_index()
            else:
                df_all = df_all.set_index("datetime").sort_index()

            # convert df_all index to UTC if not already
            if not df.index.tzinfo:
                df_all.index = df_all.index.tz_localize(ny_tz).tz_convert(pytz.utc)
            else:
                df_all.index = df_all.index.tz_convert(pytz.utc)

        if df_all is None or df_all.empty:
            df_all = df
        else:
            df_all = pd.concat([df_all, df]).sort_index()
            df_all = df_all[~df_all.index.duplicated(keep="first")]  # Remove any duplicate rows

        # NOTE: Timestamp correction is now done in get_historical_data() at line 569
        # Do NOT subtract 1 minute here as it would double-correct
        # df_all.index = df_all.index - pd.Timedelta(minutes=1)
    return df_all


def is_process_alive():
    """Check if ThetaTerminal Java process is still running"""
    import subprocess
    global THETA_DATA_PROCESS

    # First check if we have a process handle and it's still alive
    if THETA_DATA_PROCESS is not None:
        # poll() returns None if process is still running, otherwise returns exit code
        if THETA_DATA_PROCESS.poll() is None:
            return True

    # If we don't have a process handle or it died, check if any ThetaTerminal process is running
    # This handles cases where the process was started by a previous Python session
    try:
        result = subprocess.run(
            ["pgrep", "-f", "ThetaTerminal.jar"],
            capture_output=True,
            text=True,
            timeout=2
        )
        # pgrep returns 0 if processes found, 1 if none found
        return result.returncode == 0
    except Exception:
        return False


def start_theta_data_client(username: str, password: str):
    import subprocess
    import shutil
    global THETA_DATA_PROCESS, THETA_DATA_PID

    # First try shutting down any existing connection
    try:
        requests.get(f"{BASE_URL}/v2/system/terminal/shutdown")
    except Exception:
        pass

    # Create creds.txt file to avoid passing password with special characters on command line
    # This is the official ThetaData method and avoids shell escaping issues
    # Security note: creds.txt with 0o600 permissions is MORE secure than command-line args
    # which can be seen in process lists. Similar security profile to .env files.
    theta_dir = Path.home() / "ThetaData" / "ThetaTerminal"
    theta_dir.mkdir(parents=True, exist_ok=True)
    creds_file = theta_dir / "creds.txt"

    # IDEMPOTENT WRITE: Only write credentials if file doesn't exist or username changed
    # This prevents overwriting production credentials with test credentials
    should_write = False
    if not creds_file.exists():
        logger.info(f"Creating new creds.txt file at {creds_file}")
        should_write = True
    else:
        # Check if username changed
        try:
            with open(creds_file, 'r') as f:
                existing_username = f.readline().strip()
            if existing_username != username:
                logger.info(f"Username changed from {existing_username} to {username}, updating creds.txt")
                should_write = True
            else:
                logger.debug(f"Using existing creds.txt for {username}")
        except Exception as e:
            logger.warning(f"Could not read existing creds.txt: {e}, will recreate")
            should_write = True

    if should_write:
        # Write credentials to creds.txt (format: email on first line, password on second line)
        with open(creds_file, 'w') as f:
            f.write(f"{username}\n")
            f.write(f"{password}\n")

        # Set restrictive permissions on creds file (owner read/write only)
        # This prevents other users on the system from reading the credentials
        os.chmod(creds_file, 0o600)

        logger.info(f"Updated creds.txt file for user: {username}")

    # Launch ThetaTerminal directly with --creds-file to avoid shell escaping issues
    # We bypass the thetadata library's launcher which doesn't support this option
    # and has shell escaping bugs with special characters in passwords

    # Verify Java is available
    if not shutil.which("java"):
        raise RuntimeError("Java is not installed. Please install Java 11+ to use ThetaData.")

    # Find ThetaTerminal.jar
    jar_file = theta_dir / "ThetaTerminal.jar"
    if not jar_file.exists():
        # Copy ThetaTerminal.jar from lumibot package to user's ThetaData directory
        logger.info("ThetaTerminal.jar not found, copying from lumibot package...")
        import shutil as shutil_copy

        # Find the bundled jar file in the lumibot package
        lumibot_jar = Path(__file__).parent.parent.parent / "ThetaTerminal.jar"

        if lumibot_jar.exists():
            logger.info(f"Copying ThetaTerminal.jar from {lumibot_jar} to {jar_file}")
            shutil_copy.copy2(lumibot_jar, jar_file)
            logger.info(f"Successfully copied ThetaTerminal.jar to {jar_file}")
        else:
            raise FileNotFoundError(
                f"ThetaTerminal.jar not found at {lumibot_jar}. "
                f"Please ensure ThetaTerminal.jar is included in the lumibot package, "
                f"or manually place it at {jar_file}"
            )

    if not jar_file.exists():
        raise FileNotFoundError(f"ThetaTerminal.jar not found at {jar_file}")

    # Launch ThetaTerminal with --creds-file argument (no credentials on command line)
    # This avoids all shell escaping issues and is the recommended approach
    cmd = ["java", "-jar", str(jar_file), "--creds-file", str(creds_file)]

    logger.info(f"Launching ThetaTerminal with creds file: {cmd}")

    # Launch in background and store process handle
    THETA_DATA_PROCESS = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(theta_dir)
    )
    THETA_DATA_PID = THETA_DATA_PROCESS.pid
    logger.info(f"ThetaTerminal started with PID: {THETA_DATA_PID}")

    # Give it a moment to start
    time.sleep(2)

    # We don't return a ThetaClient object since we're launching manually
    # The connection will be established via HTTP/WebSocket to localhost:25510
    return THETA_DATA_PROCESS


def check_connection(username: str, password: str):
    # Do endless while loop and check if connected every 100 milliseconds
    MAX_RETRIES = 15
    counter = 0
    client = None
    connected = False

    while True:
        # FIRST: Check if already connected (most important check!)
        # This prevents unnecessary restarts that would overwrite creds.txt
        try:
            res = requests.get(f"{BASE_URL}/v2/system/mdds/status", timeout=1)
            con_text = res.text

            if con_text == "CONNECTED":
                logger.debug("Already connected to Theta Data!")
                connected = True
                break
            elif con_text == "DISCONNECTED":
                logger.debug("Disconnected from Theta Data, will attempt to start...")
                # Fall through to process check and restart logic
            else:
                logger.debug(f"Unknown connection status: {con_text}")
                # Fall through to process check and restart logic
        except Exception as e:
            # Connection endpoint not responding - process might be dead
            logger.debug(f"Cannot reach ThetaData status endpoint: {e}")
            # Fall through to process check and restart logic

        # SECOND: Check if the Java process is still alive
        if not is_process_alive():
            logger.warning("ThetaTerminal process is not running, starting...")
            client = start_theta_data_client(username=username, password=password)
            counter += 1
            time.sleep(0.5)
            continue

        # THIRD: Process is alive but not connected - wait and retry
        time.sleep(0.5)
        counter += 1

        if counter > MAX_RETRIES:
            logger.error("Cannot connect to Theta Data!")
            break

    return client, connected


def get_request(url: str, headers: dict, querystring: dict, username: str, password: str):
    all_responses = []
    next_page_url = None
    page_count = 0

    while True:
        counter = 0
        # Use next_page URL if available, otherwise use original URL with querystring
        request_url = next_page_url if next_page_url else url
        request_params = None if next_page_url else querystring

        while True:
            try:
                response = requests.get(request_url, headers=headers, params=request_params)
                # Status code 472 means "No data" - this is valid, return None
                if response.status_code == 472:
                    logger.warning(f"No data available for request: {response.text[:200]}")
                    return None
                # If status code is not 200, then we are not connected
                elif response.status_code != 200:
                    logger.warning(f"Non-200 status code {response.status_code}: {response.text[:200]}")
                    check_connection(username=username, password=password)
                else:
                    json_resp = response.json()

                    # Check if json_resp has error_type inside of header
                    if "error_type" in json_resp["header"] and json_resp["header"]["error_type"] != "null":
                        # Handle "NO_DATA" error
                        if json_resp["header"]["error_type"] == "NO_DATA":
                            logger.warning(
                                f"No data returned for querystring: {querystring}")
                            return None
                        else:
                            logger.error(
                                f"Error getting data from Theta Data: {json_resp['header']['error_type']},\nquerystring: {querystring}")
                            check_connection(username=username, password=password)
                    else:
                        break

            except Exception as e:
                logger.warning(f"Exception during request (attempt {counter + 1}): {e}")
                check_connection(username=username, password=password)
                # Give the process time to start after restart
                if counter == 0:
                    logger.info("Waiting 5 seconds for ThetaTerminal to initialize...")
                    time.sleep(5)

            counter += 1
            if counter > 1:
                raise ValueError("Cannot connect to Theta Data!")

        # Store this page's response data
        page_count += 1
        all_responses.append(json_resp["response"])

        # Check for pagination - follow next_page if it exists
        next_page = json_resp["header"].get("next_page")
        if next_page and next_page != "null" and next_page != "":
            logger.info(f"Following pagination: {page_count} page(s) downloaded, fetching next page...")
            next_page_url = next_page
        else:
            # No more pages, we're done
            break

    # Merge all pages if we got multiple pages
    if page_count > 1:
        logger.info(f"Merged {page_count} pages from ThetaData ({sum(len(r) for r in all_responses)} total rows)")
        json_resp["response"] = []
        for page_response in all_responses:
            json_resp["response"].extend(page_response)

    return json_resp


def get_historical_eod_data(asset: Asset, start_dt: datetime, end_dt: datetime, username: str, password: str, datastyle: str = "ohlc"):
    """
    Get EOD (End of Day) data from ThetaData using the /v2/hist/{asset_type}/eod endpoint.

    This endpoint provides official daily OHLC that includes the 16:00 closing auction
    and follows SIP sale-condition rules, matching Polygon and Yahoo Finance exactly.

    NOTE: ThetaData's EOD endpoint has been found to return incorrect open prices for stocks
    that don't match Polygon/Yahoo. We fix this by using the first minute bar's open price.
    Indexes don't have this issue since they are calculated values.

    Parameters
    ----------
    asset : Asset
        The asset we are getting data for
    start_dt : datetime
        The start date for the data we want
    end_dt : datetime
        The end date for the data we want
    username : str
        Your ThetaData username
    password : str
        Your ThetaData password
    datastyle : str
        The style of data to retrieve (default "ohlc")

    Returns
    -------
    pd.DataFrame
        A DataFrame with EOD data for the asset
    """
    # Convert start and end dates to strings
    start_date = start_dt.strftime("%Y%m%d")
    end_date = end_dt.strftime("%Y%m%d")

    # Use v2 EOD API endpoint (supports stock, index, option)
    url = f"{BASE_URL}/v2/hist/{asset.asset_type}/eod"

    querystring = {
        "root": asset.symbol,
        "start_date": start_date,
        "end_date": end_date
    }

    # For options, add strike, expiration, and right parameters
    if asset.asset_type == "option":
        expiration_str = asset.expiration.strftime("%Y%m%d")
        strike = int(asset.strike * 1000)
        querystring["exp"] = expiration_str
        querystring["strike"] = strike
        querystring["right"] = "C" if asset.right == "CALL" else "P"

    headers = {"Accept": "application/json"}

    # Send the request
    json_resp = get_request(url=url, headers=headers, querystring=querystring,
                            username=username, password=password)
    if json_resp is None:
        return None

    # Convert to pandas dataframe
    df = pd.DataFrame(json_resp["response"], columns=json_resp["header"]["format"])

    if df is None or df.empty:
        return df

    # Function to combine ms_of_day and date into datetime
    def combine_datetime(row):
        # Ensure the date is in integer format and then convert to string
        date_str = str(int(row["date"]))
        base_date = datetime.strptime(date_str, "%Y%m%d")
        # EOD reports are normalized at ~17:15 ET but represent the trading day
        # We use midnight of the trading day as the timestamp (consistent with daily bars)
        return base_date

    # Apply the function to each row to create a new datetime column
    datetime_combined = df.apply(combine_datetime, axis=1)

    # Assign the newly created datetime column
    df = df.assign(datetime=datetime_combined)

    # Convert the datetime column to a datetime and localize to UTC
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["datetime"] = df["datetime"].dt.tz_localize("UTC")

    # Set datetime as the index
    df = df.set_index("datetime")

    # Drop the ms_of_day, ms_of_day2, and date columns (not needed for daily bars)
    df = df.drop(columns=["ms_of_day", "ms_of_day2", "date"], errors='ignore')

    # Drop bid/ask columns if present (EOD includes NBBO but we only need OHLC)
    df = df.drop(columns=["bid_size", "bid_exchange", "bid", "bid_condition",
                          "ask_size", "ask_exchange", "ask", "ask_condition"], errors='ignore')

    # FIX: ThetaData's EOD endpoint returns incorrect open/high/low prices for STOCKS and OPTIONS
    # that don't match Polygon/Yahoo. We fix this by using minute bar data.
    # Solution: Fetch minute bars for each trading day and aggregate to get correct OHLC
    # NOTE: Indexes don't need this fix since they are calculated values, not traded securities
    if asset.asset_type in ["stock", "option"]:
        logger.info(f"Fetching 9:30 AM minute bars to correct EOD open prices...")

        # Get minute data for the date range to extract 9:30 AM opens
        minute_df = get_historical_data(
            asset=asset,
            start_dt=start_dt,
            end_dt=end_dt,
            ivl=60000,  # 1 minute
            username=username,
            password=password,
            datastyle=datastyle,
            include_after_hours=False  # RTH only
        )

        if minute_df is not None and not minute_df.empty:
            # Group by date and get the first bar's open for each day
            minute_df_copy = minute_df.copy()
            minute_df_copy['date'] = minute_df_copy.index.date

            # For each date in df, find the corresponding 9:30 AM open from minute data
            for idx in df.index:
                trade_date = idx.date()
                day_minutes = minute_df_copy[minute_df_copy['date'] == trade_date]
                if len(day_minutes) > 0:
                    # Use the first minute bar's open (9:30 AM opening auction)
                    correct_open = day_minutes.iloc[0]['open']
                    df.loc[idx, 'open'] = correct_open

    return df


def get_historical_data(asset: Asset, start_dt: datetime, end_dt: datetime, ivl: int, username: str, password: str, datastyle:str = "ohlc", include_after_hours: bool = True):
    """
    Get data from ThetaData

    Parameters
    ----------
    asset : Asset
        The asset we are getting data for
    start_dt : datetime
        The start date/time for the data we want
    end_dt : datetime
        The end date/time for the data we want
    ivl : int
        The interval for the data we want in milliseconds (eg. 60000 for 1 minute)
    username : str
        Your ThetaData username
    password : str
        Your ThetaData password
    datastyle : str
        The style of data to retrieve ("ohlc" or "quote")
    include_after_hours : bool
        Whether to include after-hours trading data (default True)

    Returns
    -------
    pd.DataFrame
        A DataFrame with the data for the asset
    """

    # Comvert start and end dates to strings
    start_date = start_dt.strftime("%Y%m%d")
    end_date = end_dt.strftime("%Y%m%d")

    # Use v2 API for ALL asset types
    url = f"{BASE_URL}/v2/hist/{asset.asset_type}/{datastyle}"

    if asset.asset_type == "option":
        # Convert the expiration date to a string
        expiration_str = asset.expiration.strftime("%Y%m%d")

        # Convert the strike price to an integer and multiply by 1000
        strike = int(asset.strike * 1000)

        querystring = {
            "root": asset.symbol,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "strike": strike,  # "140000",
            "exp": expiration_str,  # "20220930",
            "right": "C" if asset.right == "CALL" else "P",
            # include_after_hours=True means extended hours (rth=false)
            # include_after_hours=False means regular hours only (rth=true)
            "rth": "false" if include_after_hours else "true"
        }
    elif asset.asset_type == "index":
        # For indexes (SPX, VIX, etc.), don't use rth parameter
        # Indexes are calculated values, not traded securities
        querystring = {
            "root": asset.symbol,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl
        }
    else:
        # For stocks, respect include_after_hours parameter
        # rth=false means extended hours (pre-market + regular + after-hours)
        # rth=true means 9:30 AM - 4:00 PM ET (regular market hours only)
        querystring = {
            "root": asset.symbol,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": "false" if include_after_hours else "true"
        }

    headers = {"Accept": "application/json"}

    # Send the request

    json_resp = get_request(url=url, headers=headers, querystring=querystring,
                            username=username, password=password)
    if json_resp is None:
        return None

    # Convert to pandas dataframe
    df = pd.DataFrame(json_resp["response"], columns=json_resp["header"]["format"])

    # Remove any rows where count is 0 (no data - the prices will be 0 at these times too)
    # NOTE: Indexes always have count=0 since they're calculated values, not traded securities
    if "quote" in datastyle.lower():
        df = df[(df["bid_size"] != 0) | (df["ask_size"] != 0)]
    elif asset.asset_type != "index":
        # Don't filter indexes by count - they're always 0
        df = df[df["count"] != 0]

    if df is None or df.empty:
        return df

    # Function to combine ms_of_day and date into datetime
    def combine_datetime(row):
        # Ensure the date is in integer format and then convert to string
        date_str = str(int(row["date"]))
        base_date = datetime.strptime(date_str, "%Y%m%d")
        # v2 API returns correct start-stamped bars - no adjustment needed
        datetime_value = base_date + timedelta(milliseconds=int(row["ms_of_day"]))
        return datetime_value

    # Apply the function to each row to create a new datetime column

    # Create a new datetime column using the combine_datetime function
    datetime_combined = df.apply(combine_datetime, axis=1)

    # Assign the newly created datetime column
    df = df.assign(datetime=datetime_combined)

    # Convert the datetime column to a datetime and localize to Eastern Time
    df["datetime"] = pd.to_datetime(df["datetime"])

    # Localize to Eastern Time (ThetaData returns times in ET)
    df["datetime"] = df["datetime"].dt.tz_localize("America/New_York")

    # Set datetime as the index
    df = df.set_index("datetime")

    # Drop the ms_of_day and date columns
    df = df.drop(columns=["ms_of_day", "date"], errors='ignore')

    return df


def get_expirations(username: str, password: str, ticker: str, after_date: date):
    """
    Get a list of expiration dates for the given ticker

    Parameters
    ----------
    username : str
        Your ThetaData username
    password : str
        Your ThetaData password
    ticker : str
        The ticker for the asset we are getting data for

    Returns
    -------
    list[str]
        A list of expiration dates for the given ticker
    """
    # Use v2 API endpoint
    url = f"{BASE_URL}/v2/list/expirations"

    querystring = {"root": ticker}

    headers = {"Accept": "application/json"}

    # Send the request
    json_resp = get_request(url=url, headers=headers, querystring=querystring, username=username, password=password)

    # Convert to pandas dataframe
    df = pd.DataFrame(json_resp["response"], columns=json_resp["header"]["format"])

    # Convert df to a list of the first (and only) column
    expirations = df.iloc[:, 0].tolist()

    # Convert after_date to a number
    after_date_int = int(after_date.strftime("%Y%m%d"))

    # Filter out any dates before after_date
    expirations = [x for x in expirations if x >= after_date_int]

    # Convert from "YYYYMMDD" (an int) to "YYYY-MM-DD" (a string)
    expirations_final = []
    for expiration in expirations:
        expiration_str = str(expiration)
        # Add the dashes to the string
        expiration_str = f"{expiration_str[:4]}-{expiration_str[4:6]}-{expiration_str[6:]}"
        # Add the string to the list
        expirations_final.append(expiration_str)

    return expirations_final


def get_strikes(username: str, password: str, ticker: str, expiration: datetime):
    """
    Get a list of strike prices for the given ticker and expiration date

    Parameters
    ----------
    username : str
        Your ThetaData username
    password : str
        Your ThetaData password
    ticker : str
        The ticker for the asset we are getting data for
    expiration : date
        The expiration date for the options we want

    Returns
    -------
    list[float]
        A list of strike prices for the given ticker and expiration date
    """
    # Use v2 API endpoint
    url = f"{BASE_URL}/v2/list/strikes"

    # Convert the expiration date to a string
    expiration_str = expiration.strftime("%Y%m%d")

    querystring = {"root": ticker, "exp": expiration_str}

    headers = {"Accept": "application/json"}

    # Send the request
    json_resp = get_request(url=url, headers=headers, querystring=querystring, username=username, password=password)

    # Convert to pandas dataframe
    df = pd.DataFrame(json_resp["response"], columns=json_resp["header"]["format"])

    # Convert df to a list of the first (and only) column
    strikes = df.iloc[:, 0].tolist()

    # Divide each strike by 1000 to get the actual strike price
    strikes = [x / 1000.0 for x in strikes]

    return strikes


def get_chains_cached(
    username: str,
    password: str,
    asset: Asset,
    current_date: date = None
) -> dict:
    """
    Retrieve option chain with caching (MATCHES POLYGON PATTERN).

    This function follows the EXACT same caching strategy as Polygon:
    1. Check cache: LUMIBOT_CACHE_FOLDER/thetadata/option_chains/{symbol}_{date}.parquet
    2. Reuse files within RECENT_FILE_TOLERANCE_DAYS (default 7 days)
    3. If not found, fetch from ThetaData and save to cache
    4. Use pyarrow engine with snappy compression

    Parameters
    ----------
    username : str
        ThetaData username
    password : str
        ThetaData password
    asset : Asset
        Underlying asset (e.g., Asset("SPY"))
    current_date : date
        Historical date for backtest (required)

    Returns
    -------
    dict : {
        "Multiplier": 100,
        "Exchange": "SMART",
        "Chains": {
            "CALL": {"2025-09-19": [140.0, 145.0, ...], ...},
            "PUT": {"2025-09-19": [140.0, 145.0, ...], ...}
        }
    }
    """
    from collections import defaultdict

    logger.debug(f"get_chains_cached called for {asset.symbol} on {current_date}")

    # 1) If current_date is None => bail out
    if current_date is None:
        logger.debug("No current_date provided; returning None.")
        return None

    # 2) Build cache folder path
    chain_folder = Path(LUMIBOT_CACHE_FOLDER) / "thetadata" / "option_chains"
    chain_folder.mkdir(parents=True, exist_ok=True)

    # 3) Check for recent cached file (within RECENT_FILE_TOLERANCE_DAYS)
    RECENT_FILE_TOLERANCE_DAYS = 7
    earliest_okay_date = current_date - timedelta(days=RECENT_FILE_TOLERANCE_DAYS)
    pattern = f"{asset.symbol}_*.parquet"
    potential_files = sorted(chain_folder.glob(pattern), reverse=True)

    for fpath in potential_files:
        fname = fpath.stem  # e.g., "SPY_2025-09-15"
        parts = fname.split("_", maxsplit=1)
        if len(parts) != 2:
            continue
        file_symbol, date_str = parts
        if file_symbol != asset.symbol:
            continue

        try:
            file_date = date.fromisoformat(date_str)
        except ValueError:
            continue

        # If file is recent enough, reuse it
        if earliest_okay_date <= file_date <= current_date:
            logger.debug(f"Reusing chain file {fpath} (file_date={file_date})")
            df_cached = pd.read_parquet(fpath, engine='pyarrow')

            # Convert back to dict with lists (not numpy arrays)
            data = df_cached["data"][0]
            for right in data["Chains"]:
                for exp_date in data["Chains"][right]:
                    data["Chains"][right][exp_date] = list(data["Chains"][right][exp_date])

            return data

    # 4) No suitable file => fetch from ThetaData
    logger.debug(f"No suitable file found for {asset.symbol} on {current_date}. Downloading...")
    print(f"\nDownloading option chain for {asset} on {current_date}. This will be cached for future use.")

    # Get expirations and strikes using existing functions
    expirations = get_expirations(username, password, asset.symbol, current_date)

    chains_dict = {
        "Multiplier": 100,
        "Exchange": "SMART",
        "Chains": {
            "CALL": defaultdict(list),
            "PUT": defaultdict(list)
        }
    }

    for expiration_str in expirations:
        expiration = date.fromisoformat(expiration_str)
        strikes = get_strikes(username, password, asset.symbol, expiration)

        chains_dict["Chains"]["CALL"][expiration_str] = sorted(strikes)
        chains_dict["Chains"]["PUT"][expiration_str] = sorted(strikes)

    # 5) Save to cache file for future reuse
    cache_file = chain_folder / f"{asset.symbol}_{current_date.isoformat()}.parquet"
    df_to_cache = pd.DataFrame({"data": [chains_dict]})
    df_to_cache.to_parquet(cache_file, compression='snappy', engine='pyarrow')
    logger.debug(f"Saved chain cache: {cache_file}")

    return chains_dict
