# This file contains helper functions for getting data from Polygon.io
import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib3.exceptions import MaxRetryError
from urllib.parse import urlparse, urlunparse
from collections import defaultdict
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pandas_market_calendars as mcal
from lumibot import LUMIBOT_CACHE_FOLDER
from lumibot.entities import Asset

# noinspection PyPackageRequirements
from polygon import RESTClient
from typing import Iterator
from termcolor import colored
from tqdm import tqdm

from lumibot import LUMIBOT_CACHE_FOLDER
from lumibot.entities import Asset
from lumibot import LUMIBOT_DEFAULT_PYTZ
from lumibot.credentials import POLYGON_API_KEY

# Adjust as desired, in days. We'll reuse any existing chain file
# that is not older than RECENT_FILE_TOLERANCE_DAYS.
RECENT_FILE_TOLERANCE_DAYS = 14

# Maximum number of days to query in a single call to Polygon
MAX_POLYGON_DAYS = 30

# Define a cache dictionary to store schedules and a global dictionary for buffered schedules
schedule_cache = {}
buffered_schedules = {}


def get_cached_schedule(cal, start_date, end_date, buffer_days=30):
    """
    Fetch schedule with a buffer at the end. This is done to reduce the number of calls to the calendar API (which is slow).
    """
    global buffered_schedules

    buffer_end = end_date + timedelta(days=buffer_days)
    cache_key = (cal.name, start_date, end_date)

    # Check if the required range is in the schedule cache
    if cache_key in schedule_cache:
        return schedule_cache[cache_key]

    # Convert start_date and end_date to pd.Timestamp for comparison
    start_timestamp = pd.Timestamp(start_date)
    end_timestamp = pd.Timestamp(end_date)

    # Check if we have the buffered schedule for this calendar
    if cal.name in buffered_schedules:
        buffered_schedule = buffered_schedules[cal.name]
        # Check if the current buffered schedule covers the required range
        if buffered_schedule.index.min() <= start_timestamp and buffered_schedule.index.max() >= end_timestamp:
            filtered_schedule = buffered_schedule[(buffered_schedule.index >= start_timestamp) & (
                buffered_schedule.index <= end_timestamp)]
            schedule_cache[cache_key] = filtered_schedule
            return filtered_schedule

    # Fetch and cache the new buffered schedule
    buffered_schedule = cal.schedule(start_date=start_date, end_date=buffer_end)
    buffered_schedules[cal.name] = buffered_schedule  # Store the buffered schedule for this calendar

    # Filter the schedule to only include the requested date range
    filtered_schedule = buffered_schedule[(buffered_schedule.index >= start_timestamp)
                                          & (buffered_schedule.index <= end_timestamp)]

    # Cache the filtered schedule for quick lookup
    schedule_cache[cache_key] = filtered_schedule

    return filtered_schedule


def get_price_data_from_polygon(
    api_key: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    timespan: str = "minute",
    quote_asset: Optional[Asset] = None,
    force_cache_update: bool = False,
    max_workers: int = 10,
):
    """
    Queries Polygon.io for pricing data for the given asset in parallel and returns 
    a DataFrame with the data. It relies on the custom PolygonClient for rate-limit 
    handling (which sleeps 60 seconds if we hit MaxRetryError).

    Data is cached in LUMIBOT_CACHE_FOLDER/polygon so we don't redownload the same data
    on subsequent runs.

    Parameters
    ----------
    api_key : str
        The API key for Polygon.io.
    asset : Asset
        The asset we want data for (e.g., Asset("SPY")).
    start : datetime
        The start datetime for the requested data.
    end : datetime
        The end datetime for the requested data.
    timespan : str, default "minute"
        The timespan for the returned candles (e.g., "minute", "day").
    quote_asset : Asset, optional
        If needed, e.g. for Forex pairs. Usually None for stocks.
    force_cache_update : bool, default False
        If True, forces re-downloading data even if it’s in cache (e.g. if we suspect 
        splits or want the latest bars).
    max_workers : int, default 5
        The number of parallel threads for chunked downloads.

    Returns
    -------
    pd.DataFrame or None
        The DataFrame of historical data for the given asset and timeframe, or None 
        if symbol not found.

    Notes
    -----
    - The built-in `PolygonClient._get()` method in your codebase catches rate-limit 
      errors (MaxRetryError) and sleeps 60 seconds before retrying.
    - If you are on a free plan (5 calls/min), consider reducing `max_workers` 
      to avoid multiple simultaneous sleeps.
    """
    # 1) Decide where to cache the data (based on asset & timespan).
    cache_file = build_cache_filename(asset, timespan)

    # 2) Possibly invalidate the cache if we detect changed splits, etc.
    force_cache_update = validate_cache(force_cache_update, asset, cache_file, api_key)

    df_all = None
    # 3) Load from the cache if it exists and we're not forcing a re-download.
    if cache_file.exists() and not force_cache_update:
        logging.debug(f"Loading pricing data for {asset} / {quote_asset} with '{timespan}' timespan from cache file...")
        df_all = load_cache(cache_file)

    # 4) Figure out which dates are missing
    missing_dates = get_missing_dates(df_all, asset, start, end)
    if not missing_dates:
        # If none are missing, just drop known empty rows and return
        if df_all is not None:
            df_all.dropna(how="all", inplace=True)
        return df_all

    # 5) Create a PolygonClient (already includes a rate-limit loop in _get())
    polygon_client = PolygonClient.create(api_key=api_key)
    symbol = get_polygon_symbol(asset, polygon_client, quote_asset)
    if symbol is None:
        return None

    # 6) Identify a date range from the earliest missing date to the latest
    poly_start = missing_dates[0]
    poly_end = missing_dates[-1]

    # We'll break this into multiple ~30-day chunks to avoid the 50k limit
    total_days = (poly_end - poly_start).days + 1
    total_queries = (total_days // MAX_POLYGON_DAYS) + 1

    # Build the chunk list
    chunks = []
    delta = timedelta(days=MAX_POLYGON_DAYS)
    s_date = poly_start
    while s_date <= poly_end:
        e_date = min(poly_end, s_date + delta)
        chunks.append((s_date, e_date))
        s_date = e_date + timedelta(days=1)

    # 7) Prepare a progress bar
    desc_text = f"\nDownloading and caching {asset} / {quote_asset.symbol if quote_asset else ''} '{timespan}'. This will be much faster next time"
    pbar = tqdm(total=total_queries, desc=desc_text, dynamic_ncols=True)

    # Helper function for each chunk
    def fetch_chunk(start_date, end_date):
        # This call may trigger the built-in rate-limit logic in polygon_client._get()
        return polygon_client.get_aggs(
            ticker=symbol,
            from_=start_date,
            to=end_date,
            multiplier=1,  # e.g. 1 "minute"
            timespan=timespan,
            limit=50000,
        )

    # 8) Download chunks in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_range = {
            executor.submit(fetch_chunk, cstart, cend): (cstart, cend)
            for (cstart, cend) in chunks
        }

        for future in as_completed(future_to_range):
            cstart, cend = future_to_range[future]
            try:
                result = future.result()
                if result:
                    df_all = update_polygon_data(df_all, result)
            except Exception as exc:
                logging.error(f"Failed to fetch chunk {cstart} to {cend}: {exc}")
            finally:
                pbar.update(1)

    pbar.close()

    # 9) Re-check for missing data (some bars might be 0 or partial) and update cache
    missing_dates = get_missing_dates(df_all, asset, start, end)
    update_cache(cache_file, df_all, missing_dates)

    # Clean up empty rows and return
    if df_all is not None:
        df_all.dropna(how="all", inplace=True)
    return df_all


def validate_cache(force_cache_update: bool, asset: Asset, cache_file: Path, api_key: str):
    """
    If the list of splits for a stock have changed then we need to invalidate its cache
    because all of the prices will have changed (because we're using split adjusted prices).
    Get the splits data from Polygon only once per day per stock.
    Use the timestamp on the splits feather file to determine if we need to get the splits again.
    When invalidating we delete the cache file and return force_cache_update=True too.
    """
    if asset.asset_type not in [Asset.AssetType.STOCK, Asset.AssetType.OPTION]:
        return force_cache_update
    cached_splits = pd.DataFrame()
    splits_file_stale = True
    splits_file_path = Path(str(cache_file).rpartition(".feather")[0] + "_splits.feather")
    if splits_file_path.exists():
        splits_file_stale = datetime.fromtimestamp(splits_file_path.stat().st_mtime).date() != date.today()
        if splits_file_stale:
            cached_splits = pd.read_feather(splits_file_path)
    if splits_file_stale or force_cache_update:
        polygon_client = PolygonClient.create(api_key=api_key)
        # Need to get the splits in execution order to make the list comparable across invocations.
        splits = polygon_client.list_splits(ticker=asset.symbol, sort="execution_date", order="asc")
        if isinstance(splits, Iterator):
            # Convert the generator to a list so DataFrame will make a row per item.
            splits_df = pd.DataFrame(list(splits))
            if splits_file_path.exists() and cached_splits.eq(splits_df).all().all():
                # No need to rewrite contents.  Just update the timestamp.
                splits_file_path.touch()
            else:
                logging.info(f"Invalidating cache for {asset.symbol} because its splits have changed.")
                force_cache_update = True
                cache_file.unlink(missing_ok=True)
                # Create the directory if it doesn't exist
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                splits_df.to_feather(splits_file_path)
        else:
            logging.warn(f"Unexpected response getting splits for {asset.symbol} from Polygon.  Response: {splits}")
    return force_cache_update


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
    if asset.asset_type == Asset.AssetType.CRYPTO:
        # Crypto trades every day, 24/7 so we don't need to check the calendar
        return [start.date() + timedelta(days=x) for x in range((end.date() - start.date()).days + 1)]

    # Stock/Option Asset for Backtesting - Assuming NYSE trading days
    elif (
        asset.asset_type == Asset.AssetType.INDEX
        or asset.asset_type == Asset.AssetType.STOCK
        or asset.asset_type == Asset.AssetType.OPTION
    ):
        cal = mcal.get_calendar("NYSE")

    # Forex Asset for Backtesting - Forex trades weekdays, 24hrs starting Sunday 5pm EST
    # Calendar: "CME_FX"
    elif asset.asset_type == Asset.AssetType.FOREX:
        cal = mcal.get_calendar("CME_FX")

    else:
        raise ValueError(f"Unsupported asset type for polygon: {asset.asset_type}")

    # Get the trading days between the start and end dates
    df = get_cached_schedule(cal, start.date(), end.date())
    trading_days = df.index.date.tolist()
    return trading_days


def get_polygon_symbol(asset, polygon_client, quote_asset=None):
    """
    Get the symbol for the asset in a format that Polygon will understand
    Parameters
    ----------
    asset : Asset
        Asset we are getting data for
    polygon_client : RESTClient
        The RESTClient connection for Polygon Stock-Equity API
    quote_asset : Asset
        The quote asset for the asset we are getting data for

    Returns
    -------
    str
        The symbol for the asset in a format that Polygon will understand
    """
    # Crypto Asset for Backtesting
    if asset.asset_type == Asset.AssetType.CRYPTO:
        quote_asset_symbol = quote_asset.symbol if quote_asset else "USD"
        symbol = f"X:{asset.symbol}{quote_asset_symbol}"

    # Stock-Equity Asset for Backtesting
    elif asset.asset_type == Asset.AssetType.STOCK:
        symbol = asset.symbol

    elif asset.asset_type == Asset.AssetType.INDEX:
        symbol = f"I:{asset.symbol}"

    # Forex Asset for Backtesting
    elif asset.asset_type == Asset.AssetType.FOREX:
        # If quote_asset is None, throw an error
        if quote_asset is None:
            raise ValueError(f"quote_asset is required for asset type {asset.asset_type}")

        symbol = f"C:{asset.symbol}{quote_asset.symbol}"

    # Option Asset for Backtesting - Do a query to Polygon to get the ticker
    elif asset.asset_type == Asset.AssetType.OPTION:
        # Needed so BackTest both old and existing contracts
        real_today = date.today()
        expired = True if asset.expiration < real_today else False

        # Query for the historical Option Contract ticker backtest is looking for
        contracts = list(
            polygon_client.list_options_contracts(
                underlying_ticker=asset.symbol,
                expiration_date=asset.expiration,
                contract_type=asset.right.lower(),
                strike_price=asset.strike,
                expired=expired,
                limit=10,
            )
        )

        if len(contracts) == 0:
            text = colored(f"Unable to find option contract for {asset}", "red")
            logging.debug(text)
            return

        # Example: O:SPY230802C00457000
        symbol = contracts[0].ticker

    elif asset.asset_type == Asset.AssetType.INDEX:
        symbol = f"I:{asset.symbol}"

    else:
        raise ValueError(f"Unsupported asset type for polygon: {asset.asset_type}")

    return symbol


def build_cache_filename(asset: Asset, timespan: str):
    """Helper function to create the cache filename for a given asset and timespan"""

    lumibot_polygon_cache_folder = Path(LUMIBOT_CACHE_FOLDER) / "polygon"

    # If It's an option then also add the expiration date, strike price and right to the filename
    if asset.asset_type == "option":
        if asset.expiration is None:
            raise ValueError(f"Expiration date is required for option {asset} but it is None")

        # Make asset.expiration datetime into a string like "YYMMDD"
        expiry_string = asset.expiration.strftime("%y%m%d")
        uniq_str = f"{asset.symbol}_{expiry_string}_{asset.strike}_{asset.right}"
    else:
        uniq_str = asset.symbol

    cache_filename = f"{asset.asset_type}_{uniq_str}_{timespan}.feather"
    cache_file = lumibot_polygon_cache_folder / cache_filename
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

    # For Options, don't need any dates passed the expiration date
    if asset.asset_type == "option":
        trading_dates = [x for x in trading_dates if x <= asset.expiration]

    if df_all is None or not len(df_all) or df_all.empty:
        return trading_dates

    # It is possible to have full day gap in the data if previous queries were far apart
    # Example: Query for 8/1/2023, then 8/31/2023, then 8/7/2023
    # Whole days are easy to check for because we can just check the dates in the index
    dates = pd.Series(df_all.index.date).unique()
    missing_dates = sorted(set(trading_dates) - set(dates))

    # Find any dates with nan values in the df_all DataFrame. This happens for some infrequently traded assets, but
    # it is difficult to know if the data is actually missing or if it is just infrequent trading, query for it again.
    missing_dates += df_all[df_all.isnull().all(axis=1)].index.date.tolist()

    # make sure the dates are unique
    missing_dates = list(set(missing_dates))
    missing_dates.sort()

    # finally, filter out any dates that are not in start/end range (inclusive)
    missing_dates = [d for d in missing_dates if start.date() <= d <= end.date()]

    return missing_dates


def load_cache(cache_file):
    """Load the data from the cache file and return a DataFrame with a DateTimeIndex"""
    df_feather = pd.read_feather(cache_file)

    # Set the 'datetime' column as the index of the DataFrame
    df_feather.set_index("datetime", inplace=True)

    df_feather.index = pd.to_datetime(
        df_feather.index
    )  # TODO: Is there some way to speed this up? It takes several times longer than just reading the feather file
    df_feather = df_feather.sort_index()

    # Check if the index is already timezone aware
    if df_feather.index.tzinfo is None:
        # Set the timezone to UTC
        df_feather.index = df_feather.index.tz_localize("UTC")

    return df_feather

def update_cache(cache_file, df_all, missing_dates=None):
    """Update the cache file with the new data.  Missing dates are added as empty (all NaN) 
    rows before it is saved to the cache file.

    Parameters
    ----------
    cache_file : Path
        The path to the cache file
    df_all : pd.DataFrame
        The DataFrame with the data we want to cache
    missing_dates : list[datetime.date]
        A list of dates that are missing bars from Polygon"""

    if df_all is None:
        df_all = pd.DataFrame()

    if missing_dates:
        missing_df = pd.DataFrame(
            [datetime(year=d.year, month=d.month, day=d.day, tzinfo=LUMIBOT_DEFAULT_PYTZ) for d in missing_dates],
            columns=["datetime"])
        missing_df.set_index("datetime", inplace=True)
        # Set the timezone to UTC
        missing_df.index = missing_df.index.tz_convert("UTC")
        df_concat = pd.concat([df_all, missing_df]).sort_index()
        # Let's be careful and check for duplicates to avoid corrupting the feather file.
        if df_concat.index.duplicated().any():
            logging.warn(f"Duplicate index entries found when trying to update Polygon cache {cache_file}")
            if df_all.index.duplicated().any():
                logging.warn("The duplicate index entries were already in df_all")
        else:
            # All good, persist with the missing dates added
            df_all = df_concat

    if len(df_all) > 0:
        # Create the directory if it doesn't exist
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        # Reset the index to convert DatetimeIndex to a regular column
        df_all_reset = df_all.reset_index()

        # Save the data to a feather file
        df_all_reset.to_feather(cache_file)


def update_polygon_data(df_all, result):
    """
    Update the DataFrame with the new data from Polygon
    Parameters
    ----------
    df_all : pd.DataFrame
        A DataFrame with the data we already have
    result : list
        A List of dictionaries with the new data from Polygon
        Format: [{'o': 1.0, 'h': 2.0, 'l': 3.0, 'c': 4.0, 'v': 5.0, 't': 116120000000}]
    """
    df = pd.DataFrame(result)
    if not df.empty:
        # Rename columns
        df = df.rename(
            columns={
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
                "v": "volume",
            }
        )

        # Create a datetime column and set it as the index
        timestamp_col = "t" if "t" in df.columns else "timestamp"
        df = df.assign(datetime=pd.to_datetime(df[timestamp_col], unit="ms"))
        df = df.set_index("datetime").sort_index()

        # Set the timezone to UTC
        df.index = df.index.tz_localize("UTC")

        if df_all is None or df_all.empty:
            df_all = df
        else:
            df_all = pd.concat([df_all, df]).sort_index()
            df_all = df_all[~df_all.index.duplicated(keep="first")]  # Remove any duplicate rows

    return df_all

def get_chains_cached(
    api_key: str,
    asset: Asset,
    quote: Asset = None,
    exchange: str = None,
    current_date: date = None,
    polygon_client: Optional["PolygonClient"] = None,
) -> dict:
    """
    Retrieve an option chain for a given asset and historical date using Polygon, 
    with caching to reduce repeated downloads during backtests.

    Parameters
    ----------
    api_key : str
        Polygon.io API key.
    asset : Asset
        The underlying asset for which to retrieve options data (e.g., Asset("NVDA")).
    quote : Asset, optional
        The quote asset, typically unused for stock options.
    exchange : str, optional
        The exchange to consider (e.g., "NYSE").
    current_date : datetime.date, optional
        The *historical* date of interest (e.g., 2022-01-08). If omitted, this function 
        will return None immediately (no chain is fetched).
    polygon_client : PolygonClient, optional
        A reusable PolygonClient instance; if None, one will be created using the 
        given api_key.

    Returns
    -------
    dict or None
        A dictionary matching the LumiBot "option chain" structure:
        {
            "Multiplier": int,              # typically 100
            "Exchange": str,                # e.g., "NYSE"
            "Chains": {
                "CALL": {
                    "YYYY-MM-DD": [strike1, strike2, ...],
                    ...
                },
                "PUT": {
                    "YYYY-MM-DD": [...],
                    ...
                }
            }
        }
        If no current_date is specified, returns None instead.

    Notes
    -----
    1) We do *not* use the real system date in this function because it is purely 
       historical/backtest-oriented.
    2) If a suitable chain file from within RECENT_FILE_TOLERANCE_DAYS of current_date 
       exists, it is reused directly.
    3) Otherwise, the function downloads fresh data from Polygon, then saves it under 
       `LUMIBOT_CACHE_FOLDER/polygon/option_chains/{symbol}_{date}.feather`.
    4) By default, we fetch both 'expired=True' and 'expired=False', so you get 
       historical + near-future options for your specified date.
    """
    logging.debug(f"get_chains_cached called for {asset.symbol} on {current_date}")

    # 1) If current_date is None => bail out (no real date to query).
    if current_date is None:
        logging.debug("No current_date provided; returning None.")
        return None

    # 2) Ensure we have a PolygonClient
    if polygon_client is None:
        logging.debug("No polygon_client provided; creating a new one.")
        polygon_client = PolygonClient.create(api_key=api_key)

    # 3) Build the chain folder path and create if not present
    chain_folder = Path(LUMIBOT_CACHE_FOLDER) / "polygon" / "option_chains"
    chain_folder.mkdir(parents=True, exist_ok=True)

    # 4) Attempt to find a suitable recent file (reuse it if found)
    earliest_okay_date = current_date - timedelta(days=RECENT_FILE_TOLERANCE_DAYS)
    pattern = f"{asset.symbol}_*.feather"
    potential_files = sorted(chain_folder.glob(pattern), reverse=True)

    for fpath in potential_files:
        fname = fpath.stem  # e.g. "NVDA_2022-01-06"
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

        # If file_date is recent enough, reuse it
        if earliest_okay_date <= file_date <= current_date:
            logging.debug(
                f"Reusing chain file {fpath} (file_date={file_date}), "
                f"within {RECENT_FILE_TOLERANCE_DAYS} days of {current_date}."
            )
            df_cached = pd.read_feather(fpath)

            # Convert the data back to a dictionary of lists instead of NP arrays to match original return types
            data = df_cached["data"][0]
            for right in data["Chains"]:
                for exp_date in data["Chains"][right]:
                    data["Chains"][right][exp_date] = list(data["Chains"][right][exp_date])

            return data

    # 5) No suitable file => must fetch from Polygon
    logging.debug(
        f"No suitable recent file found for {asset.symbol} on {current_date}. "
        "Downloading from Polygon..."
    )
    print(f"\nDownloading option chain for {asset} on {current_date}. This will be cached for future use so it will be significantly faster the next time you run a backtest.")

    option_contracts = {
        "Multiplier": None,
        "Exchange": None,
        "Chains": {"CALL": defaultdict(list), "PUT": defaultdict(list)},
    }

    # 6) We do not use real "today" at all. By default, let's fetch both expired & unexpired 
    #    to ensure we get all relevant strikes near that historical date.
    expired_list = [True, False]

    polygon_contracts = []
    for expired in expired_list:
        contracts_gen = polygon_client.list_options_contracts(
            underlying_ticker=asset.symbol,
            expiration_date_gte=current_date,
            expired=expired,
            limit=1000,
        )
        polygon_contracts.extend(list(contracts_gen))

    # 7) Build the dictionary
    for c in polygon_contracts:
        if c.shares_per_contract != 100:
            continue

        exg = c.primary_exchange
        right = c.contract_type.upper()  # "CALL" or "PUT"
        exp_date = c.expiration_date     # "YYYY-MM-DD"
        strike = c.strike_price

        option_contracts["Multiplier"] = c.shares_per_contract
        option_contracts["Exchange"] = exg
        option_contracts["Chains"][right][exp_date].append(strike)

    # 8) Save to a new file for future reuse
    cache_file = chain_folder / f"{asset.symbol}_{current_date.isoformat()}.feather"
    df_to_cache = pd.DataFrame({"data": [option_contracts]})
    df_to_cache.to_feather(cache_file)
    logging.debug(
        f"Download complete for {asset.symbol} on {current_date}. "
        f"Saved chain file to {cache_file}"
    )

    return option_contracts


class PolygonClient(RESTClient):
    ''' Rate Limited RESTClient with factory method '''

    WAIT_SECONDS_RETRY = 60

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Time of last "rate limit reached" log (epoch time).
        self._last_rate_limit_log_time = 0.0
        # Only log once every 300s (5 minutes); tweak as you see fit.
        self._rate_limit_log_cooldown = 300.0

    @classmethod
    def create(cls, *args, **kwargs) -> RESTClient:
        """
        Factory method to create a RESTClient or PolygonClient instance.

        The method uses environment variables to determine default values for the API key 
        and subscription type. If the `api_key` is not provided in `kwargs`, it defaults 
        to the value of the `POLYGON_API_KEY` environment variable.
        If the environment variable is not set, it defaults to False.

        Keyword Arguments:
        api_key : str, optional
            The API key to authenticate with the service. Defaults to the value of the 
            `POLYGON_API_KEY` environment variable if not provided.

        Returns:
        RESTClient
            An instance of RESTClient or PolygonClient.

        Examples:
        ---------
        Using default environment variables:

        >>> client = PolygonClient.create()

        Providing an API key explicitly:

        >>> client = PolygonClient.create(api_key='your_api_key_here')

        """
        if 'api_key' not in kwargs:
            kwargs['api_key'] = POLYGON_API_KEY

        return cls(*args, **kwargs)

    def _get(self, *args, **kwargs):
        """
        Override to handle rate-limits by sleeping 60s, but *throttle*
        the log message so it isn't repeated too frequently.
        """
        while True:
            try:
                # Normal get from polygon-api-client
                return super()._get(*args, **kwargs)

            except MaxRetryError as e:
                # We interpret MaxRetryError as a rate-limit or server rejection
                url = urlunparse(urlparse(kwargs['path'])._replace(query=""))

                # Check if we've logged a rate-limit message recently
                now = time.time()
                time_since_last_log = now - self._last_rate_limit_log_time
                if time_since_last_log > self._rate_limit_log_cooldown:
                    # It's been long enough => log
                    message = (
                        "Polygon rate limit reached. "
                        f"Sleeping for {PolygonClient.WAIT_SECONDS_RETRY} seconds "
                        "before trying again.\n\n"
                        "If you want to avoid this, consider a paid subscription "
                        "with Polygon at https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10\n"
                        "Please use the full link to give us credit for the sale, "
                        "it helps support this project.\n"
                        "You can use the coupon code 'LUMI10' for 10% off."
                    )
                    colored_message = colored(message, "red")
                    logging.error(colored_message)
                    logging.debug(f"Error: {e}")

                    # Update our last log time
                    self._last_rate_limit_log_time = now
                else:
                    # If it's too soon, skip logging again
                    pass

                # Sleep for WAIT_SECONDS_RETRY, then try again
                time.sleep(PolygonClient.WAIT_SECONDS_RETRY)
