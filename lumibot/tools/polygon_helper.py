import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
import os
from urllib3.exceptions import MaxRetryError
from urllib.parse import urlparse, urlunparse

import pandas as pd
import pandas_market_calendars as mcal
from lumibot import LUMIBOT_CACHE_FOLDER
from lumibot.entities import Asset

# noinspection PyPackageRequirements
from polygon import RESTClient
from typing import Iterator
from termcolor import colored
from tqdm import tqdm

from lumibot import LUMIBOT_DEFAULT_PYTZ
from lumibot.credentials import POLYGON_API_KEY
from collections import defaultdict  # <-- Make sure we import defaultdict

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
            filtered_schedule = buffered_schedule[
                (buffered_schedule.index >= start_timestamp) & 
                (buffered_schedule.index <= end_timestamp)
            ]
            schedule_cache[cache_key] = filtered_schedule
            return filtered_schedule

    # Fetch and cache the new buffered schedule
    buffered_schedule = cal.schedule(start_date=start_date, end_date=buffer_end)
    buffered_schedules[cal.name] = buffered_schedule  # Store the buffered schedule for this calendar

    # Filter the schedule to only include the requested date range
    filtered_schedule = buffered_schedule[
        (buffered_schedule.index >= start_timestamp) & 
        (buffered_schedule.index <= end_timestamp)
    ]

    # Cache the filtered schedule for quick lookup
    schedule_cache[cache_key] = filtered_schedule

    return filtered_schedule


def get_price_data_from_polygon(
    api_key: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    timespan: str = "minute",
    quote_asset: Asset = None,
    force_cache_update: bool = False,
):
    """
    Queries Polygon.io for pricing data for the given asset and returns a DataFrame with the data. Data will be
    cached in the LUMIBOT_CACHE_FOLDER/polygon folder so that it can be reused later and we don't have to query
    Polygon.io every time we run a backtest.

    If the Polygon response has missing bars for a date, the missing bars will be added as empty (all NaN) rows
    to the cache file to avoid querying Polygon for the same missing bars in the future.  Note that means if
    a request is for a future time then we won't make a request to Polygon for it later when that data might
    be available.  That should result in an error rather than missing data from Polygon, but just in case a
    problem occurs and you want to ensure that the data is up to date, you can set force_cache_update=True.

    Parameters
    ----------
    api_key : str
        The API key for Polygon.io
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
    force_cache_update : bool
        If True, ignore and overwrite existing cache.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the pricing data for the asset
    """

    # Check if we already have data for this asset in the feather file
    cache_file = build_cache_filename(asset, timespan)
    # Check whether it might be stale because of splits.
    force_cache_update = validate_cache(force_cache_update, asset, cache_file, api_key)

    df_all = None
    # Load from the cache file if it exists.
    if cache_file.exists() and not force_cache_update:
        logging.debug(f"Loading pricing data for {asset} / {quote_asset} with '{timespan}' timespan from cache file...")
        df_all = load_cache(cache_file)

    # Check if we need to get more data
    missing_dates = get_missing_dates(df_all, asset, start, end)
    if not missing_dates:
        # TODO: Do this upstream so we don't repeatedly call for known-to-be-missing bars.
        # Drop the rows with all NaN values that were added to the feather for symbols that have missing bars.
        if df_all is not None:
            df_all.dropna(how="all", inplace=True)
        return df_all

    # RESTClient connection for Polygon Stock-Equity API
    polygon_client = PolygonClient.create(api_key=api_key)
    symbol = get_polygon_symbol(asset, polygon_client, quote_asset)  # Might do a Polygon query for option contracts

    # Check if symbol is None, which means we couldn't find the option contract
    if symbol is None:
        return None

    # Polygon only returns 50k results per query (~30 days of 1-minute bars) so we might need multiple queries
    poly_start = missing_dates[0]
    poly_end = missing_dates[-1]

    total_days = (poly_end - poly_start).days + 1
    total_queries = (total_days // MAX_POLYGON_DAYS) + 1
    description = f"\nDownloading data for {asset} / {quote_asset} '{timespan}' from Polygon..."
    pbar = tqdm(total=total_queries, desc=description, dynamic_ncols=True)

    delta = timedelta(days=MAX_POLYGON_DAYS)
    while poly_start <= poly_end:
        chunk_end = min(poly_start + delta, poly_end)

        result = polygon_client.get_aggs(
            ticker=symbol,
            from_=poly_start,
            to=chunk_end,
            multiplier=1,
            timespan=timespan,
            limit=50000,
        )
        pbar.update(1)

        if result:
            df_all = update_polygon_data(df_all, result)

        poly_start = chunk_end + timedelta(days=1)

    pbar.close()

    # Recheck for missing dates so they can be added in the feather update.
    missing_dates = get_missing_dates(df_all, asset, start, end)
    update_cache(cache_file, df_all, missing_dates)

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
    list of datetime.date
        The list of valid trading days
    """
    if asset.asset_type == Asset.AssetType.CRYPTO:
        # Crypto trades every day, 24/7 so we don't need to check the calendar
        return [start.date() + timedelta(days=x) for x in range((end.date() - start.date()).days + 1)]
    elif (
        asset.asset_type == Asset.AssetType.INDEX
        or asset.asset_type == Asset.AssetType.STOCK
        or asset.asset_type == Asset.AssetType.OPTION
    ):
        cal = mcal.get_calendar("NYSE")
    elif asset.asset_type == Asset.AssetType.FOREX:
        cal = mcal.get_calendar("CME_FX")
    else:
        raise ValueError(f"Unsupported asset type for polygon: {asset.asset_type}")

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
    if asset.asset_type == Asset.AssetType.CRYPTO:
        quote_asset_symbol = quote_asset.symbol if quote_asset else "USD"
        symbol = f"X:{asset.symbol}{quote_asset_symbol}"
    elif asset.asset_type == Asset.AssetType.STOCK:
        symbol = asset.symbol
    elif asset.asset_type == Asset.AssetType.INDEX:
        symbol = f"I:{asset.symbol}"
    elif asset.asset_type == Asset.AssetType.FOREX:
        if quote_asset is None:
            raise ValueError(f"quote_asset is required for asset type {asset.asset_type}")
        symbol = f"C:{asset.symbol}{quote_asset.symbol}"
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
    dates = pd.Series(df_all.index.date).unique()
    missing_dates = sorted(set(trading_dates) - set(dates))

    # Additional logic about NaN rows is disabled for now (see comments)
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
        A list of dates that are missing bars from Polygon
    """

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
        A list of dictionaries with the new data from Polygon
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


class PolygonClient(RESTClient):
    ''' Rate Limited RESTClient with factory method '''

    WAIT_SECONDS_RETRY = 60

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
        while True:
            try:
                return super()._get(*args, **kwargs)

            except MaxRetryError as e:
                url = urlunparse(urlparse(kwargs['path'])._replace(query=""))

                message = (
                    "Polygon rate limit reached.\n\n"
                    f"REST API call affected: {url}\n\n"
                    f"Sleeping for {PolygonClient.WAIT_SECONDS_RETRY} seconds seconds before trying again.\n\n"
                    "If you want to avoid this, consider a paid subscription with Polygon at https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10\n"
                    "Please use the full link to give us credit for the sale, it helps support this project.\n"
                    "You can use the coupon code 'LUMI10' for 10% off."
                )

                colored_message = colored(message, "red")

                logging.error(colored_message)
                logging.debug(f"Error: {e}")
                time.sleep(PolygonClient.WAIT_SECONDS_RETRY)


# -------------------------------------------------------------------------
# NEW FUNCTION: get_option_chains_with_cache
# This function is a slightly modified version of the old get_chains code,
# ensuring both CALL and PUT data is returned. We store them in a dictionary
# structure under "Chains": {"CALL": {...}, "PUT": {...}}.
# -------------------------------------------------------------------------
def get_option_chains_with_cache(polygon_client: RESTClient, asset: Asset, current_date: date):
    """
    Integrates the Polygon client library into the LumiBot backtest for Options Data, returning
    the same structure as Interactive Brokers option chain data, but with file-based caching.

    The returned dictionary has the format:
      {
          "Multiplier": 100,
          "Exchange": "NYSE",
          "Chains": {
              "CALL": { "2023-02-15": [strike1, ...], ... },
              "PUT":  { "2023-02-15": [strike9, ...], ... }
          }
      }

    Parameters
    ----------
    polygon_client : RESTClient
        The RESTClient (PolygonClient) instance used to fetch data from Polygon.
    asset : Asset
        The underlying asset to get data for.
    current_date : date
        The current date in the backtest to determine expired vs. not expired.

    Returns
    -------
    dict
        A nested dictionary with "Multiplier", "Exchange", and "Chains" keys.
        "Chains" is further broken down into "CALL" and "PUT" keys, each mapping
        expiration dates to lists of strikes.
    """
    # 1) Build a chain cache filename for this asset
    cache_file = _build_chain_filename(asset)

    # 2) Attempt to load cached data
    df_cached = _load_cached_chains(cache_file)
    if df_cached is not None and not df_cached.empty:
        # Convert DF back to the nested dict
        dict_cached = _df_to_chain_dict(df_cached)
        if dict_cached["Chains"]:
            logging.debug(f"[CHAIN CACHE] Loaded option chains for {asset.symbol} from {cache_file}")
            return dict_cached

    # 3) If cache was empty, do the original chain-fetch logic
    option_contracts = {
        "Multiplier": None,
        "Exchange": None,
        "Chains": {"CALL": defaultdict(list), "PUT": defaultdict(list)},
    }

    real_today = date.today()
    # If the strategy is using a recent backtest date, some contracts might not be expired yet
    expired_list = [True, False] if real_today - current_date <= timedelta(days=31) else [True]
    polygon_contracts_list = []
    for expired in expired_list:
        polygon_contracts_list.extend(
            list(
                polygon_client.list_options_contracts(
                    underlying_ticker=asset.symbol,
                    expiration_date_gte=current_date,
                    expired=expired,  # old + new contracts
                    limit=1000,
                )
            )
        )

    for pc in polygon_contracts_list:
        # Return to loop and skip if shares_per_contract != 100 (non-standard)
        if pc.shares_per_contract != 100:
            continue

        exchange = pc.primary_exchange
        right = pc.contract_type.upper()   # "CALL" or "PUT"
        exp_date = pc.expiration_date      # e.g. "2023-08-04"
        strike = pc.strike_price

        option_contracts["Multiplier"] = pc.shares_per_contract
        option_contracts["Exchange"] = exchange
        option_contracts["Chains"][right][exp_date].append(strike)

    # 4) Save newly fetched chains to the cache
    df_new = _chain_dict_to_df(option_contracts)
    if not df_new.empty:
        _save_cached_chains(cache_file, df_new)
        logging.debug(f"[CHAIN CACHE] Saved new option chains for {asset.symbol} to {cache_file}")

    return option_contracts


# ------------------------------ HELPER FUNCS FOR CHAIN CACHING ------------------------------
def _build_chain_filename(asset: Asset) -> Path:
    """
    Build a cache filename for the chain data, e.g.:
    ~/.lumibot_cache/polygon_chains/option_chains_SPY.feather
    """
    chain_folder = Path(LUMIBOT_CACHE_FOLDER) / "polygon_chains"
    chain_folder.mkdir(parents=True, exist_ok=True)
    file_name = f"option_chains_{asset.symbol}.feather"
    return chain_folder / file_name


def _load_cached_chains(cache_file: Path) -> pd.DataFrame:
    """Load chain data from Feather, or return empty DataFrame if not present."""
    if not cache_file.exists():
        return pd.DataFrame()
    return pd.read_feather(cache_file)


def _save_cached_chains(cache_file: Path, df: pd.DataFrame):
    """Save chain data to Feather."""
    df.reset_index(drop=True, inplace=True)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_feather(cache_file)


def _chain_dict_to_df(chain_dict: dict) -> pd.DataFrame:
    """
    Flatten the nested chain dict structure into a DataFrame:
      [Multiplier, Exchange, ContractType, Expiration, Strike]
    """
    rows = []
    mult = chain_dict["Multiplier"]
    exch = chain_dict["Exchange"]
    for ctype, exp_dict in chain_dict["Chains"].items():
        for exp_date, strike_list in exp_dict.items():
            for s in strike_list:
                rows.append({
                    "Multiplier": mult,
                    "Exchange": exch,
                    "ContractType": ctype,
                    "Expiration": exp_date,
                    "Strike": s
                })
    return pd.DataFrame(rows)


def _df_to_chain_dict(df: pd.DataFrame) -> dict:
    """
    Rebuild the chain dictionary from a DataFrame with columns:
      [Multiplier, Exchange, ContractType, Expiration, Strike]
    """
    chain_dict = {
        "Multiplier": None,
        "Exchange": None,
        "Chains": {"CALL": defaultdict(list), "PUT": defaultdict(list)},
    }
    if df.empty:
        return chain_dict

    chain_dict["Multiplier"] = df["Multiplier"].iloc[0]
    chain_dict["Exchange"] = df["Exchange"].iloc[0]

    for row in df.itertuples(index=False):
        ctype = row.ContractType   # "CALL" or "PUT"
        exp_date = row.Expiration
        strike = row.Strike
        chain_dict["Chains"][ctype][exp_date].append(strike)

    return chain_dict