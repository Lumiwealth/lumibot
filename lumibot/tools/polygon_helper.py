# This file contains helper functions for getting data from Polygon.io
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pandas_market_calendars as mcal

# noinspection PyPackageRequirements
from polygon import RESTClient

from lumibot import LUMIBOT_CACHE_FOLDER
from lumibot.entities import Asset

WAIT_TIME = 60
POLYGON_QUERY_COUNT = 0  # This is a variable that updates every time we query Polygon
MAX_POLYGON_DAYS = 30


def get_price_data_from_polygon(
    api_key: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    timespan: str = "minute",
    has_paid_subscription: bool = False,
    quote_asset: Asset = None,
):
    """
    Queries Polygon.io for pricing data for the given asset and returns a DataFrame with the data. Data will be
    cached in the LUMIBOT_CACHE_FOLDER/polygon folder so that it can be reused later and we don't have to query
    Polygon.io every time we run a backtest.

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
    has_paid_subscription : bool
        Set to True if you have a paid subscription to Polygon.io. This will prevent the script from waiting 1 minute
        between requests to avoid hitting the rate limit.
    quote_asset : Asset
        The quote asset for the asset we are getting data for. This is only needed for Forex assets.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the pricing data for the asset

    """
    print(f"\nGetting pricing data for {asset} / {quote_asset} from Polygon...")
    global POLYGON_QUERY_COUNT  # Track if we need to wait between requests

    # Check if we already have data for this asset in the csv file
    df_all = None
    df_csv = None
    cache_file = build_cache_filename(asset, timespan)
    if cache_file.exists():
        df_csv = load_cache(cache_file)
        df_all = df_csv.copy()  # Make a copy so we can check the original later for differences

    # Check if we need to get more data
    missing_dates = get_missing_dates(df_all, asset, start, end)
    if not missing_dates:
        return df_all

    # RESTClient connection for Polygon Stock-Equity API; traded_asset is standard
    # Add "trace=True" to see the API calls printed to the console for debugging
    polygon_client = RESTClient(api_key)
    symbol = get_polygon_symbol(asset, polygon_client, quote_asset)  # Will do a Polygon query for option contracts

    # To reduce calls to Polygon, we call on full date ranges instead of including hours/minutes
    # get the full range of data we need in one call and ensure that there won't be any intraday gaps in the data.
    # Option data won't have any extended hours data so the padding is extra important for those.
    poly_start = missing_dates[0]  # Data will start at 8am UTC (4am EST)
    poly_end = missing_dates[-1]   # Data will end at 23:59 UTC (7:59pm EST)

    # Polygon only returns 50k results per query (~30days of 24hr 1min-candles) so we need to break up the query into
    # multiple queries if we are requesting more than 30 days of data
    delta = timedelta(days=MAX_POLYGON_DAYS)
    while poly_start <= missing_dates[-1]:
        # If we don't have a paid subscription, we need to wait 1 minute between requests because of
        # the rate limit. Wait every other query so that we don't spend too much time waiting.
        if not has_paid_subscription and POLYGON_QUERY_COUNT % 3 == 0:
            print(
                f"\nSleeping {WAIT_TIME} seconds getting pricing data for {asset} from Polygon because "
                f"we don't have a paid subscription and we don't want to hit the rate limit. If you want to "
                f"avoid this, you can get a paid subscription at https://polygon.io/pricing and "
                f"set `polygon_has_paid_subscription=True` when starting the backtest.\n"
            )
            time.sleep(WAIT_TIME)

        if poly_end > poly_start + delta:
            poly_end = poly_start + delta

        POLYGON_QUERY_COUNT += 1
        result = polygon_client.get_aggs(
            ticker=symbol,
            from_=poly_start,  # polygon-api-client docs say 'from' but that is a reserved word in python
            to=poly_end,
            # In Polygon, multiplier is the number of "timespans" in each candle, so if you want 5min candles
            # returned you would set multiplier=5 and timespan="minute". This is very different from the
            # asset.multiplier setting for option contracts.
            multiplier=1,
            timespan=timespan,
            limit=50000,  # Max limit for Polygon
        )

        if result:
            df_all = update_polygon_data(df_all, result)

        poly_start = poly_end + timedelta(days=1)
        poly_end = poly_start + delta

    update_cache(cache_file, df_all, df_csv)
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

    # Stock/Option Asset for Backtesting - Assuming NYSE trading days
    elif asset.asset_type == "stock" or asset.asset_type == "option":
        cal = mcal.get_calendar("NYSE")

    # Forex Asset for Backtesting - Forex trades weekdays, 24hrs starting Sunday 5pm EST
    # Calendar: "CME_FX"
    elif asset.asset_type == "forex":
        cal = mcal.get_calendar("CME_FX")

    else:
        raise ValueError(f"Unsupported asset type for polygon: {asset.asset_type}")

    # Get the trading days between the start and end dates
    df = cal.schedule(start_date=start.date(), end_date=end.date())
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
    if asset.asset_type == "crypto":
        quote_asset_symbol = quote_asset.symbol if quote_asset else "USD"
        symbol = f"X:{asset.symbol}{quote_asset_symbol}"

    # Stock-Equity Asset for Backtesting
    elif asset.asset_type == "stock":
        symbol = asset.symbol

    # Forex Asset for Backtesting
    elif asset.asset_type == "forex":
        # If quote_asset is None, throw an error
        if quote_asset is None:
            raise ValueError(
                f"quote_asset is required for asset type {asset.asset_type}"
            )

        symbol = f"C:{asset.symbol}{quote_asset.symbol}"

    # Option Asset for Backtesting - Do a query to Polygon to get the ticker
    elif asset.asset_type == "option":
        # Needed so BackTest both old and existing contracts
        real_today = date.today()
        expired = True if asset.expiration < real_today else False

        # Query for the historical Option Contract ticker backtest is looking for
        contracts = list(polygon_client.list_options_contracts(
            underlying_ticker=asset.symbol,
            expiration_date=asset.expiration,
            contract_type=asset.right.lower(),
            strike_price=asset.strike,
            expired=expired,
            limit=10,
        ))

        if len(contracts) == 0:
            raise LookupError(f"Unable to find option contract for {asset}")

        # Example: O:SPY230802C00457000
        symbol = contracts[0].ticker

    else:
        raise ValueError(f"Unsupported asset type for polygon: {asset.asset_type}")

    return symbol


def build_cache_filename(asset: Asset, timespan: str):
    """Helper function to create the cache filename for a given asset and timespan"""

    lumibot_polygon_cache_folder = Path(LUMIBOT_CACHE_FOLDER) / "polygon"

    # If It's an option then also add the expiration date, strike price and right to the filename
    if asset.asset_type == "option":
        if asset.expiration is None:
            raise ValueError(
                f"Expiration date is required for option {asset} but it is None"
            )

        # Make asset.expiration datetime into a string like "YYMMDD"
        expiry_string = asset.expiration.strftime("%y%m%d")
        uniq_str = f"{asset.symbol}_{expiry_string}_{asset.strike}_{asset.right}"
    else:
        uniq_str = asset.symbol

    cache_filename = f"{asset.asset_type}_{uniq_str}_{timespan}.csv"
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
    df_csv = pd.read_csv(cache_file, index_col="datetime")
    df_csv.index = pd.to_datetime(df_csv.index)
    df_csv = df_csv.sort_index()

    # Check if the index is already timezone aware
    if df_csv.index.tzinfo is None:
        # Set the timezone to UTC
        df_csv.index = df_csv.index.tz_localize("UTC")

    return df_csv


def update_cache(cache_file, df_all, df_csv):
    """Update the cache file with the new data"""
    # Check if df_all is different from df_csv (if df_csv exists)
    if df_all is not None and len(df_all) > 0:
        # Check if the dataframes are the same
        if df_all.equals(df_csv):
            return

        # Create the directory if it doesn't exist
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        # Save the data to a csv file
        df_all.to_csv(cache_file)


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
