# This file contains helper functions for getting data from Polygon.io
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# noinspection PyPackageRequirements
from polygon import RESTClient

from lumibot import LUMIBOT_CACHE_FOLDER
from lumibot.entities import Asset

WAIT_TIME = 60
POLYGON_QUERY_COUNT = 0


def get_price_data_from_polygon(
    api_key: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    timespan: str = "minute",
    has_paid_subscription: bool = False,
    quote_asset: Asset = None,
) -> pd.DataFrame:
    """
    Queries Polygon.io for pricing data for the given asset and returns a DataFrame with the data. Data will be
    cached in the LUMIBOT_CACHE_FOLDER/polygon folder so that it can be reused later and we don't have to query
    Polygon.io every time we run a backtest.
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
    if data_is_complete(df_all, asset, start, end):
        return df_all

    # We need to get more data - A query is definitely about to happen
    # If we don't have a paid subscription, we need to wait 1 minute between requests because of
    # the rate limit
    if not has_paid_subscription and POLYGON_QUERY_COUNT:
        print(
            f"\nSleeping {WAIT_TIME} seconds getting pricing data for {asset} from Polygon because "
            f"we don't have a paid subscription and we don't want to hit the rate limit. If you want to "
            f"avoid this, you can get a paid subscription at https://polygon.io/pricing and "
            f"set `polygon_has_paid_subscription=True` when starting the backtest.\n"
        )
        time.sleep(WAIT_TIME)

    # RESTClient connection for Polygon Stock-Equity API; traded_asset is standard
    # Add "trace=True" to see the API calls printed to the console for debugging
    polygon_client = RESTClient(api_key)
    symbol = get_polygon_symbol(asset, polygon_client, quote_asset)  # Will do a Polygon query for option contracts

    # To reduce calls to Polygon, we pad an extra day to the start/end dates so that we can
    # get the full range of data we need in one call and ensure that there won't be any intraday gaps in the data.
    # Option data won't have any extended hours data so the padding is extra important for those.
    poly_start = start.date() - timedelta(days=1)  # Data will start at 8am UTC (4am EST)
    poly_end = end.date() + timedelta(days=1)      # Data will end at 23:59 UTC (7:59pm EST)

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
    )

    # Check if we got data from Polygon
    if not result:
        raise LookupError(f"No data returned from Polygon for {asset}")

    df_all = update_polygon_data(df_all, result)
    update_cache(cache_file, df_all, df_csv)
    return df_all


def get_polygon_symbol(asset, polygon_client, quote_asset=None):
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
        # Query for the historical Option Contract ticker backtest is looking for
        contracts = list(polygon_client.list_options_contracts(
            underlying_ticker=asset.symbol,
            expiration_date=asset.expiration,
            contract_type=asset.right.lower(),
            strike_price=asset.strike,
            expired=True,  # Needed so BackTest can look at old contracts to find the ticker we need
            limit=10,
        ))

        if len(contracts) == 0:
            raise LookupError(f"Unable to find option contract for {asset}")

        # Example: O:SPY230802C00457000
        symbol = contracts[0].ticker

    else:
        raise ValueError(f"Unsupported asset type for polygon: {asset.asset_type}")

    return symbol


def build_cache_filename(asset: Asset, timespan: str) -> Path:
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


def data_is_complete(df_all, asset, start, end):
    """
    Check if we have data for the full range
    Later Query to Polygon will pad an extra full day to start/end dates so that there should never
    be any gap with intraday data missing

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
    bool
        True if we have all the data we need, False if we need to get more data
    """
    if df_all is not None and len(df_all) > 0:
        data_start = df_all.index[0]
        data_end = df_all.index[-1]

        # Check if we have all the data we need
        if data_end >= end and data_start <= start:
            return True
        # If it's an option then we need to check if the last row is past the expiration date
        elif (asset.asset_type == "option" and
              data_end.date() >= asset.expiration and
              data_start <= start):
            return True

    return False


def load_cache(cache_file):
    df_csv = pd.read_csv(cache_file, index_col="datetime")
    df_csv.index = pd.to_datetime(df_csv.index)
    df_csv = df_csv.sort_index()

    # Check if the index is already timezone aware
    if df_csv.index.tzinfo is None:
        # Set the timezone to UTC
        df_csv.index = df_csv.index.tz_localize("UTC")

    return df_csv


def update_cache(cache_file, df_all, df_csv):

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
    df = pd.DataFrame(result)
    if df is not None and len(df) > 0:
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

        if df_all is None or not len(df_all):
            df_all = df
        else:
            df_all = pd.concat([df_all, df]).sort_index().drop_duplicates()

        # Remove any duplicate rows
        df_all = df_all[~df_all.index.duplicated(keep="first")]

    return df_all
