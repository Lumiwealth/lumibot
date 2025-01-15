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
from collections import defaultdict

import duckdb
import concurrent.futures
import threading

from lumibot import LUMIBOT_DEFAULT_PYTZ
from lumibot.credentials import POLYGON_API_KEY

MAX_POLYGON_DAYS = 30

# Path to local DuckDB database
DUCKDB_DB_PATH = Path(LUMIBOT_CACHE_FOLDER) / "polygon" / "polygon_cache.duckdb"
DUCKDB_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# We'll store bars in a single table 'price_data' with columns:
#   symbol, timespan, datetime, open, high, low, close, volume

# In-memory caches for schedules
schedule_cache = {}
buffered_schedules = {}

# Lock to handle concurrency for rate limits (useful on free plan). 
# Paid plan typically doesn't need this, but let's keep it to avoid confusion.
RATE_LIMIT_LOCK = threading.Lock()


def get_cached_schedule(cal, start_date, end_date, buffer_days=30):
    """
    Fetches the market schedule with a buffer, so we reduce calls to the calendar API.
    """
    global buffered_schedules

    buffer_end = end_date + timedelta(days=buffer_days)
    cache_key = (cal.name, start_date, end_date)

    if cache_key in schedule_cache:
        return schedule_cache[cache_key]

    start_timestamp = pd.Timestamp(start_date)
    end_timestamp = pd.Timestamp(end_date)

    if cal.name in buffered_schedules:
        buffered_schedule = buffered_schedules[cal.name]
        if buffered_schedule.index.min() <= start_timestamp and buffered_schedule.index.max() >= end_timestamp:
            filtered_schedule = buffered_schedule[
                (buffered_schedule.index >= start_timestamp) & (buffered_schedule.index <= end_timestamp)
            ]
            schedule_cache[cache_key] = filtered_schedule
            return filtered_schedule

    buffered_schedule = cal.schedule(start_date=start_date, end_date=buffer_end)
    buffered_schedules[cal.name] = buffered_schedule

    filtered_schedule = buffered_schedule[
        (buffered_schedule.index >= start_timestamp) & (buffered_schedule.index <= end_timestamp)
    ]
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
    Queries Polygon.io for pricing data for the given asset, caches it in DuckDB,
    then returns a DataFrame with the data (from DuckDB).

    1) We try to load existing data from DuckDB for [start, end].
    2) If some dates are missing, we fetch them (in parallel if possible).
    3) We do only one big DataFrame transformation & single write to DuckDB.
    4) We then unify the newly inserted data with any existing data and return it.

    This approach reduces repeated file writes, transformations, etc.
    """

    if not end:
        end = datetime.now()

    # 1) Attempt to load data from DuckDB
    existing_df = _load_from_duckdb(asset, timespan, start, end)

    # If force_cache_update is True, ignore existing data
    if force_cache_update:
        logging.info(f"Forcing cache update for {asset} from {start} to {end}")
        existing_df = pd.DataFrame()

    # 2) Identify missing days
    missing_dates = get_missing_dates(existing_df, asset, start, end)
    if not missing_dates:
        if not existing_df.empty:
            return existing_df.sort_index()
        return existing_df  # Could be empty if no data

    # 3) We have missing data, so fetch from Polygon
    polygon_client = PolygonClient.create(api_key=api_key)
    symbol = get_polygon_symbol(asset, polygon_client, quote_asset=quote_asset)
    if not symbol:
        # Means we couldn't find the option contract
        return None

    # Group missing days into ~30-day ranges for fewer calls
    day_ranges = _group_missing_dates(missing_dates)

    # Parallel fetch all chunks
    results_list = []
    max_workers = 10  # e.g. for paid plan, can go higher
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for (chunk_start, chunk_end) in day_ranges:
            fut = executor.submit(
                _fetch_polygon_data_chunk,
                polygon_client,
                symbol,
                chunk_start,
                chunk_end,
                timespan
            )
            futures.append(fut)

        for f in concurrent.futures.as_completed(futures):
            results_list.extend(f.result())

    # 4) Combine & transform once
    combined_df = _transform_polygon_data(results_list)
    if not combined_df.empty:
        # 5) Store new data in DuckDB
        _store_in_duckdb(asset, timespan, combined_df)

    # 6) Reload final data from DuckDB
    final_df = _load_from_duckdb(asset, timespan, start, end)
    if final_df is not None and not final_df.empty:
        final_df.dropna(how="all", inplace=True)

    return final_df


def validate_cache(force_cache_update: bool, asset: Asset, cache_file: Path, api_key: str):
    """
    Placeholder for split-check logic. 
    With DuckDB, we can adapt to re-fetch or update as needed.
    """
    return force_cache_update


def get_trading_dates(asset: Asset, start: datetime, end: datetime):
    """
    Returns a list of valid trading days (NYSE or CME_FX or crypto).
    """
    if asset.asset_type == Asset.AssetType.CRYPTO:
        return [start.date() + timedelta(days=x) for x in range((end.date() - start.date()).days + 1)]
    elif asset.asset_type in (Asset.AssetType.INDEX, Asset.AssetType.STOCK, Asset.AssetType.OPTION):
        cal = mcal.get_calendar("NYSE")
    elif asset.asset_type == Asset.AssetType.FOREX:
        cal = mcal.get_calendar("CME_FX")
    else:
        raise ValueError(f"Unsupported asset type for polygon: {asset.asset_type}")

    df = get_cached_schedule(cal, start.date(), end.date())
    return df.index.date.tolist()


def get_polygon_symbol(asset, polygon_client, quote_asset=None):
    """
    Converts our Asset into a Polygon-compatible symbol 
    e.g. "X:BTCUSD", "C:EURUSD", or "O:SPY230120C00360000" for options.
    """
    if asset.asset_type == Asset.AssetType.CRYPTO:
        quote_asset_symbol = quote_asset.symbol if quote_asset else "USD"
        return f"X:{asset.symbol}{quote_asset_symbol}"
    elif asset.asset_type == Asset.AssetType.STOCK:
        return asset.symbol
    elif asset.asset_type == Asset.AssetType.INDEX:
        return f"I:{asset.symbol}"
    elif asset.asset_type == Asset.AssetType.FOREX:
        if quote_asset is None:
            raise ValueError(f"quote_asset is required for {asset.asset_type}")
        return f"C:{asset.symbol}{quote_asset.symbol}"
    elif asset.asset_type == Asset.AssetType.OPTION:
        real_today = date.today()
        expired = True if asset.expiration < real_today else False
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
            msg = colored(f"Unable to find option contract for {asset}", "red")
            logging.debug(msg)
            return
        return contracts[0].ticker
    else:
        raise ValueError(f"Unsupported asset type: {asset.asset_type}")


def _fetch_polygon_data_chunk(polygon_client, symbol, chunk_start, chunk_end, timespan):
    """
    Fetch data for one range. We lock if needed for free plan rate limit.
    """
    with RATE_LIMIT_LOCK:
        results = polygon_client.get_aggs(
            ticker=symbol,
            from_=chunk_start,
            to=chunk_end,
            multiplier=1,
            timespan=timespan,
            limit=50000,
        )
    return results if results else []


def _transform_polygon_data(results_list):
    """
    Combine chunk results into one DataFrame, rename columns, set index, localize.
    """
    if not results_list:
        return pd.DataFrame()

    df = pd.DataFrame(results_list)
    if df.empty:
        return df

    rename_cols = {"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
    df = df.rename(columns=rename_cols, errors="ignore")

    timestamp_col = "t" if "t" in df.columns else "timestamp"
    if timestamp_col in df.columns:
        df["datetime"] = pd.to_datetime(df[timestamp_col], unit="ms")
        df.drop(columns=[timestamp_col], inplace=True)

    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)

    if df.index.tzinfo is None:
        df.index = df.index.tz_localize("UTC")

    return df


def _group_missing_dates(missing_dates):
    """
    Group consecutive missing days into ~30-day chunks for minute data, etc.
    """
    if not missing_dates:
        return []

    missing_dates = sorted(missing_dates)
    grouped = []

    chunk_start = missing_dates[0]
    chunk_end = chunk_start

    for d in missing_dates[1:]:
        if (d - chunk_end).days <= 1:
            chunk_end = d
        else:
            grouped.append((chunk_start, chunk_end))
            chunk_start = d
            chunk_end = d
    grouped.append((chunk_start, chunk_end))

    final_chunks = []
    delta_30 = timedelta(days=30)
    active_start, active_end = grouped[0]

    for (s, e) in grouped[1:]:
        if e - active_start <= delta_30:
            if e > active_end:
                active_end = e
        else:
            final_chunks.append((active_start, active_end))
            active_start, active_end = s, e
    final_chunks.append((active_start, active_end))

    # Convert to datetime range (0:00 -> 23:59)
    range_list = []
    for (s, e) in final_chunks:
        start_dt = datetime(s.year, s.month, s.day, tzinfo=LUMIBOT_DEFAULT_PYTZ)
        end_dt = datetime(e.year, e.month, e.day, 23, 59, tzinfo=LUMIBOT_DEFAULT_PYTZ)
        range_list.append((start_dt, end_dt))

    return range_list


def get_missing_dates(df_all, asset, start, end):
    """
    Identify which trading days are missing from df_all for the given date range.
    """
    trading_days = _get_trading_days(asset, start, end)
    if asset.asset_type == "option":
        trading_days = [x for x in trading_days if x <= asset.expiration]

    if df_all is None or df_all.empty:
        return trading_days

    existing_days = pd.Series(df_all.index.date).unique()
    missing = sorted(set(trading_days) - set(existing_days))
    return missing


def _get_trading_days(asset: Asset, start: datetime, end: datetime):
    return get_trading_dates(asset, start, end)


def _load_from_duckdb(asset: Asset, timespan: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Load cached data from DuckDB for the given asset/timespan/date range.
    If the table does not exist, return an empty DF.
    """
    conn = duckdb.connect(str(DUCKDB_DB_PATH), read_only=False)
    asset_key = _asset_key(asset)

    try:
        query = f"""
        SELECT *
        FROM price_data
        WHERE symbol='{asset_key}'
          AND timespan='{timespan}'
          AND datetime >= '{start.isoformat()}'
          AND datetime <= '{end.isoformat()}'
        ORDER BY datetime
        """
        df = conn.execute(query).fetchdf()

        if df.empty:
            return df

        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        df.set_index("datetime", inplace=True)
        df.sort_index(inplace=True)
        return df

    except duckdb.CatalogException:
        # If the table doesn't exist yet, return empty
        return pd.DataFrame()
    finally:
        conn.close()


def _store_in_duckdb(asset: Asset, timespan: str, df_in: pd.DataFrame):
    """
    Insert newly fetched data into the DuckDB 'price_data' table.

    - We explicitly pick only the columns needed: [datetime, open, high, low, close, volume].
    - We also add symbol & timespan columns.
    - We handle potential index issues by dropping 'datetime' if it already exists as a column.
    """

    if df_in.empty:
        return

    # Create a deep copy to avoid SettingWithCopyWarning
    new_df = df_in.copy(deep=True)

    # The columns we want to keep in the final DB
    columns_needed = ["datetime", "open", "high", "low", "close", "volume", "symbol", "timespan"]

    # Ensure they exist in new_df, fill with None if missing
    for c in columns_needed:
        if c not in new_df.columns:
            new_df.loc[:, c] = None

    # If the index is named 'datetime', we might want to reset it:
    if new_df.index.name == "datetime":
        # If there's already a 'datetime' column, drop it to avoid conflicts
        if "datetime" in new_df.columns:
            new_df.drop(columns=["datetime"], inplace=True)
        new_df.reset_index(drop=False, inplace=True)  # Now 'datetime' becomes a column

    # Now remove all columns except the needed ones
    new_df = new_df[columns_needed]

    # Setting these with loc to avoid SettingWithCopyWarning
    asset_key = _asset_key(asset)
    new_df.loc[:, "symbol"] = asset_key
    new_df.loc[:, "timespan"] = timespan

    conn = duckdb.connect(str(DUCKDB_DB_PATH), read_only=False)
    schema_ddl = """
    CREATE TABLE IF NOT EXISTS price_data (
        symbol      VARCHAR,
        timespan    VARCHAR,
        datetime    TIMESTAMP,
        open        DOUBLE,
        high        DOUBLE,
        low         DOUBLE,
        close       DOUBLE,
        volume      DOUBLE
    );
    """
    conn.execute(schema_ddl)

    # Create a temp table with same columns
    conn.execute("""
        CREATE TEMPORARY TABLE tmp_table(
            symbol VARCHAR,
            timespan VARCHAR,
            datetime TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE
        );
    """)

    conn.register("df_newdata", new_df)

    # Insert only matching columns, ignoring extras
    insert_sql = """
    INSERT INTO tmp_table
    SELECT symbol, timespan, datetime, open, high, low, close, volume
    FROM df_newdata;
    """
    conn.execute(insert_sql)

    # Upsert logic: only insert rows not already in price_data
    conn.execute("""
        INSERT INTO price_data
        SELECT t.*
        FROM tmp_table t
        LEFT JOIN price_data p
          ON t.symbol = p.symbol
          AND t.timespan = p.timespan
          AND t.datetime = p.datetime
        WHERE p.symbol IS NULL
    """)

    conn.close()


def _asset_key(asset: Asset) -> str:
    """
    Creates a unique string for storing the asset in DuckDB (e.g., SPY_230120_360_C for an option).
    """
    if asset.asset_type == "option":
        if not asset.expiration:
            raise ValueError("Option requires expiration date to build asset_key")
        expiry_str = asset.expiration.strftime("%y%m%d")
        return f"{asset.symbol}_{expiry_str}_{asset.strike}_{asset.right}"
    else:
        return asset.symbol


def get_option_chains_with_cache(polygon_client: RESTClient, asset: Asset, current_date: date):
    """
    Returns option chain data from Polygon, calls + puts.
    We do NOT store chain data in DuckDB by default here, 
    but you could adapt it to do so if you'd like.
    """
    option_contracts = {
        "Multiplier": None,
        "Exchange": None,
        "Chains": {"CALL": defaultdict(list), "PUT": defaultdict(list)},
    }
    real_today = date.today()
    expired_list = [True, False] if real_today - current_date <= timedelta(days=31) else [True]

    polygon_contracts_list = []
    for expired in expired_list:
        polygon_contracts_list.extend(
            list(
                polygon_client.list_options_contracts(
                    underlying_ticker=asset.symbol,
                    expiration_date_gte=current_date,
                    expired=expired,
                    limit=1000,
                )
            )
        )

    for pc in polygon_contracts_list:
        if pc.shares_per_contract != 100:
            continue
        exchange = pc.primary_exchange
        right = pc.contract_type.upper()
        exp_date = pc.expiration_date
        strike = pc.strike_price

        option_contracts["Multiplier"] = pc.shares_per_contract
        option_contracts["Exchange"] = exchange
        option_contracts["Chains"][right][exp_date].append(strike)

    return option_contracts


class PolygonClient(RESTClient):
    """
    Rate Limited RESTClient with a factory method.
    If hitting rate-limit or MaxRetryError, we sleep & retry.
    """

    WAIT_SECONDS_RETRY = 60

    @classmethod
    def create(cls, *args, **kwargs) -> RESTClient:
        if "api_key" not in kwargs:
            kwargs["api_key"] = POLYGON_API_KEY
        return cls(*args, **kwargs)

    def _get(self, *args, **kwargs):
        from urllib3.exceptions import MaxRetryError

        while True:
            try:
                return super()._get(*args, **kwargs)
            except MaxRetryError as e:
                url = urlunparse(urlparse(kwargs["path"])._replace(query=""))
                msg = (
                    "Polygon rate limit reached.\n\n"
                    f"REST API call: {url}\n\n"
                    f"Sleeping {PolygonClient.WAIT_SECONDS_RETRY} seconds.\n\n"
                    "Consider paid subscription at https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10\n"
                    "Use code 'LUMI10' for 10% off."
                )
                colored_msg = colored(msg, "red")
                logging.error(colored_msg)
                logging.debug(f"Error: {e}")
                time.sleep(PolygonClient.WAIT_SECONDS_RETRY)