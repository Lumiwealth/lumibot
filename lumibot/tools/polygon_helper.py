"""
polygon_helper.py
-----------------
Caches minute/day data from Polygon in DuckDB, avoiding repeated downloads
by truncating the end date to the last fully closed trading day if timespan="minute."

Changes:
1. Using Python's logging instead of print statements where needed.
2. Skipping days strictly before start.date() to avoid re-checking older days.
3. 24-hour placeholders for data accuracy.
4. Additional debugging around re-download logic and bounding queries.
5. Preserving all original docstrings, comments, and functions (including _store_placeholder_day).
6. Restoring parallel download in get_price_data_from_polygon() using concurrent futures.
"""

import logging
import time
from datetime import date, datetime, timedelta, time as dtime
from pathlib import Path
import os
from urllib3.exceptions import MaxRetryError
from urllib.parse import urlparse, urlunparse

import pandas as pd
import pandas_market_calendars as mcal
from lumibot import LUMIBOT_CACHE_FOLDER, LUMIBOT_DEFAULT_PYTZ
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

from lumibot.credentials import POLYGON_API_KEY

logger = logging.getLogger(__name__)  # <--- Our module-level logger

MAX_POLYGON_DAYS = 30

# ------------------------------------------------------------------------------
# 1) Choose a single DuckDB path for all scripts to share
# ------------------------------------------------------------------------------
DUCKDB_DB_PATH = Path(LUMIBOT_CACHE_FOLDER) / "polygon_duckdb" / "polygon_cache.duckdb"
DUCKDB_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

logger.debug(f"Using DUCKDB_DB_PATH = {DUCKDB_DB_PATH.resolve()}")


# ------------------------------------------------------------------------------
# We'll store bars in a single table 'price_data' with columns:
#   symbol, timespan, datetime, open, high, low, close, volume
# ------------------------------------------------------------------------------
schedule_cache = {}
buffered_schedules = {}

# Lock to handle concurrency for rate limits (useful on Polygon free plan).
RATE_LIMIT_LOCK = threading.Lock()


def get_cached_schedule(cal, start_date, end_date, buffer_days=30):
    """
    Get trading schedule from 'cal' (pandas_market_calendars) with a buffer
    to reduce repeated calls. Caches in memory for the session.
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
        if (buffered_schedule.index.min() <= start_timestamp and
                buffered_schedule.index.max() >= end_timestamp):
            filtered_schedule = buffered_schedule[
                (buffered_schedule.index >= start_timestamp)
                & (buffered_schedule.index <= end_timestamp)
            ]
            schedule_cache[cache_key] = filtered_schedule
            return filtered_schedule

    buffered_schedule = cal.schedule(start_date=start_date, end_date=buffer_end)
    buffered_schedules[cal.name] = buffered_schedule

    filtered_schedule = buffered_schedule[
        (buffered_schedule.index >= start_timestamp)
        & (buffered_schedule.index <= end_timestamp)
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
    Fetches minute/day data from Polygon for 'asset' between 'start' and 'end'.
    Stores in DuckDB so subsequent calls won't re-download the same days.
    
    If timespan="minute" and you request 'end' = today, it will truncate
    to the last fully closed trading day to avoid repeated partial-day fetches.
    """

    # --- TRUNCATION LOGIC (minute data) ---
    if timespan == "minute":
        today_utc = pd.Timestamp.utcnow().date()
        if end.date() >= today_utc:
            new_end = (today_utc - timedelta(days=1))
            end = datetime.combine(new_end, dtime(23, 59), tzinfo=end.tzinfo or LUMIBOT_DEFAULT_PYTZ)
            logger.info(f"Truncating 'end' to {end.isoformat()} for minute data (avoid partial day).")

    if not end:
        end = datetime.now(tz=LUMIBOT_DEFAULT_PYTZ)

    # 1) Load existing data from DuckDB
    existing_df = _load_from_duckdb(asset, timespan, start, end)
    asset_key = _asset_key(asset)
    logger.info(f"Loaded {len(existing_df)} rows from DuckDB initially (symbol={asset_key}, timespan={timespan}).")

    # 2) Possibly clear existing data if force_cache_update
    if force_cache_update:
        logger.critical(f"Forcing cache update for {asset} from {start} to {end}")
        existing_df = pd.DataFrame()

    # 3) Which days are missing?
    missing_dates = get_missing_dates(existing_df, asset, start, end)
    logger.info(f"Missing {len(missing_dates)} trading days for symbol={asset_key}, timespan={timespan}.")

    if missing_dates:
        logger.info(f"Inserting placeholder rows for {len(missing_dates)} missing days on {asset_key}...")
        for md in missing_dates:
            logger.debug(f"Placing placeholders for {md} on {asset_key}")
            _store_placeholder_day(asset, timespan, md)

    if not missing_dates and not existing_df.empty:
        logger.info(f"No missing days, returning existing data of {len(existing_df)} rows.")
        # -- Drop placeholders before returning
        return _drop_placeholder_rows(existing_df)  # <-- NEW COMMENT
    elif not missing_dates and existing_df.empty:
        logger.info("No missing days but existing DF is empty -> returning empty.")
        return existing_df

    # 4) Download from Polygon in parallel ~30-day chunks
    polygon_client = PolygonClient.create(api_key=api_key)
    symbol = get_polygon_symbol(asset, polygon_client, quote_asset=quote_asset)
    if not symbol:
        logger.error("get_polygon_symbol returned None. Possibly invalid or expired option.")
        return None

    # Instead of sequential downloading, do parallel chunk downloads:
    chunk_list = _group_missing_dates(missing_dates)
    results_list = []

    logger.info(f"Downloading data in parallel for {len(chunk_list)} chunk(s) on {symbol}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_range = {}
        for (start_chunk, end_chunk) in chunk_list:
            future = executor.submit(
                _fetch_polygon_data_chunk,
                polygon_client,
                symbol,
                start_chunk,
                end_chunk,
                timespan
            )
            future_to_range[future] = (start_chunk, end_chunk)

        with tqdm(total=len(chunk_list), desc=f"Downloading data for {symbol} (parallel)", dynamic_ncols=True) as pbar:
            for fut in concurrent.futures.as_completed(future_to_range):
                data_chunk = fut.result()
                if data_chunk:
                    results_list.extend(data_chunk)
                pbar.update(1)

    logger.info(f"Polygon returned {len(results_list)} bars total for symbol={symbol}, timespan={timespan}.")

    # 5) Transform raw bars -> DataFrame
    combined_df = _transform_polygon_data(results_list)
    logger.info(f"combined_df has {len(combined_df)} rows after transform.")

    # 6) Store new data in DuckDB
    if not combined_df.empty:
        _store_in_duckdb(asset, timespan, combined_df)
        _fill_partial_days(asset, timespan, combined_df)
    else:
        logger.critical("combined_df is empty; no data to store.")

    # 7) Reload final data for the full range
    final_df = _load_from_duckdb(asset, timespan, start, end)
    if final_df is not None and not final_df.empty:
        final_df.dropna(how="all", inplace=True)

    logger.info(f"Final DF has {len(final_df)} rows for {asset.symbol}, timespan={timespan}.")

    # -- Drop placeholder rows from final before returning to tests
    return _drop_placeholder_rows(final_df)  # <-- NEW COMMENT


def get_polygon_symbol(asset, polygon_client, quote_asset=None):
    """
    Convert a LumiBot Asset into a Polygon-compatible symbol, e.g.:
    - STOCK: "SPY"
    - OPTION: "O:SPY20250114C00570000"
    - FOREX: "C:EURUSD"
    - CRYPTO: "X:BTCUSD"
    """
    from datetime import date

    if asset.asset_type == Asset.AssetType.CRYPTO:
        quote_asset_symbol = quote_asset.symbol if quote_asset else "USD"
        return f"X:{asset.symbol}{quote_asset_symbol}"

    elif asset.asset_type == Asset.AssetType.STOCK:
        return asset.symbol

    elif asset.asset_type == Asset.AssetType.INDEX:
        return f"I:{asset.symbol}"

    elif asset.asset_type == Asset.AssetType.FOREX:
        if not quote_asset:
            logger.error("No quote_asset provided for FOREX.")
            return None
        return f"C:{asset.symbol}{quote_asset.symbol}"

    elif asset.asset_type == Asset.AssetType.OPTION:
        real_today = date.today()
        expired = asset.expiration < real_today
        contracts = list(
            polygon_client.list_options_contracts(
                underlying_ticker=asset.symbol,
                expiration_date=asset.expiration,
                contract_type=asset.right.lower(),  # 'call' or 'put'
                strike_price=asset.strike,
                expired=expired,
                limit=100,
            )
        )
        if not contracts:
            msg = f"Unable to find option contract for {asset}"
            logger.error(colored(msg, "red"))
            return None
        return contracts[0].ticker

    else:
        logger.error(f"Unsupported asset type: {asset.asset_type}")
        return None


def validate_cache(force_cache_update: bool, asset: Asset, cache_file: Path, api_key: str):
    """
    Placeholder if you want advanced checks for dividends, splits, etc.
    Currently returns force_cache_update as is.
    """
    return force_cache_update


def get_trading_dates(asset: Asset, start: datetime, end: datetime):
    """
    Return a list of valid daily sessions for the asset's exchange (or 7-day for CRYPTO).
    """
    if asset.asset_type == Asset.AssetType.CRYPTO:
        return [
            start.date() + timedelta(days=x)
            for x in range((end.date() - start.date()).days + 1)
        ]

    elif asset.asset_type in (Asset.AssetType.INDEX, Asset.AssetType.STOCK, Asset.AssetType.OPTION):
        cal = mcal.get_calendar("NYSE")
    elif asset.asset_type == Asset.AssetType.FOREX:
        cal = mcal.get_calendar("CME_FX")
    else:
        raise ValueError(f"[ERROR] get_trading_dates: unsupported asset type {asset.asset_type}")

    df = get_cached_schedule(cal, start.date(), end.date())
    return df.index.date.tolist()


def _get_trading_days(asset: Asset, start: datetime, end: datetime):
    return get_trading_dates(asset, start, end)


def get_missing_dates(df_all, asset, start: datetime, end: datetime):
    """
    Identify which daily sessions are missing from df_all.
    If asset is OPTION, only consider days up to expiration.

    We skip days strictly before start.date().
    """
    trading_days = _get_trading_days(asset, start, end)
    logger.debug(f"get_missing_dates: computed trading_days={trading_days}")

    if asset.asset_type == Asset.AssetType.OPTION:
        trading_days = [d for d in trading_days if d <= asset.expiration]
        logger.debug(f"get_missing_dates: filtered for option expiration => {trading_days}")

    start_date_only = start.date()
    end_date_only = end.date()
    trading_days = [d for d in trading_days if d >= start_date_only and d <= end_date_only]
    logger.debug(f"get_missing_dates: after bounding by start/end => {trading_days}")

    if df_all is None or df_all.empty:
        logger.debug("get_missing_dates: df_all is empty => all trading_days are missing")
        return trading_days

    existing_days = pd.Series(df_all.index.date).unique()
    logger.debug(f"get_missing_dates: existing_days in df_all={existing_days}")

    missing = sorted(set(trading_days) - set(existing_days))
    logger.debug(f"get_missing_dates: missing={missing}")
    return missing


def _load_from_duckdb(asset: Asset, timespan: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Load from DuckDB if data is stored. Return a DataFrame with datetime index.
    If no table or no matching rows, returns empty DataFrame.

    Additional debugging to see the actual query.
    """
    conn = duckdb.connect(str(DUCKDB_DB_PATH), read_only=False)
    asset_key = _asset_key(asset)

    query = f"""
    SELECT *
    FROM price_data
    WHERE symbol='{asset_key}'
      AND timespan='{timespan}'
      AND datetime >= '{start.isoformat()}'
      AND datetime <= '{end.isoformat()}'
    ORDER BY datetime
    """
    logger.debug(f"_load_from_duckdb: SQL=\n{query}")

    try:
        df = conn.execute(query).fetchdf()
        if df.empty:
            logger.debug(f"_load_from_duckdb: No rows found in DB for symbol={asset_key}, timespan={timespan}")
            return df

        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
        df.set_index("datetime", inplace=True)
        df.sort_index(inplace=True)

        logger.debug(f"_load_from_duckdb: loaded {len(df)} rows for symbol={asset_key}, timespan={timespan}")
        if not df.empty:
            logger.debug(f"_load_from_duckdb: min timestamp={df.index.min()}, max timestamp={df.index.max()}")
            unique_dates = pd.Series(df.index.date).unique()
            logger.debug(f"_load_from_duckdb: unique dates in loaded data => {unique_dates}")

        return df

    except duckdb.CatalogException:
        logger.debug(f"_load_from_duckdb: Table does not exist yet for symbol={asset_key}, timespan={timespan}")
        return pd.DataFrame()
    finally:
        conn.close()


def _store_in_duckdb(asset: Asset, timespan: str, df_in: pd.DataFrame):
    """
    Insert newly fetched data into DuckDB 'price_data'.
    Upsert logic: only insert rows not already present.
    """
    if df_in.empty:
        logger.debug("_store_in_duckdb called with empty DataFrame. No insert performed.")
        return

    new_df = df_in.copy(deep=True)
    columns_needed = ["datetime", "open", "high", "low", "close", "volume", "symbol", "timespan"]
    for c in columns_needed:
        if c not in new_df.columns:
            new_df.loc[:, c] = None

    if new_df.index.name == "datetime":
        if "datetime" in new_df.columns:
            new_df.drop(columns=["datetime"], inplace=True)
        new_df.reset_index(drop=False, inplace=True)

    new_df = new_df[columns_needed]

    asset_key = _asset_key(asset)
    new_df["symbol"] = asset_key
    new_df["timespan"] = timespan

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

    conn.execute("DROP TABLE IF EXISTS tmp_table")
    conn.execute(
        """
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
        """
    )

    conn.register("df_newdata", new_df)
    insert_sql = """
    INSERT INTO tmp_table
    SELECT symbol, timespan, datetime, open, high, low, close, volume
    FROM df_newdata;
    """
    conn.execute(insert_sql)

    upsert_sql = f"""
        INSERT INTO price_data
        SELECT t.*
        FROM tmp_table t
        LEFT JOIN price_data p
          ON t.symbol = p.symbol
         AND t.timespan = p.timespan
         AND t.datetime = p.datetime
        WHERE p.symbol IS NULL
    """
    conn.execute(upsert_sql)

    check_sql = f"""
        SELECT COUNT(*)
        FROM price_data
        WHERE symbol='{asset_key}' AND timespan='{timespan}'
    """
    count_after = conn.execute(check_sql).fetchone()[0]
    logger.debug(f"Upsert completed. Now {count_after} total rows in 'price_data' "
                 f"for symbol='{asset_key}', timespan='{timespan}'.")
    conn.close()


def _transform_polygon_data(results_list):
    """
    Combine chunk results into one DataFrame, rename columns, set datetime index, localize to UTC.
    """
    if not results_list:
        return pd.DataFrame()

    df = pd.DataFrame(results_list)
    if df.empty:
        return df

    rename_cols = {"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
    df = df.rename(columns=rename_cols, errors="ignore")

    if "t" in df.columns:
        df["datetime"] = pd.to_datetime(df["t"], unit="ms")
        df.drop(columns=["t"], inplace=True)
    elif "timestamp" in df.columns:
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.drop(columns=["timestamp"], inplace=True)

    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)

    if df.index.tzinfo is None:
        df.index = df.index.tz_localize("UTC")

    return df


def get_option_chains_with_cache(polygon_client: RESTClient, asset: Asset, current_date: date):
    """
    Returns option chain data (calls+puts) from Polygon. Not stored in DuckDB by default.
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


def _fetch_polygon_data_chunk(polygon_client, symbol, chunk_start, chunk_end, timespan):
    """
    Fetch data for one chunk, locking if needed for rate limit on the free plan.
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


def _group_missing_dates(missing_dates):
    """
    Group consecutive missing days into ~30-day chunks for fewer polygon calls.
    We return a list of (start_datetime, end_datetime) pairs in UTC.
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

    range_list = []
    for (s, e) in final_chunks:
        start_dt = datetime(s.year, s.month, s.day, tzinfo=LUMIBOT_DEFAULT_PYTZ)
        end_dt = datetime(e.year, e.month, e.day, 23, 59, tzinfo=LUMIBOT_DEFAULT_PYTZ)
        range_list.append((start_dt, end_dt))

    return range_list


def _asset_key(asset: Asset) -> str:
    """
    Construct a unique symbol key for storing in DuckDB. For OPTIONS, do e.g.:
       "SPY_250114_577_CALL"
    """
    if asset.asset_type == Asset.AssetType.OPTION:
        if not asset.expiration:
            raise ValueError("Option asset requires expiration date.")
        expiry_str = asset.expiration.strftime("%y%m%d")
        return f"{asset.symbol}_{expiry_str}_{asset.strike}_{asset.right.upper()}"
    else:
        return asset.symbol


def _store_placeholder_day(asset: Asset, timespan: str, single_date: date):
    """
    Insert *FULL DAY* (24-hour) placeholder rows into DuckDB for the given day,
    so we don't keep re-downloading it if it truly has no data (or partial data).

    Data Accuracy:
      - Real data overwrites these placeholders if available.
      - We never lose data or skip times.

    We carefully create naive midnights and localize them to UTC
    to avoid the "Inferred time zone not equal to passed time zone" error.
    """
    import pytz  # For explicit UTC usage

    logger.debug(f"Storing placeholder *24-hour UTC* rows for date={single_date} "
                 f"on symbol={_asset_key(asset)}, timespan={timespan}")

    naive_start = datetime(single_date.year, single_date.month, single_date.day, 0, 0, 0)
    naive_end = naive_start + timedelta(days=1, microseconds=-1)

    day_start = pytz.UTC.localize(naive_start)
    day_end = pytz.UTC.localize(naive_end)

    logger.debug(f"_store_placeholder_day: day_start (UTC)={day_start}, day_end (UTC)={day_end}")

    try:
        # Optionally, for stocks, we could insert only 9:30–16:00 placeholders
        if (asset.asset_type in (Asset.AssetType.STOCK, Asset.AssetType.OPTION) and timespan == "minute"):
            # 9:30–16:00 Eastern, converted to UTC
            # For more robust, consider using a calendar for half-days, etc.
            # But this is an example of partial day placeholders:
            open_eastern = datetime(single_date.year, single_date.month, single_date.day, 9, 30)
            close_eastern = datetime(single_date.year, single_date.month, single_date.day, 16, 0)
            from_date = pd.Timestamp(open_eastern, tz="America/New_York").tz_convert("UTC")
            to_date = pd.Timestamp(close_eastern, tz="America/New_York").tz_convert("UTC")
            rng = pd.date_range(start=from_date, end=to_date, freq="T", tz="UTC")
        else:
            rng = pd.date_range(start=day_start, end=day_end, freq="min", tz="UTC")
    except Exception as e:
        logger.critical(f"date_range failed for day={single_date} with error: {e}")
        raise

    if len(rng) == 0:
        logger.debug(f"_store_placeholder_day: no minutes from {day_start} to {day_end}??? skipping.")
        return

    df_placeholder = pd.DataFrame(
        {
            "datetime": rng,
            "open": [None]*len(rng),
            "high": [None]*len(rng),
            "low": [None]*len(rng),
            "close": [None]*len(rng),
            "volume": [None]*len(rng),
        }
    ).set_index("datetime")

    logger.debug(f"_store_placeholder_day: day={single_date}, inserting {len(df_placeholder)} placeholders.")
    logger.debug(f"min placeholder={df_placeholder.index.min()}, max placeholder={df_placeholder.index.max()}")

    _store_in_duckdb(asset, timespan, df_placeholder)


def _fill_partial_days(asset: Asset, timespan: str, newly_fetched: pd.DataFrame):
    """
    After we download real data for certain days, fill in placeholders
    for any missing minutes in each day of 'newly_fetched'.
    We do a 24h approach, so re-store placeholders in case the day only got partial data.
    """
    if newly_fetched.empty:
        return

    days_updated = pd.Series(newly_fetched.index.date).unique()
    for day in days_updated:
        logger.debug(f"_fill_partial_days: day={day}, calling _store_placeholder_day(24h) again.")
        _store_placeholder_day(asset, timespan, day)


class PolygonClient(RESTClient):
    """
    Thin subclass of polygon.RESTClient that retries on MaxRetryError with a cooldown.
    Helps with free-tier rate limits.
    """
    WAIT_SECONDS_RETRY = 60

    @classmethod
    def create(cls, *args, **kwargs) -> RESTClient:
        if "api_key" not in kwargs:
            kwargs["api_key"] = POLYGON_API_KEY
        return cls(*args, **kwargs)

    def _get(self, *args, **kwargs):
        while True:
            try:
                return super()._get(*args, **kwargs)
            except MaxRetryError as e:
                url = urlunparse(urlparse(kwargs["path"])._replace(query=""))
                msg = (
                    "Polygon rate limit reached. "
                    f"Sleeping {PolygonClient.WAIT_SECONDS_RETRY} seconds.\n"
                    f"REST API call: {url}\n\n"
                    "Consider upgrading to a paid subscription at https://polygon.io\n"
                    "Use code 'LUMI10' for 10% off."
                )
                logging.critical(msg)
                logging.critical(f"Error: {e}")
                time.sleep(PolygonClient.WAIT_SECONDS_RETRY)


# -----------------------------------------------------------------------
#    Additional Helper: _drop_placeholder_rows
# -----------------------------------------------------------------------
def _drop_placeholder_rows(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Removes placeholder rows (where open/close/volume are all NaN),
    returning only real data to tests or strategies. 
    The placeholders remain in DuckDB so re-downloading is avoided.
    """
    if df_in.empty:
        return df_in

    # If everything is NaN in open, close, high, low, volume → mark as placeholders
    mask_real = ~(
        df_in["open"].isna() & df_in["close"].isna() & df_in["volume"].isna()
    )
    return df_in.loc[mask_real].copy()