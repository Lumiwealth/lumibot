# This file contains helper functions for getting data from Polygon.io optimized with Polars
from __future__ import annotations

# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
# pyright: reportUnknownParameterType=false, reportMissingParameterType=false, reportMissingTypeArgument=false
# pyright: reportInvalidTypeForm=false, reportUnnecessaryComparison=false, reportArgumentType=false
# pyright: reportOptionalMemberAccess=false, reportOptionalSubscript=false, reportAttributeAccessIssue=false
# pyright: reportPrivateUsage=false, reportUnknownLambdaType=false, reportConstantRedefinition=false
# pyright: reportUnnecessaryIsInstance=false, reportMissingTypeStubs=false
import time
from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any, TypeAlias, cast

from lumibot.constants import LUMIBOT_CACHE_FOLDER
from lumibot.entities.asset import Asset
from lumibot.tools.lumibot_logger import get_logger

if TYPE_CHECKING:
    from lumibot.tools.polygon_helper import PolygonClient

logger = get_logger(__name__)
PolarsDataFrame: TypeAlias = Any  # noqa: UP040
PolarsLazyFrame: TypeAlias = Any  # noqa: UP040
PandasDataFrame: TypeAlias = Any  # noqa: UP040
Calendar: TypeAlias = Any  # noqa: UP040
PolygonClientLike: TypeAlias = Any  # noqa: UP040
OptionChain: TypeAlias = dict[str, Any]  # noqa: UP040


class _LazyPolars(ModuleType):
    _module: ModuleType | None

    __slots__ = ("_module",)

    def __init__(self) -> None:
        super().__init__("polars")
        object.__setattr__(self, "_module", None)

    def _load(self) -> ModuleType:
        module = cast(ModuleType | None, object.__getattribute__(self, "_module"))
        if module is None:
            module = import_module("polars")
            object.__setattr__(self, "_module", module)
        return module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)


pl = _LazyPolars()


class _LazyPolars:
    _module = None

    def _load(self):
        if self._module is None:
            import polars as pl

            self._module = pl
        return self._module

    def __getattr__(self, name):
        return getattr(self._load(), name)


pl = _LazyPolars()

# Adjust as desired, in days. We'll reuse any existing chain file
# that is not older than RECENT_FILE_TOLERANCE_DAYS.
RECENT_FILE_TOLERANCE_DAYS = 14

# Maximum number of days to query in a single call to Polygon
# Reduced to optimize for smaller, faster chunks
MAX_POLYGON_DAYS = 7

# Connection pool settings for async (if available)
MAX_CONCURRENT_REQUESTS = 20

# Define a cache dictionary to store schedules and a global dictionary for buffered schedules
schedule_cache: dict[tuple[Any, ...], PandasDataFrame] = {}
buffered_schedules: dict[str, PandasDataFrame] = {}
_PANDAS_MARKET_CALENDARS = None


def _get_market_calendars() -> Any:
    global _PANDAS_MARKET_CALENDARS
    if _PANDAS_MARKET_CALENDARS is None:
        import pandas_market_calendars as mcal

        _PANDAS_MARKET_CALENDARS = mcal
    return _PANDAS_MARKET_CALENDARS


def get_cached_schedule(cal: Calendar, start_date: date, end_date: date, buffer_days: int = 30) -> PandasDataFrame:
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
    import pandas as pd

    start_timestamp = pd.Timestamp(start_date)
    end_timestamp = pd.Timestamp(end_date)

    # Check if we have the buffered schedule for this calendar
    if cal.name in buffered_schedules:
        buffered_schedule = buffered_schedules[cal.name]
        # Check if the current buffered schedule covers the required range
        if buffered_schedule.index.min() <= start_timestamp and buffered_schedule.index.max() >= end_timestamp:
            filtered_schedule = buffered_schedule[
                (buffered_schedule.index >= start_timestamp) & (buffered_schedule.index <= end_timestamp)
            ]
            schedule_cache[cache_key] = filtered_schedule
            return filtered_schedule

    # Fetch and cache the new buffered schedule
    buffered_schedule = cal.schedule(start_date=start_date, end_date=buffer_end)
    buffered_schedules[cal.name] = buffered_schedule  # Store the buffered schedule for this calendar

    # Filter the schedule to only include the requested date range
    filtered_schedule = buffered_schedule[
        (buffered_schedule.index >= start_timestamp) & (buffered_schedule.index <= end_timestamp)
    ]

    # Cache the filtered schedule for quick lookup
    schedule_cache[cache_key] = filtered_schedule

    return filtered_schedule


def get_price_data_from_polygon_polars(
    api_key: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    timespan: str = "minute",
    quote_asset: Asset | None = None,
    force_cache_update: bool = False,
    max_workers: int = 20,
) -> PolarsDataFrame | None:
    """
    Query Polygon.io for historical pricing data for the given asset, using parallel downloads.
    Optimized version using Polars instead of Pandas.

    Data is cached locally (in LUMIBOT_CACHE_FOLDER/polygon) to avoid re-downloading data for dates
    that have already been checked. For any trading date with no data, a dummy row with a "missing"
    flag is stored in the cache. When returning data to the caller, dummy rows are filtered out.

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
    timespan : str, optional
        The candle timespan (e.g., "minute", "day"). Defaults to "minute".
    quote_asset : Optional[Asset], optional
        The quote asset if applicable (e.g., for Forex pairs). Defaults to None.
    force_cache_update : bool, optional
        If True, forces re-downloading data even if cached data exists. Defaults to False.
    max_workers : int, optional
        The number of parallel threads to use for downloading data. Defaults to 10.

    Returns
    -------
    Optional[pl.DataFrame]
        The DataFrame containing the historical pricing data (with dummy rows removed),
        or None if a valid symbol could not be found.
    """

    # Build the cache file path based on the asset, timespan, and quote asset.
    cache_file = build_cache_filename_polars(asset, timespan, quote_asset)
    # Validate cache (e.g., check if splits have changed) and possibly force a cache update.
    force_cache_update = validate_cache_polars(force_cache_update, asset, cache_file, api_key)
    df_all: pl.DataFrame | None = None
    # Load cached data as lazy frame for efficiency
    if cache_file.exists() and not force_cache_update:
        # Use lazy loading to minimize memory usage
        try:
            df_all_lazy = pl.scan_parquet(cache_file)
            # Only collect when needed
            df_all = df_all_lazy.collect()
        except Exception:
            df_all = load_cache_polars(cache_file)

    # Determine missing trading dates.
    missing_dates = get_missing_dates_polars(df_all, asset, start, end)
    if not missing_dates:
        if df_all is not None:
            df_all = df_all.drop_nulls()
        return df_all

    # Create a PolygonClient and get the symbol for the asset.
    from lumibot.tools.polygon_helper import PolygonClient

    polygon_client = PolygonClient.create(api_key=api_key)
    symbol = get_polygon_symbol(asset, polygon_client, quote_asset)
    if symbol is None:
        # If no valid symbol is found, mark all trading dates as checked.
        trading_dates = get_trading_dates(asset, start, end)
        df_all = update_cache_polars(cache_file, df_all, trading_dates)
        return df_all

    # Determine overall download range from the earliest to the latest missing date.
    poly_start = missing_dates[0]
    poly_end = missing_dates[-1]
    total_days = (poly_end - poly_start).days + 1
    total_queries = (total_days // MAX_POLYGON_DAYS) + 1

    # Build download chunks (each of up to MAX_POLYGON_DAYS days).
    chunks = []
    delta = timedelta(days=MAX_POLYGON_DAYS)
    s_date = poly_start
    while s_date <= poly_end:
        e_date = min(poly_end, s_date + delta)
        chunks.append((s_date, e_date))
        s_date = e_date + timedelta(days=1)

    # Download data in parallel with optimized batch processing
    from tqdm import tqdm

    pbar = tqdm(
        total=total_queries,
        desc=f"Downloading and caching {asset} / {quote_asset.symbol if quote_asset else ''} '{timespan}'",
        dynamic_ncols=True,
    )

    def fetch_chunk(start_date: datetime, end_date: datetime, retries: int = 3) -> Any | None:
        """Fetch chunk with retry logic."""
        for attempt in range(retries):
            try:
                return polygon_client.get_aggs(
                    ticker=symbol,
                    from_=start_date,
                    to=end_date,
                    multiplier=1,
                    timespan=timespan,
                    limit=50000,
                )
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(0.1 * (2**attempt))  # Exponential backoff
                else:
                    logger.warning(f"Failed to fetch {start_date} to {end_date}: {e}")
                    return None

    # Optimized batch processing with better memory management
    min(max_workers * 2, len(chunks))
    results_buffer: list[Any] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all futures at once for better concurrency
        futures = {executor.submit(fetch_chunk, cstart, cend): (cstart, cend) for (cstart, cend) in chunks}

        for future in as_completed(futures):
            try:
                result = future.result(timeout=30)
                if result:
                    results_buffer.append(result)
                    # Process buffer periodically to manage memory
                    if len(results_buffer) >= 10:  # Increased buffer size
                        # Batch process all results at once
                        for res in results_buffer:
                            df_all = update_polygon_data_polars(df_all, res)
                        results_buffer.clear()
            except Exception as e:
                logger.warning(f"Error processing chunk: {e}")
            finally:
                pbar.update(1)

    # Process remaining results
    if results_buffer:
        for res in results_buffer:
            df_all = update_polygon_data_polars(df_all, res)

    pbar.close()

    # Recompute missing dates after downloads and update the cache.
    missing_dates = get_missing_dates_polars(df_all, asset, start, end)
    df_all = update_cache_polars(cache_file, df_all, missing_dates)

    # Use lazy loading for final cache read
    try:
        # Lazy load and filter in one operation
        df_all_lazy = pl.scan_parquet(cache_file)
        if "missing" in df_all_lazy.columns:
            df_all_output = df_all_lazy.filter(~pl.col("missing").cast(pl.Boolean)).drop_nulls().collect()
        else:
            df_all_output = df_all_lazy.drop_nulls().collect()
    except Exception:
        # Fallback to regular loading
        df_all_full = load_cache_polars(cache_file)
        if "missing" in df_all_full.columns:
            df_all_output = df_all_full.filter(~pl.col("missing").cast(pl.Boolean))
        else:
            df_all_output = df_all_full
        df_all_output = df_all_output.drop_nulls()
    return df_all_output


def validate_cache_polars(force_cache_update: bool, asset: Asset, cache_file: Path, api_key: str) -> bool:
    """
    If the list of splits for a stock have changed then we need to invalidate its cache
    because all of the prices will have changed (because we're using split adjusted prices).
    Get the splits data from Polygon only once per day per stock.
    Use the timestamp on the splits parquet file to determine if we need to get the splits again.
    When invalidating we delete the cache file and return force_cache_update=True too.
    """
    if asset.asset_type not in [Asset.AssetType.STOCK, Asset.AssetType.OPTION]:
        return force_cache_update
    cached_splits = pl.DataFrame()
    splits_file_stale = True
    splits_file_path = Path(str(cache_file).rpartition(".parquet")[0] + "_splits.parquet")
    if splits_file_path.exists():
        splits_file_stale = datetime.fromtimestamp(splits_file_path.stat().st_mtime).date() != date.today()
        if splits_file_stale:
            cached_splits = pl.read_parquet(splits_file_path)
    if splits_file_stale or force_cache_update:
        from lumibot.tools.polygon_helper import PolygonClient

        polygon_client = PolygonClient.create(api_key=api_key)
        # Need to get the splits in execution order to make the list comparable across invocations.
        splits = polygon_client.list_splits(ticker=asset.symbol, sort="execution_date", order="asc")
        if isinstance(splits, Iterator):
            # Convert the generator to a list so DataFrame will make a row per item.
            splits_list = list(splits)
            if splits_list:
                # Convert to polars DataFrame
                splits_df = pl.DataFrame(splits_list)
                if splits_file_path.exists() and not cached_splits.is_empty() and cached_splits.equals(splits_df):
                    # No need to rewrite contents.  Just update the timestamp.
                    splits_file_path.touch()
                else:
                    logger.info(f"Invalidating cache for {asset.symbol} because its splits have changed.")
                    force_cache_update = True
                    cache_file.unlink(missing_ok=True)
                    # Create the directory if it doesn't exist
                    cache_file.parent.mkdir(parents=True, exist_ok=True)
                    splits_df.write_parquet(splits_file_path)
        else:
            logger.warning(f"Unexpected response getting splits for {asset.symbol} from Polygon.  Response: {splits}")
    return force_cache_update


def get_trading_dates(asset: Asset, start: datetime, end: datetime) -> list[date]:
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
        mcal = _get_market_calendars()
        cal = mcal.get_calendar("NYSE")

    # Forex Asset for Backtesting - Forex trades weekdays, 24hrs starting Sunday 5pm EST
    # Calendar: "CME_FX"
    elif asset.asset_type == Asset.AssetType.FOREX:
        mcal = _get_market_calendars()
        cal = mcal.get_calendar("CME_FX")

    else:
        raise ValueError(f"Unsupported asset type for polygon: {asset.asset_type}")

    # Get the trading days between the start and end dates
    df = get_cached_schedule(cal, start.date(), end.date())
    trading_days = df.index.date.tolist()
    return trading_days


def get_polygon_symbol(
    asset: Asset, polygon_client: PolygonClientLike, quote_asset: Asset | None = None
) -> str | None:
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
    # Import PolygonClient here to avoid circular imports
    # Crypto Asset for Backtesting
    if asset.asset_type == Asset.AssetType.CRYPTO:
        if asset.symbol is None:
            raise ValueError("Crypto assets require a symbol for Polygon data.")
        quote_asset_symbol = quote_asset.symbol if quote_asset and quote_asset.symbol else "USD"
        symbol = f"X:{asset.symbol}{quote_asset_symbol}"

    # Stock-Equity Asset for Backtesting
    elif asset.asset_type == Asset.AssetType.STOCK:
        if asset.symbol is None:
            raise ValueError("Stock assets require a symbol for Polygon data.")
        symbol = asset.symbol

    elif asset.asset_type == Asset.AssetType.INDEX:
        if asset.symbol is None:
            raise ValueError("Index assets require a symbol for Polygon data.")
        symbol = f"I:{asset.symbol}"

    # Forex Asset for Backtesting
    elif asset.asset_type == Asset.AssetType.FOREX:
        # If quote_asset is None, throw an error
        if quote_asset is None:
            raise ValueError(f"quote_asset is required for asset type {asset.asset_type}")

        if asset.symbol is None or quote_asset.symbol is None:
            raise ValueError("Forex assets and quote assets require symbols for Polygon data.")
        symbol = f"C:{asset.symbol}{quote_asset.symbol}"

    # Option Asset for Backtesting - Do a query to Polygon to get the ticker
    elif asset.asset_type == Asset.AssetType.OPTION:
        # Needed so BackTest both old and existing contracts
        real_today = date.today()
        if asset.symbol is None:
            raise ValueError("Option assets require a symbol for Polygon data.")
        if asset.expiration is None:
            raise ValueError(f"Expiration date is required for option {asset} but it is None")
        if asset.right is None:
            raise ValueError(f"Option right is required for option {asset} but it is None")
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
            from termcolor import colored

            text = colored(f"Unable to find option contract for {asset}", "red")
            logger.debug(text)
            return

        # Example: O:SPY230802C00457000
        symbol = contracts[0].ticker

    else:
        raise ValueError(f"Unsupported asset type for polygon: {asset.asset_type}")

    return symbol


def build_cache_filename_polars(asset: Asset, timespan: str, quote_asset: Asset | None = None) -> Path:
    """
    Helper function to create the cache filename for a given asset and timespan
    Uses parquet format for better performance with Polars

    Parameters
    ----------
    asset : Asset
        Asset we are getting data for
    quote_asset : Asset
        Quote asset for the asset we are getting data for
    timespan : str
        Timespan for the data requested

    Returns
    -------
    Path
        The path to the cache file
    """

    lumibot_polygon_cache_folder = Path(LUMIBOT_CACHE_FOLDER) / "polygon_polars"

    # If It's an option then also add the expiration date, strike price and right to the filename
    if asset.asset_type == "option":
        if asset.expiration is None:
            raise ValueError(f"Expiration date is required for option {asset} but it is None")
        if asset.symbol is None:
            raise ValueError(f"Symbol is required for option {asset} but it is None")

        # Make asset.expiration datetime into a string like "YYMMDD"
        expiry_string = asset.expiration.strftime("%y%m%d")
        uniq_str = f"{asset.symbol}_{expiry_string}_{asset.strike}_{asset.right}"
    elif quote_asset:
        if asset.symbol is None or quote_asset.symbol is None:
            raise ValueError("Asset and quote asset symbols are required for Polygon cache filenames.")
        uniq_str = f"{asset.symbol}_{quote_asset.symbol}"
    else:
        if asset.symbol is None:
            raise ValueError(f"Symbol is required for asset {asset} but it is None")
        uniq_str = asset.symbol

    cache_filename = f"{asset.asset_type}_{uniq_str}_{timespan}.parquet"
    cache_file = lumibot_polygon_cache_folder / cache_filename
    return cache_file


def get_missing_dates_polars(
    df_all: PolarsDataFrame | None, asset: Asset, start: datetime, end: datetime
) -> list[date]:
    """
    Determine which trading dates are missing from the cache.

    A date is considered "checked" if any row exists in the cache (whether it contains real
    data or a dummy row indicating a missing query). Trading dates are determined from the asset's
    calendar (via `get_trading_dates()`).

    Parameters
    ----------
    df_all : Optional[pl.DataFrame]
        The DataFrame loaded from the cache (may be None or empty).
    asset : Asset
        The asset for which data is being requested.
    start : datetime
        The start datetime of the requested range.
    end : datetime
        The end datetime of the requested range.

    Returns
    -------
    List[datetime.date]
        A sorted list of date objects representing the trading dates that are missing from the cache.
    """
    # Get all trading dates from the asset calendar.
    trading_dates = get_trading_dates(asset, start, end)
    # For options, limit to dates on or before the expiration.
    if asset.asset_type == "option":
        if asset.expiration is not None:
            trading_dates = [d for d in trading_dates if d <= asset.expiration]
    if df_all is None or len(df_all) == 0:
        return trading_dates
    # Use only the date portion of the cache datetime column.
    cached_dates = set(df_all["datetime"].dt.date().to_list())
    missing_dates = sorted(set(trading_dates) - cached_dates)
    # Ensure the missing dates fall within the requested range.
    missing_dates = [d for d in missing_dates if start.date() <= d <= end.date()]
    return missing_dates


def load_cache_polars(cache_file: Path) -> PolarsDataFrame:
    """
    Load cached data from a Parquet file and return a DataFrame with datetime column.

    Parameters
    ----------
    cache_file : Path
        The path to the Parquet cache file.

    Returns
    -------
    pl.DataFrame
        The DataFrame containing the cached data.

    Raises
    ------
    Exception
        If the file cannot be loaded.
    """
    df = pl.read_parquet(cache_file)
    # Ensure datetime column is sorted
    df = df.sort("datetime")
    return df


def update_cache_polars(
    cache_file: Path, df_all: PolarsDataFrame | None, missing_dates: list[date] | None = None
) -> PolarsDataFrame:
    """
    Update the cache file by adding any missing dates as dummy rows.

    For each date in `missing_dates` that is not already present in the cache,
    a dummy row is added (with a "missing" flag set to True). This ensures that
    dates which were queried but returned no data are recorded, so that they
    will not be re-downloaded on subsequent runs.

    Parameters
    ----------
    cache_file : Path
        The path to the cache file.
    df_all : Optional[pl.DataFrame]
        The existing cached DataFrame (may be None or empty).
    missing_dates : Optional[List[datetime.date]]
        List of date objects for which data is missing.

    Returns
    -------
    pl.DataFrame
        The updated DataFrame (which is also saved to the cache file).
    """
    # Ensure we have a DataFrame to work with.
    if df_all is None:
        df_all = pl.DataFrame()

    # If there is cached data, ensure it's sorted
    if len(df_all) > 0:
        df_all = df_all.sort("datetime")

    # Determine dates already present in the cache
    cached_dates = set(df_all["datetime"].dt.date().to_list()) if len(df_all) > 0 else set()

    # Create dummy rows for missing dates
    dummy_rows = []
    for d in missing_dates or []:
        if d not in cached_dates:
            # Create a datetime at the start of the day in UTC
            dt = datetime(year=d.year, month=d.month, day=d.day, tzinfo=UTC)
            dummy_rows.append(
                {
                    "datetime": dt,
                    "missing": True,
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "volume": None,
                }
            )

    # If any dummy rows were created, add them to the DataFrame.
    if dummy_rows:
        missing_df = pl.DataFrame(dummy_rows)
        if len(df_all) > 0:
            df_all = pl.concat([df_all, missing_df])
        else:
            df_all = missing_df
        df_all = df_all.sort("datetime")

    # Save the updated DataFrame to the cache file.
    if len(df_all) > 0:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        df_all.write_parquet(cache_file)

    return df_all


def update_polygon_data_polars(df_all: PolarsDataFrame | None, result: Any) -> PolarsDataFrame | None:
    """
    Update the DataFrame with the new data from Polygon.
    Optimized version with better memory handling.

    Parameters
    ----------
    df_all : pl.DataFrame
        A DataFrame with the data we already have.
    result : list
        A list of dictionaries with the new data from Polygon.
        Format: [{'o': 1.0, 'h': 2.0, 'l': 3.0, 'c': 4.0, 'v': 5.0, 't': 116120000000}]

    Returns
    -------
    pl.DataFrame
        The updated DataFrame.
    """
    if not result:
        return df_all

    # Convert result to polars DataFrame with optimized schema
    df = pl.DataFrame(
        result,
        schema_overrides={
            "o": pl.Float64,
            "h": pl.Float64,
            "l": pl.Float64,
            "c": pl.Float64,
            "v": pl.Int64,
            "t": pl.Int64,
        },
    )

    if len(df) == 0:
        return df_all

    # Batch all transformations together for efficiency
    df = (
        df.lazy()
        .select(
            [
                pl.col("o").alias("open"),
                pl.col("h").alias("high"),
                pl.col("l").alias("low"),
                pl.col("c").alias("close"),
                pl.col("v").alias("volume"),
                pl.from_epoch(pl.col("t"), time_unit="ms").alias("datetime"),
            ]
        )
        .sort("datetime")
        .collect()
    )

    if df_all is None or len(df_all) == 0:
        df_all = df
    else:
        # Use lazy evaluation for merge
        df_all = (
            pl.concat([df_all.lazy(), df.lazy()]).sort("datetime").unique(subset=["datetime"], keep="last").collect()
        )

    return df_all


def get_chains_cached(
    api_key: str,
    asset: Asset,
    quote: Asset | None = None,
    exchange: str | None = None,
    current_date: date | None = None,
    polygon_client: PolygonClientLike | None = None,
) -> OptionChain | None:
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
       `LUMIBOT_CACHE_FOLDER/polygon_polars/option_chains/{symbol}_{date}.parquet`.
    4) By default, we fetch both 'expired=True' and 'expired=False', so you get
       historical + near-future options for your specified date.
    """
    logger.debug(f"get_chains_cached called for {asset.symbol} on {current_date}")

    # 1) If current_date is None => bail out (no real date to query).
    if current_date is None:
        logger.debug("No current_date provided; returning None.")
        return None

    # 2) Ensure we have a PolygonClient
    if polygon_client is None:
        from lumibot.tools.polygon_helper import PolygonClient

        logger.debug("No polygon_client provided; creating a new one.")
        polygon_client = PolygonClient.create(api_key=api_key)

    # 3) Build the chain folder path and create if not present
    chain_folder = Path(LUMIBOT_CACHE_FOLDER) / "polygon_polars" / "option_chains"
    chain_folder.mkdir(parents=True, exist_ok=True)

    # 4) Attempt to find a suitable recent file (reuse it if found)
    earliest_okay_date = current_date - timedelta(days=RECENT_FILE_TOLERANCE_DAYS)
    pattern = f"{asset.symbol}_*.parquet"
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
            logger.debug(
                f"Reusing chain file {fpath} (file_date={file_date}), "
                f"within {RECENT_FILE_TOLERANCE_DAYS} days of {current_date}."
            )
            df_cached = pl.read_parquet(fpath)

            # Convert the data back to a dictionary of lists instead of NP arrays to match original return types
            data = df_cached["data"][0]
            for right in data["Chains"]:
                for exp_date in data["Chains"][right]:
                    data["Chains"][right][exp_date] = list(data["Chains"][right][exp_date])

            return data

    # 5) No suitable file => must fetch from Polygon
    logger.debug(f"No suitable recent file found for {asset.symbol} on {current_date}. Downloading from Polygon...")
    print(
        f"\nDownloading option chain for {asset} on {current_date}. This will be cached for future use so it will be significantly faster the next time you run a backtest."
    )

    option_contracts: OptionChain = {
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
        exp_date = c.expiration_date  # "YYYY-MM-DD"
        strike = c.strike_price

        option_contracts["Multiplier"] = c.shares_per_contract
        option_contracts["Exchange"] = exg
        option_contracts["Chains"][right][exp_date].append(strike)

    # 8) Save to a new file for future reuse
    cache_file = chain_folder / f"{asset.symbol}_{current_date.isoformat()}.parquet"
    df_to_cache = pl.DataFrame({"data": [option_contracts]})
    df_to_cache.write_parquet(cache_file)
    logger.debug(f"Download complete for {asset.symbol} on {current_date}. Saved chain file to {cache_file}")

    return option_contracts


# PolygonClient import moved to runtime to avoid circular imports
