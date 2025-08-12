# Async implementation for Polygon data downloads - significantly faster than sync version
import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import aiohttp
import polars as pl

try:
    from tqdm.asyncio import tqdm
except ImportError:
    from tqdm import tqdm
from lumibot.entities import Asset
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

# Configuration
MAX_CONCURRENT_REQUESTS = 50  # Much higher than sync version
MAX_POLYGON_DAYS = 7  # Smaller chunks for better parallelization
RATE_LIMIT_PER_MINUTE = 100  # Polygon rate limit
CONNECTION_TIMEOUT = aiohttp.ClientTimeout(total=60, connect=10, sock_read=30)
MAX_RETRIES = 3
RETRY_DELAY = 0.5

class AsyncPolygonClient:
    """Async Polygon client for high-performance data downloads."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limiter = RateLimiter(RATE_LIMIT_PER_MINUTE)

    async def __aenter__(self):
        """Create session on context enter."""
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=50,
            ttl_dns_cache=300,
            enable_cleanup_closed=True
        )
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=CONNECTION_TIMEOUT,
            headers={"Authorization": f"Bearer {self.api_key}"}
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close session on context exit."""
        if self.session:
            await self.session.close()

    async def get_aggs(
        self,
        ticker: str,
        from_date: datetime,
        to_date: datetime,
        multiplier: int = 1,
        timespan: str = "minute",
        limit: int = 50000
    ) -> Optional[List[Dict]]:
        """Get aggregated bars data."""
        await self.rate_limiter.acquire()

        # Format dates
        from_str = from_date.strftime("%Y-%m-%d")
        to_str = to_date.strftime("%Y-%m-%d")

        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_str}/{to_str}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": limit,
            "apiKey": self.api_key
        }

        for attempt in range(MAX_RETRIES):
            try:
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("results", [])
                    elif response.status == 429:  # Rate limited
                        await asyncio.sleep(RETRY_DELAY * (2 ** attempt))
                    else:
                        logger.warning(f"HTTP {response.status} for {ticker} {from_str} to {to_str}")
                        return None
            except asyncio.TimeoutError:
                logger.warning(f"Timeout for {ticker} {from_str} to {to_str}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY * (2 ** attempt))
            except Exception as e:
                logger.error(f"Error fetching {ticker}: {e}")
                return None

        return None


class RateLimiter:
    """Simple rate limiter for API calls."""

    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call = 0
        self.lock = asyncio.Lock()

    async def acquire(self):
        """Wait if necessary to respect rate limit."""
        async with self.lock:
            now = time.time()
            time_since_last = now - self.last_call
            if time_since_last < self.min_interval:
                await asyncio.sleep(self.min_interval - time_since_last)
            self.last_call = time.time()


async def download_polygon_data_async(
    api_key: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    timespan: str = "minute",
    quote_asset: Optional[Asset] = None,
    symbol: Optional[str] = None
) -> Optional[pl.DataFrame]:
    """
    Download Polygon data using async/await for maximum performance.
    
    This function downloads data in parallel using asyncio, which is
    significantly faster than the threaded approach.
    
    Parameters
    ----------
    api_key : str
        Polygon API key
    asset : Asset
        Asset to download data for
    start : datetime
        Start datetime
    end : datetime
        End datetime
    timespan : str
        Timespan (minute, hour, day)
    quote_asset : Optional[Asset]
        Quote asset for forex/crypto
    symbol : Optional[str]
        Pre-computed Polygon symbol
        
    Returns
    -------
    Optional[pl.DataFrame]
        Downloaded data as polars DataFrame
    """

    if symbol is None:
        # Get symbol using sync method (or implement async version)
        from lumibot.tools.polygon_helper import PolygonClient
        sync_client = PolygonClient.create(api_key=api_key)
        from lumibot.tools.polygon_helper_polars_optimized import get_polygon_symbol
        symbol = get_polygon_symbol(asset, sync_client, quote_asset)

    if symbol is None:
        return None

    # Calculate date chunks
    chunks = []
    current = start
    delta = timedelta(days=MAX_POLYGON_DAYS)

    while current <= end:
        chunk_end = min(current + delta, end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)

    # Download all chunks in parallel
    async with AsyncPolygonClient(api_key) as client:
        # Create progress bar
        pbar = tqdm(
            total=len(chunks),
            desc=f"Async downloading {asset.symbol} {timespan}",
            dynamic_ncols=True
        )

        # Create tasks for all chunks
        tasks = []
        for chunk_start, chunk_end in chunks:
            task = asyncio.create_task(
                client.get_aggs(symbol, chunk_start, chunk_end, timespan=timespan)
            )
            tasks.append(task)

        # Gather results with progress updates
        results = []
        for task in asyncio.as_completed(tasks):
            result = await task
            if result:
                results.extend(result)
            pbar.update(1)

        pbar.close()

    # Convert to polars DataFrame
    if not results:
        return None

    # Optimized DataFrame creation
    df = pl.DataFrame(results, schema_overrides={
        "o": pl.Float64,
        "h": pl.Float64,
        "l": pl.Float64,
        "c": pl.Float64,
        "v": pl.Int64,
        "t": pl.Int64
    })

    # Transform columns efficiently
    df = df.lazy().select([
        pl.col("o").alias("open"),
        pl.col("h").alias("high"),
        pl.col("l").alias("low"),
        pl.col("c").alias("close"),
        pl.col("v").alias("volume"),
        pl.from_epoch(pl.col("t"), time_unit="ms").alias("datetime")
    ]).sort("datetime").unique(subset=["datetime"]).collect()

    return df


def get_price_data_from_polygon_async(
    api_key: str,
    asset: Asset,
    start: datetime,
    end: datetime,
    timespan: str = "minute",
    quote_asset: Optional[Asset] = None,
    force_cache_update: bool = False
) -> Optional[pl.DataFrame]:
    """
    Wrapper function to use async download in sync context.
    
    This function manages the event loop and calls the async download function.
    """

    # Import cache functions from the optimized module
    from lumibot.tools.polygon_helper_polars_optimized import (
        build_cache_filename_polars,
        get_missing_dates_polars,
        load_cache_polars,
        update_cache_polars,
        validate_cache_polars,
    )

    # Build cache file path
    cache_file = build_cache_filename_polars(asset, timespan, quote_asset)

    # Validate cache
    force_cache_update = validate_cache_polars(force_cache_update, asset, cache_file, api_key)

    df_all: Optional[pl.DataFrame] = None

    # Load cached data if available
    if cache_file.exists() and not force_cache_update:
        df_all = load_cache_polars(cache_file)

    # Determine missing trading dates
    missing_dates = get_missing_dates_polars(df_all, asset, start, end)

    if not missing_dates:
        if df_all is not None:
            df_all = df_all.drop_nulls()
        return df_all

    # Determine download range
    poly_start = missing_dates[0]
    poly_end = missing_dates[-1]

    # Convert dates to datetime
    start_dt = datetime.combine(poly_start, datetime.min.time(), tzinfo=timezone.utc)
    end_dt = datetime.combine(poly_end, datetime.max.time(), tzinfo=timezone.utc)

    # Run async download
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    new_data = loop.run_until_complete(
        download_polygon_data_async(
            api_key, asset, start_dt, end_dt, timespan, quote_asset
        )
    )

    # Merge with existing data
    if new_data is not None and len(new_data) > 0:
        if df_all is None or len(df_all) == 0:
            df_all = new_data
        else:
            df_all = (
                pl.concat([df_all.lazy(), new_data.lazy()])
                .sort("datetime")
                .unique(subset=["datetime"], keep="last")
                .collect()
            )

    # Update cache with missing dates
    missing_dates = get_missing_dates_polars(df_all, asset, start, end)
    df_all = update_cache_polars(cache_file, df_all, missing_dates)

    # Reload and clean cache
    df_all_full = load_cache_polars(cache_file)
    if "missing" in df_all_full.columns:
        df_all_output = df_all_full.filter(~pl.col("missing").cast(pl.Boolean))
    else:
        df_all_output = df_all_full

    df_all_output = df_all_output.drop_nulls()
    return df_all_output
