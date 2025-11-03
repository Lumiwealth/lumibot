#!/usr/bin/env python3
"""
DataBento to Build Alpha Exporter

Downloads continuous futures data from DataBento and formats it for Build Alpha.

Directory structure:
    databento_exports/
        {SYMBOL_ROOT}/
            {YYYYMMDD}_{YYYYMMDD}/
                {timeframe}/
                    {symbol_root}_{start_date}_{end_date}_{timeframe}_{timezone}.csv

CSV format:
    Date,Time,Open,High,Low,Close,Vol,OI
    - Date: m/d/yyyy
    - Time: HH:MM:SS (in specified timezone)
    - OHLC: continuous futures prices
    - Vol,OI: hardcoded to 1

Usage:
    python databento_to_buildalpha.py GC 2025-01-01 2025-10-31 1m UTC
    python databento_to_buildalpha.py GC 2025-01-01 2025-10-31 1m EST

Requirements:
    - DATABENTO_API_KEY environment variable must be set
    - databento package installed
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import polars as pl
from lumibot.entities import Asset
from lumibot.tools import databento_helper_polars
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)


def format_timeframe(timeframe: str) -> tuple[str, str]:
    """
    Convert timeframe to DataBento format and Build Alpha format.

    Args:
        timeframe: Input timeframe (e.g., '1m', 'M1', '1h', '1d')

    Returns:
        Tuple of (databento_timestep, buildalpha_format)
        e.g., ('minute', '1m') or ('hour', '1h')
    """
    tf = timeframe.lower().strip()

    # Normalize to standard format
    if tf in ['1m', 'm1', 'minute']:
        return 'minute', '1m'
    elif tf in ['1h', 'h1', 'hour']:
        return 'hour', '1h'
    elif tf in ['1d', 'd1', 'day']:
        return 'day', '1d'
    else:
        raise ValueError(f"Unsupported timeframe: {timeframe}. Use 1m, 1h, or 1d")


def get_timezone_info(tz_arg: str) -> tuple[ZoneInfo, str]:
    """
    Get timezone object and label from argument.

    Args:
        tz_arg: Timezone argument ('UTC' or 'EST')

    Returns:
        Tuple of (ZoneInfo object, timezone label for filename)

    Raises:
        ValueError: If timezone argument is invalid
    """
    tz_upper = tz_arg.upper().strip()

    if tz_upper == 'UTC':
        return ZoneInfo('UTC'), 'UTC'
    elif tz_upper == 'EST':
        # Use America/New_York which automatically handles EST/EDT
        # EST = UTC-5 (Standard Time, roughly Nov-Mar)
        # EDT = UTC-4 (Daylight Time, roughly Mar-Nov)
        return ZoneInfo('America/New_York'), 'EST'
    else:
        raise ValueError(f"Unsupported timezone: {tz_arg}. Use UTC or EST")


def download_databento_data(
    symbol_root: str,
    start_date: datetime,
    end_date: datetime,
    timestep: str,
    api_key: str
) -> pl.DataFrame:
    """
    Download continuous futures data from DataBento.

    Args:
        symbol_root: Futures root symbol (e.g., 'GC', 'ES')
        start_date: Start date (timezone-aware)
        end_date: End date (timezone-aware)
        timestep: Lumibot timestep ('minute', 'hour', 'day')
        api_key: DataBento API key

    Returns:
        Polars DataFrame with OHLCV data
    """
    logger.info(f"Downloading {symbol_root} data from {start_date} to {end_date}")

    # Create continuous futures asset
    asset = Asset(symbol=symbol_root, asset_type=Asset.AssetType.CONT_FUTURE)

    # Fetch data using existing helper
    df = databento_helper_polars.get_price_data_from_databento_polars(
        api_key=api_key,
        asset=asset,
        start=start_date,
        end=end_date,
        timestep=timestep,
        force_cache_update=False
    )

    if df is None or df.is_empty():
        raise ValueError(f"No data returned from DataBento for {symbol_root}")

    logger.info(f"Downloaded {len(df)} rows for {symbol_root}")
    return df


def convert_to_buildalpha_format(df: pl.DataFrame, target_timezone: ZoneInfo) -> pl.DataFrame:
    """
    Convert DataBento DataFrame to Build Alpha format with timezone conversion.

    Args:
        df: Polars DataFrame with columns: datetime, open, high, low, close, volume
        target_timezone: Target timezone for timestamps (ZoneInfo object)

    Returns:
        Polars DataFrame with Build Alpha columns: Date, Time, Open, High, Low, Close, Vol, OI
    """
    # Ensure datetime column exists and is timezone-aware
    if 'datetime' not in df.columns:
        raise ValueError("DataFrame must have 'datetime' column")

    # Ensure datetime is timezone-aware (convert to UTC if naive)
    datetime_dtype = df.schema.get('datetime')
    if isinstance(datetime_dtype, pl.Datetime):
        if not datetime_dtype.time_zone:
            df = df.with_columns(pl.col('datetime').dt.replace_time_zone('UTC'))

    # Convert to target timezone
    # Polars uses string timezone names, so extract from ZoneInfo
    target_tz_str = str(target_timezone)
    df = df.with_columns(
        pl.col('datetime').dt.convert_time_zone(target_tz_str).alias('datetime')
    )

    # IMPORTANT: Sort by datetime FIRST before converting to strings
    df = df.sort('datetime')

    # Remove duplicates based on datetime (keep first occurrence)
    df = df.unique(subset=['datetime'], keep='first')

    # Create Build Alpha format
    buildalpha_df = df.select([
        # Date in m/d/yyyy format
        pl.col('datetime').dt.strftime('%m/%d/%Y').alias('Date'),
        # Time in HH:MM:SS format
        pl.col('datetime').dt.strftime('%H:%M:%S').alias('Time'),
        # OHLC columns (uppercase)
        pl.col('open').alias('Open'),
        pl.col('high').alias('High'),
        pl.col('low').alias('Low'),
        pl.col('close').alias('Close'),
        # Vol and OI hardcoded to 1
        pl.lit(1).alias('Vol'),
        pl.lit(1).alias('OI'),
    ])

    return buildalpha_df


def save_buildalpha_export(
    df: pl.DataFrame,
    symbol_root: str,
    start_date: datetime,
    end_date: datetime,
    timeframe: str,
    timezone_label: str,
    base_dir: str = "databento_exports"
) -> Path:
    """
    Save DataFrame to Build Alpha CSV format in proper directory structure.

    Args:
        df: Polars DataFrame in Build Alpha format
        symbol_root: Futures root symbol
        start_date: Start date
        end_date: End date
        timeframe: Build Alpha timeframe format (e.g., '1m', '1h')
        timezone_label: Timezone label for filename (e.g., 'UTC', 'EST')
        base_dir: Base directory for exports

    Returns:
        Path to saved CSV file
    """
    # Format dates as YYYYMMDD
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')

    # Build directory structure
    export_dir = Path(base_dir) / symbol_root / f"{start_str}_{end_str}" / timeframe
    export_dir.mkdir(parents=True, exist_ok=True)

    # Build filename with timezone
    filename = f"{symbol_root}_{start_str}_{end_str}_{timeframe}_{timezone_label}.csv"
    filepath = export_dir / filename

    # Save to CSV
    logger.info(f"Saving {len(df)} rows to {filepath}")
    df.write_csv(filepath)

    return filepath


def main():
    """Main entry point for script."""
    # Parse command line arguments
    if len(sys.argv) != 6:
        print("Usage: python databento_to_buildalpha.py SYMBOL START_DATE END_DATE TIMEFRAME TIMEZONE")
        print("")
        print("Arguments:")
        print("  SYMBOL      - Futures root symbol (e.g., GC, ES, NQ)")
        print("  START_DATE  - Start date in YYYY-MM-DD format")
        print("  END_DATE    - End date in YYYY-MM-DD format")
        print("  TIMEFRAME   - Candle timeframe (1m, 1h, or 1d)")
        print("  TIMEZONE    - Output timezone (UTC or EST)")
        print("")
        print("Examples:")
        print("  python databento_to_buildalpha.py GC 2025-01-01 2025-10-31 1m UTC")
        print("  python databento_to_buildalpha.py ES 2025-01-01 2025-10-31 1m EST")
        print("")
        print("Timezone Notes:")
        print("  UTC - Coordinated Universal Time")
        print("  EST - US Eastern Time (automatically handles EST/EDT daylight savings)")
        print("")
        print("Environment:")
        print("  DATABENTO_API_KEY must be set")
        sys.exit(1)

    symbol_root = sys.argv[1].upper()
    start_date_str = sys.argv[2]
    end_date_str = sys.argv[3]
    timeframe_input = sys.argv[4]
    timezone_input = sys.argv[5]

    # Get API key from environment
    api_key = os.getenv('DATABENTO_API_KEY')
    if not api_key:
        print("ERROR: DATABENTO_API_KEY environment variable not set")
        sys.exit(1)

    try:
        # Parse dates
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

        # Make timezone-aware (UTC)
        start_date = start_date.replace(tzinfo=timezone.utc)
        end_date = end_date.replace(tzinfo=timezone.utc)

        # Add one day to end_date to make it inclusive
        end_date = end_date + timedelta(days=1)

        # Format timeframe
        databento_timestep, buildalpha_timeframe = format_timeframe(timeframe_input)

        # Get timezone info
        target_timezone, timezone_label = get_timezone_info(timezone_input)

        print(f"DataBento to Build Alpha Export")
        print(f"================================")
        print(f"Symbol:     {symbol_root}")
        print(f"Start:      {start_date_str}")
        print(f"End:        {end_date_str}")
        print(f"Timeframe:  {buildalpha_timeframe}")
        print(f"Timezone:   {timezone_label} ({target_timezone})")
        print(f"")

        # Step 1: Download data from DataBento
        print(f"Step 1: Downloading data from DataBento...")
        df = download_databento_data(
            symbol_root=symbol_root,
            start_date=start_date,
            end_date=end_date,
            timestep=databento_timestep,
            api_key=api_key
        )
        print(f"  ✓ Downloaded {len(df)} rows")
        print(f"")

        # Step 2: Convert to Build Alpha format
        print(f"Step 2: Converting to Build Alpha format...")
        buildalpha_df = convert_to_buildalpha_format(df, target_timezone)
        print(f"  ✓ Converted {len(buildalpha_df)} rows")
        print(f"  ✓ Date range: {buildalpha_df['Date'].min()} to {buildalpha_df['Date'].max()}")
        print(f"  ✓ Timezone: {timezone_label}")
        print(f"")

        # Step 3: Save to CSV
        print(f"Step 3: Saving to CSV...")
        filepath = save_buildalpha_export(
            df=buildalpha_df,
            symbol_root=symbol_root,
            start_date=start_date,
            end_date=end_date - timedelta(days=1),  # Remove the +1 day for filename
            timeframe=buildalpha_timeframe,
            timezone_label=timezone_label
        )
        print(f"  ✓ Saved to: {filepath}")
        print(f"")

        # Show sample of data
        print(f"Sample data (first 5 rows):")
        print(buildalpha_df.head(5))
        print(f"")

        print(f"✅ Export complete!")
        print(f"   Timestamps are in {timezone_label} timezone")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
