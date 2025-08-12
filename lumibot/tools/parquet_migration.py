"""
Utility functions for migrating from Feather to Parquet format.
Parquet provides better compression, column pruning, and predicate pushdown.
"""

import logging
from pathlib import Path
from typing import Any, Dict, Union

import pandas as pd

logger = logging.getLogger(__name__)

# Configuration for Parquet writing
PARQUET_CONFIG = {
    'compression': 'snappy',  # Fast compression with good ratio
    'engine': 'pyarrow',
    'index': False,  # Don't write index unless needed
    'use_deprecated_int96_timestamps': False,
    'coerce_timestamps': 'ms',  # Millisecond precision is usually enough
}

def migrate_feather_to_parquet(feather_path: Path, delete_original: bool = False) -> Path:
    """
    Migrate a single feather file to parquet format.
    
    Parameters
    ----------
    feather_path : Path
        Path to the feather file to migrate
    delete_original : bool
        Whether to delete the original feather file after migration
        
    Returns
    -------
    Path
        Path to the new parquet file
    """
    if not feather_path.exists():
        raise FileNotFoundError(f"Feather file not found: {feather_path}")

    # Create parquet path
    parquet_path = feather_path.with_suffix('.parquet')

    # Check if already migrated
    if parquet_path.exists():
        logger.info(f"Parquet file already exists: {parquet_path}")
        if delete_original:
            feather_path.unlink()
            logger.info(f"Deleted original feather file: {feather_path}")
        return parquet_path

    # Read feather and write parquet
    logger.info(f"Migrating {feather_path} to parquet...")
    df = pd.read_feather(feather_path)

    # Write with optimized settings
    df.to_parquet(parquet_path, **PARQUET_CONFIG)

    # Verify the migration
    df_verify = pd.read_parquet(parquet_path, engine='pyarrow')
    if not df.equals(df_verify):
        logger.warning(f"Data verification failed for {parquet_path}")
        parquet_path.unlink()
        raise ValueError("Data integrity check failed during migration")

    # Report compression ratio
    feather_size = feather_path.stat().st_size
    parquet_size = parquet_path.stat().st_size
    compression_ratio = (1 - parquet_size / feather_size) * 100
    logger.info(f"Migration complete: {feather_size:,} -> {parquet_size:,} bytes "
                f"({compression_ratio:.1f}% reduction)")

    if delete_original:
        feather_path.unlink()
        logger.info(f"Deleted original feather file: {feather_path}")

    return parquet_path


def read_cache_file(cache_path: Union[str, Path], **kwargs) -> pd.DataFrame:
    """
    Read cache file, supporting both feather and parquet formats.
    Automatically migrates feather to parquet if found.
    
    Parameters
    ----------
    cache_path : Union[str, Path]
        Path to the cache file (can be .feather or .parquet)
    **kwargs
        Additional arguments passed to read functions
        
    Returns
    -------
    pd.DataFrame
        The cached data
    """
    cache_path = Path(cache_path)

    # Check for parquet first (preferred)
    if cache_path.suffix == '.parquet':
        if cache_path.exists():
            return pd.read_parquet(cache_path, engine='pyarrow', **kwargs)
        # Check if feather exists to migrate
        feather_path = cache_path.with_suffix('.feather')
        if feather_path.exists():
            logger.info(f"Migrating {feather_path} to parquet...")
            migrate_feather_to_parquet(feather_path, delete_original=True)
            return pd.read_parquet(cache_path, engine='pyarrow', **kwargs)
        raise FileNotFoundError(f"Cache file not found: {cache_path}")

    # Handle feather files
    elif cache_path.suffix == '.feather':
        if cache_path.exists():
            # Migrate to parquet on the fly
            logger.info(f"Found feather file, migrating to parquet: {cache_path}")
            parquet_path = migrate_feather_to_parquet(cache_path, delete_original=False)
            return pd.read_parquet(parquet_path, engine='pyarrow', **kwargs)
        # Check if parquet already exists
        parquet_path = cache_path.with_suffix('.parquet')
        if parquet_path.exists():
            return pd.read_parquet(parquet_path, engine='pyarrow', **kwargs)
        raise FileNotFoundError(f"Cache file not found: {cache_path}")

    # Handle files without extension or wrong extension
    else:
        # Try parquet first
        parquet_path = Path(str(cache_path) + '.parquet')
        if parquet_path.exists():
            return pd.read_parquet(parquet_path, engine='pyarrow', **kwargs)

        # Try feather and migrate
        feather_path = Path(str(cache_path) + '.feather')
        if feather_path.exists():
            logger.info(f"Found feather file, migrating to parquet: {feather_path}")
            parquet_path = migrate_feather_to_parquet(feather_path, delete_original=False)
            return pd.read_parquet(parquet_path, engine='pyarrow', **kwargs)

        raise FileNotFoundError(f"No cache file found for: {cache_path}")


def write_cache_file(df: pd.DataFrame, cache_path: Union[str, Path], **kwargs) -> Path:
    """
    Write DataFrame to cache file using Parquet format.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to cache
    cache_path : Union[str, Path]
        Path where to save the cache file
    **kwargs
        Additional arguments for to_parquet
        
    Returns
    -------
    Path
        Path to the written file
    """
    cache_path = Path(cache_path)

    # Ensure .parquet extension
    if cache_path.suffix == '.feather':
        cache_path = cache_path.with_suffix('.parquet')
    elif cache_path.suffix != '.parquet':
        cache_path = Path(str(cache_path) + '.parquet')

    # Create directory if needed
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    # Merge configs
    write_config = PARQUET_CONFIG.copy()
    write_config.update(kwargs)

    # Write the file
    df.to_parquet(cache_path, **write_config)

    return cache_path


def get_cache_filename(asset, timespan: str, use_parquet: bool = True) -> str:
    """
    Generate cache filename for an asset and timespan.
    
    Parameters
    ----------
    asset : Asset
        The asset object
    timespan : str
        The timespan (e.g., 'minute', 'day')
    use_parquet : bool
        If True, use .parquet extension, otherwise .feather
        
    Returns
    -------
    str
        The cache filename
    """
    if hasattr(asset, 'expiration') and asset.expiration:
        uniq_str = f"{asset.symbol}_{asset.expiration}_{asset.strike}_{asset.right}"
    else:
        uniq_str = asset.symbol

    extension = '.parquet' if use_parquet else '.feather'
    return f"{asset.asset_type}_{uniq_str}_{timespan}{extension}"


def migrate_all_cache_files(cache_dir: Union[str, Path], delete_originals: bool = False) -> Dict[str, Any]:
    """
    Migrate all feather files in a directory to parquet format.
    
    Parameters
    ----------
    cache_dir : Union[str, Path]
        Directory containing cache files
    delete_originals : bool
        Whether to delete original feather files after migration
        
    Returns
    -------
    Dict[str, Any]
        Statistics about the migration
    """
    cache_dir = Path(cache_dir)
    if not cache_dir.exists():
        raise FileNotFoundError(f"Cache directory not found: {cache_dir}")

    stats = {
        'files_migrated': 0,
        'files_skipped': 0,
        'total_size_before': 0,
        'total_size_after': 0,
        'errors': []
    }

    # Find all feather files
    feather_files = list(cache_dir.rglob('*.feather'))
    logger.info(f"Found {len(feather_files)} feather files to migrate")

    for feather_path in feather_files:
        try:
            # Check if already migrated
            parquet_path = feather_path.with_suffix('.parquet')
            if parquet_path.exists():
                logger.info(f"Skipping {feather_path.name} - already migrated")
                stats['files_skipped'] += 1
                continue

            # Get original size
            original_size = feather_path.stat().st_size
            stats['total_size_before'] += original_size

            # Migrate
            new_path = migrate_feather_to_parquet(feather_path, delete_originals)
            new_size = new_path.stat().st_size
            stats['total_size_after'] += new_size
            stats['files_migrated'] += 1

        except Exception as e:
            logger.error(f"Error migrating {feather_path}: {e}")
            stats['errors'].append({'file': str(feather_path), 'error': str(e)})

    # Calculate compression
    if stats['total_size_before'] > 0:
        stats['compression_ratio'] = (1 - stats['total_size_after'] / stats['total_size_before']) * 100
    else:
        stats['compression_ratio'] = 0

    logger.info(f"Migration complete: {stats['files_migrated']} files migrated, "
                f"{stats['files_skipped']} skipped, {len(stats['errors'])} errors")
    logger.info(f"Total compression: {stats['compression_ratio']:.1f}%")

    return stats
