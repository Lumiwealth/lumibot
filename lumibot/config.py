"""Configuration module for Lumibot.

This module provides configuration options for the Lumibot library,
including the ability to switch between pandas and polars for data processing.
"""

import os
from typing import Literal

# Data backend configuration
DataBackend = Literal["pandas", "polars"]

# Default to polars for better performance
_DATA_BACKEND: DataBackend = "polars"

# Check environment variable
_env_backend = os.environ.get("LUMIBOT_DATA_BACKEND", "").lower()
if _env_backend in ["pandas", "polars"]:
    _DATA_BACKEND = _env_backend


def get_data_backend() -> DataBackend:
    """Get the current data backend (pandas or polars)."""
    return _DATA_BACKEND


def set_data_backend(backend: DataBackend) -> None:
    """Set the data backend to use.
    
    Parameters
    ----------
    backend : str
        Either "pandas" or "polars"
    """
    global _DATA_BACKEND
    if backend not in ["pandas", "polars"]:
        raise ValueError(f"Invalid backend: {backend}. Must be 'pandas' or 'polars'")
    _DATA_BACKEND = backend


def use_polars() -> bool:
    """Check if polars backend is enabled."""
    return _DATA_BACKEND == "polars"


# Performance optimization settings
POLARS_SETTINGS = {
    # Number of threads for polars operations (None = use all available)
    "n_threads": None,
    # Enable string cache for better performance with categorical data
    "enable_string_cache": True,
    # Table formatting settings
    "fmt_table_cell_list_len": 5,
    "fmt_str_lengths": 50,
}


def configure_polars():
    """Configure polars with optimized settings."""
    try:
        import polars as pl
        
        if POLARS_SETTINGS["n_threads"] is not None:
            pl.Config.set_streaming_n_threads(POLARS_SETTINGS["n_threads"])
        
        if POLARS_SETTINGS["enable_string_cache"]:
            pl.enable_string_cache()
        
        pl.Config.set_fmt_table_cell_list_len(POLARS_SETTINGS["fmt_table_cell_list_len"])
        pl.Config.set_fmt_str_lengths(POLARS_SETTINGS["fmt_str_lengths"])
        
    except ImportError:
        pass  # Polars not installed