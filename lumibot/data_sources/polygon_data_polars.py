"""Ultra-optimized Polygon data source using pure polars with zero pandas conversions.

This implementation:
1. Eliminates datalines - uses polars columnar storage directly
2. Zero pandas conversions - pure polars throughout
3. Lazy evaluation for maximum performance
4. Efficient caching with parquet files
5. Vectorized operations only
"""

# NOTE: This module is intentionally disabled. The DataBento Polars migration only
# supports Polars for DataBento; other data sources must use the pandas implementations.
raise RuntimeError("Yahoo/Polygon Polars backends are not production-ready; use the pandas data sources instead.")
