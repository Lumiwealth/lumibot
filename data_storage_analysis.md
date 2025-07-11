# Lumibot Data Storage and Retrieval Analysis

## Current State Analysis

### Data Storage Patterns

The Lumibot backtesting framework currently uses several different data storage approaches across different data sources:

#### 1. In-Memory Storage (`_data_store`)
**Location**: `lumibot/data_sources/pandas_data.py`, `lumibot/strategies/_strategy.py`

**Current Implementation**:
- Data is stored in `self._data_store` dictionary with Asset objects as keys
- Each data source maintains its own in-memory pandas DataFrames
- All historical data is loaded into memory at once during backtesting

**Issues Identified**:
- **Memory Inefficiency**: All data is kept in memory simultaneously
- **Redundant Storage**: Multiple data sources may store overlapping data
- **No Query Optimization**: Linear searches through data structures
- **Limited Scalability**: Memory usage grows linearly with data size and time range

#### 2. File-Based Caching (Polygon)
**Location**: `lumibot/tools/polygon_helper.py`, `lumibot/backtesting/polygon_backtesting.py`

**Current Implementation**:
- Uses Feather files for caching downloaded data
- Implements cache validation based on split adjustments
- Parallel downloads with progress tracking
- Missing data tracking with dummy records

**Issues Identified**:
- **Storage Fragmentation**: One file per asset/timeframe combination
- **No Relational Queries**: Can't efficiently query across multiple assets
- **Cache Management**: Manual cache invalidation and cleanup
- **Limited Indexing**: No efficient time-based indexing

#### 3. DuckDB Implementation (CCXT)
**Location**: `lumibot/tools/ccxt_data_store.py`

**Current Implementation**:
- Already uses DuckDB for CCXT cryptocurrency data
- Stores OHLCV data with missing data indicators
- Implements range-based cache management
- Two-table design: `candles` + `cache_dt_ranges`

**Advantages**:
- **Efficient Storage**: Column-oriented storage with compression
- **Fast Queries**: SQL-based data retrieval
- **Good Cache Management**: Range-based caching system

#### 4. Memory Limits (Polygon)
**Location**: `lumibot/backtesting/polygon_backtesting.py` lines 48-54

```python
def _enforce_storage_limit(pandas_data: OrderedDict):
    storage_used = sum(data.df.memory_usage().sum() for data in pandas_data.values())
    logging.info(f"{storage_used = :,} bytes for {len(pandas_data)} items")
    while storage_used > PolygonDataBacktesting.MAX_STORAGE_BYTES:
        k, d = pandas_data.popitem(last=False)
        mu = d.df.memory_usage().sum()
        storage_used -= mu
        logging.info(f"Storage limit exceeded. Evicted LRU data: {k} used {mu:,} bytes")
```

## Major Inefficiencies Identified

### 1. Data Redundancy
- **Multiple Storage Systems**: Polygon uses Feather files, CCXT uses DuckDB, others use pure memory
- **Overlapping Data**: Same assets may be stored across multiple data sources
- **No Unified Cache**: Each data source manages its own cache independently

### 2. Memory Management Issues
- **Eager Loading**: All data loaded upfront regardless of actual usage
- **No Lazy Loading**: Cannot load data on-demand during backtesting iterations
- **Poor Eviction**: Simple LRU eviction without considering query patterns

### 3. Query Inefficiencies
- **Linear Searches**: Finding assets requires iterating through dictionaries
- **No Indexing**: Time-based queries scan entire DataFrames
- **No Aggregations**: Cannot efficiently compute cross-asset statistics

### 4. Scalability Problems
- **Memory Bound**: Large backtests limited by available RAM
- **Single Threaded**: Data retrieval mostly single-threaded
- **No Partitioning**: Cannot efficiently handle very large date ranges

## DuckDB Integration Benefits

### 1. Unified Storage Layer
```python
# Proposed unified storage interface
class LumibotDataStore:
    def __init__(self, cache_dir: str):
        self.conn = duckdb.connect(f"{cache_dir}/lumibot_data.duckdb")
        self._setup_schema()
    
    def store_ohlcv_data(self, asset: Asset, data: pd.DataFrame, source: str):
        # Store data with metadata
        pass
    
    def get_historical_prices(self, asset: Asset, start: datetime, end: datetime) -> pd.DataFrame:
        # Efficient time-range queries
        pass
```

### 2. Query Optimization
- **Column Storage**: Efficient for OHLCV data queries
- **Time Indexing**: Fast time-range filtering
- **SQL Interface**: Complex queries across multiple assets
- **Compression**: Automatic data compression

### 3. Memory Efficiency
- **Lazy Loading**: Load only required data
- **Streaming**: Process large datasets without full memory load
- **Caching**: Intelligent query result caching

### 4. Advanced Features
- **Window Functions**: Technical indicators computed in database
- **Aggregations**: Cross-asset statistics and correlation analysis
- **Joins**: Efficient multi-asset analysis

## Proposed DuckDB Schema

```sql
-- Main OHLCV data table
CREATE TABLE ohlcv_data (
    asset_symbol VARCHAR,
    asset_type VARCHAR,
    timestamp TIMESTAMP,
    timeframe VARCHAR,
    open DECIMAL(18,8),
    high DECIMAL(18,8),
    low DECIMAL(18,8),
    close DECIMAL(18,8),
    volume DECIMAL(18,8),
    source VARCHAR,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (asset_symbol, asset_type, timestamp, timeframe, source)
);

-- Asset metadata
CREATE TABLE assets (
    symbol VARCHAR,
    asset_type VARCHAR,
    exchange VARCHAR,
    currency VARCHAR,
    metadata JSON,
    PRIMARY KEY (symbol, asset_type)
);

-- Data source metadata
CREATE TABLE data_sources (
    source_name VARCHAR PRIMARY KEY,
    last_updated TIMESTAMP,
    config JSON
);

-- Cache management
CREATE TABLE cache_ranges (
    id UUID PRIMARY KEY,
    asset_symbol VARCHAR,
    asset_type VARCHAR,
    source VARCHAR,
    timeframe VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);
```

## Implementation Strategy

### Phase 1: Unified Data Interface
1. Create abstract `DataStorage` interface
2. Implement DuckDB-based storage backend
3. Add compatibility layer for existing data sources

### Phase 2: Migration
1. Migrate CCXT implementation to new schema
2. Add DuckDB support to Polygon data source
3. Implement lazy loading for pandas data sources

### Phase 3: Optimization
1. Add intelligent caching strategies
2. Implement query optimization
3. Add parallel data loading

### Phase 4: Advanced Features
1. Technical indicator computation in database
2. Cross-asset analysis capabilities
3. Real-time data streaming integration

## Expected Performance Improvements

### Memory Usage
- **50-80% reduction** in memory usage through lazy loading
- **Compression**: 60-90% storage reduction through columnar storage
- **Scalability**: Handle 10x larger datasets within same memory constraints

### Query Performance
- **10-100x faster** time-range queries through indexing
- **Near-instant** asset lookup through optimized schemas
- **Efficient aggregations** for technical analysis

### Development Experience
- **Unified API**: Single interface for all data operations
- **SQL Flexibility**: Complex queries without pandas complexity
- **Better Error Handling**: Database constraints and validation

## Migration Path

### 1. Backward Compatibility
```python
class BackwardCompatibleDataSource(DataSourceBacktesting):
    def __init__(self, *args, use_duckdb=True, **kwargs):
        self.use_duckdb = use_duckdb
        if use_duckdb:
            self.storage = LumibotDataStore()
        else:
            # Legacy implementation
            super().__init__(*args, **kwargs)
```

### 2. Gradual Migration
- Start with new features using DuckDB
- Migrate high-volume data sources first
- Maintain pandas interface for user scripts

### 3. Performance Monitoring
- Track memory usage improvements
- Monitor query performance gains
- Measure user experience impact

## Conclusion

Integrating DuckDB into Lumibot's data storage layer would address major scalability and efficiency issues while providing a foundation for advanced analytics features. The existing CCXT implementation proves DuckDB works well in this context, and extending it to other data sources would create a more unified, efficient, and scalable backtesting framework.

The key benefits include:
- **Dramatic memory usage reduction** through lazy loading
- **Significant performance improvements** through query optimization
- **Enhanced scalability** for large-scale backtesting
- **Unified data interface** reducing complexity
- **Advanced analytics capabilities** through SQL

The migration can be done incrementally while maintaining backward compatibility, making it a low-risk, high-reward improvement to the framework.
