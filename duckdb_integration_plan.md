# DuckDB Integration Implementation Plan for Lumibot

## Executive Summary

Based on the analysis of Lumibot's current data storage patterns, integrating DuckDB can provide significant improvements in memory usage, query performance, and scalability. The CCXT data source already uses DuckDB successfully, proving the concept works well in this context.

## Current Issues Summary

### 1. Memory Inefficiencies
- **Problem**: All historical data loaded into memory simultaneously
- **Impact**: Limited scalability, high memory usage for large backtests
- **Example**: In `polygon_backtesting.py`, memory enforcement shows this is already a known issue

### 2. Data Redundancy
- **Problem**: Multiple storage systems (Feather files, in-memory DataFrames, DuckDB for CCXT only)
- **Impact**: Complex maintenance, inconsistent performance characteristics
- **Evidence**: Different data sources use completely different storage approaches

### 3. Query Performance
- **Problem**: Linear searches through dictionaries and DataFrames
- **Impact**: Slow data retrieval, especially for time-range queries
- **Evidence**: `_data_store` lookups and pandas DataFrame filtering

### 4. Cache Management
- **Problem**: Each data source implements its own caching strategy
- **Impact**: Inconsistent behavior, complex maintenance
- **Evidence**: Polygon uses Feather files, CCXT uses DuckDB, others use no persistent cache

## Proposed Solution: Unified DuckDB Storage Layer

### Phase 1: Core Infrastructure (Week 1-2)

#### 1.1 Create Abstract Storage Interface
```python
# lumibot/data_sources/storage_interface.py
class DataStorageInterface(ABC):
    @abstractmethod
    def store_ohlcv_data(self, asset: Asset, data: pd.DataFrame, source: str, timeframe: str) -> None:
        pass
    
    @abstractmethod  
    def get_historical_prices(self, asset: Asset, start: datetime, end: datetime, 
                            timeframe: str = "minute", source: Optional[str] = None) -> Optional[pd.DataFrame]:
        pass
```

#### 1.2 Implement DuckDB Storage Backend
```python
# lumibot/data_sources/duckdb_storage.py
class DuckDBDataStorage(DataStorageInterface):
    def __init__(self, cache_dir: Optional[str] = None):
        # Implementation with proper schema and indexing
```

#### 1.3 Add Backward Compatibility Layer
```python
# lumibot/data_sources/enhanced_data_source.py
class EnhancedDataSourceBacktesting(DataSourceBacktesting):
    def __init__(self, *args, use_duckdb: bool = True, **kwargs):
        # Maintains existing API while using DuckDB backend
```

### Phase 2: Data Source Migration (Week 3-4)

#### 2.1 Migrate High-Volume Sources First
1. **Polygon Data Source**
   - Replace Feather file caching with DuckDB
   - Maintain existing API for backward compatibility
   - Add lazy loading capabilities

2. **Yahoo Data Source**
   - Replace in-memory storage with DuckDB
   - Implement streaming data loading

3. **Alpaca Data Source**
   - Unify with DuckDB storage
   - Add efficient CSV caching

#### 2.2 Enhance CCXT Implementation
- Extend existing DuckDB schema to match unified schema
- Add cross-asset query capabilities
- Implement better cache management

### Phase 3: Advanced Features (Week 5-6)

#### 3.1 Technical Indicator Computation
```sql
-- Example: Compute moving averages in SQL
SELECT timestamp, close,
       AVG(close) OVER (ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20,
       -- Bollinger Bands
       AVG(close) OVER (ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) + 
       2 * STDDEV(close) OVER (ORDER BY timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as bb_upper
FROM ohlcv_data 
WHERE asset_symbol = ? AND timeframe = ?
ORDER BY timestamp
```

#### 3.2 Cross-Asset Analysis
```sql
-- Example: Correlation analysis across assets
SELECT 
    a.timestamp,
    a.close as spy_close,
    b.close as qqq_close,
    CORR(a.close, b.close) OVER (ORDER BY a.timestamp ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) as correlation_100
FROM ohlcv_data a
JOIN ohlcv_data b ON a.timestamp = b.timestamp
WHERE a.asset_symbol = 'SPY' AND b.asset_symbol = 'QQQ'
```

### Phase 4: Performance Optimization (Week 7-8)

#### 4.1 Query Optimization
- Add strategic indexes for common query patterns
- Implement query caching
- Add connection pooling

#### 4.2 Storage Optimization
- Implement data compression
- Add partition pruning by date ranges
- Implement automatic cleanup of old data

## Implementation Details

### Schema Design
```sql
-- Unified schema that supports all data sources
CREATE TABLE ohlcv_data (
    asset_symbol VARCHAR,
    asset_type VARCHAR,
    exchange VARCHAR,
    timestamp TIMESTAMP,
    timeframe VARCHAR,
    source VARCHAR,
    open DECIMAL(18,8),
    high DECIMAL(18,8), 
    low DECIMAL(18,8),
    close DECIMAL(18,8),
    volume DECIMAL(18,8),
    adj_close DECIMAL(18,8),  -- For split/dividend adjustments
    dividend DECIMAL(18,8),   -- Dividend data
    split_ratio DECIMAL(10,4), -- Split ratio data
    metadata JSON,            -- Flexible metadata storage
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (asset_symbol, asset_type, exchange, timestamp, timeframe, source)
);

-- Indexes for performance
CREATE INDEX idx_ohlcv_lookup ON ohlcv_data (asset_symbol, asset_type, timeframe, source, timestamp);
CREATE INDEX idx_ohlcv_timestamp ON ohlcv_data (timestamp);
CREATE INDEX idx_ohlcv_symbol_time ON ohlcv_data (asset_symbol, timestamp);
```

### Migration Strategy

#### 1. Gradual Rollout
```python
# Environment variable to control DuckDB usage
USE_DUCKDB = os.getenv('LUMIBOT_USE_DUCKDB', 'true').lower() == 'true'

class PolygonDataBacktesting(PandasData):
    def __init__(self, *args, use_duckdb=USE_DUCKDB, **kwargs):
        self.use_duckdb = use_duckdb
        if use_duckdb:
            self.storage = DuckDBDataStorage()
        # ... rest of initialization
```

#### 2. A/B Testing
- Run backtests with both storage methods
- Compare performance metrics
- Gradually increase DuckDB usage percentage

#### 3. Backward Compatibility
```python
# Maintain existing API
def get_historical_prices(self, asset, length, timestep, **kwargs):
    if self.use_duckdb:
        return self._get_historical_prices_duckdb(asset, length, timestep, **kwargs)
    else:
        return self._get_historical_prices_legacy(asset, length, timestep, **kwargs)
```

## Expected Performance Improvements

### Memory Usage
- **50-80% reduction** through lazy loading
- **60-90% storage reduction** through compression
- **Ability to handle 10x larger datasets** within same memory constraints

### Query Performance  
- **10-100x faster** time-range queries
- **Near-instant** asset lookup
- **Efficient aggregations** for technical analysis

### Development Benefits
- **Unified API** across all data sources
- **SQL flexibility** for complex queries
- **Better error handling** with database constraints

## Risk Mitigation

### 1. Performance Regression
- **Mitigation**: A/B testing and performance monitoring
- **Rollback Plan**: Environment variable to disable DuckDB

### 2. Data Migration Issues
- **Mitigation**: Gradual migration with validation
- **Rollback Plan**: Keep legacy code paths active

### 3. User Experience Impact
- **Mitigation**: Maintain exact same API
- **Testing**: Comprehensive test suite with existing strategies

## Implementation Priority

### High Priority (Immediate Benefits)
1. **Memory efficiency** - Critical for large backtests
2. **Polygon data source migration** - Highest volume usage
3. **Unified caching** - Reduces complexity

### Medium Priority (Performance Gains)
1. **Query optimization** - Better user experience
2. **Technical indicators in SQL** - New capabilities
3. **Cross-asset analysis** - Advanced features

### Low Priority (Future Enhancements)
1. **Real-time streaming** - For live trading
2. **Advanced analytics** - Machine learning integration
3. **Cloud storage** - For distributed computing

## Success Metrics

### Technical Metrics
- Memory usage reduction: Target 50%+ reduction
- Query performance: Target 10x improvement for time-range queries
- Storage efficiency: Target 60%+ compression

### User Experience Metrics
- Backward compatibility: 100% existing strategies work unchanged
- Stability: No increase in error rates
- Documentation: Complete migration guide

### Development Metrics
- Code complexity: Reduced through unified storage layer
- Maintenance overhead: Reduced through single storage system
- Test coverage: Maintained at current levels

## Conclusion

Integrating DuckDB into Lumibot represents a significant architectural improvement that addresses current scalability limitations while providing a foundation for advanced analytics features. The existing CCXT implementation proves the concept works, and the proposed gradual migration strategy minimizes risk while maximizing benefits.

The key success factors are:
1. **Maintaining backward compatibility** during migration
2. **Gradual rollout** with performance monitoring
3. **Comprehensive testing** with existing strategies
4. **Clear documentation** for developers and users

This implementation would position Lumibot as a more scalable, efficient, and feature-rich backtesting framework capable of handling enterprise-scale workloads.
