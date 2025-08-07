"""
Wrapper class for Polars DataFrames to provide pandas-like compatibility
"""
import polars as pl
import pandas as pd


class PolarsDataFrameWrapper:
    """
    A wrapper around Polars DataFrame that provides pandas-like attributes and methods
    for backward compatibility with code expecting pandas DataFrames.
    """
    
    def __init__(self, df):
        """Initialize with a polars DataFrame"""
        if not isinstance(df, pl.DataFrame):
            raise TypeError("PolarsDataFrameWrapper requires a polars DataFrame")
        self._df = df
    
    # Delegate most attributes to the underlying polars DataFrame
    def __getattr__(self, name):
        """Delegate attribute access to the underlying polars DataFrame"""
        if name == 'empty':
            # Provide pandas-like .empty property
            return self._df.is_empty()
        elif name == 'index':
            # Provide pandas-like index
            if 'datetime' in self._df.columns:
                return self._df['datetime'].to_pandas()
            else:
                return pd.RangeIndex(len(self._df))
        elif name == 'columns':
            return self._df.columns
        elif name == 'shape':
            return self._df.shape
        elif name == 'dtypes':
            return self._df.dtypes
        else:
            # Delegate to the underlying polars DataFrame
            return getattr(self._df, name)
    
    def __getitem__(self, key):
        """Enable bracket indexing like pandas"""
        result = self._df[key]
        if isinstance(result, pl.DataFrame):
            return PolarsDataFrameWrapper(result)
        return result
    
    def __setitem__(self, key, value):
        """Enable bracket assignment like pandas"""
        self._df = self._df.with_columns(pl.lit(value).alias(key))
    
    def __len__(self):
        """Return the number of rows"""
        return len(self._df)
    
    def __repr__(self):
        return repr(self._df)
    
    def _repr_html_(self):
        return self._df._repr_html_()
    
    def to_pandas(self):
        """Convert to pandas DataFrame"""
        return self._df.to_pandas()
    
    def reset_index(self, drop=False):
        """Mimic pandas reset_index"""
        if 'datetime' in self._df.columns and not drop:
            return PolarsDataFrameWrapper(self._df)
        return PolarsDataFrameWrapper(self._df)
    
    def set_index(self, col):
        """Mimic pandas set_index"""
        # Polars doesn't have indices like pandas, so we just ensure the column exists
        if col in self._df.columns:
            return PolarsDataFrameWrapper(self._df)
        raise KeyError(f"Column {col} not found")
    
    def resample(self, rule):
        """Provide basic resample functionality"""
        # This is a simplified implementation
        # In practice, you'd need to handle this more robustly
        class Resampler:
            def __init__(self, wrapper, rule):
                self.wrapper = wrapper
                self.rule = rule
            
            def agg(self, agg_dict):
                # Convert to pandas for resampling (for now)
                pdf = self.wrapper.to_pandas()
                if 'datetime' in pdf.columns:
                    pdf = pdf.set_index('datetime')
                elif not isinstance(pdf.index, pd.DatetimeIndex):
                    raise ValueError("DataFrame must have datetime index for resampling")
                
                result = pdf.resample(self.rule).agg(agg_dict)
                # Return as pandas DataFrame directly since main12.py expects pandas
                return result
        
        return Resampler(self, rule)
    
    def dropna(self):
        """Drop null values"""
        return PolarsDataFrameWrapper(self._df.drop_nulls())
    
    def tail(self, n=5):
        """Get last n rows"""
        return PolarsDataFrameWrapper(self._df.tail(n))
    
    def head(self, n=5):
        """Get first n rows"""
        return PolarsDataFrameWrapper(self._df.head(n))
    
    @property
    def iloc(self):
        """Provide iloc-like functionality"""
        class ILocIndexer:
            def __init__(self, df):
                self.df = df
            
            def __getitem__(self, key):
                if isinstance(key, int):
                    return self.df._df.row(key)
                elif isinstance(key, slice):
                    return PolarsDataFrameWrapper(self.df._df[key])
                else:
                    raise TypeError("iloc indices must be integers or slices")
        
        return ILocIndexer(self)
    
    # Pass through to underlying DataFrame for iteration
    def iterrows(self):
        """Iterate over rows"""
        for i, row in enumerate(self._df.iter_rows(named=True)):
            yield i, row
    
    def to_dict(self, orient='dict'):
        """Convert to dictionary"""
        return self._df.to_dict(as_series=(orient != 'records'))