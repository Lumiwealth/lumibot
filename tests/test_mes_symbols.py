#!/usr/bin/env python3

"""
Test MES symbol resolution in DataBento GLBX.MDP3 dataset
Check what actual symbols are available for Micro E-mini S&P 500 futures
"""

import os
import pytest
from datetime import datetime, timedelta

# Only import databento if API key is available
databento = pytest.importorskip("databento", reason="databento not available")

class TestMESSymbols:
    """Test MES symbol resolution in DataBento."""
    
    @pytest.fixture(scope="class")
    def databento_client(self):
        """Create DataBento client if API key is available."""
        api_key = os.environ.get('DATABENTO_API_KEY')
        if not api_key:
            pytest.skip("DATABENTO_API_KEY environment variable not set")
        
        return databento.Historical(api_key)
    
    def test_mes_symbol_availability(self, databento_client):
        """Test which MES symbols are available in DataBento GLBX.MDP3 dataset."""
        # Test date range - last few days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3)
        
        # Try different MES symbol formats that might work
        symbols_to_test = [
            'MES',        # Base symbol
            'MES1!',      # Generic front month
            'MES.c.0',    # Continuous format
            'MESU5',      # Sep 2025 (expected current active)
            'MES.U5',     # Sep 2025 with dot
            'MESZ4',      # Dec 2024 
            'MES.Z4',     # Dec 2024 with dot
            'MESH5',      # Mar 2025
            'MES.H5',     # Mar 2025 with dot
        ]
        
        successful_symbols = []
        
        # Test each symbol
        for symbol in symbols_to_test:
            try:
                # Try to get just 1 minute of data to test if symbol exists
                data = databento_client.timeseries.get_range(
                    dataset="GLBX.MDP3",
                    symbols=[symbol],
                    schema="ohlcv-1m",
                    start=start_date.strftime('%Y-%m-%d'),
                    end=end_date.strftime('%Y-%m-%d'),
                    limit=1
                )
                
                if data and hasattr(data, 'to_df'):
                    df = data.to_df()
                    if not df.empty:
                        successful_symbols.append(symbol)
                        
            except Exception as e:
                # Expected for invalid symbols
                pass
        
        # Assert that at least one symbol format works
        assert len(successful_symbols) > 0, f"No working MES symbols found. Tested: {symbols_to_test}"
        
        print(f"Working MES symbols: {successful_symbols}")

if __name__ == "__main__":
    # Run the test if executed directly (for debugging)
    pytest.main([__file__, "-v"])
