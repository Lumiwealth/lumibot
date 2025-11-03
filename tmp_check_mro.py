#!/usr/bin/env python3
"""Check MRO for get_historical_prices"""

import json

try:
    from lumibot.backtesting.databento_backtesting_polars import DataBentoDataBacktestingPolars

    # Get MRO
    mro = [c.__name__ for c in DataBentoDataBacktestingPolars.__mro__]

    # Find which class defines get_historical_prices
    ghp_class = None
    for cls in DataBentoDataBacktestingPolars.__mro__:
        if hasattr(cls, 'get_historical_prices') and 'get_historical_prices' in cls.__dict__:
            ghp_class = cls.__name__
            break

    results = {
        "success": True,
        "mro": mro,
        "get_historical_prices_defined_in": ghp_class
    }

except Exception as e:
    results = {
        "success": False,
        "error": str(e),
        "error_type": type(e).__name__
    }

print(json.dumps(results, indent=2))
