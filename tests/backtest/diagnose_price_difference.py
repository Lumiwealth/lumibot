"""
Diagnostic script to examine raw bar data from ThetaData and Polygon
to understand why prices differ.
"""

import datetime
import os
from lumibot.backtesting import ThetaDataBacktesting, PolygonDataBacktesting
from lumibot.entities import Asset
from lumibot.credentials import POLYGON_API_KEY


def examine_data_source(data_source_class, name):
    """Examine raw data from a data source."""
    print(f"\n{'='*80}")
    print(f"{name} Data Source")
    print(f"{'='*80}")

    start = datetime.datetime(2024, 8, 1, 9, 30)
    end = datetime.datetime(2024, 8, 1, 10, 0)

    if data_source_class == ThetaDataBacktesting:
        data_source = ThetaDataBacktesting(
            datetime_start=start,
            datetime_end=end,
            username=os.environ.get("THETADATA_USERNAME"),
            password=os.environ.get("THETADATA_PASSWORD"),
        )
    else:
        data_source = PolygonDataBacktesting(
            datetime_start=start,
            datetime_end=end,
            api_key=POLYGON_API_KEY,
        )

    # Set datetime to 9:30 AM
    data_source.datetime = datetime.datetime(2024, 8, 1, 9, 30, tzinfo=datetime.timezone(datetime.timedelta(hours=-4)))

    asset = Asset("AMZN", asset_type="stock")

    # Get historical bars
    bars = data_source.get_historical_prices(asset, 5, "minute")

    if bars:
        df = bars.df
        print(f"\nFirst 5 minute bars:")
        print(df.head(10))
        print(f"\nColumns: {df.columns.tolist()}")
        print(f"\nFirst bar details:")
        first_bar = df.iloc[0]
        for col in df.columns:
            print(f"  {col}: {first_bar[col]}")
    else:
        print("No bars returned")

    # Get last price
    price = data_source.get_last_price(asset)
    print(f"\nget_last_price(): ${price}")

    # Get the Data object and check what bar it's using
    tuple_to_find = data_source.find_asset_in_data_store(asset, None)
    if tuple_to_find in data_source._data_store:
        data = data_source._data_store[tuple_to_find]
        dt = data_source.get_datetime()
        iter_count = data.get_iter_count(dt)
        print(f"\niter_count: {iter_count}")
        print(f"Bar datetime: {data.datalines['datetime'].dataline[iter_count]}")
        print(f"Bar open: {data.datalines['open'].dataline[iter_count]}")
        print(f"Bar close: {data.datalines['close'].dataline[iter_count]}")
        print(f"Current dt: {dt}")
        print(f"dt > bar_datetime: {dt > data.datalines['datetime'].dataline[iter_count]}")

    # Get quote
    quote = data_source.get_quote(asset)
    print(f"\nget_quote():")
    print(f"  price: {quote.price}")
    print(f"  bid: {quote.bid}")
    print(f"  ask: {quote.ask}")
    print(f"  volume: {quote.volume}")
    print(f"  timestamp: {quote.timestamp}")
    if hasattr(quote, 'raw_data') and quote.raw_data:
        print(f"  raw_data: {quote.raw_data}")


if __name__ == "__main__":
    examine_data_source(ThetaDataBacktesting, "THETADATA")
    examine_data_source(PolygonDataBacktesting, "POLYGON")

    print(f"\n{'='*80}")
    print("ANALYSIS")
    print(f"{'='*80}")
    print("If the 'open' prices differ in the first bar, that's the root cause.")
    print("We need to investigate WHY the open prices differ:")
    print("  1. Are they pulling from different exchanges?")
    print("  2. Are they using different data types (trade vs NBBO)?")
    print("  3. Is there a timestamp alignment issue?")
    print("  4. Is one source incorrect?")
