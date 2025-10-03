"""
Comprehensive comparison test between ThetaData and Polygon across:
- Multiple stocks (AMZN, AAPL, SPY, TSLA, PLTR)
- Multiple times of day (9:30, 10:00, 12:00, 15:00, 15:30)
- Check for systematic patterns vs random differences
"""

import datetime
import os
import pandas as pd
from lumibot.backtesting import ThetaDataBacktesting, PolygonDataBacktesting
from lumibot.entities import Asset
from lumibot.credentials import POLYGON_API_KEY


def get_bar_at_time(data_source_class, symbol, date, hour, minute):
    """Get a specific bar from a data source."""
    start = datetime.datetime(date.year, date.month, date.day, 9, 0)
    end = datetime.datetime(date.year, date.month, date.day, 16, 0)

    if data_source_class == ThetaDataBacktesting:
        ds = ThetaDataBacktesting(
            datetime_start=start,
            datetime_end=end,
            username=os.environ.get("THETADATA_USERNAME"),
            password=os.environ.get("THETADATA_PASSWORD"),
        )
    else:
        ds = PolygonDataBacktesting(
            datetime_start=start,
            datetime_end=end,
            api_key=POLYGON_API_KEY,
        )

    asset = Asset(symbol, asset_type="stock")

    # Get all bars for the day
    bars = ds.get_historical_prices_between_dates(asset, "minute", start_date=start, end_date=end)

    if bars and not bars.df.empty:
        # Find the bar at our target time
        df = bars.df

        # Try to find bars matching our target time
        for idx in df.index:
            if idx.hour == hour and idx.minute == minute:
                bar = df.loc[idx]
                return {
                    "symbol": symbol,
                    "datetime": idx,
                    "open": float(bar["open"]),
                    "high": float(bar["high"]),
                    "low": float(bar["low"]),
                    "close": float(bar["close"]),
                    "volume": float(bar["volume"]),
                }

    return None


def compare_providers():
    """Compare ThetaData vs Polygon across multiple stocks and times."""

    test_date = datetime.date(2024, 8, 1)
    symbols = ["AMZN", "AAPL", "SPY", "TSLA", "PLTR"]
    times = [
        (9, 30, "Market Open"),
        (10, 0, "Early Morning"),
        (12, 0, "Midday"),
        (14, 0, "Afternoon"),
        (15, 30, "Near Close"),
    ]

    results = []

    print(f"\nComprehensive ThetaData vs Polygon Comparison")
    print(f"Date: {test_date}")
    print(f"=" * 120)

    for symbol in symbols:
        print(f"\n{symbol}:")
        print("-" * 120)

        for hour, minute, label in times:
            print(f"\n  {label} ({hour}:{minute:02d} ET):")

            try:
                theta_bar = get_bar_at_time(ThetaDataBacktesting, symbol, test_date, hour, minute)
                polygon_bar = get_bar_at_time(PolygonDataBacktesting, symbol, test_date, hour, minute)

                if theta_bar and polygon_bar:
                    # Calculate differences
                    open_diff = theta_bar["open"] - polygon_bar["open"]
                    close_diff = theta_bar["close"] - polygon_bar["close"]
                    volume_diff = theta_bar["volume"] - polygon_bar["volume"]
                    volume_pct = (volume_diff / polygon_bar["volume"] * 100) if polygon_bar["volume"] > 0 else 0

                    print(f"    ThetaData : O=${theta_bar['open']:.2f} H=${theta_bar['high']:.2f} L=${theta_bar['low']:.2f} C=${theta_bar['close']:.2f} V={theta_bar['volume']:,.0f}")
                    print(f"    Polygon   : O=${polygon_bar['open']:.2f} H=${polygon_bar['high']:.2f} L=${polygon_bar['low']:.2f} C=${polygon_bar['close']:.2f} V={polygon_bar['volume']:,.0f}")
                    print(f"    Difference: O=${open_diff:+.3f} C=${close_diff:+.3f} V={volume_diff:+,.0f} ({volume_pct:+.1f}%)")

                    results.append({
                        "symbol": symbol,
                        "time": f"{hour}:{minute:02d}",
                        "label": label,
                        "theta_open": theta_bar["open"],
                        "polygon_open": polygon_bar["open"],
                        "open_diff": open_diff,
                        "theta_close": theta_bar["close"],
                        "polygon_close": polygon_bar["close"],
                        "close_diff": close_diff,
                        "theta_volume": theta_bar["volume"],
                        "polygon_volume": polygon_bar["volume"],
                        "volume_diff": volume_diff,
                        "volume_pct_diff": volume_pct,
                    })
                else:
                    print(f"    ❌ Missing data (Theta: {theta_bar is not None}, Polygon: {polygon_bar is not None})")

            except Exception as e:
                print(f"    ❌ Error: {e}")

    # Create summary statistics
    if results:
        df = pd.DataFrame(results)

        print(f"\n{'=' * 120}")
        print(f"SUMMARY STATISTICS")
        print(f"{'=' * 120}")

        print(f"\nPrice Differences (Open):")
        print(f"  Mean: ${df['open_diff'].mean():.4f}")
        print(f"  Std:  ${df['open_diff'].std():.4f}")
        print(f"  Min:  ${df['open_diff'].min():.4f}")
        print(f"  Max:  ${df['open_diff'].max():.4f}")
        print(f"  Abs Mean: ${df['open_diff'].abs().mean():.4f}")

        print(f"\nPrice Differences (Close):")
        print(f"  Mean: ${df['close_diff'].mean():.4f}")
        print(f"  Std:  ${df['close_diff'].std():.4f}")
        print(f"  Min:  ${df['close_diff'].min():.4f}")
        print(f"  Max:  ${df['close_diff'].max():.4f}")
        print(f"  Abs Mean: ${df['close_diff'].abs().mean():.4f}")

        print(f"\nVolume Differences:")
        print(f"  Mean: {df['volume_diff'].mean():,.0f} ({df['volume_pct_diff'].mean():+.2f}%)")
        print(f"  Std:  {df['volume_diff'].std():,.0f}")
        print(f"  Min:  {df['volume_diff'].min():,.0f} ({df['volume_pct_diff'].min():+.2f}%)")
        print(f"  Max:  {df['volume_diff'].max():,.0f} ({df['volume_pct_diff'].max():+.2f}%)")

        # Check if ThetaData consistently has higher volume
        higher_volume_count = (df['volume_diff'] > 0).sum()
        total_count = len(df)
        print(f"\nThetaData has HIGHER volume in {higher_volume_count}/{total_count} cases ({higher_volume_count/total_count*100:.1f}%)")

        # Save to CSV
        df.to_csv("thetadata_vs_polygon_comparison.csv", index=False)
        print(f"\n✓ Results saved to thetadata_vs_polygon_comparison.csv")

        return df

    return None


if __name__ == "__main__":
    compare_providers()
