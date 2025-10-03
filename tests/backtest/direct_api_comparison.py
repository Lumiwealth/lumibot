"""
Direct API comparison between ThetaData and Polygon.
Bypasses Lumibot classes to isolate any framework issues.
"""

import os
import datetime
import pandas as pd
import requests
from polygon import RESTClient
from lumibot.credentials import POLYGON_API_KEY


def get_thetadata_bars(symbol, date_str):
    """Get minute bars from ThetaData API directly."""
    url = "http://127.0.0.1:25510/hist/stock/ohlc"
    params = {
        "root": symbol,
        "start_date": date_str,
        "end_date": date_str,
        "ivl": 60000,  # 1 minute
        "rth": "true"
    }

    response = requests.get(url, params=params)
    data = response.json()

    if data and "response" in data:
        # Convert to DataFrame
        df = pd.DataFrame(data["response"], columns=data["header"]["format"])

        # Convert to datetime - ThetaData returns ms_of_day in Eastern Time!
        df["datetime"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d") + pd.to_timedelta(df["ms_of_day"], unit="ms")
        df = df.set_index("datetime")
        # Localize to Eastern Time (not UTC!)
        df.index = df.index.tz_localize("America/New_York")
        df = df[["open", "high", "low", "close", "volume"]]

        return df

    return None


def get_polygon_bars(symbol, date):
    """Get minute bars from Polygon API directly."""
    client = RESTClient(POLYGON_API_KEY)

    aggs = client.get_aggs(
        ticker=symbol,
        multiplier=1,
        timespan="minute",
        from_=date,
        to=date,
        limit=50000
    )

    bars = []
    for agg in aggs:
        dt = datetime.datetime.fromtimestamp(agg.timestamp/1000, tz=datetime.timezone.utc)
        bars.append({
            "datetime": dt,
            "open": agg.open,
            "high": agg.high,
            "low": agg.low,
            "close": agg.close,
            "volume": agg.volume,
        })

    if bars:
        df = pd.DataFrame(bars)
        df = df.set_index("datetime")
        return df

    return None


def compare_bar(theta_bar, polygon_bar, symbol, time_str):
    """Compare a single bar from both sources."""
    open_diff = theta_bar["open"] - polygon_bar["open"]
    high_diff = theta_bar["high"] - polygon_bar["high"]
    low_diff = theta_bar["low"] - polygon_bar["low"]
    close_diff = theta_bar["close"] - polygon_bar["close"]
    volume_diff = theta_bar["volume"] - polygon_bar["volume"]
    volume_pct = (volume_diff / polygon_bar["volume"] * 100) if polygon_bar["volume"] > 0 else 0

    print(f"\n  {time_str}:")
    print(f"    ThetaData : O=${theta_bar['open']:.3f} H=${theta_bar['high']:.3f} L=${theta_bar['low']:.3f} C=${theta_bar['close']:.3f} V={theta_bar['volume']:,.0f}")
    print(f"    Polygon   : O=${polygon_bar['open']:.3f} H=${polygon_bar['high']:.3f} L=${polygon_bar['low']:.3f} C=${polygon_bar['close']:.3f} V={polygon_bar['volume']:,.0f}")
    print(f"    Difference: O=${open_diff:+.3f} H=${high_diff:+.3f} L=${low_diff:+.3f} C=${close_diff:+.3f} V={volume_diff:+,.0f} ({volume_pct:+.1f}%)")

    return {
        "symbol": symbol,
        "time": time_str,
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
    }


def main():
    date = datetime.date(2024, 8, 1)
    date_str = "20240801"
    symbols = ["AMZN", "AAPL", "SPY", "TSLA", "PLTR"]

    # Times to check (in UTC, not ET!)
    times_to_check = [
        ("09:30", "Market Open"),
        ("10:00", "Early Morning"),
        ("12:00", "Midday"),
        ("14:00", "Afternoon"),
        ("15:30", "Near Close"),
    ]

    results = []

    print(f"\n{'='*120}")
    print(f"Direct API Comparison: ThetaData vs Polygon")
    print(f"Date: {date}")
    print(f"{'='*120}")

    for symbol in symbols:
        print(f"\n{symbol}:")
        print("-" * 120)

        # Get all bars for the day from both sources
        theta_df = get_thetadata_bars(symbol, date_str)
        polygon_df = get_polygon_bars(symbol, date)

        if theta_df is None:
            print(f"  ❌ No ThetaData bars")
            continue

        if polygon_df is None:
            print(f"  ❌ No Polygon bars")
            continue

        # ThetaData is already in ET, Polygon needs conversion from UTC to ET
        polygon_df.index = polygon_df.index.tz_convert("America/New_York")

        # Compare specific times
        for time_str, label in times_to_check:
            hour, minute = map(int, time_str.split(":"))

            # Find matching bars
            theta_matches = theta_df[(theta_df.index.hour == hour) & (theta_df.index.minute == minute)]
            polygon_matches = polygon_df[(polygon_df.index.hour == hour) & (polygon_df.index.minute == minute)]

            if not theta_matches.empty and not polygon_matches.empty:
                theta_bar = theta_matches.iloc[0]
                polygon_bar = polygon_matches.iloc[0]

                result = compare_bar(theta_bar, polygon_bar, symbol, f"{label} ({time_str})")
                results.append(result)
            else:
                print(f"\n  {label} ({time_str}): ❌ Missing bars (Theta: {not theta_matches.empty}, Polygon: {not polygon_matches.empty})")

    # Summary statistics
    if results:
        df = pd.DataFrame(results)

        print(f"\n{'='*120}")
        print(f"SUMMARY STATISTICS ({len(results)} comparisons)")
        print(f"{'='*120}")

        print(f"\nPrice Differences (Open):")
        print(f"  Mean:     ${df['open_diff'].mean():.4f}")
        print(f"  Std:      ${df['open_diff'].std():.4f}")
        print(f"  Min:      ${df['open_diff'].min():.4f}")
        print(f"  Max:      ${df['open_diff'].max():.4f}")
        print(f"  Abs Mean: ${df['open_diff'].abs().mean():.4f}")

        print(f"\nPrice Differences (Close):")
        print(f"  Mean:     ${df['close_diff'].mean():.4f}")
        print(f"  Std:      ${df['close_diff'].std():.4f}")
        print(f"  Min:      ${df['close_diff'].min():.4f}")
        print(f"  Max:      ${df['close_diff'].max():.4f}")
        print(f"  Abs Mean: ${df['close_diff'].abs().mean():.4f}")

        print(f"\nVolume Differences:")
        print(f"  Mean:     {df['volume_diff'].mean():,.0f} ({df['volume_pct_diff'].mean():+.2f}%)")
        print(f"  Std:      {df['volume_diff'].std():,.0f}")
        print(f"  Min:      {df['volume_diff'].min():,.0f} ({df['volume_pct_diff'].min():+.2f}%)")
        print(f"  Max:      {df['volume_diff'].max():,.0f} ({df['volume_pct_diff'].max():+.2f}%)")

        # Check patterns
        higher_volume_count = (df['volume_diff'] > 0).sum()
        total_count = len(df)
        print(f"\nThetaData has HIGHER volume in {higher_volume_count}/{total_count} cases ({higher_volume_count/total_count*100:.1f}%)")

        # Save results
        df.to_csv("direct_api_comparison.csv", index=False)
        print(f"\n✓ Results saved to direct_api_comparison.csv")


if __name__ == "__main__":
    main()
