"""
Check for timing offsets between ThetaData and Polygon.
Test if bars are offset by 1-3 minutes.
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
        "ivl": 60000,
        "rth": "true"
    }

    response = requests.get(url, params=params)
    data = response.json()

    if data and "response" in data:
        df = pd.DataFrame(data["response"], columns=data["header"]["format"])
        df["datetime"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d") + pd.to_timedelta(df["ms_of_day"], unit="ms")
        df = df.set_index("datetime")
        df.index = df.index.tz_localize("America/New_York")
        df = df[["open", "high", "low", "close", "volume"]]
        return df
    return None


def get_polygon_bars(symbol, date):
    """Get minute bars from Polygon API directly."""
    client = RESTClient(POLYGON_API_KEY)
    aggs = client.get_aggs(ticker=symbol, multiplier=1, timespan="minute", from_=date, to=date, limit=50000)

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


def test_timing_offset(symbol, date, date_str):
    """Test different timing offsets to find best match."""
    print(f"\n{'='*120}")
    print(f"Testing Timing Offsets for {symbol}")
    print(f"{'='*120}")

    # Get data
    theta_df = get_thetadata_bars(symbol, date_str)
    polygon_df = get_polygon_bars(symbol, date)

    if theta_df is None or polygon_df is None:
        print(f"Missing data for {symbol}")
        return

    polygon_df.index = polygon_df.index.tz_convert("America/New_York")

    # Show first 10 bars from each to examine timestamps
    print(f"\nFirst 10 ThetaData bars:")
    print(theta_df.head(10)[["open", "close", "volume"]])

    print(f"\nFirst 10 Polygon bars:")
    print(polygon_df.head(10)[["open", "close", "volume"]])

    # Test offsets from -3 minutes to +3 minutes
    print(f"\n{'='*120}")
    print(f"Testing Different Time Offsets")
    print(f"{'='*120}")

    best_offset = None
    best_score = float('inf')

    for offset_minutes in range(-3, 4):
        # Shift ThetaData bars by offset
        theta_shifted = theta_df.copy()
        theta_shifted.index = theta_shifted.index + pd.Timedelta(minutes=offset_minutes)

        # Find matching times around market open (9:30-9:35)
        matches = []
        for hour in [9, 10]:
            for minute in range(0 if hour == 10 else 30, 60 if hour == 9 else 5):
                time_str = f"{hour:02d}:{minute:02d}"

                theta_matches = theta_shifted[(theta_shifted.index.hour == hour) & (theta_shifted.index.minute == minute)]
                polygon_matches = polygon_df[(polygon_df.index.hour == hour) & (polygon_df.index.minute == minute)]

                if not theta_matches.empty and not polygon_matches.empty:
                    theta_bar = theta_matches.iloc[0]
                    polygon_bar = polygon_matches.iloc[0]

                    open_diff = abs(theta_bar["open"] - polygon_bar["open"])
                    close_diff = abs(theta_bar["close"] - polygon_bar["close"])
                    volume_diff = abs(theta_bar["volume"] - polygon_bar["volume"])
                    volume_pct_diff = abs(volume_diff / polygon_bar["volume"] * 100) if polygon_bar["volume"] > 0 else 0

                    matches.append({
                        "time": time_str,
                        "open_diff": open_diff,
                        "close_diff": close_diff,
                        "volume_pct_diff": volume_pct_diff,
                    })

        if matches:
            avg_open_diff = sum(m["open_diff"] for m in matches) / len(matches)
            avg_close_diff = sum(m["close_diff"] for m in matches) / len(matches)
            avg_volume_pct_diff = sum(m["volume_pct_diff"] for m in matches) / len(matches)

            # Score based on price and volume differences
            score = avg_open_diff + avg_close_diff + (avg_volume_pct_diff / 100)

            print(f"\nOffset: {offset_minutes:+2d} minutes | Avg Open Diff: ${avg_open_diff:.3f} | Avg Close Diff: ${avg_close_diff:.3f} | Avg Vol Diff: {avg_volume_pct_diff:.1f}% | Score: {score:.3f}")

            if score < best_score:
                best_score = score
                best_offset = offset_minutes

    print(f"\n{'='*120}")
    print(f"BEST OFFSET: {best_offset:+d} minutes (Score: {best_score:.3f})")
    print(f"{'='*120}")

    # Show detailed comparison at best offset
    if best_offset is not None:
        theta_shifted = theta_df.copy()
        theta_shifted.index = theta_shifted.index + pd.Timedelta(minutes=best_offset)

        print(f"\nDetailed Comparison at Market Open with {best_offset:+d} minute offset:")
        print("-" * 120)

        for minute in range(30, 35):
            theta_matches = theta_shifted[(theta_shifted.index.hour == 9) & (theta_shifted.index.minute == minute)]
            polygon_matches = polygon_df[(polygon_df.index.hour == 9) & (polygon_df.index.minute == minute)]

            if not theta_matches.empty and not polygon_matches.empty:
                theta_bar = theta_matches.iloc[0]
                polygon_bar = polygon_matches.iloc[0]

                open_diff = theta_bar["open"] - polygon_bar["open"]
                close_diff = theta_bar["close"] - polygon_bar["close"]
                volume_diff = theta_bar["volume"] - polygon_bar["volume"]
                volume_pct = (volume_diff / polygon_bar["volume"] * 100) if polygon_bar["volume"] > 0 else 0

                print(f"\n  9:{minute:02d} AM:")
                print(f"    ThetaData : O=${theta_bar['open']:.3f} C=${theta_bar['close']:.3f} V={theta_bar['volume']:,.0f}")
                print(f"    Polygon   : O=${polygon_bar['open']:.3f} C=${polygon_bar['close']:.3f} V={polygon_bar['volume']:,.0f}")
                print(f"    Difference: O=${open_diff:+.3f} C=${close_diff:+.3f} V={volume_diff:+,.0f} ({volume_pct:+.1f}%)")

    return best_offset


def main():
    date = datetime.date(2024, 8, 1)
    date_str = "20240801"

    symbols = ["AMZN", "AAPL", "SPY"]

    offsets = {}
    for symbol in symbols:
        offset = test_timing_offset(symbol, date, date_str)
        if offset is not None:
            offsets[symbol] = offset

    print(f"\n{'='*120}")
    print(f"SUMMARY")
    print(f"{'='*120}")
    for symbol, offset in offsets.items():
        print(f"{symbol}: {offset:+d} minutes")

    if offsets:
        all_same = len(set(offsets.values())) == 1
        if all_same:
            print(f"\n✓ All symbols have the same offset: {list(offsets.values())[0]:+d} minutes")
            print(f"  This indicates a systematic timing issue in ThetaData!")
        else:
            print(f"\n⚠ Different offsets detected - issue may be more complex")


if __name__ == "__main__":
    main()
