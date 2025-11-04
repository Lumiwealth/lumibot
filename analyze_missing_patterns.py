#!/usr/bin/env python3
"""
Detailed analysis of missing minutes patterns in GC futures data.
Identifies weekday patterns, holidays, and data quality issues.
"""

import sys
from pathlib import Path

import pandas as pd


def analyze_patterns(filepath: str):
    """Analyze missing minutes patterns in detail."""

    print("=" * 70)
    print("GC Futures - Detailed Missing Minutes Pattern Analysis")
    print("=" * 70)

    # Load CSV
    df = pd.read_csv(filepath)
    df["datetime"] = pd.to_datetime(df["Date"] + " " + df["Time"], format="%m/%d/%Y %H:%M:%S")
    df["date"] = df["datetime"].dt.date
    df["weekday"] = df["datetime"].dt.day_name()

    # Calculate missing minutes per day
    daily_counts = (
        df.groupby("date")
        .agg(
            {
                "datetime": "count",
            }
        )
        .reset_index()
    )
    daily_counts.columns = ["date", "actual_bars"]
    daily_counts["missing_minutes"] = 1440 - daily_counts["actual_bars"]

    # Add weekday information
    daily_counts["date"] = pd.to_datetime(daily_counts["date"])
    daily_counts["weekday"] = daily_counts["date"].dt.day_name()
    daily_counts["day_of_week"] = daily_counts["date"].dt.dayofweek  # 0=Monday, 6=Sunday

    print("\n📊 Dataset Overview:")
    date_min = daily_counts["date"].min().strftime("%Y-%m-%d")
    date_max = daily_counts["date"].max().strftime("%Y-%m-%d")
    print(f"   Date range: {date_min} to {date_max}")
    print(f"   Total calendar days: {len(daily_counts)}")
    print(f"   Total 1-minute bars: {daily_counts['actual_bars'].sum():,}")

    # Analyze by weekday
    print("\n📅 Average Missing Minutes by Weekday:")
    weekday_stats = daily_counts.groupby("weekday")["missing_minutes"].agg(["mean", "min", "max", "count"])
    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weekday_stats = weekday_stats.reindex(weekday_order)

    for day in weekday_order:
        if day in weekday_stats.index:
            stats = weekday_stats.loc[day]
            print(
                f"   {day:9s}: avg={stats['mean']:7.1f} min  "
                f"range=[{stats['min']:4.0f}-{stats['max']:4.0f}]  "
                f"count={stats['count']:.0f} days"
            )

    # Identify anomalies (days that don't match typical pattern)
    print("\n🔍 Anomalous Days (unusual missing minute counts):")

    # Define expected ranges for each weekday
    expected_ranges = {
        "Monday": (50, 70),  # Normal day + maintenance
        "Tuesday": (50, 70),  # Normal day + maintenance
        "Wednesday": (50, 70),  # Normal day + maintenance
        "Thursday": (50, 70),  # Normal day + maintenance
        "Friday": (400, 440),  # Closes at 5 PM
        "Saturday": (1430, 1441),  # No trading
        "Sunday": (1070, 1090),  # Opens at 6 PM
    }

    anomalies = []
    for _, row in daily_counts.iterrows():
        expected_min, expected_max = expected_ranges[row["weekday"]]
        if not (expected_min <= row["missing_minutes"] <= expected_max):
            anomalies.append(row)

    if anomalies:
        print(f"   Found {len(anomalies)} anomalous days:")
        for anom in anomalies[:20]:  # Show first 20
            print(
                f"   {anom['date'].strftime('%Y-%m-%d')} ({anom['weekday']:9s}): "
                f"{anom['missing_minutes']:4.0f} missing mins "
                f"[expected {expected_ranges[anom['weekday']][0]}-{expected_ranges[anom['weekday']][1]}] "
                f"({anom['actual_bars']:4.0f} bars)"
            )
    else:
        print("   ✓ No anomalies detected - all days match expected patterns")

    # Data quality check
    print("\n✅ Data Quality Assessment:")

    # Check for gaps in expected trading days
    weekday_bars = daily_counts[daily_counts["day_of_week"] < 5]  # Mon-Fri
    very_low_bars = weekday_bars[weekday_bars["actual_bars"] < 500]

    if len(very_low_bars) > 0:
        print(f"   ⚠️  {len(very_low_bars)} weekdays with suspiciously few bars (<500):")
        for _, row in very_low_bars.iterrows():
            print(f"      {row['date'].strftime('%Y-%m-%d')} ({row['weekday']}): {row['actual_bars']} bars")
    else:
        print("   ✓ All weekdays have reasonable bar counts")

    # Weekend trading check
    saturday_with_trading = daily_counts[(daily_counts["weekday"] == "Saturday") & (daily_counts["actual_bars"] > 10)]
    if len(saturday_with_trading) > 0:
        print(f"   ℹ️  {len(saturday_with_trading)} Saturdays with unexpected trading:")
        for _, row in saturday_with_trading.head(5).iterrows():
            print(f"      {row['date'].strftime('%Y-%m-%d')}: {row['actual_bars']} bars")

    # Calculate weekly pattern consistency
    print("\n📈 Trading Week Pattern:")
    print("   Expected normal week:")
    print("      Mon-Thu: ~1380 bars each (23 hours trading)")
    print("      Friday:  ~1020 bars (closes at 5 PM)")
    print("      Saturday: ~0 bars (no trading)")
    print("      Sunday:  ~360 bars (opens at 6 PM)")

    actual_avg = daily_counts.groupby("weekday")["actual_bars"].mean().reindex(weekday_order)
    print("\n   Actual average bars per weekday:")
    for day in weekday_order:
        if day in actual_avg.index:
            print(f"      {day:9s}: {actual_avg[day]:7.1f} bars")

    # Price movement analysis
    print("\n💰 Price Movement Summary:")
    df_sorted = df.sort_values("datetime")
    first_price = df_sorted.iloc[0]["Close"]
    last_price = df_sorted.iloc[-1]["Close"]
    price_change = last_price - first_price
    price_change_pct = (price_change / first_price) * 100

    print(f"   First close: ${first_price:,.2f} on {df_sorted.iloc[0]['datetime'].strftime('%Y-%m-%d %H:%M')}")
    print(f"   Last close:  ${last_price:,.2f} on {df_sorted.iloc[-1]['datetime'].strftime('%Y-%m-%d %H:%M')}")
    print(f"   Change:      ${price_change:+,.2f} ({price_change_pct:+.2f}%)")
    print(f"   High:        ${df['Close'].max():,.2f}")
    print(f"   Low:         ${df['Close'].min():,.2f}")

    print("\n" + "=" * 70)
    print("✅ Analysis complete!")
    print("=" * 70)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python analyze_missing_patterns.py <csv_file_path>")
        sys.exit(1)

    filepath = sys.argv[1]
    if not Path(filepath).exists():
        print(f"ERROR: File not found: {filepath}")
        sys.exit(1)

    analyze_patterns(filepath)
