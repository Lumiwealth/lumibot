#!/usr/bin/env python3
"""
Re-download GC data with corrected roll logic and plot it.
"""

import os
import subprocess
from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from lumibot.entities import Asset
from lumibot.tools import databento_helper_polars as databento_helper


def convert_to_buildalpha_format(df, output_path):
    """Convert DataBento data to Build Alpha CSV format."""
    print("Converting to Build Alpha format...")

    # Check if it's Polars or Pandas
    if hasattr(df, "to_pandas"):
        # It's a Polars DataFrame
        pandas_df = df.to_pandas()
    else:
        # It's already Pandas
        pandas_df = df.copy()

    # Ensure datetime is the index
    if "datetime" in pandas_df.columns:
        pandas_df.set_index("datetime", inplace=True)

    # Create Build Alpha format DataFrame
    ba_df = pd.DataFrame()

    # Split datetime into Date and Time
    ba_df["Date"] = pandas_df.index.strftime("%m/%d/%Y")
    ba_df["Time"] = pandas_df.index.strftime("%H:%M:%S")

    # Add OHLCV columns
    ba_df["Open"] = pandas_df["open"]
    ba_df["High"] = pandas_df["high"]
    ba_df["Low"] = pandas_df["low"]
    ba_df["Close"] = pandas_df["close"]
    ba_df["Vol"] = pandas_df.get("volume", 1)
    ba_df["OI"] = 1  # Placeholder for open interest

    # Save to CSV
    ba_df.to_csv(output_path, index=False)
    print(f"  ✓ Saved to: {output_path}")
    return ba_df


def calculate_missing_minutes_from_df(ba_df):
    """Calculate missing minutes from Build Alpha format DataFrame."""
    # Recreate datetime column
    datetime_str = ba_df["Date"] + " " + ba_df["Time"]
    ba_df["datetime"] = pd.to_datetime(datetime_str, format="%m/%d/%Y %H:%M:%S")

    # Extract date
    ba_df["date"] = ba_df["datetime"].dt.date

    # Count bars per calendar day
    daily_counts = ba_df.groupby("date").size().reset_index(name="actual_bars")

    # Calculate missing minutes
    daily_counts["missing_minutes"] = 1440 - daily_counts["actual_bars"]

    # Convert date back to datetime for plotting
    daily_counts["date"] = pd.to_datetime(daily_counts["date"])

    return daily_counts


def main():
    """Re-download and plot GC data with corrected roll logic."""

    print("\n" + "=" * 70)
    print(" RE-DOWNLOADING GC DATA WITH CORRECTED ROLL LOGIC")
    print("=" * 70)
    print("\nUsing correct liquid contract months: Feb/Apr/Jun/Aug/Dec (2,4,6,8,12)")

    # Check API key
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        print("❌ DATABENTO_API_KEY not found in environment")
        print("   Please set your DataBento API key to re-download data")
        return

    # Define asset and date range
    gc_asset = Asset(symbol="GC", asset_type="cont_future")
    start_datetime = datetime(2025, 1, 1)
    end_datetime = datetime(2025, 10, 31)

    print("\n📥 Downloading from DataBento...")
    print(f"   Date range: {start_datetime.date()} to {end_datetime.date()}")

    # Get data from DataBento (will use corrected roll logic)
    df = databento_helper.get_price_data_from_databento(
        api_key=api_key,
        asset=gc_asset,
        start=start_datetime,
        end=end_datetime,
        timestep="minute",
        venue=None,
        force_cache_update=True,  # Force fresh download with new roll logic
    )

    if df is None:
        print("❌ Failed to download data from DataBento")
        return

    print(f"  ✓ Downloaded {df.height if hasattr(df, 'height') else len(df)} bars")

    # Convert to Build Alpha format
    ba_csv_path = "databento_exports/GC/20250101_20251031/1m/GC_20250101_20251031_1m_EST_NEW.csv"
    Path(ba_csv_path).parent.mkdir(parents=True, exist_ok=True)
    ba_df = convert_to_buildalpha_format(df, ba_csv_path)

    # Calculate missing minutes
    daily_missing = calculate_missing_minutes_from_df(ba_df)

    print("\n📊 Creating visualization...")

    # Create visualization
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    fig.suptitle(
        "GC Futures Data Analysis - NEW (Correct Liquid Contract Roll Logic)\nFeb/Apr/Jun/Aug/Dec",
        fontsize=14,
        fontweight="bold",
    )

    # Subplot 1: Close Prices
    datetime_str = ba_df["Date"] + " " + ba_df["Time"]
    ba_df["datetime"] = pd.to_datetime(datetime_str, format="%m/%d/%Y %H:%M:%S")
    ba_df.set_index("datetime", inplace=True)

    plot_df = ba_df
    max_plot_points = 50000
    if len(ba_df) > max_plot_points:
        step = len(ba_df) // max_plot_points + 1
        plot_df = ba_df.iloc[::step]

    ax1.plot(plot_df.index, plot_df["Close"], linewidth=0.8, color="green", alpha=0.8)
    ax1.set_title("GC Futures Close Prices", fontsize=12, fontweight="bold")
    ax1.set_xlabel("Date", fontsize=10)
    ax1.set_ylabel("Close Price", fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # Subplot 2: Missing Minutes per Day
    ax2.bar(
        daily_missing["date"],
        daily_missing["missing_minutes"],
        width=0.8,
        color="darkgreen",
        alpha=0.6,
        label="Missing minutes",
    )

    # Reference lines
    ax2.axhline(
        y=61, color="grey", linestyle="--", linewidth=2, alpha=0.3, label="Maintenance window (60 min)", zorder=1
    )
    ax2.axhline(
        y=420, color="blue", linestyle="--", linewidth=2, alpha=0.5, label="Friday expected (420 min)", zorder=1
    )
    ax2.axhline(
        y=1080, color="orange", linestyle="--", linewidth=2, alpha=0.5, label="Sunday expected (1080 min)", zorder=1
    )

    ax2.set_title("Missing Minutes per Calendar Day (out of 1440 total)", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Date", fontsize=10)
    ax2.set_ylabel("Missing Minutes", fontsize=10)
    ax2.grid(True, alpha=0.3, axis="y")
    ax2.legend(loc="upper right", fontsize=9)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax2.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha="right")
    ax2.set_ylim(0, max(1500, daily_missing["missing_minutes"].max() + 100))

    plt.tight_layout()

    # Save plot
    output_path = Path("GC_analysis_new.png")
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"  ✓ Plot saved to: {output_path}")
    plt.close()

    # Calculate statistics
    total_days = len(daily_missing)
    good_days = len(daily_missing[daily_missing["missing_minutes"] <= 500])
    percentage_good = (good_days / total_days * 100) if total_days > 0 else 0

    print("\n📈 NEW DATA STATISTICS (Liquid: Feb/Apr/Jun/Aug/Dec):")
    print(f"   Total days analyzed: {total_days}")
    print(f"   Good days (≤500 missing mins): {good_days}")
    print(f"   Bad days (>500 missing mins): {total_days - good_days}")
    print(f"   Percentage good: {percentage_good:.1f}%")
    print(f"   Percentage bad: {100 - percentage_good:.1f}%")
    print(f"   Average missing minutes: {daily_missing['missing_minutes'].mean():.1f}")

    # Compare with old data
    print("\n🎯 IMPROVEMENT SUMMARY:")
    print("   Old data (Quarterly): 32.7% good")
    print(f"   New data (Liquid):    {percentage_good:.1f}% good")
    print(f"   Improvement:          {percentage_good - 32.7:+.1f} percentage points!")

    # Open the image
    print("\n🖼️ Opening the plot...")
    subprocess.run(["open", str(output_path)])

    print("\n✅ Analysis complete!")
    print("   Both plots are now available:")
    print("   - GC_analysis_old.png: Wrong quarterly roll logic (32.7% good)")
    print(f"   - GC_analysis_new.png: Correct liquid roll logic ({percentage_good:.1f}% good)")


if __name__ == "__main__":
    main()
