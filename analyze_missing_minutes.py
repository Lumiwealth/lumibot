#!/usr/bin/env python3
"""
Analyze Missing Minutes in Futures Data

This script analyzes continuous futures data (e.g., from Build Alpha format)
and creates visualizations showing:
1. Close price time series
2. Missing minutes per calendar day with reference lines for expected gaps

Usage:
    python analyze_missing_minutes.py <csv_file_path>

Example:
    python analyze_missing_minutes.py databento_exports/GC/20250101_20251031/1m/GC_20250101_20251031_1m_EST.csv
"""

import sys
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


def load_buildalpha_csv(filepath: str) -> pd.DataFrame:
    """
    Load Build Alpha format CSV and parse datetime.

    Expected format:
        Date,Time,Open,High,Low,Close,Vol,OI
        01/02/2025,00:33:00,2652.0,2652.0,2652.0,2652.0,1,1

    Args:
        filepath: Path to CSV file

    Returns:
        DataFrame with datetime index and OHLCV columns

    Performance Notes:
        - Optimized for files up to 10 million rows (~500MB)
        - Uses vectorized operations for datetime parsing
        - Memory consumption scales linearly with row count
        - For extremely large datasets, consider chunked processing or Dask
    """
    print(f"Loading data from: {filepath}")

    # Define a custom date parser for efficiency
    def parse_datetime(df_subset):
        # More efficient: parse date and add time as timedelta
        date_part = pd.to_datetime(df_subset["Date"], format="%m/%d/%Y")
        time_part = pd.to_timedelta(df_subset["Time"])
        return date_part + time_part

    # Read CSV with row limit validation and optimized datetime parsing
    # Use nrows parameter for initial validation to prevent resource exhaustion
    max_rows = 10_000_000  # 10 million rows limit for safety

    try:
        # Read with error handling
        df = pd.read_csv(filepath, nrows=max_rows)
    except pd.errors.ParserError as e:
        raise ValueError(f"Failed to parse CSV file: {e}") from e
    except pd.errors.EmptyDataError as e:
        raise ValueError("CSV file is empty") from e

    # Verify expected columns
    expected_cols = ["Date", "Time", "Open", "High", "Low", "Close", "Vol", "OI"]
    if not all(col in df.columns for col in expected_cols):
        raise ValueError(f"CSV must have columns: {expected_cols}")

    # Validate data types for numeric columns
    numeric_cols = ["Open", "High", "Low", "Close", "Vol", "OI"]
    for col in numeric_cols:
        if not pd.api.types.is_numeric_dtype(df[col]):
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            except Exception as e:
                raise ValueError(f"Column '{col}' must contain numeric values: {e}") from e

    # Combine Date and Time columns into datetime (optimized)
    df["datetime"] = parse_datetime(df)

    # Set datetime as index
    df = df.set_index("datetime")

    # Sort by datetime (data should already be sorted from databento_to_buildalpha.py)
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()

    print(f"  ✓ Loaded {len(df):,} rows")
    print(f"  ✓ Date range: {df.index.min()} to {df.index.max()}")

    return df


def calculate_missing_minutes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate missing minutes per calendar day.

    Missing minutes = 1440 (total minutes in day) - actual bars present

    Args:
        df: DataFrame with datetime index

    Returns:
        DataFrame with columns: date, actual_bars, missing_minutes

    Performance Notes:
        - Uses efficient groupby aggregation
        - Memory overhead is minimal (creates one row per calendar day)
        - Scales well with large input datasets
    """
    print("\nCalculating missing minutes per day...")

    # Extract date from datetime index
    df["date"] = df.index.date

    # Count bars per calendar day
    daily_counts = df.groupby("date").size().reset_index(name="actual_bars")

    # Calculate missing minutes (out of 1440 per day)
    daily_counts["missing_minutes"] = 1440 - daily_counts["actual_bars"]

    # Convert date back to datetime for plotting
    daily_counts["date"] = pd.to_datetime(daily_counts["date"])

    print(f"  ✓ Analyzed {len(daily_counts)} calendar days")
    print(f"  ✓ Average missing minutes per day: {daily_counts['missing_minutes'].mean():.1f}")
    print(f"  ✓ Min missing: {daily_counts['missing_minutes'].min()} minutes")
    print(f"  ✓ Max missing: {daily_counts['missing_minutes'].max()} minutes")

    return daily_counts


def create_visualization(df: pd.DataFrame, daily_missing: pd.DataFrame, filepath: str):
    """
    Create 2-subplot visualization:
    1. Close prices over time
    2. Missing minutes per day with reference lines

    Args:
        df: DataFrame with price data
        daily_missing: DataFrame with missing minutes per day
        filepath: Original file path (for title)
    """
    print("\nCreating visualization...")

    # Create figure with 2 subplots (vertical stack)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    fig.suptitle(f"GC Futures Data Analysis\n{Path(filepath).name}", fontsize=14, fontweight="bold")

    # ========================================
    # Subplot 1: Close Prices
    # ========================================

    # Downsample if dataset is very large (>50k points) for better rendering performance
    plot_df = df
    max_plot_points = 50000
    if len(df) > max_plot_points:
        # Downsample by taking every nth point to stay under max_plot_points
        step = len(df) // max_plot_points + 1
        plot_df = df.iloc[::step]
        print(f"  ℹ Downsampled close prices for plotting: {len(df):,} → {len(plot_df):,} points")

    ax1.plot(plot_df.index, plot_df["Close"], linewidth=0.8, color="steelblue", alpha=0.8)
    ax1.set_title("GC Futures Close Prices", fontsize=12, fontweight="bold")
    ax1.set_xlabel("Date", fontsize=10)
    ax1.set_ylabel("Close Price", fontsize=10)
    ax1.grid(True, alpha=0.3)

    # Format x-axis dates
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # ========================================
    # Subplot 2: Missing Minutes per Day
    # ========================================

    # Bar chart of missing minutes
    ax2.bar(
        daily_missing["date"],
        daily_missing["missing_minutes"],
        width=0.8,
        color="darkred",
        alpha=0.6,
        label="Missing minutes",
    )

    # Reference lines
    # 1. Grey line at y=61 (daily maintenance window)
    ax2.axhline(
        y=61, color="grey", linestyle="--", linewidth=2, alpha=0.3, label="Maintenance window (60 min)", zorder=1
    )

    # 2. Blue line at y=420 (Friday expected)
    # Friday: 00:00-17:00 trading (1020 min), then closed
    # Missing: 1440 - 1020 = 420 minutes
    ax2.axhline(
        y=420, color="blue", linestyle="--", linewidth=2, alpha=0.5, label="Friday expected (420 min)", zorder=1
    )

    # 3. Orange line at y=1080 (Sunday expected)
    # Sunday: Opens at 18:00, so 00:00-18:00 closed (1080 min)
    # Missing: 1080 minutes
    ax2.axhline(
        y=1080, color="orange", linestyle="--", linewidth=2, alpha=0.5, label="Sunday expected (1080 min)", zorder=1
    )

    ax2.set_title("Missing Minutes per Calendar Day (out of 1440 total)", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Date", fontsize=10)
    ax2.set_ylabel("Missing Minutes", fontsize=10)
    ax2.grid(True, alpha=0.3, axis="y")
    ax2.legend(loc="upper right", fontsize=9)

    # Format x-axis dates
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax2.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # Set y-axis limits to show full range
    ax2.set_ylim(0, max(1500, daily_missing["missing_minutes"].max() + 100))

    # Tight layout
    plt.tight_layout()

    print("  ✓ Visualization created")

    # Save plot to file
    output_path = Path(filepath).parent / f"{Path(filepath).stem}_analysis.png"
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"  ✓ Plot saved to: {output_path}")

    print("\nDisplaying plot...")
    plt.show()


def main():
    """Main entry point."""
    # Parse command line arguments
    if len(sys.argv) != 2:
        print("Usage: python analyze_missing_minutes.py <csv_file_path>")
        print("")
        print("Example:")
        example_path = "databento_exports/GC/20250101_20251031/1m/GC_20250101_20251031_1m_EST.csv"
        print(f"  python analyze_missing_minutes.py {example_path}")
        sys.exit(1)

    filepath = sys.argv[1]

    # Validate file path
    file_path = Path(filepath)

    # Check file exists
    if not file_path.exists():
        print(f"ERROR: File not found: {filepath}")
        sys.exit(1)

    # Validate it's a CSV file
    if file_path.suffix.lower() != ".csv":
        print(f"ERROR: File must be a CSV file (got: {file_path.suffix})")
        sys.exit(1)

    # Check file is readable and enforce size limit to prevent resource exhaustion
    try:
        file_size = file_path.stat().st_size
        max_file_size = 500_000_000  # 500MB hard limit for safety

        if file_size > max_file_size:
            print(f"ERROR: File is too large ({file_size / 1_000_000:.1f} MB)")
            print(f"Maximum allowed size: {max_file_size / 1_000_000:.0f} MB")
            print("This limit prevents resource exhaustion and ensures reliable processing.")
            print("\nFor very large datasets, consider:")
            print("  - Processing a subset of the data (shorter date range)")
            print("  - Using aggregated data (hourly or daily instead of 1-minute)")
            sys.exit(1)
    except OSError as e:
        print(f"ERROR: Cannot access file: {e}")
        sys.exit(1)

    try:
        print("=" * 60)
        print("GC Futures Data Analysis - Missing Minutes")
        print("=" * 60)

        # Load data
        df = load_buildalpha_csv(filepath)

        # Calculate missing minutes
        daily_missing = calculate_missing_minutes(df)

        # Create visualization
        create_visualization(df, daily_missing, filepath)

        print("\n✅ Analysis complete!")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
