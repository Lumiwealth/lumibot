#!/usr/bin/env python3
"""
Analyze GC futures contract rollover and data quality by week.
Identifies good weeks vs bad weeks and investigates contract issues.
"""

import sys
from pathlib import Path

import pandas as pd


def analyze_weekly_patterns(filepath: str):
    """Analyze data quality week by week to identify rollover issues."""

    print("=" * 80)
    print("GC Futures Contract Rollover Analysis")
    print("=" * 80)

    # Load CSV
    df = pd.read_csv(filepath)
    df["datetime"] = pd.to_datetime(df["Date"] + " " + df["Time"], format="%m/%d/%Y %H:%M:%S")
    df = df.sort_values("datetime")

    print("\n📊 Dataset Overview:")
    print(f"   Total rows: {len(df):,}")
    print(f"   Date range: {df['datetime'].min()} to {df['datetime'].max()}")

    # Group by week
    df["week_start"] = df["datetime"].dt.to_period("W").dt.start_time
    df["date"] = df["datetime"].dt.date

    # Calculate daily bars per week
    daily_bars = df.groupby(["week_start", "date"]).size().reset_index(name="bars")

    # Calculate weekly statistics
    weekly_stats = daily_bars.groupby("week_start").agg({"bars": ["mean", "std", "min", "max", "count"]}).reset_index()
    weekly_stats.columns = ["week_start", "avg_bars", "std_bars", "min_bars", "max_bars", "days"]

    # Calculate missing minutes for better classification
    weekly_stats["avg_missing"] = 1440 - weekly_stats["avg_bars"]

    print(f"\n   Total weeks analyzed: {len(weekly_stats)}")

    # Classify weeks as GOOD or BAD
    # Good weeks should have avg_bars > 800 for weekdays (accounting for weekends)
    # Bad weeks will have avg_bars much lower, indicating sparse data
    weekly_stats["quality"] = weekly_stats["avg_bars"].apply(lambda x: "GOOD" if x > 800 else "BAD")

    good_weeks = weekly_stats[weekly_stats["quality"] == "GOOD"]
    bad_weeks = weekly_stats[weekly_stats["quality"] == "BAD"]

    print("\n📈 Week Classification:")
    print(f"   GOOD weeks (avg > 800 bars/day): {len(good_weeks)} weeks")
    print(f"   BAD weeks (avg ≤ 800 bars/day):  {len(bad_weeks)} weeks")
    print(
        f"   Good/Total ratio: {len(good_weeks)}/{len(weekly_stats)} = " f"{len(good_weeks)/len(weekly_stats)*100:.1f}%"
    )

    # Show statistics for each category
    print("\n📊 Statistics by Quality Category:")
    print("\n   GOOD weeks:")
    if len(good_weeks) > 0:
        print(f"      Average bars/day: {good_weeks['avg_bars'].mean():.1f}")
        print(f"      Std dev: {good_weeks['avg_bars'].std():.1f}")
        print(f"      Range: [{good_weeks['avg_bars'].min():.0f}, {good_weeks['avg_bars'].max():.0f}]")

    print("\n   BAD weeks:")
    if len(bad_weeks) > 0:
        print(f"      Average bars/day: {bad_weeks['avg_bars'].mean():.1f}")
        print(f"      Std dev: {bad_weeks['avg_bars'].std():.1f}")
        print(f"      Range: [{bad_weeks['avg_bars'].min():.0f}, {bad_weeks['avg_bars'].max():.0f}]")

    # Identify continuous periods of good/bad weeks
    print("\n📅 Continuous Periods:")
    weekly_stats = weekly_stats.sort_values("week_start")

    periods = []
    current_quality = None
    period_start = None
    period_weeks = 0
    prev_week_start = None

    for _, week in weekly_stats.iterrows():
        if week["quality"] != current_quality:
            # Save previous period
            if current_quality is not None and prev_week_start is not None:
                periods.append(
                    {"quality": current_quality, "start": period_start, "end": prev_week_start, "weeks": period_weeks}
                )
            # Start new period
            current_quality = week["quality"]
            period_start = week["week_start"]
            period_weeks = 1
        else:
            period_weeks += 1

        prev_week_start = week["week_start"]

    # Add final period
    if current_quality is not None:
        periods.append(
            {"quality": current_quality, "start": period_start, "end": prev_week_start, "weeks": period_weeks}
        )

    print(f"\n   Found {len(periods)} distinct quality periods:\n")
    for i, period in enumerate(periods, 1):
        duration_days = (period["end"] - period["start"]).days + 7  # +7 for last week
        print(
            f"   Period {i}: {period['quality']:4s} | "
            f"{period['start'].strftime('%Y-%m-%d')} to {period['end'].strftime('%Y-%m-%d')} | "
            f"{period['weeks']:2d} weeks ({duration_days:3d} days)"
        )

    # Detailed week-by-week listing
    print("\n📋 Week-by-Week Detail:")
    print(f"   {'Week Starting':<12} {'Quality':<7} {'Avg Bars':<9} {'Min':<5} {'Max':<5} {'Days':<5}")
    print(f"   {'-'*12} {'-'*7} {'-'*9} {'-'*5} {'-'*5} {'-'*5}")

    for _, week in weekly_stats.iterrows():
        quality_mark = "✓" if week["quality"] == "GOOD" else "✗"
        print(
            f"   {week['week_start'].strftime('%Y-%m-%d'):<12} "
            f"{quality_mark} {week['quality']:<5} "
            f"{week['avg_bars']:8.1f}  "
            f"{week['min_bars']:4.0f}  "
            f"{week['max_bars']:4.0f}  "
            f"{week['days']:4.0f}"
        )

    # Export detailed CSV for further analysis
    output_path = Path(filepath).parent / "weekly_analysis.csv"
    weekly_stats.to_csv(output_path, index=False)
    print(f"\n💾 Detailed weekly data saved to: {output_path}")

    return weekly_stats, periods


def investigate_contracts(filepath: str):
    """Investigate which futures contracts might be involved."""

    print("\n" + "=" * 80)
    print("Contract Investigation")
    print("=" * 80)

    # Load just a sample to check structure
    df = pd.read_csv(filepath, nrows=100)

    print("\n📄 CSV Columns Available:")
    for col in df.columns:
        print(f"   - {col}")

    # Check if there's any contract identifier in the data
    # Build Alpha format typically doesn't include contract symbols
    # But let's check the source data structure

    print("\n⚠️  Build Alpha CSV Format Limitation:")
    print("   The exported CSV does not contain contract symbols (e.g., GCZ5, GCG6).")
    print("   This is because databento_to_buildalpha.py uses continuous futures")
    print("   and only exports OHLCV data without contract metadata.")

    print("\n🔍 To Identify Contracts, We Need To:")
    print("   1. Examine the raw DataBento response before conversion")
    print("   2. Check the continuous futures rollover schedule used")
    print("   3. Review the lumibot DataBento helper implementation")

    print("\n📌 Next Steps:")
    print("   - Examine lumibot/tools/databento_helper_polars.py")
    print("   - Check how continuous futures are constructed")
    print("   - Identify the rollover schedule DataBento uses")
    print("   - Compare expected vs actual contract dates")


def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python analyze_contract_rollover.py <csv_file_path>")
        print("")
        print("Example:")
        print(
            "  python analyze_contract_rollover.py "
            "databento_exports/GC/20250101_20251031/1m/GC_20250101_20251031_1m_EST.csv"
        )
        sys.exit(1)

    filepath = sys.argv[1]
    if not Path(filepath).exists():
        print(f"ERROR: File not found: {filepath}")
        sys.exit(1)

    try:
        # Run weekly analysis
        weekly_stats, periods = analyze_weekly_patterns(filepath)

        # Investigate contract structure
        investigate_contracts(filepath)

        print("\n" + "=" * 80)
        print("✅ Analysis Complete!")
        print("=" * 80)

        # Summary recommendations
        print("\n💡 Key Findings:")
        good_pct = (weekly_stats["quality"] == "GOOD").sum() / len(weekly_stats) * 100
        bad_pct = 100 - good_pct

        print("\n   1. Data Quality Split:")
        print(f"      - {good_pct:.1f}% of weeks have GOOD data (>800 bars/day avg)")
        print(f"      - {bad_pct:.1f}% of weeks have BAD data (≤800 bars/day avg)")

        if bad_pct > 50:
            print("\n   2. 🚨 CRITICAL ISSUE:")
            print("      Over half the data has severe quality issues.")
            print("      This strongly suggests contract rollover problems.")

        print("\n   3. 🔍 Investigation Needed:")
        print("      - Check DataBento continuous futures rollover schedule")
        print("      - Verify contract selection logic in databento_helper_polars.py")
        print("      - Compare against CME contract specs for GC futures")
        print("      - Test with explicit contract symbols instead of continuous")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
