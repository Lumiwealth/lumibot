#!/usr/bin/env python3
"""
Compare GC Data Quality: Before and After Roll Logic Fix
"""

import subprocess
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

# Import the analysis functions from analyze_missing_minutes.py
from analyze_missing_minutes import calculate_missing_minutes, load_buildalpha_csv


def plot_gc_data(csv_path, output_name):
    """Plot GC data and save with specified output name."""
    print(f"\n{'='*60}")
    print(f"Processing: {output_name}")
    print("=" * 60)

    # Load data
    df = load_buildalpha_csv(csv_path)

    # Calculate missing minutes
    daily_missing = calculate_missing_minutes(df)

    # Create visualization
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    fig.suptitle(f"GC Futures Data Analysis - {output_name}\n{Path(csv_path).name}", fontsize=14, fontweight="bold")

    # Subplot 1: Close Prices
    plot_df = df
    max_plot_points = 50000
    if len(df) > max_plot_points:
        step = len(df) // max_plot_points + 1
        plot_df = df.iloc[::step]

    ax1.plot(plot_df.index, plot_df["Close"], linewidth=0.8, color="steelblue", alpha=0.8)
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
        color="darkred",
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
    output_path = Path(f"GC_analysis_{output_name}.png")
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"  ✓ Plot saved to: {output_path}")
    plt.close()

    # Return statistics
    total_days = len(daily_missing)
    good_days = len(daily_missing[daily_missing["missing_minutes"] <= 500])
    percentage_good = (good_days / total_days * 100) if total_days > 0 else 0

    return {
        "total_days": total_days,
        "good_days": good_days,
        "percentage_good": percentage_good,
        "avg_missing": daily_missing["missing_minutes"].mean(),
        "output_path": output_path,
    }


def main():
    """Main comparison workflow."""

    print("\n" + "=" * 70)
    print(" GC DATA QUALITY COMPARISON: BEFORE AND AFTER ROLL LOGIC FIX")
    print("=" * 70)

    # Step 1: Analyze OLD data (with wrong roll logic)
    old_csv = "databento_exports/GC/20250101_20251031/1m/GC_20250101_20251031_1m_EST.csv"

    print("\n📊 ANALYZING OLD DATA (Wrong Roll Logic: Quarterly)")
    old_stats = plot_gc_data(old_csv, "old")

    # Step 2: Re-download NEW data (with correct roll logic)
    print("\n📥 RE-DOWNLOADING GC DATA WITH CORRECT ROLL LOGIC...")
    print("   (Using Feb/Apr/Jun/Aug/Dec contract cycle)")

    # Run the download script
    download_script = """
import sys
sys.path.insert(0, '/Users/marvin/repos/lumibot')

from download_databento_data import download_databento_data
from datetime import datetime

# Download GC data with corrected roll logic
download_databento_data(
    symbols=['GC'],
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 10, 31),
    data_type='ohlcv-1m',
    stype_in='continuous',
    symbols_universe='fut.FUT',
    output_format='buildalpha'
)
"""

    # Save and run the download script
    with open("tmp_download_gc.py", "w") as f:
        f.write(download_script)

    result = subprocess.run(["python", "tmp_download_gc.py"], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error downloading: {result.stderr}")
    else:
        print("  ✓ Download completed")

    # Clean up temp script
    Path("tmp_download_gc.py").unlink()

    # Step 3: Find the newly downloaded file
    # It should be in the same location with a new timestamp
    new_csv = "databento_exports/GC/20250101_20251031/1m/GC_20250101_20251031_1m_EST.csv"

    # Step 4: Analyze NEW data
    print("\n📊 ANALYZING NEW DATA (Correct Roll Logic: Feb/Apr/Jun/Aug/Dec)")
    new_stats = plot_gc_data(new_csv, "new")

    # Step 5: Display comparison
    print("\n" + "=" * 70)
    print(" COMPARISON RESULTS")
    print("=" * 70)

    print("\n📈 OLD DATA (Quarterly: Mar/Jun/Sep/Dec):")
    print(f"   Total days analyzed: {old_stats['total_days']}")
    print(f"   Good days (≤500 missing mins): {old_stats['good_days']}")
    print(f"   Percentage good: {old_stats['percentage_good']:.1f}%")
    print(f"   Average missing minutes: {old_stats['avg_missing']:.1f}")

    print("\n📈 NEW DATA (Liquid: Feb/Apr/Jun/Aug/Dec):")
    print(f"   Total days analyzed: {new_stats['total_days']}")
    print(f"   Good days (≤500 missing mins): {new_stats['good_days']}")
    print(f"   Percentage good: {new_stats['percentage_good']:.1f}%")
    print(f"   Average missing minutes: {new_stats['avg_missing']:.1f}")

    print("\n🎯 IMPROVEMENT:")
    improvement = new_stats["percentage_good"] - old_stats["percentage_good"]
    print(f"   Data quality improvement: {improvement:+.1f} percentage points")
    print(f"   From {old_stats['percentage_good']:.1f}% → {new_stats['percentage_good']:.1f}% good data")

    # Step 6: Open both images
    print("\n🖼️ Opening comparison images...")
    subprocess.run(["open", str(old_stats["output_path"])])
    subprocess.run(["open", str(new_stats["output_path"])])

    print("\n✅ Analysis complete! Both images are now open for comparison.")
    print("   - GC_analysis_old.png: Data with wrong quarterly roll logic")
    print("   - GC_analysis_new.png: Data with correct liquid contract roll logic")


if __name__ == "__main__":
    main()
