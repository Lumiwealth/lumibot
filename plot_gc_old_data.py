#!/usr/bin/env python3
"""
Plot the existing GC data (with wrong roll logic) to establish baseline.
"""

import subprocess
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

# Import the analysis functions
from analyze_missing_minutes import calculate_missing_minutes, load_buildalpha_csv


def main():
    """Plot existing GC data."""

    print("\n" + "=" * 70)
    print(" ANALYZING EXISTING GC DATA (Wrong Roll Logic: Quarterly)")
    print("=" * 70)

    # Path to existing data
    csv_path = "databento_exports/GC/20250101_20251031/1m/GC_20250101_20251031_1m_EST.csv"

    # Load data
    print(f"\nLoading data from: {csv_path}")
    df = load_buildalpha_csv(csv_path)

    # Calculate missing minutes
    daily_missing = calculate_missing_minutes(df)

    # Create visualization
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    fig.suptitle(
        f"GC Futures Data Analysis - OLD (Wrong Quarterly Roll Logic)\n{Path(csv_path).name}",
        fontsize=14,
        fontweight="bold",
    )

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
    output_path = Path("GC_analysis_old.png")
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"\n✓ Plot saved to: {output_path}")
    plt.close()

    # Calculate statistics
    total_days = len(daily_missing)
    good_days = len(daily_missing[daily_missing["missing_minutes"] <= 500])
    percentage_good = (good_days / total_days * 100) if total_days > 0 else 0

    print("\n📈 OLD DATA STATISTICS (Quarterly: Mar/Jun/Sep/Dec):")
    print(f"   Total days analyzed: {total_days}")
    print(f"   Good days (≤500 missing mins): {good_days}")
    print(f"   Bad days (>500 missing mins): {total_days - good_days}")
    print(f"   Percentage good: {percentage_good:.1f}%")
    print(f"   Percentage bad: {100 - percentage_good:.1f}%")
    print(f"   Average missing minutes: {daily_missing['missing_minutes'].mean():.1f}")

    # Open the image
    print("\n🖼️ Opening the plot...")
    subprocess.run(["open", str(output_path)])

    print("\n✅ Analysis complete!")
    print("   This shows the data quality with the WRONG quarterly roll logic.")
    print("   61.4% of data was unusable due to incorrect contract selection.")


if __name__ == "__main__":
    main()
