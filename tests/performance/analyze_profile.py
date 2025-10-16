"""Analyze yappi profile to find bottlenecks."""

import pstats
import sys
from pathlib import Path

def analyze_profile(profile_path: str, top_n: int = 30):
    """Print top N functions by cumulative time."""
    print(f"\n{'='*80}")
    print(f"Profile Analysis: {Path(profile_path).name}")
    print(f"{'='*80}\n")

    stats = pstats.Stats(profile_path)
    stats.strip_dirs()
    stats.sort_stats('cumulative')

    print("Top functions by CUMULATIVE time:")
    print("-" * 80)
    stats.print_stats(top_n)

    print(f"\n{'='*80}")
    print("Top functions by TOTAL (self) time:")
    print("-" * 80)
    stats.sort_stats('tottime')
    stats.print_stats(top_n)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_profile.py <profile_file>")
        sys.exit(1)

    analyze_profile(sys.argv[1])
