#!/usr/bin/env python3
"""
Test Data Generator for Live Equity Curve GUI

This script generates synthetic equity curve data and writes it to a JSONL file
every 250ms to simulate a live backtest. Use this to test the GUI before
integrating with real backtesting code.

Usage:
    python3 test_live_equity_curve_generator.py

Press Ctrl+C to stop.
"""

import json
import time
import random
from datetime import datetime, timedelta
from pathlib import Path


class EquityCurveGenerator:
    """Generates synthetic equity curve data for testing"""

    def __init__(self,
                 starting_value=100000.0,
                 volatility=0.002,
                 trend=0.0001,
                 update_interval_ms=250,
                 duration_seconds=120,
                 total_iterations=1000):
        """
        Initialize the equity curve generator

        Parameters
        ----------
        starting_value : float
            Starting portfolio value in dollars
        volatility : float
            Standard deviation of random returns per step
        trend : float
            Mean return per step (positive = upward trend)
        update_interval_ms : int
            Milliseconds between updates (default 250ms)
        duration_seconds : int
            How long to run before stopping (default 120s = 2 minutes)
        total_iterations : int
            Total number of "backtest iterations" to simulate
        """
        self.portfolio_value = starting_value
        self.volatility = volatility
        self.trend = trend
        self.update_interval = update_interval_ms / 1000.0  # Convert to seconds
        self.duration = duration_seconds
        self.total_iterations = total_iterations

        # Calculate how many writes we'll do
        self.total_writes = int(duration_seconds / self.update_interval)
        self.iteration_step = total_iterations / self.total_writes

        self.current_iteration = 0
        self.writes = 0

        # Start time for calculating timestamps
        self.start_time = datetime(2025, 1, 1, 9, 30, 0)

        # Output file
        self.output_file = Path("logs/backtest_live_equity.jsonl")

    def generate_next_value(self):
        """Generate next portfolio value using random walk with trend"""
        # Random return with trend
        return_pct = random.gauss(self.trend, self.volatility)
        self.portfolio_value *= (1 + return_pct)

        # Keep it positive
        self.portfolio_value = max(self.portfolio_value, 1000.0)

        return self.portfolio_value

    def get_timestamp(self):
        """Get current simulated timestamp"""
        # Simulate 5-minute intervals in backtest time
        minutes_elapsed = int(self.current_iteration * 5)
        return self.start_time + timedelta(minutes=minutes_elapsed)

    def write_data_point(self):
        """Write one data point to JSONL file"""
        # Generate next value
        value = self.generate_next_value()

        # Calculate progress
        percent = (self.writes / self.total_writes) * 100

        # Get timestamp
        timestamp = self.get_timestamp()

        # Create data point
        data = {
            "timestamp": timestamp.isoformat(),
            "portfolio_value": round(value, 2),
            "percent": round(percent, 2)
        }

        # Write to file
        with open(self.output_file, "a") as f:
            f.write(json.dumps(data) + "\n")

        # Update counters
        self.current_iteration += self.iteration_step
        self.writes += 1

        return value, percent

    def run(self):
        """Run the generator"""
        print("=" * 80)
        print("Live Equity Curve Test Data Generator")
        print("=" * 80)
        print(f"Starting portfolio value: ${self.portfolio_value:,.2f}")
        print(f"Update interval: {self.update_interval * 1000:.0f}ms")
        print(f"Duration: {self.duration}s")
        print(f"Total updates: {self.total_writes}")
        print(f"Output file: {self.output_file}")
        print()
        print("Press Ctrl+C to stop early")
        print("=" * 80)
        print()

        # Create logs directory
        self.output_file.parent.mkdir(exist_ok=True)

        # Clear existing file
        if self.output_file.exists():
            self.output_file.unlink()
            print(f"Cleared existing file: {self.output_file}")

        print("Starting data generation...")
        print()

        start_real_time = time.time()

        try:
            while self.writes < self.total_writes:
                # Write data point
                value, percent = self.write_data_point()

                # Print status every 10 writes
                if self.writes % 10 == 0:
                    elapsed = time.time() - start_real_time
                    print(f"[{self.writes:4d}/{self.total_writes}] "
                          f"{percent:5.1f}% - "
                          f"Portfolio: ${value:,.2f} - "
                          f"Elapsed: {elapsed:.1f}s")

                # Sleep until next update
                time.sleep(self.update_interval)

        except KeyboardInterrupt:
            print()
            print("Interrupted by user")

        print()
        print("=" * 80)
        print(f"Generation complete!")
        print(f"Total writes: {self.writes}")
        print(f"Final portfolio value: ${self.portfolio_value:,.2f}")
        print(f"File: {self.output_file}")
        print("=" * 80)
        print()
        print("You can now launch the GUI:")
        print("  python3 show_live_backtest_progress.py")
        print()


def main():
    """Main entry point"""
    # Create generator with test parameters
    generator = EquityCurveGenerator(
        starting_value=100000.0,
        volatility=0.002,      # 0.2% volatility per step
        trend=0.0001,          # 0.01% upward trend per step
        update_interval_ms=250,  # 250ms updates
        duration_seconds=120,    # Run for 2 minutes
        total_iterations=1000    # Simulate 1000 backtest iterations
    )

    # Run the generator
    generator.run()


if __name__ == "__main__":
    main()
