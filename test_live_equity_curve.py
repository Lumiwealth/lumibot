#!/usr/bin/env python3
"""
Test Runner for Live Equity Curve System

This script launches both the data generator and GUI viewer together
for testing the live equity curve functionality before integrating
with real backtesting code.

Usage:
    python3 test_live_equity_curve.py

This will:
1. Clear any existing equity data file
2. Launch the data generator in a subprocess
3. Launch the GUI viewer in a subprocess
4. Wait for user to press Ctrl+C
5. Clean up processes

Press Ctrl+C to stop all processes.
"""

import os
import sys
import time
import signal
import subprocess
from pathlib import Path


class TestRunner:
    """Test runner for live equity curve system"""

    def __init__(self):
        self.processes = []
        self.data_file = Path("logs/backtest_live_equity.jsonl")

    def cleanup(self, signum=None, frame=None):
        """Clean up all subprocesses"""
        print()
        print("=" * 80)
        print("Cleaning up processes...")
        print("=" * 80)

        for name, process in self.processes:
            if process.poll() is None:  # Still running
                print(f"Terminating {name}...")
                process.terminate()
                try:
                    process.wait(timeout=2)
                    print(f"  ✅ {name} stopped")
                except subprocess.TimeoutExpired:
                    print(f"  ⚠️  {name} didn't stop gracefully, killing...")
                    process.kill()
                    process.wait()
                    print(f"  ✅ {name} killed")
            else:
                print(f"  ✅ {name} already stopped")

        print()
        print("All processes cleaned up")
        print("=" * 80)
        sys.exit(0)

    def clear_data_file(self):
        """Clear existing equity data file"""
        if self.data_file.exists():
            self.data_file.unlink()
            print(f"✅ Cleared existing data file: {self.data_file}")
        else:
            print(f"ℹ️  No existing data file to clear")

        # Create logs directory
        self.data_file.parent.mkdir(exist_ok=True)

    def launch_generator(self):
        """Launch the data generator subprocess"""
        print()
        print("Starting data generator...")

        # Use python3 explicitly
        process = subprocess.Popen(
            [sys.executable, "test_live_equity_curve_generator.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        self.processes.append(("Data Generator", process))
        print("✅ Data generator started")
        return process

    def launch_gui(self):
        """Launch the GUI viewer subprocess"""
        print()
        print("Starting GUI viewer...")

        # Use python3 explicitly
        # Don't redirect output so we can see errors
        process = subprocess.Popen(
            [sys.executable, "show_live_backtest_progress.py"]
        )

        self.processes.append(("GUI Viewer", process))
        print("✅ GUI viewer started")
        return process

    def monitor_processes(self):
        """Monitor running processes and handle output"""
        print()
        print("=" * 80)
        print("Both processes running!")
        print("=" * 80)
        print()
        print("💡 TIP: Try these tests:")
        print("   1. Watch the equity curve update in real-time")
        print("   2. Close the GUI window - generator should keep running")
        print("   3. Re-run: python3 show_live_backtest_progress.py")
        print("   4. The GUI should pick up from current progress")
        print()
        print("Press Ctrl+C to stop all processes...")
        print("=" * 80)
        print()

        reported_stopped = set()  # Track which processes we've already reported

        try:
            while True:
                # Check if any process has stopped
                for name, process in self.processes:
                    # Skip if already reported
                    if name in reported_stopped:
                        continue

                    # Check if process stopped
                    returncode = process.poll()
                    if returncode is not None:
                        # Mark as reported
                        reported_stopped.add(name)

                        # Report completion
                        if returncode == 0:
                            print(f"✅ {name} completed successfully")
                        else:
                            print(f"⚠️  {name} exited with code {returncode}")

                        # If generator stopped, we're done
                        if "Generator" in name:
                            print()
                            print("Data generator finished!")
                            print()
                            print("💡 GUI is still running - you can keep it open to review the final curve")
                            print("   Or close it and press Ctrl+C to exit")
                            print()

                # Sleep a bit
                time.sleep(1)

        except KeyboardInterrupt:
            print()
            print("Interrupted by user")
            self.cleanup()

    def run(self):
        """Run the test"""
        print("=" * 80)
        print("Live Equity Curve Test Runner")
        print("=" * 80)
        print()
        print("This will test the live equity curve system by:")
        print("  1. Launching a data generator (writes test data every 250ms)")
        print("  2. Launching the GUI viewer (updates chart every 500ms)")
        print()

        # Set up signal handler for Ctrl+C
        signal.signal(signal.SIGINT, self.cleanup)
        signal.signal(signal.SIGTERM, self.cleanup)

        # Clear data file
        print("Preparing...")
        self.clear_data_file()

        # Launch processes
        self.launch_generator()

        # Give generator time to start and create file
        print()
        print("Waiting 2 seconds for generator to create data file...")
        time.sleep(2)

        self.launch_gui()

        # Monitor processes
        self.monitor_processes()


def main():
    """Main entry point"""
    # Check if we're in the right directory
    if not Path("lumibot").exists():
        print("⚠️  Warning: Run this script from the lumibot repository root")
        print("   Current directory:", Path.cwd())
        print()
        response = input("Continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(1)

    # Create and run test
    runner = TestRunner()
    runner.run()


if __name__ == "__main__":
    main()
