#!/usr/bin/env python3
"""
Live Backtest Progress Viewer

Real-time equity curve visualization for Lumibot backtests.
Reads data from logs/backtest_live_equity.jsonl and displays a live-updating chart.

This GUI runs in a separate process and can be:
- Launched automatically when backtest starts (if SHOW_LIVE_EQUITY_CURVE=True)
- Launched manually at any time: python3 show_live_backtest_progress.py
- Closed and re-launched without affecting the backtest
- Kept open after backtest completes to review final results

Usage:
    python3 show_live_backtest_progress.py [--file <path>]

Arguments:
    --file    Path to equity JSONL file (default: logs/backtest_live_equity.jsonl)
"""

import argparse
import json
import logging
import sys
from collections import deque
from datetime import datetime
from pathlib import Path

import matplotlib

# Use MacOSX native backend on macOS, fallback to TkAgg on other platforms

if sys.platform == "darwin":
    matplotlib.use("MacOSX")  # Native macOS backend
else:
    matplotlib.use("TkAgg")  # Tk backend for Windows/Linux
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# Configure matplotlib for better window behavior
plt.rcParams["figure.raise_window"] = True  # Bring window to front

# Get logger for this module
logger = logging.getLogger(__name__)


class LiveEquityCurveViewer:
    """Real-time equity curve viewer using matplotlib"""

    def __init__(
        self,
        data_file="logs/backtest_live_equity.jsonl",
        max_points=None,
        show_drawdown=False,
        dark_mode=False,
    ):
        """
        Initialize the live equity curve viewer

        Parameters
        ----------
        data_file : str
            Path to JSONL file containing equity curve data
        max_points : int, optional
            Maximum number of points to keep in memory. None = unlimited (keeps all data).
            Default None to show complete equity curve without scrolling.
        show_drawdown : bool, optional
            If True, displays a second subplot showing percentage drawdown from peak.
            Default False for backward compatibility.
        dark_mode : bool, optional
            If True, uses dark mode theme with dark background and light colors.
            Default False for light mode.
        """
        self.data_file = Path(data_file)
        self.max_points = max_points
        self.show_drawdown = show_drawdown
        self.dark_mode = dark_mode

        # Data storage - NO LIMIT to prevent scrolling after 10k points
        # Memory usage: ~50 bytes per point = 5MB for 100k points (negligible)
        self.timestamps = [] if max_points is None else deque(maxlen=max_points)
        self.portfolio_values = [] if max_points is None else deque(maxlen=max_points)
        self.last_file_position = 0
        self.last_percent = 0.0
        self.data_count = 0

        # Track equity peaks
        self.peak_indices = []  # Indices where new peaks occur
        self.peak_values = []  # Corresponding portfolio values at peaks
        self.current_peak = float("-inf")  # Track the highest value seen so far

        # Track drawdown data if enabled
        if self.show_drawdown:
            self.drawdown_values = [] if max_points is None else deque(maxlen=max_points)

        # Track if file exists and has data
        self.file_exists = False
        self.has_data = False

        # Define color scheme based on mode
        if self.dark_mode:
            self.bg_color = "#1e1e1e"  # Dark background
            self.fg_color = "#ffffff"  # White text
            self.grid_color = "#404040"  # Dark gray grid
            self.line_color = "#00d4ff"  # Bright cyan for equity line
            self.peak_color = "#00ff00"  # Bright green for peaks
            self.drawdown_color = "#ff4444"  # Bright red for drawdown
        else:
            self.bg_color = "#ffffff"  # White background
            self.fg_color = "#000000"  # Black text
            self.grid_color = "#cccccc"  # Light gray grid
            self.line_color = "darkblue"  # Dark blue for equity line
            self.peak_color = "lime"  # Lime green for peaks
            self.drawdown_color = "red"  # Red for drawdown

        # Setup the plot with conditional subplot layout
        if self.show_drawdown:
            # Create 2 subplots sharing x-axis: equity on top, drawdown on bottom
            # height_ratios=[2, 1] makes equity plot 2x taller than drawdown
            self.fig, (self.ax_equity, self.ax_drawdown) = plt.subplots(
                nrows=2,
                ncols=1,
                figsize=(12, 8),
                sharex=True,  # CRITICAL: Share x-axis for synchronized zooming/panning
                gridspec_kw={"height_ratios": [2, 1], "hspace": 0.05},
            )
            self.ax = self.ax_equity  # Alias for backward compatibility
        else:
            # Single plot (existing behavior)
            self.fig, self.ax = plt.subplots(figsize=(12, 6))
            self.ax_equity = self.ax  # Alias for consistency
            self.ax_drawdown = None

        # Disable the navigation toolbar to prevent manual pan/zoom
        self.fig.canvas.toolbar_visible = False

        # Apply dark mode styling to figure and axes if enabled
        if self.dark_mode:
            self.fig.patch.set_facecolor(self.bg_color)
            self.ax_equity.set_facecolor(self.bg_color)
            self.ax_equity.spines["bottom"].set_color(self.fg_color)
            self.ax_equity.spines["top"].set_color(self.fg_color)
            self.ax_equity.spines["left"].set_color(self.fg_color)
            self.ax_equity.spines["right"].set_color(self.fg_color)
            self.ax_equity.tick_params(colors=self.fg_color, which="both")
            if self.show_drawdown:
                self.ax_drawdown.set_facecolor(self.bg_color)
                self.ax_drawdown.spines["bottom"].set_color(self.fg_color)
                self.ax_drawdown.spines["top"].set_color(self.fg_color)
                self.ax_drawdown.spines["left"].set_color(self.fg_color)
                self.ax_drawdown.spines["right"].set_color(self.fg_color)
                self.ax_drawdown.tick_params(colors=self.fg_color, which="both")

        # Configure equity plot
        (self.line,) = self.ax_equity.plot([], [], color=self.line_color, linewidth=2, label="Portfolio Value")

        # Add scatter plot for equity peaks
        edge_color = self.bg_color if self.dark_mode else "black"
        self.peak_scatter = self.ax_equity.scatter(
            [],
            [],
            c=self.peak_color,
            s=25,
            marker="o",
            label="New Peak",
            zorder=5,
            edgecolors=edge_color,
            linewidths=0.5,
        )

        # Configure equity plot styling
        if not self.show_drawdown:
            self.ax_equity.set_xlabel("Time", fontsize=12, color=self.fg_color)
        self.ax_equity.set_ylabel("Portfolio Value ($)", fontsize=12, color=self.fg_color)
        self.ax_equity.grid(True, alpha=0.3, linestyle="--", color=self.grid_color)
        legend = self.ax_equity.legend(loc="upper left", fontsize=10)
        if self.dark_mode:
            legend.get_frame().set_facecolor(self.bg_color)
            legend.get_frame().set_edgecolor(self.fg_color)
            for text in legend.get_texts():
                text.set_color(self.fg_color)

        # Format y-axis as currency
        self.ax_equity.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"${x:,.0f}"))

        # CRITICAL: Disable autoscaling to prevent scrolling
        self.ax_equity.set_autoscale_on(False)
        self.ax_equity.autoscale(enable=False, axis="both")

        # Disable interactive navigation (pan/zoom) on axes
        self.ax_equity.set_navigate(False)

        # Set initial axis limits to prevent any auto-ranging
        self.ax_equity.set_xlim(0, 10)
        self.ax_equity.set_ylim(0, 100000)

        # Configure drawdown plot if enabled
        if self.show_drawdown:
            (self.drawdown_line,) = self.ax_drawdown.plot(
                [], [], color=self.drawdown_color, linewidth=2, label="Drawdown %"
            )
            # Add filled area for drawdown visualization
            self.drawdown_fill = self.ax_drawdown.fill_between([], [], 0, color=self.drawdown_color, alpha=0.3)
            self.ax_drawdown.set_xlabel("Time", fontsize=12, color=self.fg_color)
            self.ax_drawdown.set_ylabel("Drawdown (%)", fontsize=12, color=self.fg_color)
            self.ax_drawdown.grid(True, alpha=0.3, linestyle="--", color=self.grid_color)
            dd_legend = self.ax_drawdown.legend(loc="upper left", fontsize=10)
            if self.dark_mode:
                dd_legend.get_frame().set_facecolor(self.bg_color)
                dd_legend.get_frame().set_edgecolor(self.fg_color)
                for text in dd_legend.get_texts():
                    text.set_color(self.fg_color)
            self.ax_drawdown.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x:.1f}%"))
            self.ax_drawdown.set_autoscale_on(False)
            self.ax_drawdown.autoscale(enable=False, axis="both")
            self.ax_drawdown.set_navigate(False)
            self.ax_drawdown.set_xlim(0, 10)  # Shares x with equity plot
            self.ax_drawdown.set_ylim(-10, 0)  # Start with -10% to 0% range

            # Invert y-axis so drawdown appears as downward from 0%
            self.ax_drawdown.invert_yaxis()
        else:
            self.drawdown_line = None

        # Initialize title
        self.update_title()

        # Tight layout
        self.fig.tight_layout()

    def update_title(self, portfolio_value=None, percent=None):
        """Update the plot title with current status"""
        title_parts = ["Lumibot Backtest - Live Equity Curve"]

        if portfolio_value is not None:
            title_parts.append(f"Current Value: ${portfolio_value:,.2f}")

        if percent is not None:
            title_parts.append(f"Progress: {percent:.1f}%")

        if not self.file_exists:
            title_parts.append("[Waiting for backtest to start...]")
        elif not self.has_data:
            title_parts.append("[No data yet]")

        # Place title on top subplot (equity) when using dual-plot mode
        if self.show_drawdown:
            self.ax_equity.set_title(" | ".join(title_parts), fontsize=14, pad=15, color=self.fg_color)
        else:
            self.ax.set_title(" | ".join(title_parts), fontsize=14, pad=15, color=self.fg_color)

    def read_new_data(self):
        """Read new data from JSONL file since last read"""
        try:
            # Check if file exists
            if not self.data_file.exists():
                if self.file_exists:  # File disappeared
                    print(f"Warning: Data file {self.data_file} no longer exists")
                    self.file_exists = False
                return

            self.file_exists = True

            # Read from last position
            with open(self.data_file, "r") as f:
                # Seek to last position
                f.seek(self.last_file_position)

                # Read new lines
                new_lines = f.readlines()

                # Update file position
                self.last_file_position = f.tell()

                # Process new lines
                for line in new_lines:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        data = json.loads(line)

                        # Extract data
                        timestamp_str = data.get("timestamp")
                        portfolio_value = data.get("portfolio_value")
                        percent = data.get("percent", 0)

                        if timestamp_str and portfolio_value is not None:
                            # Parse timestamp
                            try:
                                timestamp = datetime.fromisoformat(timestamp_str)
                            except (ValueError, TypeError) as e:
                                # Fallback to using data count as x-axis
                                # Log the error for debugging (only for first few occurrences to avoid spam)
                                if self.data_count < 3:
                                    logger.warning("Failed to parse timestamp, using data count as fallback: %s", e)
                                timestamp = self.data_count

                            # Add to data (lists by default = unlimited storage)
                            self.timestamps.append(timestamp)
                            self.portfolio_values.append(portfolio_value)
                            self.last_percent = percent

                            # Check if this is a new equity peak
                            if portfolio_value > self.current_peak:
                                self.current_peak = portfolio_value
                                self.peak_indices.append(self.data_count)
                                self.peak_values.append(portfolio_value)

                            # Calculate drawdown if enabled
                            if self.show_drawdown:
                                # Calculate drawdown: (current - peak) / peak * 100
                                if self.current_peak > 0:
                                    drawdown_pct = (
                                        (portfolio_value - self.current_peak)
                                        / self.current_peak
                                        * 100
                                    )
                                else:
                                    drawdown_pct = 0.0
                                self.drawdown_values.append(drawdown_pct)

                            self.data_count += 1
                            self.has_data = True

                    except json.JSONDecodeError:
                        # Generic error message to avoid potential information leakage
                        print(f"Warning: Failed to parse JSON line: {line[:50]}...")
                        continue

        except Exception as e:
            print(f"Error reading data file: {e}")

    def update_plot(self, frame):
        """Update the plot with new data (called by FuncAnimation)"""
        # Read new data from file
        self.read_new_data()

        # If no data yet, just update title
        if not self.has_data:
            self.update_title()
            return

        # Update line data
        if self.timestamps and self.portfolio_values:
            num_points = len(self.timestamps)
            if num_points > 0:
                # Calculate new axis limits
                # X-axis: ALWAYS anchored at 0, extend to show all data
                new_xlim = (0, max(num_points - 1, 10))

                # Y-axis: Scale to fit all data with padding
                min_val = min(self.portfolio_values)
                max_val = max(self.portfolio_values)
                # Add 5% padding on top and bottom for better visualization
                if max_val != min_val:
                    padding = (max_val - min_val) * 0.05
                else:
                    padding = max_val * 0.05 if max_val > 0 else 1000
                new_ylim = (min_val - padding, max_val + padding)

                # Force set axis limits BEFORE updating line data
                self.ax_equity.set_xlim(new_xlim)
                self.ax_equity.set_ylim(new_ylim)

                # Double-check autoscale is disabled (in case matplotlib reset it)
                self.ax_equity.set_autoscale_on(False)

            # Update the line data (keeping ALL data since we use unlimited lists)
            self.line.set_data(range(num_points), list(self.portfolio_values))

            # Update peak markers
            if self.peak_indices and self.peak_values:
                self.peak_scatter.set_offsets(list(zip(self.peak_indices, self.peak_values)))

            # Update drawdown plot if enabled
            if self.show_drawdown and self.drawdown_values:
                # Update drawdown line data
                self.drawdown_line.set_data(range(num_points), list(self.drawdown_values))

                # Update the filled area
                # Remove old fill and create new one (fill_between doesn't support set_data)
                self.drawdown_fill.remove()
                self.drawdown_fill = self.ax_drawdown.fill_between(
                    range(num_points), list(self.drawdown_values), 0, color=self.drawdown_color, alpha=0.3
                )

                # Calculate drawdown axis limits
                min_drawdown = min(self.drawdown_values)
                max_drawdown = max(self.drawdown_values)  # Should be 0 or very close

                # Add padding (10% of range, minimum 1% padding)
                dd_range = abs(min_drawdown - max_drawdown)
                dd_padding = max(dd_range * 0.1, 1.0)  # At least 1% padding

                # Y-axis: min_drawdown - padding to 0 (capped at 0)
                # (inverted, so this appears as downward from 0)
                # Upper limit is always 0 since drawdown cannot be positive
                new_dd_ylim = (min_drawdown - dd_padding, 0)

                self.ax_drawdown.set_xlim(new_xlim)  # Match equity x-axis
                self.ax_drawdown.set_ylim(new_dd_ylim)
                self.ax_drawdown.set_autoscale_on(False)

            # Update title with current value
            if self.portfolio_values:
                current_value = self.portfolio_values[-1]
                self.update_title(
                    portfolio_value=current_value, percent=self.last_percent
                )

            # Format x-axis
            if len(self.timestamps) > 1:
                # If timestamps are datetime objects, format nicely
                if isinstance(self.timestamps[0], datetime):
                    # Use time formatting if we have datetime objects
                    # (This would require converting x-axis back to timestamps)
                    pass
                else:
                    # Just use iteration numbers
                    pass

    def run(self):
        """Start the GUI and animation"""
        print("=" * 80)
        print("Live Backtest Equity Curve Viewer")
        print("=" * 80)
        print(f"Monitoring file: {self.data_file}")
        print()

        if not self.data_file.exists():
            print("⏳ Waiting for backtest to start...")
            print(f"   Expected file: {self.data_file.absolute()}")
            print()
        else:
            print("✅ Data file found, reading initial data...")
            self.read_new_data()
            print(f"   Loaded {self.data_count} data points")
            print()

        print("📈 Starting live chart...")
        print("   - Chart updates every 500ms")
        print("   - Close window to exit")
        print("   - You can re-launch this script anytime to resume monitoring")
        print("=" * 80)
        print()

        # Create animation
        # Update every 500ms (interval in milliseconds)
        # Store animation reference to prevent garbage collection
        # CRITICAL: blit=False is required when changing axis limits dynamically
        # (blitting only redraws artists, not axes, causing scrolling issues)
        self.anim = FuncAnimation(
            self.fig,
            self.update_plot,
            interval=500,  # Update every 500ms
            blit=False,  # Must be False to allow axis limit updates
            cache_frame_data=False,
        )

        # Show the plot (blocking)
        try:
            plt.show(block=True)  # block=True keeps window open
        except KeyboardInterrupt:
            print("\nViewer closed by user")
        except Exception as e:
            print(f"\nError displaying plot: {e}")

        print()
        print("=" * 80)
        print("Viewer closed")
        if self.has_data:
            print(f"Final portfolio value: ${self.portfolio_values[-1]:,.2f}")
            print(f"Total data points: {self.data_count}")
        print("=" * 80)


def main():
    """Main entry point"""
    # Configure logging for standalone execution
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(
        description="Live Backtest Equity Curve Viewer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Monitor default file
  python3 show_live_backtest_progress.py

  # Monitor custom file
  python3 show_live_backtest_progress.py --file /path/to/equity.jsonl

Notes:
  - GUI updates every 500ms
  - Can be closed and relaunched without affecting backtest
  - Stays open after backtest completes
  - Handles missing/empty files gracefully
        """,
    )

    parser.add_argument(
        "--file",
        default="logs/backtest_live_equity.jsonl",
        help="Path to equity JSONL file (default: logs/backtest_live_equity.jsonl)",
    )

    parser.add_argument(
        "--show-drawdown",
        action="store_true",
        default=False,
        help="Display drawdown subplot below equity curve (default: False)",
    )

    args = parser.parse_args()

    # Create and run viewer
    viewer = LiveEquityCurveViewer(
        data_file=args.file, show_drawdown=args.show_drawdown
    )
    viewer.run()


if __name__ == "__main__":
    main()
