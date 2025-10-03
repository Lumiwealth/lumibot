"""
Performance tracking for backtest tests.
Automatically records execution time and key metrics to CSV for long-term tracking.
"""
import csv
import datetime
import os
from pathlib import Path


class PerformanceTracker:
    """Track backtest performance over time"""

    # Default CSV file location - in tests/backtest directory
    DEFAULT_CSV_PATH = Path(__file__).parent / "backtest_performance_history.csv"

    # CSV columns
    COLUMNS = [
        "timestamp",
        "test_name",
        "data_source",
        "trading_days",
        "execution_time_seconds",
        "git_commit",
        "lumibot_version",
        "strategy_name",
        "start_date",
        "end_date",
        "sleeptime",
        "notes"
    ]

    def __init__(self, csv_path=None):
        """Initialize the performance tracker

        Args:
            csv_path: Path to CSV file. If None, uses default location.
        """
        self.csv_path = Path(csv_path) if csv_path else self.DEFAULT_CSV_PATH
        self._ensure_csv_exists()

    def _ensure_csv_exists(self):
        """Create CSV file with headers if it doesn't exist"""
        if not self.csv_path.exists():
            with open(self.csv_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.COLUMNS)
                writer.writeheader()

    def _get_git_commit(self):
        """Get current git commit hash, or None if not in git repo"""
        try:
            import subprocess
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return None

    def _get_lumibot_version(self):
        """Get Lumibot version"""
        try:
            import lumibot
            return lumibot.__version__
        except Exception:
            return None

    def record_backtest(
        self,
        test_name,
        data_source,
        execution_time_seconds,
        trading_days=None,
        strategy_name=None,
        start_date=None,
        end_date=None,
        sleeptime=None,
        notes=None
    ):
        """Record a backtest performance measurement

        Args:
            test_name: Name of the test (e.g., "test_yahoo_last_price")
            data_source: Data source name (e.g., "Yahoo", "Polygon", "Databento")
            execution_time_seconds: How long the backtest took to run
            trading_days: Number of trading days in the backtest
            strategy_name: Name of strategy class
            start_date: Backtest start date
            end_date: Backtest end date
            sleeptime: Strategy sleep time (e.g., "1D", "1M")
            notes: Any additional notes
        """
        row = {
            "timestamp": datetime.datetime.now().isoformat(),
            "test_name": test_name,
            "data_source": data_source,
            "trading_days": trading_days,
            "execution_time_seconds": round(execution_time_seconds, 3),
            "git_commit": self._get_git_commit(),
            "lumibot_version": self._get_lumibot_version(),
            "strategy_name": strategy_name,
            "start_date": str(start_date) if start_date else None,
            "end_date": str(end_date) if end_date else None,
            "sleeptime": sleeptime,
            "notes": notes
        }

        with open(self.csv_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.COLUMNS)
            writer.writerow(row)

    def get_recent_performance(self, test_name=None, limit=10):
        """Get recent performance data

        Args:
            test_name: Filter by test name (optional)
            limit: Max number of records to return

        Returns:
            List of performance records (dicts)
        """
        if not self.csv_path.exists():
            return []

        with open(self.csv_path, 'r') as f:
            reader = csv.DictReader(f)
            records = list(reader)

        # Filter by test name if provided
        if test_name:
            records = [r for r in records if r['test_name'] == test_name]

        # Return most recent records
        return records[-limit:]


# Global instance for easy access
_tracker = PerformanceTracker()


def record_backtest_performance(*args, **kwargs):
    """Convenience function to record backtest performance using global tracker"""
    return _tracker.record_backtest(*args, **kwargs)


def get_recent_performance(*args, **kwargs):
    """Convenience function to get recent performance using global tracker"""
    return _tracker.get_recent_performance(*args, **kwargs)
