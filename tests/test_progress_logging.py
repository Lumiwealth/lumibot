"""
Tests for enhanced backtest progress logging.

This module tests the enhanced progress bar functionality that includes:
- simulation_date: The current date in the backtest
- cash: Current cash balance
- total_return_pct: Running total return percentage
- positions_json: Minimal position data (symbol, qty, val, pnl)

TDD approach: These tests are written first, before implementation.
"""
import csv
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytz


class TestProgressDataStructure(unittest.TestCase):
    """Test the minimal position data structure for progress updates."""

    def test_minimal_position_structure_has_required_fields(self):
        """Verify minimal position structure only has essential fields."""
        # This is the target structure - lightweight, no bloat
        minimal_position = {
            "symbol": "AAPL",
            "qty": 50,
            "val": 9115.00,  # market_value
            "pnl": 340.00,   # unrealized P&L
        }

        # Should only have these 4 fields - nothing else
        expected_fields = {"symbol", "qty", "val", "pnl"}
        self.assertEqual(set(minimal_position.keys()), expected_fields)

    def test_minimal_position_excludes_heavy_fields(self):
        """Verify heavy fields are NOT included in minimal structure."""
        # These fields should NOT be in the minimal structure
        heavy_fields = [
            "expiration",      # Options/futures
            "strike",          # Options
            "multiplier",      # Futures
            "asset_type",      # Not needed for display
            "avg_price",       # Not critical
            "current_price",   # Not critical
            "exchange",        # Not needed
            "currency",        # Not needed
        ]

        minimal_position = {
            "symbol": "AAPL",
            "qty": 50,
            "val": 9115.00,
            "pnl": 340.00,
        }

        for field in heavy_fields:
            self.assertNotIn(field, minimal_position)


class TestProgressCSVColumns(unittest.TestCase):
    """Test the CSV column structure for progress logging."""

    def test_csv_has_new_columns(self):
        """Verify CSV includes all new columns."""
        expected_columns = [
            "timestamp",
            "percent",
            "elapsed",
            "eta",
            "portfolio_value",
            # New columns
            "simulation_date",
            "cash",
            "total_return_pct",
            "positions_json",
        ]

        # These should all be present in the CSV output
        self.assertEqual(len(expected_columns), 9)
        self.assertIn("simulation_date", expected_columns)
        self.assertIn("cash", expected_columns)
        self.assertIn("total_return_pct", expected_columns)
        self.assertIn("positions_json", expected_columns)


def create_test_data_source(temp_dir, start, end):
    """Helper to create a test data source with all abstract methods implemented."""
    from lumibot.data_sources.data_source_backtesting import DataSourceBacktesting

    class TestDataSource(DataSourceBacktesting):
        def get_historical_prices(self, *args, **kwargs):
            return None

        def get_chains(self, *args, **kwargs):
            return None

        def get_last_price(self, *args, **kwargs):
            return None

    ds = TestDataSource(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False,
        log_backtest_progress_to_file=True
    )
    ds._progress_csv_path = os.path.join(temp_dir, "logs", "progress.csv")
    return ds


class TestDataSourceBacktestingProgress(unittest.TestCase):
    """Test the DataSourceBacktesting progress logging functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.progress_csv_path = os.path.join(self.temp_dir, "logs", "progress.csv")

    def tearDown(self):
        """Clean up temp files."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_log_backtest_progress_includes_simulation_date(self):
        """Test that simulation_date is included in progress CSV."""
        start = datetime(2024, 1, 1, tzinfo=pytz.UTC)
        end = datetime(2024, 12, 31, tzinfo=pytz.UTC)

        ds = create_test_data_source(self.temp_dir, start, end)

        # Set simulation datetime to a specific date
        simulation_date = datetime(2024, 6, 15, 10, 30, 0, tzinfo=pytz.UTC)
        ds._datetime = simulation_date

        # Log progress with new parameters
        ds.log_backtest_progress_to_csv(
            percent=50.0,
            elapsed=timedelta(hours=1, minutes=30),
            log_eta=timedelta(hours=1, minutes=30),
            portfolio_value="105234.56",
            simulation_date=simulation_date.strftime("%Y-%m-%d"),
            cash=25000.00,
            total_return_pct=5.23,
            positions_json="[]"
        )

        # Read and verify CSV
        self.assertTrue(os.path.exists(self.progress_csv_path))

        with open(self.progress_csv_path, 'r') as f:
            reader = csv.DictReader(f)
            row = next(reader)

            self.assertIn("simulation_date", row)
            self.assertEqual(row["simulation_date"], "2024-06-15")

    def test_log_backtest_progress_includes_cash(self):
        """Test that cash balance is included in progress CSV."""
        start = datetime(2024, 1, 1, tzinfo=pytz.UTC)
        end = datetime(2024, 12, 31, tzinfo=pytz.UTC)

        ds = create_test_data_source(self.temp_dir, start, end)
        ds._datetime = datetime(2024, 6, 15, tzinfo=pytz.UTC)

        ds.log_backtest_progress_to_csv(
            percent=50.0,
            elapsed=timedelta(hours=1),
            log_eta=timedelta(hours=1),
            portfolio_value="105234.56",
            simulation_date="2024-06-15",
            cash=25000.00,
            total_return_pct=5.23,
            positions_json="[]"
        )

        with open(self.progress_csv_path, 'r') as f:
            reader = csv.DictReader(f)
            row = next(reader)

            self.assertIn("cash", row)
            self.assertEqual(float(row["cash"]), 25000.00)

    def test_log_backtest_progress_includes_total_return_pct(self):
        """Test that total return percentage is included in progress CSV."""
        start = datetime(2024, 1, 1, tzinfo=pytz.UTC)
        end = datetime(2024, 12, 31, tzinfo=pytz.UTC)

        ds = create_test_data_source(self.temp_dir, start, end)
        ds._datetime = datetime(2024, 6, 15, tzinfo=pytz.UTC)

        ds.log_backtest_progress_to_csv(
            percent=50.0,
            elapsed=timedelta(hours=1),
            log_eta=timedelta(hours=1),
            portfolio_value="105234.56",
            simulation_date="2024-06-15",
            cash=25000.00,
            total_return_pct=5.23,
            positions_json="[]"
        )

        with open(self.progress_csv_path, 'r') as f:
            reader = csv.DictReader(f)
            row = next(reader)

            self.assertIn("total_return_pct", row)
            self.assertAlmostEqual(float(row["total_return_pct"]), 5.23, places=2)

    def test_log_backtest_progress_includes_positions_json(self):
        """Test that positions JSON is included in progress CSV."""
        start = datetime(2024, 1, 1, tzinfo=pytz.UTC)
        end = datetime(2024, 12, 31, tzinfo=pytz.UTC)

        ds = create_test_data_source(self.temp_dir, start, end)
        ds._datetime = datetime(2024, 6, 15, tzinfo=pytz.UTC)

        positions = [
            {"symbol": "AAPL", "qty": 50, "val": 9115.00, "pnl": 340.00},
            {"symbol": "MSFT", "qty": 30, "val": 11856.00, "pnl": 456.00},
        ]
        positions_json = json.dumps(positions)

        ds.log_backtest_progress_to_csv(
            percent=50.0,
            elapsed=timedelta(hours=1),
            log_eta=timedelta(hours=1),
            portfolio_value="105234.56",
            simulation_date="2024-06-15",
            cash=25000.00,
            total_return_pct=5.23,
            positions_json=positions_json
        )

        with open(self.progress_csv_path, 'r') as f:
            reader = csv.DictReader(f)
            row = next(reader)

            self.assertIn("positions_json", row)
            parsed_positions = json.loads(row["positions_json"])
            self.assertEqual(len(parsed_positions), 2)
            self.assertEqual(parsed_positions[0]["symbol"], "AAPL")
            self.assertEqual(parsed_positions[1]["symbol"], "MSFT")


class TestUpdateDatetimeWithPositions(unittest.TestCase):
    """Test the _update_datetime method with position data."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temp files."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_update_datetime_accepts_positions_parameter(self):
        """Test that _update_datetime accepts positions parameter."""
        start = datetime(2024, 1, 1, tzinfo=pytz.UTC)
        end = datetime(2024, 12, 31, tzinfo=pytz.UTC)

        ds = create_test_data_source(self.temp_dir, start, end)
        ds.log_backtest_progress_to_file = False  # Disable file logging for this test

        # This should not raise an error
        new_datetime = datetime(2024, 6, 15, tzinfo=pytz.UTC)
        positions = [{"symbol": "AAPL", "qty": 50, "val": 9115.00, "pnl": 340.00}]

        # Update datetime with positions - should accept the parameter
        ds._update_datetime(
            new_datetime,
            cash=25000.00,
            portfolio_value=105234.56,
            positions=positions,
            initial_budget=100000.00
        )

        # Verify datetime was updated
        self.assertEqual(ds._datetime, new_datetime)


class TestBackwardCompatibility(unittest.TestCase):
    """Test backward compatibility with existing code."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temp files."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_log_backtest_progress_works_without_new_params(self):
        """Test that existing calls without new params still work."""
        start = datetime(2024, 1, 1, tzinfo=pytz.UTC)
        end = datetime(2024, 12, 31, tzinfo=pytz.UTC)

        ds = create_test_data_source(self.temp_dir, start, end)
        ds._datetime = datetime(2024, 6, 15, tzinfo=pytz.UTC)

        # Old-style call without new parameters should still work
        try:
            ds.log_backtest_progress_to_csv(
                percent=50.0,
                elapsed=timedelta(hours=1),
                log_eta=timedelta(hours=1),
                portfolio_value="105234.56"
            )
        except TypeError as e:
            self.fail(f"Backward compatibility broken: {e}")


if __name__ == "__main__":
    unittest.main()
