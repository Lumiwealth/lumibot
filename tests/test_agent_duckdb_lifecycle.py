"""Owned eval databases must release native workers even while fixtures are retained."""

import subprocess
import sys
import time
from types import SimpleNamespace

import duckdb
import pandas as pd
import psutil
import pytest

from lumibot.components.agents.duckdb_tools import DuckDBQueryLayer
from scripts.agent_eval_production_fixture import ProductionFixture
from scripts.run_agent_evals import build_fixture


def test_layer_close_releases_registered_data_and_preserves_other_connections():
    layer = DuckDBQueryLayer(SimpleNamespace())
    other = DuckDBQueryLayer(SimpleNamespace())
    try:
        layer.register_document_table("values.csv", pd.DataFrame({"value": [7]}), source="fixture")
        assert layer.query(sql="SELECT value FROM doc_values")["rows"] == [{"value": 7}]
        layer.close()
        layer.close()
        with pytest.raises(duckdb.ConnectionException, match="closed"):
            layer.connection.execute("SELECT 1")
        assert not layer._frames
        assert not layer._table_meta
        assert not layer._source_tables
        assert not layer._history_cache
        assert other.query(sql="SELECT 2 AS value")["rows"] == [{"value": 2}]
    finally:
        layer.connection.close()
        other.connection.close()


@pytest.mark.parametrize("stop_raises", [False, True])
def test_fixture_close_releases_database_even_if_broker_stop_fails(monkeypatch, stop_raises):
    production = ProductionFixture(build_fixture("flat_stock_account"))
    connection = production.manager.duckdb.connection
    original_stop = production.broker.stream.stop
    if stop_raises:
        def failing_stop():
            original_stop()
            raise RuntimeError("fixture stop failed")
        monkeypatch.setattr(production.broker.stream, "stop", failing_stop)
    try:
        if stop_raises:
            with pytest.raises(RuntimeError, match="fixture stop failed"):
                production.close()
        else:
            production.close()
        with pytest.raises(duckdb.ConnectionException, match="closed"):
            connection.execute("SELECT 1")
        assert not production.root.exists()
        production.close()
    finally:
        original_stop()
        connection.close()
        production.directory.cleanup()


def test_fixture_web_cleanup_failure_still_closes_database_and_directory(monkeypatch):
    production = ProductionFixture(build_fixture("web_documents"))
    connection = production.manager.duckdb.connection
    client = production.strategy._agent_web_client
    original_close = client.close

    def failing_close():
        original_close()
        raise RuntimeError("fixture web close failed")

    monkeypatch.setattr(client, "close", failing_close)
    try:
        with pytest.raises(RuntimeError, match="fixture web close failed"):
            production.close()
        with pytest.raises(duckdb.ConnectionException, match="closed"):
            connection.execute("SELECT 1")
        assert not production.root.exists()
        assert production.broker.stream._stop_event.is_set()
    finally:
        original_close()
        production.broker.stream.stop()
        connection.close()
        production.directory.cleanup()


def _check_repeated_fixture_teardown_releases_native_threads():
    # Keep one active owner and all completed owners alive. This catches relying
    # on garbage collection of strategy/manager cycles to close native databases.
    active = ProductionFixture(build_fixture("flat_stock_account"))
    retained = []
    process = psutil.Process()
    active.manager.duckdb.query(sql="SELECT 4 AS value")
    baseline = process.num_threads()
    try:
        for _ in range(3):
            production = ProductionFixture(build_fixture("flat_stock_account"))
            retained.append(production)
            assert production.manager.duckdb.query(sql="SELECT 3 AS value")["rows"] == [{"value": 3}]
            production.close()
            with pytest.raises(duckdb.ConnectionException, match="closed"):
                production.manager.duckdb.connection.execute("SELECT 1")
            # Broker-owned stream threads observe their stop event on the next
            # queue timeout. Wait for teardown, never for resource admission.
            while process.num_threads() > baseline:
                time.sleep(0.01)
            assert process.num_threads() <= baseline
            assert active.manager.duckdb.query(sql="SELECT 4 AS value")["rows"] == [{"value": 4}]
    finally:
        for production in [*retained, active]:
            production.close()
            production.manager.duckdb.connection.close()


def test_repeated_fixture_teardown_releases_native_threads_without_garbage_collection():
    # Earlier broker tests can retain unrelated SDK threads in the pytest worker.
    # A fresh process measures only this probe's actual native owners. Retain the
    # closed owners and the live sibling, and require teardown without a timing
    # acceptance cutoff or garbage collection.
    subprocess.run(
        [sys.executable, "-c", (
            "from tests.test_agent_duckdb_lifecycle import "
            "_check_repeated_fixture_teardown_releases_native_threads as check; check()"
        )],
        check=True,
    )


def test_auto_named_history_slices_from_earlier_iterations_are_released():
    # Two AI-agent backtests (3.4 and 4 hours, Nov 2026) were killed with exit code -9.
    # Each history load whose bars were not already in the data store registered a new
    # auto-named frame (spy_stock_hour_1, _2, ...) and kept every one for the whole run,
    # plus a cache entry per simulated time. Earlier iterations' slices are point-in-time
    # copies nobody can read again; only the current iteration's tables are kept.
    clock = {"now": pd.Timestamp("2026-03-02 10:00", tz="UTC")}
    frame = pd.DataFrame({"close": range(500)},
                         index=pd.date_range("2026-01-01", periods=500, freq="h", tz="UTC"))
    strategy = SimpleNamespace(
        get_datetime=lambda: clock["now"],
        get_historical_prices=lambda *a, **k: SimpleNamespace(pandas_df=frame),
    )
    layer = DuckDBQueryLayer(strategy)
    try:
        for step in range(200):
            clock["now"] = pd.Timestamp("2026-03-02 10:00", tz="UTC") + pd.Timedelta(hours=step)
            first = layer.load_history_table(symbol="SPY", length=50, timestep="hour")
            second = layer.load_history_table(symbol="QQQ", length=50, timestep="hour")
            # Tables loaded in this iteration stay readable together.
            rows = layer.query(sql=f'SELECT COUNT(*) AS n FROM "{first["table_name"]}", "{second["table_name"]}"')["rows"]
            assert rows and rows[0]["n"] > 0
        assert len(layer._frames) <= 2
        assert len(layer._history_cache) <= 2
        names = {row[0] for row in layer.connection.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall()}
        assert len(names) <= 2
        # A table the agent named itself is kept and replaced in place.
        layer.load_history_table(symbol="SPY", length=50, timestep="hour", table_name="spy_named")
        clock["now"] += pd.Timedelta(hours=1)
        layer.load_history_table(symbol="SPY", length=50, timestep="hour")
        assert "spy_named" in layer._frames
    finally:
        layer.close()


def test_history_loads_from_the_data_store_across_iterations_leave_no_open_result():
    # DuckDB 2.0 refuses new work on a connection that still has an unread result
    # ("connection has an unfinished result from execute()"). The data-store fast path
    # created a view, read one row of COUNT(*) and dropped earlier views without reading
    # their results, so the next history load raised. A hosted hourly backtest crashed
    # this way on its second history load.
    index = pd.date_range("2025-01-01", periods=2000, freq="h", tz="America/New_York")
    frames = {
        symbol: pd.DataFrame(
            {"open": 1.0, "high": 1.0, "low": 1.0, "close": [float(i) for i in range(2000)], "volume": 1},
            index=index,
        )
        for symbol in ("TQQQ", "SQQQ", "QQQ")
    }
    clock = {"now": index[600]}

    class Store:
        def __init__(self):
            self._data_store = {
                (symbol, "hour"): SimpleNamespace(df=frame, timestep="hour") for symbol, frame in frames.items()
            }

        def find_asset_in_data_store(self, asset, quote, timestep):
            return (asset.symbol, "hour")

    strategy = SimpleNamespace(
        get_datetime=lambda: clock["now"],
        get_historical_prices=lambda *a, **k: None,
        broker=SimpleNamespace(data_source=Store()),
    )
    layer = DuckDBQueryLayer(strategy)
    try:
        for step in range(4):
            clock["now"] = index[600 + step]
            tables = [layer.load_history_table(symbol=s, length=500, timestep="hour") for s in ("TQQQ", "SQQQ", "QQQ")]
            assert all(t["kind"] == "visible_view" and t["row_count"] == 500 for t in tables)
            rows = layer.query(sql=f'SELECT MAX(close) AS c FROM "{tables[-1]["table_name"]}"')["rows"]
            assert rows[0]["c"] == 599 + step
            # Every statement's result was read, so the connection accepts new work.
            layer.connection.register("probe", pd.DataFrame({"x": [1]}))
            layer.connection.unregister("probe")
    finally:
        layer.close()
