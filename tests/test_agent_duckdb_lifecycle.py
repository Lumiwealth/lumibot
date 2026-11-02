"""Owned eval databases must release native workers even while fixtures are retained."""

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


def test_repeated_fixture_teardown_releases_native_threads_without_garbage_collection():
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
            deadline = time.monotonic() + 2
            while process.num_threads() > baseline and time.monotonic() < deadline:
                time.sleep(0.01)
            assert process.num_threads() <= baseline
            assert active.manager.duckdb.query(sql="SELECT 4 AS value")["rows"] == [{"value": 4}]
    finally:
        for production in [*retained, active]:
            production.close()
            production.manager.duckdb.connection.close()
