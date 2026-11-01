"""Agent SQL results expose absolute instants in UTC without changing source text."""
from types import SimpleNamespace

from lumibot.components.agents.duckdb_tools import DuckDBQueryLayer


def test_typed_query_instants_use_utc_and_preserve_source_text_and_wall_clocks():
    layer = DuckDBQueryLayer(SimpleNamespace())
    try:
        layer.connection.execute("SET TimeZone='America/New_York'")
        result = layer.query(sql="""
            SELECT source_time, CAST(source_time AS TIMESTAMPTZ) AS instant,
                   TIMESTAMP '2026-08-11 09:30:00' AS local_wall_clock
            FROM (VALUES ('2026-08-11T10:30:00-04:00'),
                         ('2026-08-11T14:45:00+00:00')) AS evidence(source_time)
            ORDER BY instant
        """)
        assert result["row_count"] == 2
        assert result["truncated"] is False
        rows = result["rows"]
        assert [row["instant"].isoformat() for row in rows] == [
            "2026-08-11T14:30:00+00:00", "2026-08-11T14:45:00+00:00"
        ]
        assert all(row["instant"].utcoffset().total_seconds() == 0 for row in rows)
        assert rows[0]["source_time"] == "2026-08-11T10:30:00-04:00"
        assert rows[1]["source_time"] == "2026-08-11T14:45:00+00:00"
        assert all(row["local_wall_clock"].hour == 9 and row["local_wall_clock"].tzinfo is None for row in rows)
        # Query execution retains its configured market timezone. Only returned
        # aware instants are normalized; local-hour SQL keeps its usual meaning.
        assert layer.query(sql="SELECT EXTRACT(hour FROM TIMESTAMPTZ '2026-08-11T14:30:00Z') AS hour")["rows"][0]["hour"] == 10
    finally:
        layer.connection.close()
