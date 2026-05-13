import csv

from tests.backtest.performance_tracker import PerformanceTracker


def test_performance_tracker_uses_lf_newlines_for_create_and_append(tmp_path, monkeypatch):
    monkeypatch.setattr(PerformanceTracker, "_get_git_commit", lambda self: "abc123")
    monkeypatch.setattr(PerformanceTracker, "_get_lumibot_version", lambda self: "test")

    csv_path = tmp_path / "history.csv"
    tracker = PerformanceTracker(csv_path)

    created = csv_path.read_bytes()
    assert b"\r\n" not in created
    assert created.endswith(b"\n")

    tracker.record_backtest(
        test_name="test_fast_path",
        data_source="Unit",
        execution_time_seconds=1.2345,
        trading_days=2,
    )

    appended = csv_path.read_bytes()
    assert b"\r\n" not in appended
    assert appended.endswith(b"\n")

    rows = list(csv.DictReader(csv_path.read_text().splitlines()))
    assert len(rows) == 1
    assert rows[0]["test_name"] == "test_fast_path"
