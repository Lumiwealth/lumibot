"""The yappi profile export keeps memory bounded (production: 4 GB, task killed, no file)."""

import csv
import threading

import pytest

yappi = pytest.importorskip("yappi")

from lumibot.traders.trader import PROFILE_COLUMNS, write_yappi_profile_csv  # noqa: E402


def _work(n):
    total = 0
    for i in range(n):
        total += sum(range(50)) + i
    return total


def _profile_threads():
    yappi.clear_stats()
    yappi.set_clock_type("wall")
    yappi.start()
    threads = [threading.Thread(target=_work, args=(2000,)) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    _work(500)
    yappi.stop()


def test_export_folds_threads_into_one_row_per_function_and_keeps_nothing(tmp_path, monkeypatch):
    _profile_threads()
    kept = []
    original = yappi.get_func_stats

    def recording(*args, **kwargs):
        stats = original(*args, **kwargs)
        kept.append(len(list(stats)))
        return stats

    monkeypatch.setattr(yappi, "get_func_stats", recording)
    path = tmp_path / "profile.csv"
    written = write_yappi_profile_csv(yappi, path)
    rows = list(csv.DictReader(open(path)))
    assert list(rows[0]) == PROFILE_COLUMNS
    assert written == len(rows) > 0
    assert kept == [0]  # yappi built no retained stat objects (and no callee lists)
    names = [row["full_name"] for row in rows]
    assert len(names) == len(set(names))  # one row per function, all threads together
    work = next(row for row in rows if row["name"] == "_work")
    assert int(work["ncall"]) == 5  # four threads and the main thread
    assert {row["ctx_name"] for row in rows} == {"all"}
    totals = [float(row["ttot_s"]) for row in rows]
    assert totals == sorted(totals, reverse=True)
    yappi.clear_stats()


def test_export_caps_rows(tmp_path):
    _profile_threads()
    path = tmp_path / "profile.csv"
    assert write_yappi_profile_csv(yappi, path, max_rows=3) == 3
    assert len(list(csv.DictReader(open(path)))) == 3
    yappi.clear_stats()
