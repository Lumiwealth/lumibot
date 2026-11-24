"""Contract of the per-run telemetry file (lumibot.tools.backtest_telemetry).

Tools read this file to show where a backtest spent its time, so field names are a stable
contract: tests pin them. Values are counts and seconds; the file never holds secrets.
"""

import json
import threading

from lumibot.tools import backtest_telemetry as bt

TOP_LEVEL = ["schema", "lumibot_version", "wall_seconds", "phases", "iterations", "providers", "caches", "machine"]
PHASES = ["data_setup_seconds", "simulation_seconds", "reports_seconds", "uploads_seconds", "simulation_cpu_seconds"]
MACHINE = ["architecture", "cpu_count", "cpu_model"]
ITERATIONS = ["count", "total_seconds", "slowest_seconds", "slowest_at"]
PROVIDER = ["source", "asset_class", "requests", "provider_seconds", "rows", "retries",
            "rate_limit_waits", "rate_limit_wait_seconds", "backoff_waits", "backoff_wait_seconds"]
CACHE = ["layer", "source", "asset_class", "reads", "hits", "misses", "read_bytes", "read_seconds",
         "uploads", "upload_bytes", "upload_seconds", "upload_errors"]


def test_field_names_are_a_stable_contract():
    bt.reset()
    with bt.provider_scope("alpaca", "option"):
        bt.record_provider_request(0.25, rows=10)
        bt.record_cache("hit", layer="shared_bars", nbytes=5)
    data = bt.snapshot()
    assert data["schema"] == "lumibot.backtest_telemetry/1"
    assert list(data) == TOP_LEVEL
    assert list(data["phases"]) == PHASES
    assert list(data["iterations"]) == ITERATIONS
    assert list(data["providers"][0]) == PROVIDER
    assert list(data["caches"][0]) == CACHE
    assert list(data["machine"]) == MACHINE


def test_work_is_attributed_to_the_innermost_scope():
    bt.reset()
    with bt.provider_scope("ibkr", "stock"):
        bt.record_provider_request(1.5, rows=1000)
        with bt.provider_scope(asset_class="index"):
            bt.record_provider_request(0.5)
        bt.record_wait("backoff", 2.0)
        bt.record_retry()
    bt.record_provider_request(0.1)  # outside any scope
    rows = {(r["source"], r["asset_class"]): r for r in bt.snapshot()["providers"]}
    stock = rows[("ibkr", "stock")]
    assert (stock["requests"], stock["provider_seconds"], stock["rows"]) == (1, 1.5, 1000)
    assert (stock["backoff_waits"], stock["backoff_wait_seconds"], stock["retries"]) == (1, 2.0, 1)
    assert rows[("ibkr", "index")]["requests"] == 1
    assert rows[("unknown", "unknown")]["requests"] == 1


def test_rate_limit_waits_and_cache_events_are_counted():
    bt.reset()
    with bt.provider_scope("alpaca", "stock"):
        bt.record_wait("rate_limit", 3.0)
        bt.record_wait("rate_limit", 1.0)
        bt.record_cache("miss", layer="shared_bars_s3")
        bt.record_cache("hit", layer="shared_bars_s3", nbytes=100, seconds=0.5)
        bt.record_cache("upload", layer="shared_bars_s3", nbytes=40)
        bt.record_cache("upload_error", layer="shared_bars_s3")
    data = bt.snapshot()
    provider = data["providers"][0]
    assert (provider["rate_limit_waits"], provider["rate_limit_wait_seconds"]) == (2, 4.0)
    cache = data["caches"][0]
    assert (cache["reads"], cache["hits"], cache["misses"], cache["read_bytes"]) == (2, 1, 1, 100)
    assert (cache["uploads"], cache["upload_bytes"], cache["upload_errors"]) == (1, 40, 1)


def test_iterations_and_phases():
    bt.reset()
    for _ in range(3):
        bt.iteration_finished(bt.iteration_started(), None)
    bt.mark_simulation_end()
    with bt.phase("reports"):
        pass
    data = bt.snapshot()
    assert data["iterations"]["count"] == 3
    assert data["iterations"]["slowest_seconds"] <= data["iterations"]["total_seconds"]
    assert data["phases"]["simulation_seconds"] >= 0 and data["phases"]["reports_seconds"] >= 0


def test_file_is_written_atomically_and_holds_no_secrets(tmp_path):
    bt.reset()
    with bt.provider_scope("thetadata", "option"):
        bt.record_provider_request(0.2, rows=3)
    path = tmp_path / "logs" / "run_telemetry.json"
    bt.write(str(path))
    written = json.loads(path.read_text())
    assert written["schema"] == bt.SCHEMA
    assert written["providers"][0]["rows"] == 3
    assert not list(path.parent.glob("*.tmp"))
    text = path.read_text().lower()
    for forbidden in ("key", "secret", "token", "password", "bucket", "http", "amazonaws"):
        assert forbidden not in text


def test_recording_is_thread_safe():
    bt.reset()

    def work():
        with bt.provider_scope("ccxt", "crypto"):
            for _ in range(1000):
                bt.record_provider_request(0.0)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert bt.snapshot()["providers"][0]["requests"] == 8000


def test_simulation_cpu_seconds_separate_a_slower_machine_from_more_work():
    """Same CPU seconds with more wall seconds means a slower or busier machine, not more work."""
    bt.reset()
    started = bt.iteration_started()
    deadline = __import__("time").process_time() + 0.05
    while __import__("time").process_time() < deadline:
        pass  # burn CPU inside the simulation
    __import__("time").sleep(0.05)  # wall time with no CPU
    bt.iteration_finished(started, None)
    bt.mark_simulation_end()
    phases = bt.snapshot()["phases"]
    assert 0.04 <= phases["simulation_cpu_seconds"] < phases["simulation_seconds"]


def test_machine_names_the_cpu_without_host_details(monkeypatch, tmp_path):
    cpuinfo = tmp_path / "cpuinfo"
    cpuinfo.write_text("processor\t: 0\nCPU implementer\t: 0x41\nCPU part\t: 0xd40\n\n"
                       "processor\t: 1\nCPU implementer\t: 0x41\nCPU part\t: 0xd40\n")
    monkeypatch.setattr(bt, "_CPUINFO_PATH", str(cpuinfo))
    bt._machine_cache.clear()
    machine = bt.machine()
    assert machine["cpu_model"] == "Arm Neoverse-V1"
    assert isinstance(machine["cpu_count"], int) and machine["cpu_count"] >= 1
    cpuinfo.write_text("processor\t: 0\nmodel name\t: Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz\n")
    bt._machine_cache.clear()
    assert bt.machine()["cpu_model"] == "Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz"
