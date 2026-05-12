import json

import pytest

from lumibot.tools import runtime_telemetry as rt


def test_read_cgroup_memory_v2(monkeypatch):
    def fake_read_int(path):
        if path.endswith("memory.current"):
            return 100
        return None

    def fake_read_text(path):
        if path.endswith("memory.max"):
            return "200\n"
        return None

    monkeypatch.setattr(rt, "_read_int", fake_read_int)
    monkeypatch.setattr(rt, "_read_text", fake_read_text)

    out = rt.read_cgroup_memory()
    assert out["cgroup_version"] == 2
    assert out["cgroup_mem_current_bytes"] == 100
    assert out["cgroup_mem_limit_bytes"] == 200
    assert pytest.approx(out["cgroup_mem_pct"], rel=1e-6) == 0.5


def test_read_cgroup_memory_v2_max_unlimited(monkeypatch):
    monkeypatch.setattr(rt, "_read_int", lambda p: 123 if p.endswith("memory.current") else None)
    monkeypatch.setattr(rt, "_read_text", lambda p: "max\n" if p.endswith("memory.max") else None)
    out = rt.read_cgroup_memory()
    assert out["cgroup_version"] == 2
    assert out["cgroup_mem_current_bytes"] == 123
    assert "cgroup_mem_limit_bytes" not in out
    assert "cgroup_mem_pct" not in out


def test_read_proc_self_status_parsing(monkeypatch):
    status = "Name:\tpython\nThreads:\t7\nVmRSS:\t  12345 kB\nVmSize:\t  98765 kB\n"
    monkeypatch.setattr(rt, "_read_text", lambda p: status if p == "/proc/self/status" else None)
    out = rt.read_proc_self_status()
    assert out["process_rss_bytes"] == 12345 * 1024
    assert out["process_vms_bytes"] == 98765 * 1024
    assert out["threads_count"] == 7


def test_runtime_telemetry_config_defaults_disable_under_pytest(monkeypatch):
    monkeypatch.delenv("LUMIBOT_TELEMETRY", raising=False)
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "1")
    cfg = rt.RuntimeTelemetryConfig.from_env(is_backtesting=False)
    assert cfg.enabled is False


def test_runtime_telemetry_config_respects_override(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "1")
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "1")
    cfg = rt.RuntimeTelemetryConfig.from_env(is_backtesting=False)
    assert cfg.enabled is True


def test_runtime_telemetry_emits_json(monkeypatch):
    monkeypatch.setattr(rt, "read_cgroup_memory", lambda: {"cgroup_mem_current_bytes": 1, "cgroup_version": 2})
    monkeypatch.setattr(rt, "read_proc_self_status", lambda: {"process_rss_bytes": 2})
    monkeypatch.setattr(rt, "read_fd_count", lambda: 3)
    monkeypatch.setattr(rt, "read_ru_maxrss_bytes", lambda: 4)

    class DummyBroker:
        name = "Dummy"
        _strategy_name = "S"

        def _telemetry_snapshot(self):
            return {"orders_new": 1}

    cfg = rt.RuntimeTelemetryConfig(
        enabled=True,
        base_interval_s=300,
        burst_threshold_pct=0.8,
        burst_interval_s=15,
        deep_enabled=False,
        deep_threshold_pct=0.9,
        deep_cooldown_s=3600,
    )
    emitter = rt.RuntimeTelemetryEmitter(broker=DummyBroker(), stop_event=rt.threading.Event(), config=cfg)
    payload = emitter._build_payload()  # noqa: SLF001
    assert payload["v"] == 1
    encoded = json.dumps(payload)
    decoded = json.loads(encoded)
    assert decoded["process_rss_bytes"] == 2
