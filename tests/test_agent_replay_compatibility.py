"""Replay compatibility and recovery through the real agent/cache owners."""

import pytest

from lumibot.components.agents import AgentManager, AgentRunResult, AgentTraceEvent
from tests.test_agent_tool_permissions import _Strategy, _Vars


class _RecordedRuntime:
    def __init__(self, failed=False):
        self.failed = failed
        self.calls = 0

    def run(self, request):
        self.calls += 1
        # Shape recorded from a real missing optional dependency response.
        payload = (
            {"arguments": {}, "error": {"message": "Optional dependency unavailable.", "type": "RuntimeError"},
             "ok": False, "tool_error": True, "tool_name": "read_evidence"}
            if self.failed else {"ok": True, "value": 42}
        )
        return AgentRunResult(
            summary="Evidence unavailable." if self.failed else "Evidence read.",
            model=request.model,
            events=[AgentTraceEvent(kind="tool_result", tool_name="read_evidence", payload=payload)],
        )


def _agent(monkeypatch, tmp_path, runtime):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path))
    strategy = _Strategy()
    strategy.parameters = {}
    strategy.vars = _Vars()
    return AgentManager(strategy).create(
        name="researcher", model="fixture", allow_trading=False,
        include_builtin_tools=False, include_builtin_skills=False, _runtime=runtime,
    )


def test_failed_tool_decision_runs_fresh_after_dependency_repair(monkeypatch, tmp_path):
    runtime = _RecordedRuntime(failed=True)
    agent = _agent(monkeypatch, tmp_path, runtime)
    failed = agent.run(task_prompt="Read current evidence.")
    assert not failed.cache_hit
    runtime.failed = False
    repaired = _agent(monkeypatch, tmp_path, runtime).run(task_prompt="Read current evidence.")
    assert not repaired.cache_hit
    assert runtime.calls == 2
    assert repaired.tool_results[0].payload == {"ok": True, "value": 42}
    warm = _agent(monkeypatch, tmp_path, runtime).run(task_prompt="Read current evidence.")
    assert warm.cache_hit
    assert runtime.calls == 2


def test_compatible_success_replays_without_another_runtime_call(monkeypatch, tmp_path):
    runtime = _RecordedRuntime()
    agent = _agent(monkeypatch, tmp_path, runtime)
    cold = agent.run(task_prompt="Read current evidence.")
    warm = _agent(monkeypatch, tmp_path, runtime).run(task_prompt="Read current evidence.")
    assert warm.cache_hit
    assert warm.cache_key == cold.cache_key
    assert runtime.calls == 1


@pytest.mark.parametrize("change", ["dependency", "implementation"])
def test_changed_runtime_capabilities_change_the_replay_identity(monkeypatch, tmp_path, change):
    from lumibot.components.agents import replay_cache

    runtime = _RecordedRuntime()
    identity = {"dependency": "1", "implementation": "1"}
    monkeypatch.setattr(replay_cache, "runtime_capability_fingerprint", lambda *_: dict(identity), raising=False)
    agent = _agent(monkeypatch, tmp_path, runtime)
    cold = agent.run(task_prompt="Read current evidence.")
    identity[change] = "2"
    changed = _agent(monkeypatch, tmp_path, runtime).run(task_prompt="Read current evidence.")
    assert not changed.cache_hit
    assert changed.cache_key != cold.cache_key
    assert runtime.calls == 2


def test_capability_identity_uses_versions_and_source_hashes_without_local_paths(monkeypatch, tmp_path):
    import json
    import sys
    from types import ModuleType
    from lumibot.components.agents import replay_cache

    source = tmp_path / "runtime.py"
    source.write_text("def run(request): return 1\n")
    module = ModuleType("local_runtime_fixture")
    module.__file__ = str(source)
    exec(compile(source.read_text(), str(source), "exec"), module.__dict__)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(replay_cache, "_installed_distributions", lambda: (("example-dependency", "1.0"),))
    first = replay_cache.runtime_capability_fingerprint([], module)
    assert first == replay_cache.runtime_capability_fingerprint([], module)
    assert first["dependencies"] == (("example-dependency", "1.0"),)
    assert len(first["implementations"][module.__name__]) == 64
    assert str(tmp_path) not in json.dumps(first)
    source.write_text("def run(request): return 200\n")
    assert replay_cache.runtime_capability_fingerprint([], module) != first


@pytest.mark.parametrize("payload,failed", [
    ({"ok": False}, True), ({"tool_error": True}, True), ({"isError": True}, True),
    ({"ok": True, "text": "The report discusses error rates."}, False),
    ({"value": 42}, False), (None, False),
])
def test_replay_failure_detection_uses_explicit_result_status(payload, failed):
    from lumibot.components.agents.replay_cache import has_failed_tool_result

    assert has_failed_tool_result({"events": [{"kind": "tool_result", "payload": payload}]}) is failed
