import json
from pathlib import Path

import pytest

from lumibot.components.agents.browser_tools import (
    BrowserCredentialProfile,
    BrowserSessionManager,
)
from lumibot.components.agents.builtins import BuiltinTools


class _FakeEngine:
    def __init__(self):
        self.sessions = {}
        self.actions = []

    def open(self, *, session_id, profile_dir, headless):
        self.sessions[session_id] = {
            "profile_dir": str(profile_dir),
            "headless": headless,
            "tabs": [{"id": "tab-1", "url": "about:blank", "title": "Blank"}],
            "active": "tab-1",
        }

    def close(self, session_id):
        self.sessions.pop(session_id)

    def navigate(self, session_id, url, wait_until):
        tab = self._tab(session_id)
        tab.update(url=url, title="Fixture")
        return {"url": url, "title": "Fixture"}

    def observe(self, session_id, include_screenshot):
        tab = self._tab(session_id)
        return {**tab, "text": "fixture page", "screenshot_base64": "abc" if include_screenshot else None}

    def act(self, session_id, action, selector, value, timeout_seconds):
        self.actions.append((session_id, action, selector, value))
        result = {"action": action, "selector": selector, "url": self._tab(session_id)["url"]}
        if action == "download":
            Path(value).write_bytes(b"download")
            result["download_path"] = str(value)
        return result

    def tabs(self, session_id, operation, tab_id=None, url=None):
        session = self.sessions[session_id]
        if operation == "open":
            new_id = f"tab-{len(session['tabs']) + 1}"
            session["tabs"].append({"id": new_id, "url": url or "about:blank", "title": "New"})
            session["active"] = new_id
        elif operation == "switch":
            session["active"] = tab_id
        elif operation == "close":
            session["tabs"] = [tab for tab in session["tabs"] if tab["id"] != tab_id]
            session["active"] = session["tabs"][0]["id"]
        return {"active_tab_id": session["active"], "tabs": list(session["tabs"])}

    def extract(self, session_id, selector, attribute):
        return {"selector": selector, "attribute": attribute, "values": ["fixture"]}

    def save_storage_state(self, session_id, path):
        Path(path).write_text('{"cookies": []}')
        return str(path)

    def screenshot(self, session_id, path, full_page):
        Path(path).write_bytes(b"png")
        return str(path)

    def _tab(self, session_id):
        session = self.sessions[session_id]
        return next(tab for tab in session["tabs"] if tab["id"] == session["active"])


def test_browser_sessions_are_stateful_multitab_and_emit_action_receipts(tmp_path):
    manager = BrowserSessionManager(engine=_FakeEngine(), state_root=tmp_path)

    opened = manager.open(profile="research", headless=True)
    session_id = opened["session_id"]
    manager.navigate(session_id, "https://example.test/dashboard")
    manager.tabs(session_id, "open", url="https://example.test/news")
    tabs = manager.tabs(session_id, "list")
    action = manager.act(session_id, action="click", selector="button.publish")
    observed = manager.observe(session_id, include_screenshot=True)

    assert len(tabs["tabs"]) == 2
    assert tabs["active_tab_id"] == "tab-2"
    assert action["receipt"]["action"] == "click"
    assert action["receipt"]["receipt_sha256"]
    assert observed["screenshot_base64"] == "abc"
    assert Path(opened["profile_dir"]).name == "research"


def test_browser_wait_forwards_explicit_locator_state(tmp_path):
    engine = _FakeEngine()
    manager = BrowserSessionManager(engine=engine, state_root=tmp_path)
    opened = manager.open(profile="research", headless=True)

    manager.act(
        opened["session_id"],
        action="wait",
        selector="#indexeddb-status[data-ready=true]",
        value="attached",
    )

    assert engine.actions[-1][1:] == (
        "wait",
        "#indexeddb-status[data-ready=true]",
        "attached",
    )


def test_browser_session_writes_a_redacted_append_only_action_trace(tmp_path):
    engine = _FakeEngine()
    profile = BrowserCredentialProfile(
        name="portal",
        allowed_hosts=("research.example.test",),
        username="trace-user@example.test",
        password="trace-secret",
    )
    manager = BrowserSessionManager(
        engine=engine,
        state_root=tmp_path,
        credential_profiles={"portal": profile},
    )
    opened = manager.open(profile="trace")
    session_id = opened["session_id"]
    manager.navigate(session_id, "https://research.example.test/login")
    manager.login(
        session_id,
        credential_profile="portal",
        username_selector="#username",
        password_selector="#password",
        submit_selector="#submit",
    )
    manager.act(session_id, action="fill", selector="#note", value="sensitive-note")
    closed = manager.close(session_id)

    trace_path = Path(closed["trace_path"])
    events = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    serialized = json.dumps(events)
    assert [event["event"] for event in events] == ["open", "navigate", "login", "act", "close"]
    assert "trace-secret" not in serialized
    assert "trace-user@example.test" not in serialized
    assert "sensitive-note" not in serialized
    assert closed["trace_sha256"]


def test_browser_login_injects_host_scoped_credentials_without_returning_secrets(tmp_path):
    engine = _FakeEngine()
    profile = BrowserCredentialProfile(
        name="portal",
        allowed_hosts=("research.example.test",),
        username="trader@example.test",
        password="secret-password",
    )
    manager = BrowserSessionManager(
        engine=engine,
        state_root=tmp_path,
        credential_profiles={"portal": profile},
    )
    session_id = manager.open(profile="login")["session_id"]
    manager.navigate(session_id, "https://research.example.test/login")

    result = manager.login(
        session_id,
        credential_profile="portal",
        username_selector="#username",
        password_selector="#password",
        submit_selector="button[type=submit]",
    )

    assert engine.actions[-3:] == [
        (session_id, "fill", "#username", "trader@example.test"),
        (session_id, "fill", "#password", "secret-password"),
        (session_id, "click", "button[type=submit]", None),
    ]
    assert "secret-password" not in json.dumps(result)
    assert "trader@example.test" not in json.dumps(result)
    manager.navigate(session_id, "https://evil.example.test/login")
    with pytest.raises(ValueError, match="not allowed for host"):
        manager.login(
            session_id,
            credential_profile="portal",
            username_selector="#u",
            password_selector="#p",
        )


def test_browser_artifacts_stay_within_managed_state_root(tmp_path):
    manager = BrowserSessionManager(engine=_FakeEngine(), state_root=tmp_path)
    session_id = manager.open(profile="proof")["session_id"]

    screenshot = manager.screenshot(session_id, name="evidence", full_page=True)
    storage = manager.save_storage_state(session_id)
    download = manager.act(session_id, action="download", selector="a.export", value="orders.csv")

    assert Path(screenshot["path"]).is_relative_to(tmp_path)
    assert Path(storage["path"]).is_relative_to(tmp_path)
    assert Path(download["download_path"]).is_relative_to(tmp_path)
    assert Path(download["download_path"]).read_bytes() == b"download"
    with pytest.raises(ValueError, match="safe filename"):
        manager.screenshot(session_id, name="../../escape")


def test_browser_uploads_are_limited_to_the_configured_managed_root(tmp_path):
    upload_root = tmp_path / "uploads"
    upload_root.mkdir()
    allowed = upload_root / "report.csv"
    allowed.write_text("symbol,signal\nSPY,buy\n", encoding="utf-8")
    outside = tmp_path / "secret.txt"
    outside.write_text("not uploadable", encoding="utf-8")
    engine = _FakeEngine()
    manager = BrowserSessionManager(engine=engine, state_root=tmp_path / "browser", upload_root=upload_root)
    session_id = manager.open(profile="upload")["session_id"]

    manager.act(session_id, action="upload", selector="input[type=file]", value="report.csv")

    assert engine.actions[-1][-1] == str(allowed.resolve())
    with pytest.raises(ValueError, match="managed upload root"):
        manager.act(session_id, action="upload", selector="input[type=file]", value=str(outside))


def test_browser_tool_surface_is_available_to_agents():
    names = {definition.name for definition in BuiltinTools.all()}
    assert {
        "browser_session_open",
        "browser_session_close",
        "browser_session_recover",
        "browser_navigate",
        "browser_observe",
        "browser_act",
        "browser_tabs",
        "browser_extract",
        "browser_login",
        "browser_storage_state",
        "browser_screenshot",
    }.issubset(names)


def test_browser_session_recovery_reopens_the_same_profile_and_current_url(tmp_path):
    class CrashedEngine(_FakeEngine):
        def __init__(self):
            super().__init__()
            self.open_calls = []
            self.navigations = []

        def open(self, *, session_id, profile_dir, headless):
            self.open_calls.append((session_id, str(profile_dir), headless))
            super().open(session_id=session_id, profile_dir=profile_dir, headless=headless)

        def close(self, session_id):
            raise RuntimeError("browser process already crashed")

        def navigate(self, session_id, url, wait_until):
            self.navigations.append((session_id, url, wait_until))
            return super().navigate(session_id, url, wait_until)

    engine = CrashedEngine()
    manager = BrowserSessionManager(engine=engine, state_root=tmp_path)
    opened = manager.open(profile="recoverable", headless=True)
    session_id = opened["session_id"]
    manager.navigate(session_id, "https://research.example.test/dashboard")

    recovered = manager.recover(session_id)

    assert recovered["ok"] is True
    assert recovered["session_id"] == session_id
    assert recovered["profile"] == "recoverable"
    assert recovered["resumed_url"] == "https://research.example.test/dashboard"
    assert len(engine.open_calls) == 2
    assert engine.navigations[-1] == (
        session_id,
        "https://research.example.test/dashboard",
        "domcontentloaded",
    )


def test_browser_types_are_public_agent_exports():
    from lumibot.components.agents import BrowserCredentialProfile as ExportedProfile
    from lumibot.components.agents import BrowserSessionManager as ExportedManager
    from lumibot.components.agents import CamoufoxEngine as ExportedCamoufoxEngine
    from lumibot.components.agents import PatchrightEngine as ExportedPatchrightEngine

    assert ExportedProfile is BrowserCredentialProfile
    assert ExportedManager is BrowserSessionManager
    assert ExportedCamoufoxEngine.__name__ == "CamoufoxEngine"
    assert ExportedPatchrightEngine.__name__ == "PatchrightEngine"
