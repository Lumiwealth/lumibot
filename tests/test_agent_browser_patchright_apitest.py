import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import parse_qs

import pytest

pytestmark = pytest.mark.browsertest
pytest.importorskip("patchright")

from lumibot.components.agents.browser_tools import (  # noqa: E402
    BrowserCredentialProfile,
    BrowserSessionManager,
    PatchrightEngine,
)


class _FixtureHandler(BaseHTTPRequestHandler):
    publications = []
    publication_keys = set()

    def log_message(self, *args):
        return

    def do_GET(self):
        if self.path == "/login":
            body = b"""<!doctype html><html><body>
              <form method='post' action='/login'>
                <input id='username' name='username'><input id='password' name='password' type='password'>
                <button id='login' type='submit'>Log in</button>
              </form></body></html>"""
            return self._send(200, body)
        if self.path == "/dashboard":
            if "session=fixture" not in (self.headers.get("cookie") or ""):
                return self._send(401, b"not authenticated")
            body = b"""<!doctype html><html><body><h1>Authenticated research dashboard</h1>
              <div id='signal'>Fixture signal: cautious bullish</div>
              <input id='note'><button id='save'
                onclick="saveState()">Save</button>
              <div id='local-storage-status'></div><div id='indexeddb-status'></div>
              <script>
                document.querySelector('#local-storage-status').textContent = localStorage.getItem('note') || '';
                const openRequest = indexedDB.open('lumibot-fixture', 1);
                openRequest.onupgradeneeded = () => openRequest.result.createObjectStore('state');
                openRequest.onsuccess = () => {
                  const db = openRequest.result;
                  const read = db.transaction('state').objectStore('state').get('note');
                  read.onsuccess = () => {
                    document.querySelector('#indexeddb-status').textContent = read.result || '';
                    document.querySelector('#indexeddb-status').dataset.ready = 'true';
                  };
                  window.saveState = () => {
                    const note = document.querySelector('#note').value;
                    localStorage.setItem('note', note);
                    document.querySelector('#local-storage-status').textContent = note;
                    document.querySelector('#indexeddb-status').dataset.ready = 'false';
                    const write = db.transaction('state', 'readwrite').objectStore('state').put(note, 'note');
                    write.onsuccess = () => {
                      document.querySelector('#indexeddb-status').textContent = note;
                      document.querySelector('#indexeddb-status').dataset.ready = 'true';
                    };
                  };
                };
                document.body.dataset.ready='true';
              </script></body></html>"""
            return self._send(200, body)
        if self.path == "/news":
            return self._send(200, b"<html><body><h1>Second tab</h1></body></html>")
        if self.path == "/community":
            body = b"""<!doctype html><html><body><h1>Owned test community</h1>
              <form method='post' action='/publish'>
                <input id='idempotency-key' name='idempotency_key'>
                <input id='receipt' name='receipt'>
                <button id='publish' type='submit'>Publish receipt</button>
              </form>
              <input id='upload' type='file' onchange="const r=new FileReader();
                r.onload=()=>document.querySelector('#upload-result').textContent=this.files[0].name+':'+r.result;
                r.readAsText(this.files[0]);">
              <div id='upload-result'></div>
              <a id='download' href='/download' download='receipt.txt'>Download receipt</a>
              </body></html>"""
            return self._send(200, body)
        if self.path == "/download":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Disposition", "attachment; filename=receipt.txt")
            self.send_header("Content-Length", "15")
            self.end_headers()
            self.wfile.write(b"download-proof\n")
            return
        return self._send(404, b"missing")

    def do_POST(self):
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length)
        if self.path == "/login":
            self.send_response(303)
            self.send_header("Set-Cookie", "session=fixture; Path=/; HttpOnly")
            self.send_header("Location", "/dashboard")
            self.end_headers()
            return
        if self.path == "/publish":
            fields = parse_qs(body.decode())
            idempotency_key = fields.get("idempotency_key", [""])[0]
            if idempotency_key and idempotency_key not in self.publication_keys:
                self.publication_keys.add(idempotency_key)
                self.publications.append(fields)
                return self._send(201, b"published")
            return self._send(200, b"already published")
        return self._send(404, b"missing")

    def _send(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def browser_fixture_server():
    _FixtureHandler.publications.clear()
    _FixtureHandler.publication_keys.clear()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FixtureHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_patchright_session_works_inside_running_asyncio_loop(browser_fixture_server, tmp_path):
    async def exercise_browser():
        manager = BrowserSessionManager(engine=PatchrightEngine(), state_root=tmp_path)
        opened = manager.open(profile="async-agent", headless=True)
        session_id = opened["session_id"]
        manager.navigate(session_id, browser_fixture_server)
        observed = manager.observe(session_id)
        manager.close(session_id)
        return observed

    observed = asyncio.run(exercise_browser())

    assert observed["url"] == f"{browser_fixture_server}/"
    assert "missing" in observed["text"]


def test_patchright_stateful_login_tabs_storage_and_screenshot(browser_fixture_server, tmp_path):
    host = "127.0.0.1"
    upload_path = tmp_path / "uploads" / "proof.txt"
    upload_path.parent.mkdir(parents=True)
    upload_path.write_text("upload-proof", encoding="utf-8")
    manager = BrowserSessionManager(
        engine=PatchrightEngine(),
        state_root=tmp_path,
        credential_profiles={
            "fixture": BrowserCredentialProfile(
                name="fixture",
                allowed_hosts=(host,),
                username="browser-user",
                password="browser-password",
            )
        },
    )
    opened = manager.open(profile="acceptance", headless=True)
    session_id = opened["session_id"]
    manager.navigate(session_id, f"{browser_fixture_server}/login")
    manager.login(
        session_id,
        credential_profile="fixture",
        username_selector="#username",
        password_selector="#password",
        submit_selector="#login",
    )
    observed = manager.observe(session_id)
    assert "Authenticated research dashboard" in observed["text"]
    manager.act(session_id, action="wait", selector="#indexeddb-status[data-ready=true]", value="attached")
    manager.act(session_id, action="fill", selector="#note", value="persistent-note")
    manager.act(session_id, action="click", selector="#save")
    manager.act(session_id, action="wait_text", selector="#indexeddb-status", value="persistent-note")
    assert "persistent-note" in manager.extract(session_id, selector="#indexeddb-status")["values"]
    screenshot = manager.screenshot(session_id, name="authenticated-dashboard")
    manager.tabs(session_id, "open", url=f"{browser_fixture_server}/news")
    assert len(manager.tabs(session_id, "list")["tabs"]) == 2
    manager.tabs(session_id, "open", url=f"{browser_fixture_server}/community")
    manager.act(session_id, action="upload", selector="#upload", value="proof.txt")
    manager.act(session_id, action="wait", selector="#upload-result")
    assert "proof.txt:upload-proof" in manager.observe(session_id)["text"]
    download = manager.act(session_id, action="download", selector="#download", value="receipt.txt")
    assert Path(download["download_path"]).read_text(encoding="utf-8") == "download-proof\n"
    manager.act(session_id, action="fill", selector="#idempotency-key", value="trade-receipt-1")
    manager.act(session_id, action="fill", selector="#receipt", value="Sandbox trade accepted: NVDA")
    manager.act(session_id, action="click", selector="#publish")
    published_screenshot = manager.screenshot(session_id, name="published-trade-receipt")
    assert _FixtureHandler.publications == [
        {"idempotency_key": ["trade-receipt-1"], "receipt": ["Sandbox trade accepted: NVDA"]}
    ]
    assert Path(published_screenshot["path"]).stat().st_size > 0
    storage = manager.save_storage_state(session_id)
    assert Path(screenshot["path"]).stat().st_size > 0
    assert Path(storage["path"]).stat().st_size > 0
    manager.close(session_id)

    reopened = manager.open(profile="acceptance", headless=True)
    manager.navigate(reopened["session_id"], f"{browser_fixture_server}/dashboard")
    persisted = manager.observe(reopened["session_id"])
    assert "Authenticated research dashboard" in persisted["text"]
    manager.act(
        reopened["session_id"],
        action="wait",
        selector="#indexeddb-status[data-ready=true]",
        value="attached",
    )
    persisted = manager.observe(reopened["session_id"])
    assert persisted["text"].count("persistent-note") >= 2
    closed = manager.close(reopened["session_id"])
    trace_text = Path(closed["trace_path"]).read_text(encoding="utf-8")
    assert '"event":"navigate"' in trace_text
    assert '"event":"close"' in trace_text


@pytest.mark.browserstress
def test_patchright_profile_reopens_100_times_without_deadlock(tmp_path):
    manager = BrowserSessionManager(engine=PatchrightEngine(), state_root=tmp_path)

    for iteration in range(100):
        opened = manager.open(profile="soak", headless=True)
        observed = manager.observe(opened["session_id"])
        assert observed["url"] == "about:blank", iteration
        manager.close(opened["session_id"])
