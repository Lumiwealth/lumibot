from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, Protocol
from urllib.parse import urlsplit

_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def _host_matches(hostname: str, pattern: str) -> bool:
    hostname = hostname.lower().rstrip(".")
    pattern = pattern.lower().rstrip(".")
    if pattern.startswith("*."):
        suffix = pattern[1:]
        return hostname.endswith(suffix) and hostname != suffix[1:]
    return hostname == pattern


def _restrict_to_owner(path: Path, mode: int) -> None:
    """Best-effort owner-only permissions for profile data (POSIX only).

    Persistent browser profiles and exported storage state hold live session
    cookies and local-storage tokens, which are equivalent to a login.
    """
    if os.name == "nt":
        return
    os.chmod(path, mode)


def _redacted_url(url: str) -> str:
    parsed = urlsplit(str(url or ""))
    if not parsed.scheme:
        return str(url or "")
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{parsed.hostname or ''}{port}{parsed.path}"


@dataclass(frozen=True)
class BrowserCredentialProfile:
    name: str
    allowed_hosts: tuple[str, ...]
    # Excluded from repr so a profile printed in a log, traceback, or tool
    # error can never reveal the credential it carries.
    username: str = field(repr=False)
    password: str = field(repr=False)

    def ensure_host_allowed(self, hostname: str) -> None:
        if not any(_host_matches(hostname, pattern) for pattern in self.allowed_hosts):
            raise ValueError(f"Browser credential profile {self.name!r} is not allowed for host {hostname!r}.")

    @classmethod
    def from_mapping(cls, name: str, value: dict[str, Any]) -> BrowserCredentialProfile:
        return cls(
            name=name,
            allowed_hosts=tuple(str(host) for host in value.get("allowed_hosts") or ()),
            username=str(value.get("username") or ""),
            password=str(value.get("password") or ""),
        )


class BrowserEngine(Protocol):
    def open(self, *, session_id: str, profile_dir: Path, headless: bool) -> None: ...
    def close(self, session_id: str) -> None: ...
    def navigate(self, session_id: str, url: str, wait_until: str) -> dict[str, Any]: ...
    def observe(self, session_id: str, include_screenshot: bool) -> dict[str, Any]: ...
    def act(
        self,
        session_id: str,
        action: str,
        selector: str | None,
        value: Any,
        timeout_seconds: float,
    ) -> dict[str, Any]: ...
    def tabs(
        self,
        session_id: str,
        operation: str,
        tab_id: str | None = None,
        url: str | None = None,
    ) -> dict[str, Any]: ...
    def extract(self, session_id: str, selector: str, attribute: str | None) -> dict[str, Any]: ...
    def save_storage_state(self, session_id: str, path: Path) -> str: ...
    def screenshot(self, session_id: str, path: Path, full_page: bool) -> str: ...


class BrowserSessionManager:
    """Owns stateful, resumable browser sessions and their auditable artifacts."""

    def __init__(
        self,
        *,
        engine: BrowserEngine | None = None,
        state_root: str | Path | None = None,
        upload_root: str | Path | None = None,
        credential_profiles: dict[str, BrowserCredentialProfile] | None = None,
    ) -> None:
        self.state_root = Path(state_root or Path.home() / ".lumibot" / "browser")
        self.state_root.mkdir(parents=True, exist_ok=True)
        self.upload_root = Path(upload_root or self.state_root / "uploads").resolve()
        self.upload_root.mkdir(parents=True, exist_ok=True)
        self.engine = engine or PatchrightEngine()
        self.credential_profiles = dict(credential_profiles or {})
        self._sessions: dict[str, dict[str, Any]] = {}
        self._open_profiles: dict[str, str] = {}

    @staticmethod
    def _safe_name(value: str, field_name: str) -> str:
        normalized = str(value or "").strip()
        if not _SAFE_NAME.fullmatch(normalized):
            raise ValueError(f"{field_name} must be a safe filename containing only letters, numbers, '.', '_' or '-'.")
        return normalized

    def _session(self, session_id: str) -> dict[str, Any]:
        session = self._sessions.get(session_id)
        if session is None:
            raise ValueError(f"Unknown browser session {session_id!r}.")
        return session

    def _trace(self, session_id: str, event: str, **details: Any) -> None:
        session = self._session(session_id)
        path = Path(session["trace_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "event": event,
            "session_id": session_id,
            "profile": session["profile"],
            "recorded_at": time.time(),
            **details,
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")

    def open(self, *, profile: str = "default", headless: bool = True) -> dict[str, Any]:
        profile_name = self._safe_name(profile, "profile")
        if profile_name in self._open_profiles:
            existing = self._open_profiles[profile_name]
            raise ValueError(f"Browser profile {profile_name!r} is already open in session {existing!r}.")
        session_id = uuid.uuid4().hex
        profile_dir = self.state_root / "profiles" / profile_name
        profile_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        _restrict_to_owner(profile_dir, 0o700)
        self.engine.open(session_id=session_id, profile_dir=profile_dir, headless=bool(headless))
        trace_path = self.state_root / "artifacts" / session_id / "action-trace.jsonl"
        self._sessions[session_id] = {
            "profile": profile_name,
            "profile_dir": str(profile_dir),
            "headless": bool(headless),
            "opened_at": time.time(),
            "current_url": "about:blank",
            "trace_path": str(trace_path),
        }
        self._open_profiles[profile_name] = session_id
        self._trace(session_id, "open", headless=bool(headless))
        return {
            "ok": True,
            "session_id": session_id,
            "profile": profile_name,
            "profile_dir": str(profile_dir),
            "headless": bool(headless),
            "trace_path": str(trace_path),
        }

    def close(self, session_id: str) -> dict[str, Any]:
        session = self._session(session_id)
        self._trace(session_id, "close", current_url=_redacted_url(session["current_url"]))
        trace_path = Path(session["trace_path"])
        try:
            self.engine.close(session_id)
        finally:
            self._sessions.pop(session_id, None)
            self._open_profiles.pop(session["profile"], None)
        return {
            "ok": True,
            "session_id": session_id,
            "closed": True,
            "trace_path": str(trace_path),
            "trace_sha256": hashlib.sha256(trace_path.read_bytes()).hexdigest(),
        }

    def recover(self, session_id: str, *, resume_current_url: bool = True) -> dict[str, Any]:
        """Restart a crashed session in place using its persistent profile."""
        session = self._session(session_id)
        try:
            self.engine.close(session_id)
        except Exception:
            pass
        self.engine.open(
            session_id=session_id,
            profile_dir=Path(session["profile_dir"]),
            headless=bool(session["headless"]),
        )
        resumed_url = None
        current_url = str(session.get("current_url") or "about:blank")
        if resume_current_url and current_url != "about:blank":
            result = self.engine.navigate(session_id, current_url, "domcontentloaded")
            resumed_url = str(result.get("url") or current_url)
            session["current_url"] = resumed_url
        self._trace(session_id, "recover", resumed_url=_redacted_url(resumed_url or ""))
        return {
            "ok": True,
            "session_id": session_id,
            "profile": session["profile"],
            "recovered": True,
            "resumed_url": resumed_url,
        }

    def navigate(self, session_id: str, url: str, *, wait_until: str = "domcontentloaded") -> dict[str, Any]:
        session = self._session(session_id)
        result = self.engine.navigate(session_id, str(url), str(wait_until))
        session["current_url"] = str(result.get("url") or url)
        self._trace(
            session_id,
            "navigate",
            url=_redacted_url(session["current_url"]),
            wait_until=str(wait_until),
            status_code=result.get("status_code"),
        )
        return {"ok": True, "session_id": session_id, **result}

    def observe(self, session_id: str, *, include_screenshot: bool = False) -> dict[str, Any]:
        session = self._session(session_id)
        result = self.engine.observe(session_id, bool(include_screenshot))
        session["current_url"] = str(result.get("url") or session["current_url"])
        self._trace(
            session_id,
            "observe",
            url=_redacted_url(session["current_url"]),
            include_screenshot=bool(include_screenshot),
        )
        return {"ok": True, "session_id": session_id, **result}

    def act(
        self,
        session_id: str,
        *,
        action: str,
        selector: str | None = None,
        value: Any = None,
        timeout_seconds: float = 30.0,
    ) -> dict[str, Any]:
        session = self._session(session_id)
        normalized_action = str(action).strip().lower()
        engine_value = value
        if normalized_action == "download":
            safe_name = self._safe_name(str(value or "download.bin"), "download name")
            artifact_dir = self.state_root / "artifacts" / session_id / "downloads"
            artifact_dir.mkdir(parents=True, exist_ok=True)
            engine_value = str(artifact_dir / safe_name)
        elif normalized_action == "upload":
            values = value if isinstance(value, (list, tuple)) else [value]
            resolved_uploads = []
            for item in values:
                candidate = Path(str(item or ""))
                if not candidate.is_absolute():
                    candidate = self.upload_root / candidate
                try:
                    resolved = candidate.resolve(strict=True)
                except FileNotFoundError as exc:
                    raise ValueError(f"Upload file {candidate} does not exist.") from exc
                if not resolved.is_file() or not resolved.is_relative_to(self.upload_root):
                    raise ValueError(
                        f"Upload file must be an existing file inside the managed upload root {self.upload_root}."
                    )
                resolved_uploads.append(str(resolved))
            engine_value = resolved_uploads if isinstance(value, (list, tuple)) else resolved_uploads[0]
        result = self.engine.act(session_id, normalized_action, selector, engine_value, float(timeout_seconds))
        session["current_url"] = str(result.get("url") or session["current_url"])
        receipt_payload = {
            "session_id": session_id,
            "action": normalized_action,
            "selector": selector,
            "url": session["current_url"],
            "completed_at": time.time(),
        }
        receipt_json = json.dumps(receipt_payload, sort_keys=True, separators=(",", ":"))
        receipt_payload["receipt_sha256"] = hashlib.sha256(receipt_json.encode()).hexdigest()
        self._trace(
            session_id,
            "act",
            action=normalized_action,
            selector=selector,
            url=_redacted_url(session["current_url"]),
            receipt_sha256=receipt_payload["receipt_sha256"],
        )
        return {"ok": True, "session_id": session_id, **result, "receipt": receipt_payload}

    def tabs(
        self,
        session_id: str,
        operation: str = "list",
        *,
        tab_id: str | None = None,
        url: str | None = None,
    ) -> dict[str, Any]:
        self._session(session_id)
        result = self.engine.tabs(session_id, str(operation).lower(), tab_id, url)
        self._trace(
            session_id,
            "tabs",
            operation=str(operation).lower(),
            tab_id=tab_id,
            url=_redacted_url(url or ""),
        )
        return {"ok": True, "session_id": session_id, **result}

    def extract(self, session_id: str, *, selector: str = "body", attribute: str | None = None) -> dict[str, Any]:
        self._session(session_id)
        result = self.engine.extract(session_id, selector, attribute)
        self._trace(session_id, "extract", selector=selector, attribute=attribute, count=result.get("count"))
        return {"ok": True, "session_id": session_id, **result}

    def login(
        self,
        session_id: str,
        *,
        credential_profile: str,
        username_selector: str,
        password_selector: str,
        submit_selector: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> dict[str, Any]:
        session = self._session(session_id)
        profile = self.credential_profiles.get(credential_profile)
        if profile is None:
            raise ValueError(f"Unknown browser credential profile {credential_profile!r}.")
        hostname = urlsplit(session["current_url"]).hostname or ""
        profile.ensure_host_allowed(hostname)
        steps = [("fill", username_selector, profile.username), ("fill", password_selector, profile.password)]
        if submit_selector:
            steps.append(("click", submit_selector, None))
        for action, selector, value in steps:
            try:
                self.engine.act(session_id, action, selector, value, float(timeout_seconds))
            except Exception as exc:
                # Engine errors (for example a Playwright call log) can echo the
                # filled value. Tool errors are returned to the model and logged,
                # so scrub the credential and drop the original exception chain.
                detail = str(exc)
                for secret in (profile.password, profile.username):
                    if secret:
                        detail = detail.replace(secret, "[REDACTED]")
                raise RuntimeError(
                    f"browser_login failed during {action} on {selector!r}: {type(exc).__name__}: {detail}"
                ) from None
        receipt = {
            "session_id": session_id,
            "credential_profile": credential_profile,
            "host": hostname,
            "submitted": bool(submit_selector),
            "completed_at": time.time(),
        }
        receipt["receipt_sha256"] = hashlib.sha256(
            json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        self._trace(
            session_id,
            "login",
            credential_profile=credential_profile,
            host=hostname,
            submitted=bool(submit_selector),
            receipt_sha256=receipt["receipt_sha256"],
        )
        return {"ok": True, "session_id": session_id, "receipt": receipt}

    def save_storage_state(self, session_id: str) -> dict[str, Any]:
        session = self._session(session_id)
        path = Path(session["profile_dir"]) / "storage-state.json"
        if os.name != "nt" and not path.exists():
            # Create the file owner-only before the engine writes cookies into it.
            os.close(os.open(path, os.O_WRONLY | os.O_CREAT, 0o600))
        saved = self.engine.save_storage_state(session_id, path)
        _restrict_to_owner(Path(saved), 0o600)
        self._trace(session_id, "storage_state", path=str(saved))
        return {"ok": True, "session_id": session_id, "path": saved}

    def screenshot(self, session_id: str, *, name: str = "screenshot", full_page: bool = True) -> dict[str, Any]:
        self._session(session_id)
        safe_name = self._safe_name(name, "name")
        artifact_dir = self.state_root / "artifacts" / session_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        path = artifact_dir / f"{safe_name}.png"
        saved = self.engine.screenshot(session_id, path, bool(full_page))
        digest = hashlib.sha256(Path(saved).read_bytes()).hexdigest()
        self._trace(session_id, "screenshot", path=str(saved), sha256=digest, full_page=bool(full_page))
        return {"ok": True, "session_id": session_id, "path": saved, "sha256": digest}


class PatchrightEngine:
    """Patchright-backed browser engine loaded only when browser use is requested."""

    def __init__(self, *, channel: str | None = None) -> None:
        self.channel = channel
        self._playwright = None
        self._sessions: dict[str, dict[str, Any]] = {}
        self._worker: ThreadPoolExecutor | None = None
        self._worker_lock = Lock()

    def _call(self, function, *args, **kwargs):
        """Keep Patchright's synchronous runtime on one non-async worker thread."""
        with self._worker_lock:
            if self._worker is None:
                self._worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix="lumibot-browser")
            worker = self._worker
        return worker.submit(function, *args, **kwargs).result()

    def _shutdown_worker(self) -> None:
        with self._worker_lock:
            worker = self._worker
            self._worker = None
        if worker is not None:
            worker.shutdown(wait=True)

    def _runtime(self):
        if self._playwright is None:
            try:
                from patchright.sync_api import sync_playwright
            except ImportError as exc:
                raise RuntimeError(
                    "Browser tools require the optional 'browser' dependencies. Install lumibot[browser] and run "
                    "'patchright install chromium' for this runtime image."
                ) from exc
            self._playwright = sync_playwright().start()
        return self._playwright

    def open(self, *, session_id: str, profile_dir: Path, headless: bool) -> None:
        try:
            self._call(self._open, session_id=session_id, profile_dir=profile_dir, headless=headless)
        except Exception:
            self._shutdown_worker()
            raise

    def _open(self, *, session_id: str, profile_dir: Path, headless: bool) -> None:
        chromium = self._runtime().chromium
        kwargs: dict[str, Any] = {"headless": headless}
        if self.channel:
            kwargs["channel"] = self.channel
        context = chromium.launch_persistent_context(str(profile_dir), **kwargs)
        storage_state_path = profile_dir / "storage-state.json"
        if storage_state_path.exists():
            saved_state = json.loads(storage_state_path.read_text(encoding="utf-8"))
            cookies = saved_state.get("cookies")
            if isinstance(cookies, list) and cookies:
                context.add_cookies(cookies)
            origins = saved_state.get("origins")
            if isinstance(origins, list) and origins:
                serialized_origins = json.dumps(origins).replace("</", "<\\/")
                context.add_init_script(
                    f"""(() => {{
                      const savedOrigins = {serialized_origins};
                      const match = savedOrigins.find(item => item.origin === location.origin);
                      if (!match) return;
                      for (const entry of match.localStorage || []) localStorage.setItem(entry.name, entry.value);
                    }})()"""
                )
        if not context.pages:
            context.new_page()
        self._sessions[session_id] = {"context": context, "active_index": 0}

    def close(self, session_id: str) -> None:
        is_idle = self._call(self._close, session_id)
        if is_idle:
            self._shutdown_worker()

    def _close(self, session_id: str) -> bool:
        session = self._sessions.pop(session_id)
        session["context"].close()
        if not self._sessions and self._playwright is not None:
            self._playwright.stop()
            self._playwright = None
        return not self._sessions

    def _session(self, session_id: str) -> dict[str, Any]:
        if session_id not in self._sessions:
            raise ValueError(f"Unknown Patchright session {session_id!r}.")
        return self._sessions[session_id]

    def _page(self, session_id: str):
        session = self._session(session_id)
        pages = session["context"].pages
        if not pages:
            pages = [session["context"].new_page()]
        session["active_index"] = min(session["active_index"], len(pages) - 1)
        return pages[session["active_index"]]

    def navigate(self, session_id: str, url: str, wait_until: str) -> dict[str, Any]:
        return self._call(self._navigate, session_id, url, wait_until)

    def _navigate(self, session_id: str, url: str, wait_until: str) -> dict[str, Any]:
        page = self._page(session_id)
        response = page.goto(url, wait_until=wait_until)
        return {
            "url": page.url,
            "title": page.title(),
            "status_code": response.status if response is not None else None,
        }

    def observe(self, session_id: str, include_screenshot: bool) -> dict[str, Any]:
        return self._call(self._observe, session_id, include_screenshot)

    def _observe(self, session_id: str, include_screenshot: bool) -> dict[str, Any]:
        page = self._page(session_id)
        result = {
            "url": page.url,
            "title": page.title(),
            "text": page.locator("body").inner_text(timeout=10_000)[:100_000],
        }
        if include_screenshot:
            result["screenshot_base64"] = base64.b64encode(page.screenshot(full_page=True)).decode("ascii")
        return result

    def act(
        self,
        session_id: str,
        action: str,
        selector: str | None,
        value: Any,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        return self._call(self._act, session_id, action, selector, value, timeout_seconds)

    def _act(
        self,
        session_id: str,
        action: str,
        selector: str | None,
        value: Any,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        page = self._page(session_id)
        timeout_ms = max(float(timeout_seconds), 0.1) * 1000
        locator = page.locator(selector) if selector else None
        if action == "click" and locator is not None:
            locator.click(timeout=timeout_ms)
        elif action == "fill" and locator is not None:
            locator.fill(str(value or ""), timeout=timeout_ms)
        elif action == "type" and locator is not None:
            locator.press_sequentially(str(value or ""), timeout=timeout_ms)
        elif action == "press":
            if locator is not None:
                locator.press(str(value), timeout=timeout_ms)
            else:
                page.keyboard.press(str(value))
        elif action == "select" and locator is not None:
            locator.select_option(value, timeout=timeout_ms)
        elif action == "check" and locator is not None:
            locator.check(timeout=timeout_ms)
        elif action == "uncheck" and locator is not None:
            locator.uncheck(timeout=timeout_ms)
        elif action == "scroll":
            amount = int(value or 600)
            page.mouse.wheel(0, amount)
        elif action == "wait":
            if selector:
                state = str(value or "visible").lower()
                if state not in {"attached", "detached", "visible", "hidden"}:
                    raise ValueError("Browser wait state must be attached, detached, visible, or hidden.")
                page.locator(selector).wait_for(state=state, timeout=timeout_ms)
            else:
                page.wait_for_timeout(int(timeout_ms))
        elif action == "wait_text" and locator is not None:
            expected_text = str(value or "")
            if not expected_text:
                raise ValueError("Browser wait_text requires a non-empty value.")
            locator.filter(has_text=expected_text).wait_for(state="visible", timeout=timeout_ms)
        elif action == "upload" and locator is not None:
            locator.set_input_files(value, timeout=timeout_ms)
        elif action == "download" and locator is not None:
            destination = Path(str(value))
            destination.parent.mkdir(parents=True, exist_ok=True)
            with page.expect_download(timeout=timeout_ms) as download_info:
                locator.click(timeout=timeout_ms)
            download_info.value.save_as(str(destination))
            return {
                "action": action,
                "selector": selector,
                "url": page.url,
                "title": page.title(),
                "download_path": str(destination),
                "download_sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
            }
        else:
            raise ValueError(
                "Unsupported browser action. Use click, fill, type, press, select, check, uncheck, scroll, wait, "
                "wait_text, upload, or download."
            )
        return {"action": action, "selector": selector, "url": page.url, "title": page.title()}

    def tabs(
        self,
        session_id: str,
        operation: str,
        tab_id: str | None = None,
        url: str | None = None,
    ) -> dict[str, Any]:
        return self._call(self._tabs, session_id, operation, tab_id, url)

    def _tabs(
        self,
        session_id: str,
        operation: str,
        tab_id: str | None = None,
        url: str | None = None,
    ) -> dict[str, Any]:
        session = self._session(session_id)
        context = session["context"]
        if operation == "open":
            page = context.new_page()
            session["active_index"] = len(context.pages) - 1
            if url:
                page.goto(url, wait_until="domcontentloaded")
        elif operation in {"switch", "close"}:
            if not tab_id or not tab_id.startswith("tab-"):
                raise ValueError("tab_id must be a value returned by browser_tabs.")
            try:
                index = int(tab_id.split("-", 1)[1]) - 1
                page = context.pages[index]
            except (ValueError, IndexError) as exc:
                raise ValueError(f"Unknown tab_id {tab_id!r}.") from exc
            if operation == "switch":
                session["active_index"] = index
                page.bring_to_front()
            else:
                page.close()
                session["active_index"] = max(min(session["active_index"], len(context.pages) - 1), 0)
        elif operation != "list":
            raise ValueError("Browser tab operation must be list, open, switch, or close.")
        pages = context.pages
        return {
            "active_tab_id": f"tab-{session['active_index'] + 1}" if pages else None,
            "tabs": [
                {"id": f"tab-{index + 1}", "url": page.url, "title": page.title()} for index, page in enumerate(pages)
            ],
        }

    def extract(self, session_id: str, selector: str, attribute: str | None) -> dict[str, Any]:
        return self._call(self._extract, session_id, selector, attribute)

    def _extract(self, session_id: str, selector: str, attribute: str | None) -> dict[str, Any]:
        locator = self._page(session_id).locator(selector)
        count = min(locator.count(), 1000)
        if attribute:
            values = [locator.nth(index).get_attribute(attribute) for index in range(count)]
        else:
            values = [locator.nth(index).inner_text() for index in range(count)]
        return {"selector": selector, "attribute": attribute, "values": values, "count": count}

    def save_storage_state(self, session_id: str, path: Path) -> str:
        return self._call(self._save_storage_state, session_id, path)

    def _save_storage_state(self, session_id: str, path: Path) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._session(session_id)["context"].storage_state(path=str(path))
        return str(path)

    def screenshot(self, session_id: str, path: Path, full_page: bool) -> str:
        return self._call(self._screenshot, session_id, path, full_page)

    def _screenshot(self, session_id: str, path: Path, full_page: bool) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._page(session_id).screenshot(path=str(path), full_page=full_page)
        return str(path)


class CamoufoxEngine(PatchrightEngine):
    """Optional Camoufox engine implementing the same session contract."""

    def __init__(self) -> None:
        super().__init__()
        self._launchers: dict[str, Any] = {}

    def open(self, *, session_id: str, profile_dir: Path, headless: bool) -> None:
        try:
            self._call(self._open_camoufox, session_id=session_id, profile_dir=profile_dir, headless=headless)
        except Exception:
            self._shutdown_worker()
            raise

    def _open_camoufox(self, *, session_id: str, profile_dir: Path, headless: bool) -> None:
        try:
            from camoufox.sync_api import Camoufox
        except ImportError as exc:
            raise RuntimeError(
                "Camoufox browser tests require the optional 'camoufox' package and browser build. "
                "Install camoufox and run 'python -m camoufox fetch'."
            ) from exc

        profile_dir.mkdir(parents=True, exist_ok=True)
        launcher = Camoufox(
            headless=bool(headless),
            persistent_context=True,
            user_data_dir=str(profile_dir),
        )
        context = launcher.__enter__()
        if not context.pages:
            context.new_page()
        self._sessions[session_id] = {"context": context, "active_index": 0}
        self._launchers[session_id] = launcher

    def close(self, session_id: str) -> None:
        is_idle = self._call(self._close_camoufox, session_id)
        if is_idle:
            self._shutdown_worker()

    def _close_camoufox(self, session_id: str) -> bool:
        self._session(session_id)
        launcher = self._launchers.pop(session_id)
        self._sessions.pop(session_id, None)
        launcher.__exit__(None, None, None)
        return not self._sessions
