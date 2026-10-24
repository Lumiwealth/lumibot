"""Optional BotSpot research connection: the one-line hint and its setup helpers.

LumiBot strategy agents can attach BotSpot's read-only research server
(public macro data, SEC research, Marketplace and Community reads). BotSpot
attaches it automatically on hosted runs. External users have to link a
key themselves, and most never learn the option exists.

Invariants (Rob, 2026-10-20):
- Show the hint at most once per Python process, through the strategy logger.
- Stay silent in CI and when ``LUMIBOT_NO_BOTSPOT_HINT`` or ``DO_NOT_TRACK``
  is set. Coding agents usually run commands without a terminal, so a
  non-terminal is not a reason to hide it.
- State facts only. Log text that gives orders to an AI reads like prompt
  injection, so the hint never tells an agent what to do.
- Carry a tracked link so signups from it can be counted.
"""

from __future__ import annotations

import json
import os
import threading
import urllib.error
import urllib.request
from pathlib import Path

RESEARCH_MCP_URL = "https://api.botspot.trade/research-mcp"
OPT_OUT_ENV = "LUMIBOT_NO_BOTSPOT_HINT"
# Documented off switch for the startup tip (Rob, 2026-10-23). Either variable
# silences both the startup tip and the agent hint.
HIDE_TIP_ENV = "LUMIBOT_HIDE_BOTSPOT_TIP"
AGENTS_PAGE_URL = "https://botspot.trade/agents"
# Any of these means BotSpot is already linked (hosted runs set the research
# variables automatically; coding agents use BOTSPOT_API_KEY).
_CONNECTED_ENVS = ("BOTSPOT_RESEARCH_MCP_TOKEN", "BOTSPOT_RESEARCH_MCP_RENEW_URL", "BOTSPOT_API_KEY")
_API_KEYS_URL = "https://botspot.trade/account-settings/api-keys"
_UTM = "utm_source=lumibot&utm_medium={medium}&utm_campaign=mcp_nudge&utm_content={content}"
_TRUTHY_CI_ENVS = ("CI", "GITHUB_ACTIONS", "GITLAB_CI", "BUILDKITE", "CIRCLECI", "TF_BUILD")
_PRESENCE_CI_ENVS = ("JENKINS_URL",)
_FALSE_VALUES = {"", "0", "false", "no", "off"}

_lock = threading.Lock()
_shown = False


def connect_url(medium: str = "log", content: str = "agent_manager") -> str:
    return f"{_API_KEYS_URL}?{_UTM.format(medium=medium, content=content)}"


def _is_true(value: str | None) -> bool:
    return value is not None and value.strip().lower() not in _FALSE_VALUES


def hint_suppressed(environ=None) -> bool:
    env = os.environ if environ is None else environ
    if _is_true(env.get(OPT_OUT_ENV)) or _is_true(env.get(HIDE_TIP_ENV)) or _is_true(env.get("DO_NOT_TRACK")):
        return True
    if any(_is_true(env.get(name)) for name in _TRUTHY_CI_ENVS):
        return True
    return any(env.get(name) for name in _PRESENCE_CI_ENVS)


def claim_unlinked_hint() -> bool:
    """Return True exactly once per process, unless the hint is suppressed."""
    global _shown
    if hint_suppressed():
        return False
    with _lock:
        if _shown:
            return False
        _shown = True
        return True


def _reset_for_tests() -> None:
    global _shown
    with _lock:
        _shown = False


def botspot_connected(environ=None) -> bool:
    env = os.environ if environ is None else environ
    return any(str(env.get(name) or "").strip() for name in _CONNECTED_ENVS)


def startup_tip_message() -> str:
    link = f"{AGENTS_PAGE_URL}?{_UTM.format(medium='log', content='startup_tip')}"
    rule = "=" * 72
    return (
        f"\n{rule}\n"
        "BotSpot MCP is not connected. Connect it free to give your agents skills, free data\n"
        "(news, economic data, SEC filings), community and marketplace access.\n"
        f"  {link}\n"
        "  Or run: lumibot connect botspot\n"
        f"Hide this message: {HIDE_TIP_ENV}=1\n"
        f"{rule}"
    )


def show_startup_tip(log=None) -> bool:
    """Log the startup tip once per run when BotSpot is not connected.

    Called at the start of ``Trader.run_all`` (backtests and live runs). It
    shares the once-per-process claim with the agent hint, so a run shows one
    BotSpot message at most. It never raises.
    """
    try:
        if botspot_connected() or not claim_unlinked_hint():
            return False
        if log is None:
            import logging

            log = logging.getLogger("lumibot")
        log.warning(startup_tip_message())
        return True
    except Exception:
        return False


def unlinked_research_message() -> str:
    return (
        "BotSpot research is not connected (optional, free BotSpot account). It gives strategy "
        "agents public macro data, SEC research, and BotSpot Marketplace and Community reads. "
        "It is attached automatically on BotSpot; external LumiBot users can link a BotSpot account. "
        f"Set up: run `lumibot connect botspot`, or create a Research key at {connect_url()} and set "
        f"BOTSPOT_RESEARCH_MCP_URL={RESEARCH_MCP_URL} and BOTSPOT_RESEARCH_MCP_TOKEN. "
        "Ordinary LumiBot tools and strategy execution remain available. "
        f"Hide this message: {OPT_OUT_ENV}=1."
    )


def verify_research_key(url: str, token: str, timeout: float = 15.0) -> tuple[bool, str]:
    """Send one MCP ``initialize`` request. Any non-auth answer proves the key."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "lumibot-connect", "version": "1"},
        },
    }
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return True, f"HTTP {response.status}"
    except urllib.error.HTTPError as error:
        if error.code in (401, 403):
            return False, f"HTTP {error.code}: BotSpot rejected this key"
        # The server authenticated the request but disliked the handshake.
        return True, f"HTTP {error.code}"
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        return False, f"could not reach {url}: {error}"


# ---- device login (OAuth 2.0 device authorization grant, RFC 8628) ----------
#
# `lumibot connect botspot` gets a key without a website trip or copy and
# paste: it shows a short code, the person approves it once (in an AI app
# connected to BotSpot, or on the BotSpot API keys page), and the key arrives
# straight in the .env file. The key is never printed.

API_URL_ENV = "BOTSPOT_API_URL"
_DEFAULT_API_URL = "https://api.botspot.trade"
DEVICE_CLIENT_NAME = "LumiBot CLI"
DEVICE_PRESET = "lumibot_agent"


def api_base_url(environ=None) -> str:
    """BotSpot API origin; ``BOTSPOT_API_URL`` points at another BotSpot environment."""
    env = os.environ if environ is None else environ
    value = str(env.get(API_URL_ENV) or "").strip() or _DEFAULT_API_URL
    return value.rstrip("/")


def research_mcp_url_for(api_base: str) -> str:
    return f"{api_base.rstrip('/')}/research-mcp"


def _post_json(url: str, body: dict, timeout: float = 30.0) -> dict:
    """POST JSON and return the JSON answer, including RFC 8628 400 error bodies."""
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        try:
            answer = json.loads(error.read() or b"{}")
        except ValueError:
            answer = {}
        if isinstance(answer, dict) and answer.get("error"):
            return answer
        return {"error": "http_error", "error_description": f"HTTP {error.code}", "status": error.code}


def start_device_login(api_base: str, client_name: str) -> dict:
    return _post_json(
        f"{api_base}/mcp-api-keys/device/code",
        {"client_name": client_name, "preset": DEVICE_PRESET},
    )


def poll_device_login(api_base: str, device_code: str) -> dict:
    return _post_json(f"{api_base}/mcp-api-keys/device/token", {"device_code": device_code})


def _sleep(seconds: float) -> None:
    import time

    time.sleep(seconds)


def _same_origin(url: str, api_base: str) -> bool:
    from urllib.parse import urlparse

    left, right = urlparse(url), urlparse(api_base)
    return (left.scheme, left.netloc) == (right.scheme, right.netloc)


class DeviceLoginUnavailable(Exception):
    """This BotSpot server has no device login yet; use the key page flow instead."""


# HTTP answers that mean "this server has no device-login route" rather than
# "the login failed". An older BotSpot API answers 404 "Cannot POST".
_DEVICE_ROUTE_MISSING = {404, 405, 501}
# Upper bound on how long the command waits for approval, whatever the server says.
DEFAULT_DEVICE_TIMEOUT_SECONDS = 900


def run_device_login(
    api_base: str, out, err, open_browser=None, timeout: float | None = None
) -> tuple[str, str] | None:
    """Run the device login. Returns (research MCP URL, key) or None after a clear error.

    Raises ``DeviceLoginUnavailable`` when the server does not offer device
    login, so the caller can fall back to the key page flow.
    """
    try:
        started = start_device_login(api_base, DEVICE_CLIENT_NAME)
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as error:
        print(f"Could not reach BotSpot at {api_base}: {error}", file=err)
        return None
    if started.get("error") == "http_error" and started.get("status") in _DEVICE_ROUTE_MISSING:
        raise DeviceLoginUnavailable(str(started.get("error_description") or "not found"))
    if started.get("error") or not started.get("device_code") or not started.get("user_code"):
        detail = started.get("error_description") or started.get("error") or "no login code"
        print(f"BotSpot could not start the login ({detail}). Nothing was changed.", file=err)
        return None
    code = started["user_code"]
    page = started.get("verification_uri_complete") or started.get("verification_uri") or ""
    expires_in = int(started.get("expires_in") or 600)
    print("Connect BotSpot to your LumiBot strategy agents. Connecting is free.", file=out)
    print(f"Your login code: {code}", file=out)
    print("Approve it in one of two ways:", file=out)
    print(f"  1. Open {page}, sign in (a free BotSpot account is created if you do not have one) and approve the code.", file=out)
    print(f"  2. Or ask your AI app that is connected to BotSpot: \"approve BotSpot login code {code}\"", file=out)
    print(f"Waiting for approval (the code works for {max(1, round(expires_in / 60))} minutes)...", file=out, flush=True)
    if open_browser and page:
        try:
            open_browser(page)
        except Exception:  # noqa: BLE001 - a browser that cannot open is not fatal
            pass
    interval = int(started.get("interval") or 5)
    limit = DEFAULT_DEVICE_TIMEOUT_SECONDS if timeout is None else float(timeout)
    deadline = min(expires_in, limit)
    waited = 0
    while waited < deadline:
        _sleep(interval)
        waited += interval
        try:
            answer = poll_device_login(api_base, started["device_code"])
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            continue
        error = answer.get("error")
        if error == "authorization_pending":
            continue
        if error == "slow_down":
            interval += 5
            continue
        if error:
            reason = {
                "access_denied": "The login was denied.",
                "expired_token": "The login code expired or was already used. Run the command again.",
            }.get(error, answer.get("error_description") or error)
            print(f"{reason} Nothing was changed.", file=err)
            return None
        key = str(answer.get("api_key") or "")
        if not key:
            print("BotSpot did not return a key. Nothing was changed.", file=err)
            return None
        url = str(answer.get("research_mcp_url") or "")
        if not url or not _same_origin(url, api_base):
            url = research_mcp_url_for(api_base)
        return url, key
    print("The login was not approved in time. Nothing was changed. Run the command again.", file=err)
    return None


def write_env_values(env_file: Path, values: dict[str, str]) -> None:
    """Set ``KEY=value`` lines in a dotenv file, replacing old values in place."""
    lines = env_file.read_text().splitlines() if env_file.exists() else []
    remaining = dict(values)
    updated = []
    for line in lines:
        key = line.split("=", 1)[0].strip().removeprefix("export ").strip()
        if key in values:
            if key in remaining:
                updated.append(f"{key}={remaining.pop(key)}")
            continue
        updated.append(line)
    updated.extend(f"{key}={value}" for key, value in remaining.items())
    env_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write("\n".join(updated) + "\n")
    os.chmod(env_file, 0o600)
