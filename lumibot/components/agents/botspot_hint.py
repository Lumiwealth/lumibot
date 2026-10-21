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
    if _is_true(env.get(OPT_OUT_ENV)) or _is_true(env.get("DO_NOT_TRACK")):
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
