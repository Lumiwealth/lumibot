"""Sign in with ChatGPT using the public OAuth and Responses interfaces.

Credentials belong to the local user, outside source control. No API-key or
Codex-session fallback is provided. Select ``chatgpt/<model slug>`` explicitly.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import stat
import time
import webbrowser
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit
from uuid import UUID, uuid4

import httpx
from filelock import FileLock

AUTH = "https://auth.openai.com"
RESOURCE = "https://api.openai.com/v1"
PLAN_SCOPE = "chatgpt.tokens.use.direct"


class ChatGPTAuthorizationError(RuntimeError):
    code = "provider_auth_failed"


class ChatGPTRefreshError(RuntimeError):
    def __init__(self, status_code: int):
        self.status_code = status_code
        super().__init__(f"ChatGPT refresh failed (HTTP {status_code}); no API fallback")


def config_root() -> Path:
    return Path(os.environ.get("LUMIBOT_CHATGPT_HOME") or str(Path.home() / ".config" / "lumibot-chatgpt")).expanduser()


def _private_parent(path: Path) -> None:
    if os.name != "posix":
        return
    parent = path.parent.absolute()
    resolved = parent.resolve(strict=True)
    pending = [parent, *parent.parents, resolved, *resolved.parents]
    checked = set()
    while pending:
        directory = pending.pop()
        if directory in checked:
            continue
        checked.add(directory)
        info = directory.lstat()
        trusted_owner = info.st_uid in (0, os.getuid())
        # Sticky system temp directories cannot rename another owner's child.
        sticky_system = info.st_uid == 0 and bool(info.st_mode & stat.S_ISVTX)
        if not trusted_owner or (
            not stat.S_ISLNK(info.st_mode) and info.st_mode & 0o022 and not sticky_system
        ):
            raise ChatGPTAuthorizationError("ChatGPT directory must be writable only by its owner")
        if stat.S_ISLNK(info.st_mode):
            target = Path(os.readlink(directory))
            if not target.is_absolute():
                target = directory.parent / target
            pending.extend([target, *target.parents])


def _private_metadata(info: os.stat_result, *, permission_mask: int = 0o077) -> None:
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or (
        os.name == "posix" and (info.st_uid != os.getuid() or info.st_mode & permission_mask)
    ):
        raise ChatGPTAuthorizationError("ChatGPT file must be an owner-only regular file with one link")


@contextmanager
def private_lock(path: Path, *, timeout: int = 60):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    _private_parent(path)
    if path.exists() or path.is_symlink():
        # Legacy locks can be readable; only their owner may write or replace them.
        _private_metadata(path.lstat(), permission_mask=0o022)
    with FileLock(str(path), mode=0o600, timeout=timeout):
        yield


def save_private(path: Path, value: dict) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    _private_parent(path)
    temporary = path.with_name(path.name + "." + secrets.token_hex(8))
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_private(path: Path) -> dict:
    _private_parent(path)
    _private_metadata(path.lstat())
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor) as stream:
        _private_metadata(os.fstat(stream.fileno()))
        value = json.load(stream)
    if not isinstance(value, dict):
        raise RuntimeError("Invalid ChatGPT profile")
    return value


def profile_path() -> Path:
    explicit = os.environ.get("LUMIBOT_CHATGPT_PROFILE_PATH")
    if explicit:
        return Path(explicit).expanduser()
    selected = os.environ.get("LUMIBOT_CHATGPT_PROFILE")
    if not selected:
        try:
            selected = read_private(config_root() / "active.json")["profile_id"]
        except (OSError, ValueError, KeyError) as exc:
            raise ChatGPTAuthorizationError("Run python -m lumibot.components.agents.chatgpt_auth login; no API fallback") from exc
    profile_id = str(UUID(selected))
    os.environ.setdefault("LUMIBOT_CHATGPT_PROFILE", profile_id)
    return config_root() / "profiles" / (profile_id + ".json")


# When one connected account reaches its plan usage limit, the next connected
# account in the same home takes over. A shared pool.json records the refused
# account for an hour so other calls and processes skip it. The plan endpoint
# gives no reset time.
USAGE_LIMIT_CODES = frozenset({"subscription_sharing_usage_limit_exceeded", "usage_limit_reached"})
USAGE_LIMIT_COOLDOWN_SECONDS = 3600


def _account_key(path: Path) -> str:
    try:
        return str(read_private(path).get("email") or path.stem)
    except (OSError, ValueError, RuntimeError):
        return path.stem


def _pool_cooldowns() -> dict:
    try:
        cooldowns = read_private(config_root() / "pool.json").get("cooldowns", {})
    except (OSError, ValueError, AttributeError):
        return {}
    return cooldowns if isinstance(cooldowns, dict) else {}


def is_cooling(path: Path) -> bool:
    return float(_pool_cooldowns().get(_account_key(path), 0)) > time.time()


def mark_exhausted(path: Path) -> None:
    pool = config_root() / "pool.json"
    with private_lock(config_root() / "pool.lock"):
        try:
            state = read_private(pool)
        except (OSError, ValueError):
            state = {}
        if not isinstance(state, dict):
            state = {}
        existing = state.get("cooldowns")
        cooldowns = dict(existing) if isinstance(existing, dict) else {}
        cooldowns[_account_key(path)] = time.time() + USAGE_LIMIT_COOLDOWN_SECONDS
        state["cooldowns"] = cooldowns
        save_private(pool, state)


def next_connected_profile(current: Path) -> Path | None:
    """Another connected plan account in this home that is not cooling down."""
    skip = _account_key(current)
    for path in sorted((config_root() / "profiles").glob("*.json")):
        try:
            record = read_private(path)
        except (OSError, ValueError, RuntimeError):
            continue
        if record.get("status") != "connected" or PLAN_SCOPE not in record.get("scopes", []):
            continue
        if _account_key(path) != skip and not is_cooling(path):
            return path
    return None


def access_token(path: Path) -> str:
    """Serialize rotating refresh tokens, including across independent workers."""
    # Check permissions before opening a sibling lock or contacting OpenAI.
    _private_parent(path)
    _private_metadata(path.lstat())
    with private_lock(Path(str(path) + ".lock")):
        record = read_private(path)
        if record.get("credential_kind") == "run":
            from .chatgpt_run import run_profile
            return str(run_profile(path)["access_token"])
        if record.get("status") != "connected" or PLAN_SCOPE not in record.get("scopes", []):
            raise ChatGPTAuthorizationError("ChatGPT plan permission is missing; no API fallback")
        if time.time() >= float(record["saved_at"]) + float(record["expires_in"]) - 60:
            response = httpx.post(AUTH + "/api/accounts/oauth/token", data={
                "grant_type": "refresh_token", "client_id": record["client_id"],
                "refresh_token": record["refresh_token"], "resource": RESOURCE,
            }, timeout=30)
            if response.status_code != 200:
                raise ChatGPTRefreshError(response.status_code)
            replacement = response.json()
            if not all(replacement.get(key) for key in ("access_token", "refresh_token", "expires_in")):
                raise RuntimeError("Incomplete ChatGPT refresh; no API fallback")
            record.update(replacement)
            record["saved_at"] = time.time()
            record["scopes"] = replacement.get("scope", " ".join(record["scopes"])).split()
            save_private(path, record)
            if PLAN_SCOPE not in record["scopes"]:
                raise ChatGPTAuthorizationError("ChatGPT plan permission was withdrawn")
        return str(record["access_token"])


def available_models(path: Path | None = None) -> list[dict]:
    response = httpx.get(RESOURCE + "/models", headers={"Authorization": "Bearer " + access_token(path or profile_path())}, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"ChatGPT model discovery failed (HTTP {response.status_code})")
    models = response.json().get("models")
    if not isinstance(models, list):
        raise RuntimeError("Invalid ChatGPT subscription model catalog")
    return [model for model in models if isinstance(model, dict) and model.get("visibility") == "list"]


def validate_callback(params: dict[str, list[str]], state: str, expected_client: str | None = None) -> tuple[str, str]:
    if params.get("state") != [state]:
        raise ValueError("OAuth state mismatch")
    if "error" in params:
        raise ValueError("ChatGPT authorization was not granted")
    if len(params.get("code", [])) != 1 or not params["code"][0]:
        raise ValueError("Missing or ambiguous OAuth code")
    clients = params.get("client_id", [expected_client] if expected_client else [])
    if len(clients) != 1 or not clients[0] or clients[0] == "dynamic_agent_client":
        raise ValueError("Missing or ambiguous issued client ID")
    if expected_client and clients[0] != expected_client:
        raise ValueError("OAuth client mismatch")
    return params["code"][0], clients[0]


def login(*, root: Path | None = None, profile_id: str | None = None, new_profile: bool = False, open_browser: bool = True, timeout_seconds: int = 300) -> None:
    """Register once, or reconnect the selected registration and account."""
    import jwt

    root = root or config_root()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    host_path = root / "host.json"
    with private_lock(Path(str(host_path) + ".lock"), timeout=30):
        if not host_path.exists():
            save_private(host_path, {"ext_agent_host_id": "urn:uuid:" + str(uuid4())})
        host = read_private(host_path)["ext_agent_host_id"]
    previous = {}
    if not profile_id and not new_profile and (root / "active.json").exists():
        profile_id = read_private(root / "active.json")["profile_id"]
    if profile_id:
        profile_id = str(UUID(profile_id))
        previous = read_private(root / "profiles" / (profile_id + ".json"))
    expected_client = previous.get("client_id")
    state, nonce, verifier = (secrets.token_urlsafe(32) for _ in range(3))
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
    result: dict[str, list[str]] = {}

    class Callback(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            parsed = urlsplit(self.path)
            params = parse_qs(parsed.query)
            if parsed.path != "/auth/callback" or params.get("state") != [state]:
                self.send_error(400, "Invalid callback")
                return
            result.update(params)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            body = b"Authorization received. Return to LumiBot while identity verification finishes."
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    pending = root / "pending-browser.json"
    try:
        with HTTPServer(("127.0.0.1", 0), Callback) as server:
            server.timeout = 1
            redirect = f"http://127.0.0.1:{server.server_port}/auth/callback"
            arguments = {
                "client_id": expected_client or "dynamic_agent_client", "ext_agent_host_id": host,
                "response_type": "code", "redirect_uri": redirect,
                "scope": "openid profile email offline_access resource.invoke " + PLAN_SCOPE,
                "resource": RESOURCE, "state": state, "nonce": nonce,
                "code_challenge_method": "S256", "code_challenge": challenge,
            }
            if not expected_client:
                arguments["agent_name_hint"] = "LumiBot"
            elif previous.get("id_token"):
                arguments["id_token_hint"] = previous["id_token"]
            url = AUTH + "/oauth/authorize?" + urlencode(arguments)
            save_private(pending, {"url": url, "expires_at": time.time() + timeout_seconds})
            if open_browser:
                webbrowser.open(url)
            print("Waiting for ChatGPT sign-in. The private pending-browser.json contains the link for a forwarded loopback session.", flush=True)
            deadline = time.monotonic() + timeout_seconds
            while not result and time.monotonic() < deadline:
                server.handle_request()
        code, client_id = validate_callback(result, state, expected_client)
    finally:
        pending.unlink(missing_ok=True)
    profile_id = profile_id or str(uuid4())
    path = root / "profiles" / (profile_id + ".json")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Exchange/reconnect and rotating refresh share the same write owner. Do not
    # replace a working connection until the new identity/grant is validated.
    with private_lock(Path(str(path) + ".lock")):
        current = read_private(path) if path.exists() else {}
        if current.get("client_id") and current["client_id"] != client_id:
            raise ValueError("ChatGPT registration changed during sign-in")
        if current.get("status") != "connected":
            # A newly issued registration remains recoverable after failure.
            save_private(path, {**current, "client_id": client_id, "ext_agent_host_id": host, "status": "pending"})
        with httpx.Client(timeout=30) as client:
            response = client.post(AUTH + "/api/accounts/oauth/token", data={
                "grant_type": "authorization_code", "client_id": client_id, "code": code,
                "code_verifier": verifier, "redirect_uri": redirect, "resource": RESOURCE,
            })
            if response.status_code != 200:
                raise RuntimeError(f"ChatGPT authorization exchange failed (HTTP {response.status_code})")
            token = response.json()
            discovery = client.get(AUTH + "/.well-known/openid-configuration")
            discovery.raise_for_status()
            jwks_uri = discovery.json()["jwks_uri"]
            if urlsplit(jwks_uri).scheme != "https" or urlsplit(jwks_uri).hostname != "auth.openai.com":
                raise ValueError("Unexpected identity key host")
            key = jwt.PyJWKClient(jwks_uri).get_signing_key_from_jwt(token["id_token"])
            identity = jwt.decode(token["id_token"], key.key, algorithms=["RS256"], audience=client_id, issuer=AUTH,
                                  options={"require": ["exp", "iss", "aud", "sub", "nonce"]})
            subjects = {record["subject"] for record in (previous, current) if record.get("subject")}
            if identity["nonce"] != nonce or any(identity["sub"] != subject for subject in subjects):
                raise ValueError("ChatGPT identity or nonce mismatch")
            scopes = token.get("scope", "").split()
            if PLAN_SCOPE not in scopes or not all(token.get(key) for key in ("access_token", "refresh_token", "expires_in")):
                raise ValueError("Renewable ChatGPT plan permission is missing")
            save_private(path, {**token, "client_id": client_id, "ext_agent_host_id": host,
                                "subject": identity["sub"], "scopes": scopes,
                                "saved_at": time.time(), "status": "connected"})
            save_private(root / "active.json", {"profile_id": profile_id})
    print("ChatGPT connected. Run the models command to choose an available model.")


def logout(path: Path | None = None) -> bool:
    path = path or profile_path()
    with private_lock(Path(str(path) + ".lock")):
        record = read_private(path)
        revoked = False
        try:
            with httpx.Client(timeout=30) as client:
                discovery = client.get(AUTH + "/.well-known/openid-configuration")
                discovery.raise_for_status()
                endpoint = discovery.json()["revocation_endpoint"]
                if urlsplit(endpoint).scheme != "https" or urlsplit(endpoint).hostname != "auth.openai.com":
                    raise ValueError("Unexpected revocation host")
                for attempt in range(3):
                    response = client.post(endpoint, data={"token": record["refresh_token"],
                        "token_type_hint": "refresh_token", "client_id": record["client_id"]})
                    revoked = response.status_code == 200
                    if response.status_code < 500:
                        break
        except (httpx.HTTPError, ValueError, KeyError):
            revoked = False
        finally:
            # Keep stable registration/account identity. Do not retain tokens.
            save_private(path, {key: record[key] for key in ("client_id", "subject", "ext_agent_host_id") if key in record} | {"status": "disconnected"})
        return revoked


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["login", "status", "models", "logout"])
    parser.add_argument("--profile", help="Reconnect an existing profile registration")
    parser.add_argument("--new-profile", action="store_true", help="Create a separate registration for another account")
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "login":
            login(profile_id=args.profile, new_profile=args.new_profile, open_browser=not args.no_browser)
        elif args.command == "models":
            print(json.dumps(available_models()))
        elif args.command == "logout":
            print("Signed out. Revocation confirmed." if logout() else "Local credentials cleared; remote revocation was not confirmed.")
        else:
            record = read_private(profile_path())
            print(json.dumps({"status": record.get("status"), "plan_permission": PLAN_SCOPE in record.get("scopes", [])}))
    except Exception as error:
        # Provider exception messages may contain request credentials.
        print("ChatGPT operation failed: " + type(error).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
