"""Native public-client OAuth refresh with durable token-rotation recovery."""

from __future__ import annotations

import base64
import json
import math
import os
import stat
import tempfile
import time
from collections.abc import Callable
from dataclasses import asdict, replace
from pathlib import Path
from threading import RLock
from urllib.parse import urlsplit

import requests
from filelock import FileLock

from .provider import Credentials, MetaMaskError, json_object

ISSUER = "https://oidc.api.cx.metamask.io"
CLIENT_ID = "cli-agent-client"


def _token_expiry(token: str) -> float | None:
    """Read a scheduling hint only; decoded claims never establish identity."""
    try:
        segment = token.split(".")[1]
        document = json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))
        value = document.get("exp")
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            return None
        return float(value)
    except (ValueError, IndexError, TypeError, AttributeError):
        return None


class TokenManager:
    """Explicit credentials or an explicitly selected private session file.

    A timed-out refresh can rotate a single-use refresh token. Persist that
    uncertainty before transmission and require reconnect rather than replay.
    """

    def __init__(
        self,
        credentials: Credentials | None = None,
        *,
        path: Path | None = None,
        session: requests.Session | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self.path = Path(path) if path is not None else None
        self.session = session if session is not None else requests.Session()
        self.session.trust_env = False
        self._owns_session = session is None
        self._clock = clock
        self._lock = RLock()
        self._metadata: dict[str, object] | None = None
        self._refresh_pending = False
        self._credentials = credentials
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            parent = self.path.parent.stat()
            if parent.st_uid != os.getuid() or parent.st_mode & 0o022:
                raise MetaMaskError(
                    "UNSAFE_CREDENTIAL_DIRECTORY", "Session directory must be writable only by its owner"
                )
            if Path(str(self.path) + ".lock").is_symlink():
                raise MetaMaskError("UNSAFE_CREDENTIAL_FILE", "Credential lock must not be a symlink")
            self._file_lock = FileLock(str(self.path) + ".lock", mode=0o600)
            with self._file_lock:
                if self.path.exists():
                    if credentials is not None:
                        raise ValueError(
                            "Use existing SESSION_FILE or new credentials; do not overwrite an existing session"
                        )
                    self._load()
                elif credentials is None:
                    raise ValueError("SESSION_FILE does not exist")
                else:
                    self._save()
        else:
            self._file_lock = None
        if self._credentials is None:
            raise ValueError("Explicit MetaMask credentials or SESSION_FILE is required")

    def _load(self):
        if self.path is None:
            return
        try:
            descriptor = os.open(self.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            with os.fdopen(descriptor) as stream:
                metadata = os.fstat(stream.fileno())
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_nlink != 1
                    or metadata.st_uid != os.getuid()
                    or metadata.st_mode & 0o077
                ):
                    raise MetaMaskError(
                        "UNSAFE_CREDENTIAL_FILE", "Session must be an owner-only, unlinked regular file"
                    )
                raw = stream.read(1_000_001)
            if len(raw) > 1_000_000:
                raise ValueError("Oversized session file")
            record = json_object(json.loads(raw), "session file")
            if record.get("version") != 1 or not isinstance(record.get("refresh_pending"), bool):
                raise ValueError("Unsupported session schema")
            self._credentials = Credentials.from_config(
                {
                    "PROJECT_ID": record.get("project_id"),
                    "ACCESS_TOKEN": record.get("access_token"),
                    "REFRESH_TOKEN": record.get("refresh_token"),
                    "EXPIRES_AT": record.get("expires_at"),
                }
            )
            self._refresh_pending = record["refresh_pending"]
        except (ValueError, TypeError):
            raise MetaMaskError("INVALID_SESSION_FILE", "Session schema is invalid; reconnect explicitly") from None

    def _save(self):
        if self.path is None or self._credentials is None:
            return
        record = {"version": 1, **asdict(self._credentials), "refresh_pending": self._refresh_pending}
        descriptor, temporary = tempfile.mkstemp(prefix=".metamask-session-", dir=self.path.parent)
        try:
            with os.fdopen(descriptor, "w") as stream:
                json.dump(record, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            if os.name != "nt":
                directory = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def _discover(self) -> str:
        if self._metadata is None:
            try:
                response = self.session.request(
                    "GET",
                    ISSUER + "/.well-known/openid-configuration",
                    headers={"Accept": "application/json"},
                    timeout=(5, 20),
                    allow_redirects=False,
                )
            except requests.RequestException:
                raise MetaMaskError("AUTHENTICATION_UNAVAILABLE", "MetaMask identity metadata is unavailable") from None
            if response.status_code != 200:
                raise MetaMaskError("AUTHENTICATION_UNAVAILABLE", "MetaMask identity discovery did not succeed")
            try:
                self._metadata = json_object(response.json(), "OIDC discovery")
            except ValueError:
                raise MetaMaskError("INVALID_AUTH_METADATA", "Identity discovery returned invalid JSON") from None
        endpoint = self._metadata.get("token_endpoint")
        if self._metadata.get("issuer") != ISSUER or not isinstance(endpoint, str):
            raise MetaMaskError("INVALID_AUTH_METADATA", "Identity issuer or token endpoint is invalid")
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme != "https"
            or parsed.hostname != urlsplit(ISSUER).hostname
            or parsed.port not in (None, 443)
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise MetaMaskError("INVALID_AUTH_METADATA", "Token endpoint differs from the trusted HTTPS issuer")
        return endpoint

    def _current(self) -> Credentials:
        if self._credentials is None:
            raise MetaMaskError("AUTHENTICATION_REQUIRED", "No MetaMask credentials are available")
        if self._refresh_pending:
            raise MetaMaskError(
                "REFRESH_OUTCOME_UNKNOWN", "Previous token rotation has an unknown outcome; reconnect explicitly"
            )
        current = self._credentials
        expiry = current.expires_at if current.expires_at is not None else _token_expiry(current.access_token)
        if expiry is None or expiry > self._clock() + 60:
            return replace(current, expires_at=expiry)
        if not current.refresh_token:
            raise MetaMaskError("AUTHENTICATION_REQUIRED", "MetaMask access token expired without a refresh credential")
        if self.path is None:
            raise MetaMaskError("DURABLE_SESSION_REQUIRED", "Automatic token rotation requires a private SESSION_FILE")
        endpoint = self._discover()
        self._refresh_pending = True
        self._save()
        try:
            response = self.session.request(
                "POST",
                endpoint,
                data={"grant_type": "refresh_token", "refresh_token": current.refresh_token, "client_id": CLIENT_ID},
                headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
                timeout=(5, 45),
                allow_redirects=False,
            )
        except requests.RequestException:
            raise MetaMaskError(
                "REFRESH_OUTCOME_UNKNOWN", "Token rotation lost its response; reconnect without replay"
            ) from None
        if response.status_code != 200:
            raise MetaMaskError(
                "AUTHENTICATION_REQUIRED", "MetaMask rejected or could not confirm token rotation; reconnect"
            )
        try:
            result = json_object(response.json(), "token response")
            token = result.get("access_token")
            token_type = result.get("token_type")
            refresh = result.get("refresh_token", current.refresh_token)
            lifetime = result.get("expires_in")
            if (
                not isinstance(token, str)
                or not token
                or not isinstance(token_type, str)
                or token_type.lower() != "bearer"
            ):
                raise ValueError("Invalid access token")
            if not isinstance(refresh, str) or not refresh:
                raise ValueError("Invalid refresh token")
            if lifetime is not None and (
                isinstance(lifetime, bool)
                or not isinstance(lifetime, (int, float))
                or not math.isfinite(lifetime)
                or lifetime <= 0
            ):
                raise ValueError("Invalid token lifetime")
            expiry = self._clock() + lifetime if lifetime is not None else _token_expiry(token)
            renewed = Credentials(current.project_id, token, refresh, expiry)
        except (ValueError, TypeError, MetaMaskError):
            raise MetaMaskError(
                "REFRESH_OUTCOME_UNKNOWN", "Token rotation returned unusable credentials; reconnect"
            ) from None
        self._credentials = renewed
        self._save()
        self.verify_identity(renewed)
        self._refresh_pending = False
        self._save()
        return renewed

    def verify_identity(self, credentials: Credentials):
        try:
            segment = credentials.access_token.split(".")[1]
            claims = json_object(json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))))
            token_id = claims.get("jti")
            subject = claims.get("sub")
            if not isinstance(token_id, str) or not token_id or subject != credentials.project_id:
                raise ValueError("Identity claims differ")
            response = self.session.request(
                "POST",
                "https://authentication.api.cx.metamask.io/api/v2/token/introspect",
                json={"token": credentials.access_token, "jti": token_id},
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                timeout=(5, 20),
                allow_redirects=False,
            )
            if response.status_code != 200 or json_object(response.json()).get("active") is not True:
                raise ValueError("Refreshed token is not active")
        except (ValueError, IndexError, requests.RequestException, MetaMaskError):
            raise MetaMaskError(
                "REFRESH_IDENTITY_UNVERIFIED",
                "Rotated token identity is unverified; credentials retained for reconnect",
            ) from None

    def __call__(self) -> Credentials:
        with self._lock:
            if self._file_lock is not None:
                with self._file_lock:
                    self._load()
                    return self._current()
            return self._current()

    def close(self):
        if self._owns_session:
            self.session.close()
        if getattr(self, "legacy_owner", None) is not None:
            self.legacy_owner.close()
            self.legacy_owner = None
