"""Explicit Python owner login and migration; broker startup never creates wallets."""

import base64
import json
import os
import sqlite3
import stat
import webbrowser
from getpass import getpass
from pathlib import Path

from .auth import TokenManager
from .journal import sqlite_busy
from .provider import Credentials, MetaMaskError, identifier, json_object

LOGIN_URL = "https://developer.metamask.io/agentic/login"


def login(session_file, *, project_id=None, browser=True):
    """Authorize in MetaMask's browser page, then save a private Python session.

    Use the original Google/email method to retain the expected wallet identity.
    This neither creates a wallet nor changes provider Guard policy.
    """
    path = Path(session_file).expanduser()
    if path.exists():
        raise ValueError("Choose a new session file for reconnect; preserve the existing recovery record")
    if browser:
        webbrowser.open(LOGIN_URL)
    envelope = getpass(f"Authorize at {LOGIN_URL}, then paste the CLI token: ").strip()
    access = envelope.split(":", 1)[0]
    if project_id is None:
        try:
            segment = access.split(".")[1]
            claims = json_object(json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))))
            project_id = identifier(claims.get("sub"), "project identity")
        except (ValueError, IndexError):
            raise MetaMaskError(
                "AUTHENTICATION_REQUIRED", "Browser token lacks a verifiable project identity"
            ) from None
    credentials = Credentials.from_config({"PROJECT_ID": project_id, "CLI_TOKEN": envelope})
    verifier = TokenManager(credentials)
    try:
        verifier.verify_identity(credentials)
    finally:
        verifier.close()
    manager = TokenManager(credentials, path=path)
    manager.close()
    return {"SESSION_FILE": str(path), "PROJECT_ID": project_id}


def migrate_session(source_file, session_file):
    """Import an explicitly selected retired-service session, holding its fence.

    The source stays intact. Rotated credentials belong to the new session;
    return the TokenManager to the caller and close it after use.
    """
    source, destination = Path(source_file).expanduser(), Path(session_file).expanduser()
    if source.resolve() == destination.resolve():
        raise ValueError("Native session must use a distinct path from the legacy session")
    metadata = source.parent.stat()
    if metadata.st_uid != os.getuid() or metadata.st_mode & 0o022:
        raise MetaMaskError("UNSAFE_CREDENTIAL_DIRECTORY", "Legacy session directory must be owner-controlled")
    owner_path = source.parent / "session.owner.sqlite"
    if owner_path.exists():
        info = owner_path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
            raise MetaMaskError("UNSAFE_CREDENTIAL_FILE", "Legacy session fence is not an owned regular file")
    owner = sqlite3.connect(owner_path, timeout=0, check_same_thread=False)
    try:
        os.chmod(owner_path, 0o600)
        owner.executescript(
            "PRAGMA journal_mode=DELETE; CREATE TABLE IF NOT EXISTS owner(id INTEGER); BEGIN EXCLUSIVE;"
        )
        if destination.exists():
            manager = TokenManager(path=destination)
        else:
            descriptor = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            with os.fdopen(descriptor) as stream:
                info = os.fstat(stream.fileno())
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_nlink != 1
                    or info.st_uid != os.getuid()
                    or info.st_mode & 0o077
                ):
                    raise MetaMaskError("UNSAFE_CREDENTIAL_FILE", "Legacy session must be an owner-only regular file")
                raw = stream.read(1_000_001)
            if len(raw) > 1_000_000:
                raise MetaMaskError("INVALID_SESSION_FILE", "Legacy session is oversized")
            record = json_object(json.loads(raw), "legacy session")
            data = json_object(record.get("data"), "legacy session data")
            if record.get("schemaVersion") != "1.0.0":
                raise MetaMaskError("INVALID_SESSION_FILE", "Unsupported legacy session schema")
            credentials = Credentials.from_config(
                {
                    "PROJECT_ID": data.get("projectId"),
                    "ACCESS_TOKEN": data.get("cliToken"),
                    "REFRESH_TOKEN": data.get("cliRefreshToken"),
                }
            )
            manager = TokenManager(credentials, path=destination)
        manager.legacy_owner = owner
        return manager
    except sqlite3.OperationalError as error:
        owner.close()
        if sqlite_busy(error):
            raise MetaMaskError(
                "STATE_IN_USE", "Retired service still owns its session; preserve its active runtime"
            ) from None
        raise
    except BaseException:
        owner.close()
        raise
