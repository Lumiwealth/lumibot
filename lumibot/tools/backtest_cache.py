from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, Optional

from lumibot.constants import LUMIBOT_CACHE_FOLDER
from lumibot.credentials import CACHE_REMOTE_CONFIG
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)


class CacheMode(str, Enum):
    DISABLED = "disabled"
    S3_READWRITE = "s3_readwrite"
    S3_READONLY = "s3_readonly"


@dataclass(frozen=True)
class BacktestCacheSettings:
    backend: str
    mode: CacheMode
    bucket: Optional[str] = None
    prefix: str = ""
    region: Optional[str] = None
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    version: str = "v1"

    @staticmethod
    def from_env(env: Dict[str, Optional[str]]) -> Optional["BacktestCacheSettings"]:
        backend = (env.get("backend") or "local").strip().lower()
        mode_raw = (env.get("mode") or "disabled").strip().lower()

        if backend != "s3":
            return None

        if mode_raw in ("disabled", "off", "local"):
            return None

        if mode_raw in ("readwrite", "rw", "s3_readwrite"):
            mode = CacheMode.S3_READWRITE
        elif mode_raw in ("readonly", "ro", "s3_readonly"):
            mode = CacheMode.S3_READONLY
        else:
            raise ValueError(
                f"Unsupported LUMIBOT_CACHE_MODE '{mode_raw}'. "
                "Expected one of: disabled, readwrite, readonly."
            )

        bucket = (env.get("s3_bucket") or "").strip()
        if not bucket:
            raise ValueError("LUMIBOT_CACHE_S3_BUCKET must be set when using the S3 cache backend.")

        prefix = (env.get("s3_prefix") or "").strip().strip("/")
        region = (env.get("s3_region") or "").strip() or None
        access_key_id = (env.get("s3_access_key_id") or "").strip() or None
        secret_access_key = (env.get("s3_secret_access_key") or "").strip() or None
        session_token = (env.get("s3_session_token") or "").strip() or None
        version = (env.get("s3_version") or "v1").strip().strip("/")

        if not version:
            version = "v1"

        return BacktestCacheSettings(
            backend=backend,
            mode=mode,
            bucket=bucket,
            prefix=prefix,
            region=region,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            version=version,
        )


class _StubbedS3ErrorCodes:
    NOT_FOUND = {"404", "400", "NoSuchKey", "NotFound"}


class BacktestCacheManager:
    def __init__(
        self,
        settings: Optional[BacktestCacheSettings],
        client_factory: Optional[Callable[[BacktestCacheSettings], object]] = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory
        self._client = None
        self._client_lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return bool(self._settings and self._settings.mode != CacheMode.DISABLED)

    @property
    def mode(self) -> CacheMode:
        if not self.enabled:
            return CacheMode.DISABLED
        return self._settings.mode  # type: ignore[return-value]

    def ensure_local_file(
        self,
        local_path: Path,
        payload: Optional[Dict[str, object]] = None,
        force_download: bool = False,
    ) -> bool:
        if not self.enabled:
            return False

        if not isinstance(local_path, Path):
            local_path = Path(local_path)

        if local_path.exists() and not force_download:
            return False

        remote_key = self.remote_key_for(local_path, payload)
        if remote_key is None:
            return False

        client = self._get_client()
        tmp_path = local_path.with_suffix(local_path.suffix + ".s3tmp")
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            client.download_file(self._settings.bucket, remote_key, str(tmp_path))
            os.replace(tmp_path, local_path)
            logger.debug(
                "[REMOTE_CACHE][DOWNLOAD] %s -> %s", remote_key, local_path.as_posix()
            )
            return True
        except Exception as exc:  # pragma: no cover - narrow in helper
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)  # type: ignore[attr-defined]
            if self._is_not_found_error(exc):
                logger.debug(
                    "[REMOTE_CACHE][MISS] %s (reason=%s)", remote_key, self._describe_error(exc)
                )
                return False
            raise

    def on_local_update(
        self,
        local_path: Path,
        payload: Optional[Dict[str, object]] = None,
    ) -> bool:
        if not self.enabled or self.mode != CacheMode.S3_READWRITE:
            return False

        if not isinstance(local_path, Path):
            local_path = Path(local_path)

        if not local_path.exists():
            logger.warning(
                "[REMOTE_CACHE][UPLOAD_SKIP] Local file %s does not exist.", local_path.as_posix()
            )
            return False

        remote_key = self.remote_key_for(local_path, payload)
        if remote_key is None:
            return False

        client = self._get_client()
        client.upload_file(str(local_path), self._settings.bucket, remote_key)
        logger.debug(
            "[REMOTE_CACHE][UPLOAD] %s <- %s", remote_key, local_path.as_posix()
        )
        return True

    def remote_key_for(
        self,
        local_path: Path,
        payload: Optional[Dict[str, object]] = None,
    ) -> Optional[str]:
        if not self.enabled:
            return None

        if not isinstance(local_path, Path):
            local_path = Path(local_path)

        try:
            relative_path = local_path.resolve().relative_to(Path(LUMIBOT_CACHE_FOLDER).resolve())
        except ValueError:
            logger.debug(
                "[REMOTE_CACHE][SKIP] %s is outside the cache root.", local_path.as_posix()
            )
            return None

        components = [
            self._settings.prefix if self._settings and self._settings.prefix else None,
            self._settings.version if self._settings else None,
            relative_path.as_posix(),
        ]
        sanitized = [c.strip("/") for c in components if c]
        remote_key = "/".join(sanitized)
        return remote_key

    def _get_client(self):
        if not self.enabled:
            raise RuntimeError("Remote cache manager is disabled.")

        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    if self._client_factory:
                        self._client = self._client_factory(self._settings)
                    else:
                        self._client = self._create_s3_client()
        return self._client

    def _create_s3_client(self):
        try:
            import boto3  # type: ignore
        except ImportError as exc:  # pragma: no cover - exercised when boto3 missing
            raise RuntimeError(
                "S3 cache backend requires boto3. Install it or disable the remote cache."
            ) from exc

        session = boto3.session.Session(
            aws_access_key_id=self._settings.access_key_id,
            aws_secret_access_key=self._settings.secret_access_key,
            aws_session_token=self._settings.session_token,
            region_name=self._settings.region,
        )
        return session.client("s3")

    @staticmethod
    def _is_not_found_error(exc: Exception) -> bool:
        # Prefer botocore error codes if available
        response = getattr(exc, "response", None)
        if isinstance(response, dict):
            error = response.get("Error") or {}
            code = error.get("Code")
            if isinstance(code, str) and code in _StubbedS3ErrorCodes.NOT_FOUND:
                return True

        # Handle stubbed errors (FileNotFoundError or message-based)
        if isinstance(exc, FileNotFoundError):
            return True

        message = str(exc)
        for token in _StubbedS3ErrorCodes.NOT_FOUND:
            if token in message:
                return True
        return False

    @staticmethod
    def _describe_error(exc: Exception) -> str:
        response = getattr(exc, "response", None)
        if isinstance(response, dict):
            error = response.get("Error") or {}
            code = error.get("Code")
            message = error.get("Message")
            return f"{code}: {message}" if code or message else "unknown"
        return str(exc)


_MANAGER_LOCK = threading.Lock()
_MANAGER_INSTANCE: Optional[BacktestCacheManager] = None


def get_backtest_cache() -> BacktestCacheManager:
    global _MANAGER_INSTANCE
    if _MANAGER_INSTANCE is None:
        with _MANAGER_LOCK:
            if _MANAGER_INSTANCE is None:
                settings = BacktestCacheSettings.from_env(CACHE_REMOTE_CONFIG)
                _MANAGER_INSTANCE = BacktestCacheManager(settings)
    return _MANAGER_INSTANCE


def reset_backtest_cache_manager(for_testing: bool = False) -> None:
    """Reset the cached manager instance (intended for unit tests)."""
    global _MANAGER_INSTANCE
    with _MANAGER_LOCK:
        _MANAGER_INSTANCE = None
        if not for_testing:
            logger.debug("[REMOTE_CACHE] Manager reset requested.")
