from __future__ import annotations

import os
import threading
import time
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
        # When using the S3 backend we want "fresh per run" semantics (never trust an on-disk file that
        # may have been produced by a previous cache version), but re-downloading the same object
        # repeatedly within a single backtest is prohibitively slow. Track which remote keys have been
        # downloaded in this process so we can safely reuse the local copy for the remainder of the run.
        self._downloaded_remote_keys: set[str] = set()
        self._downloaded_remote_keys_lock = threading.Lock()
        # Negative cache for remote keys that are missing in S3. Some backtest code paths can
        # repeatedly request the same cache file (especially when coverage metadata thrashes).
        # Without a miss-cache we end up doing thousands of failing S3 calls, which can dominate
        # cold-local/warm-S3 production runs.
        self._missing_remote_keys: set[str] = set()
        self._missing_remote_keys_lock = threading.Lock()

        # Lightweight per-process accounting so we can quantify S3 hydration cost in production.
        # NOTE: This deliberately avoids logging per-object at INFO (too spammy); we log a single
        # summary line at the end of the backtest instead.
        self._stats_lock = threading.Lock()
        self._stats: Dict[str, float] = {
            "downloads": 0.0,
            "download_bytes": 0.0,
            "download_s": 0.0,
            "misses": 0.0,
            "local_reuse": 0.0,
            "inprocess_reuse": 0.0,
            "uploads": 0.0,
            "upload_bytes": 0.0,
            "upload_s": 0.0,
        }

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

        remote_key = self.remote_key_for(local_path, payload)
        if remote_key is None:
            return False

        if self._settings and self._settings.backend == "s3":
            marker_path = local_path.with_suffix(local_path.suffix + ".s3key")

            # If the file was already downloaded (or produced) for this exact remote key, allow reuse
            # across backtest runs. This preserves cache-version isolation because `remote_key`
            # already includes `self._settings.version`.
            if local_path.exists() and not force_download and marker_path.exists():
                try:
                    marker_value = marker_path.read_text(encoding="utf-8").strip()
                except Exception:
                    marker_value = ""

                if marker_value == remote_key:
                    with self._stats_lock:
                        self._stats["local_reuse"] += 1.0
                    with self._downloaded_remote_keys_lock:
                        self._downloaded_remote_keys.add(remote_key)
                    return False

            # If we've already observed this remote key missing in S3 during the current process,
            # don't keep re-hitting S3 (miss storms can dominate option-heavy runs).
            if not local_path.exists() and not force_download:
                with self._missing_remote_keys_lock:
                    if remote_key in self._missing_remote_keys:
                        return False

            with self._downloaded_remote_keys_lock:
                already_downloaded = remote_key in self._downloaded_remote_keys

            # S3 cache mode: ensure we download the object at least once per process (fresh run
            # semantics), but then allow local reuse to avoid repeated downloads during the same
            # backtest.
            if local_path.exists() and already_downloaded and not force_download:
                with self._stats_lock:
                    self._stats["inprocess_reuse"] += 1.0
                return False

            if local_path.exists():
                try:
                    local_path.unlink()
                except Exception:
                    pass
            if marker_path.exists():
                try:
                    marker_path.unlink()
                except Exception:
                    pass
            force_download = True
        elif local_path.exists() and not force_download:
            return False

        client = self._get_client()
        tmp_path = local_path.with_suffix(local_path.suffix + ".s3tmp")
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            started = time.perf_counter()
            if hasattr(client, "get_object"):
                # PERF: `boto3`'s `download_file` uses the high-level S3Transfer manager which can add
                # substantial overhead when hydrating thousands of *small* cache objects (common in
                # option-heavy backtests, where each contract has its own parquet file).
                #
                # Streaming `get_object` directly to disk avoids that overhead and is materially faster
                # for small objects. We still write via a temp file + atomic rename to keep the cache
                # consistent if a download is interrupted.
                response = client.get_object(Bucket=self._settings.bucket, Key=remote_key)
                body = response.get("Body")
                if body is None:
                    raise RuntimeError(f"S3 get_object missing Body for key={remote_key!r}")
                with tmp_path.open("wb") as handle:
                    while True:
                        chunk = body.read(1024 * 1024)
                        if not chunk:
                            break
                        handle.write(chunk)
                try:
                    body.close()
                except Exception:
                    pass
            else:
                # Test doubles may only implement the legacy `download_file` API.
                client.download_file(self._settings.bucket, remote_key, str(tmp_path))
            elapsed = time.perf_counter() - started
            downloaded_bytes = 0
            try:
                downloaded_bytes = tmp_path.stat().st_size
            except Exception:
                downloaded_bytes = 0
            os.replace(tmp_path, local_path)
            # Persist a tiny marker so future runs can reuse the file without an extra S3 roundtrip,
            # as long as the remote key (including cache version) matches.
            try:
                marker_path = local_path.with_suffix(local_path.suffix + ".s3key")
                marker_tmp = marker_path.with_suffix(marker_path.suffix + ".tmp")
                marker_path.parent.mkdir(parents=True, exist_ok=True)
                marker_tmp.write_text(remote_key, encoding="utf-8")
                os.replace(marker_tmp, marker_path)
            except Exception:
                pass
            logger.debug(
                "[REMOTE_CACHE][DOWNLOAD] %s -> %s", remote_key, local_path.as_posix()
            )
            with self._downloaded_remote_keys_lock:
                self._downloaded_remote_keys.add(remote_key)
            with self._stats_lock:
                self._stats["downloads"] += 1.0
                self._stats["download_s"] += float(elapsed)
                self._stats["download_bytes"] += float(downloaded_bytes)
            return True
        except Exception as exc:  # pragma: no cover - narrow in helper
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)  # type: ignore[attr-defined]
            if self._is_not_found_error(exc):
                logger.debug(
                    "[REMOTE_CACHE][MISS] %s (reason=%s)", remote_key, self._describe_error(exc)
                )
                with self._missing_remote_keys_lock:
                    self._missing_remote_keys.add(remote_key)
                # In S3 mode, we intentionally leave no local cache on a miss to force fresh fetch.
                if local_path.exists():
                    try:
                        local_path.unlink()
                    except Exception:
                        pass
                try:
                    marker_path = local_path.with_suffix(local_path.suffix + ".s3key")
                    if marker_path.exists():
                        marker_path.unlink()
                except Exception:
                    pass
                with self._stats_lock:
                    self._stats["misses"] += 1.0
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
        started = time.perf_counter()
        client.upload_file(str(local_path), self._settings.bucket, remote_key)
        elapsed = time.perf_counter() - started
        uploaded_bytes = 0
        try:
            uploaded_bytes = local_path.stat().st_size
        except Exception:
            uploaded_bytes = 0
        logger.debug(
            "[REMOTE_CACHE][UPLOAD] %s <- %s", remote_key, local_path.as_posix()
        )
        # Persist a tiny marker so future runs can reuse the file without an extra S3 roundtrip,
        # as long as the remote key (including cache version) matches.
        try:
            marker_path = local_path.with_suffix(local_path.suffix + ".s3key")
            marker_tmp = marker_path.with_suffix(marker_path.suffix + ".tmp")
            marker_path.parent.mkdir(parents=True, exist_ok=True)
            marker_tmp.write_text(remote_key, encoding="utf-8")
            os.replace(marker_tmp, marker_path)
        except Exception:
            pass
        with self._stats_lock:
            self._stats["uploads"] += 1.0
            self._stats["upload_s"] += float(elapsed)
            self._stats["upload_bytes"] += float(uploaded_bytes)
        return True

    def stats_snapshot(self) -> Dict[str, float]:
        """Return a copy of the current in-process stats (numbers only; safe for logs)."""
        with self._stats_lock:
            return dict(self._stats)

    def log_summary(self) -> None:
        """Emit a single INFO line describing cache hydration/upload cost for this run."""
        if not self.enabled:
            return
        snap = self.stats_snapshot()
        settings = self._settings
        if not settings:
            return
        logger.info(
            "[REMOTE_CACHE][SUMMARY] mode=%s bucket=%s prefix=%s version=%s "
            "downloads=%d misses=%d local_reuse=%d inprocess_reuse=%d download_bytes=%d download_s=%.3f "
            "uploads=%d upload_bytes=%d upload_s=%.3f",
            settings.mode,
            settings.bucket,
            settings.prefix,
            settings.version,
            int(snap.get("downloads", 0.0)),
            int(snap.get("misses", 0.0)),
            int(snap.get("local_reuse", 0.0)),
            int(snap.get("inprocess_reuse", 0.0)),
            int(snap.get("download_bytes", 0.0)),
            float(snap.get("download_s", 0.0)),
            int(snap.get("uploads", 0.0)),
            int(snap.get("upload_bytes", 0.0)),
            float(snap.get("upload_s", 0.0)),
        )

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
            from botocore.config import Config  # type: ignore
        except ImportError as exc:  # pragma: no cover - exercised when boto3 missing
            raise RuntimeError(
                "S3 cache backend requires boto3. Install it or disable the remote cache."
            ) from exc

        # IMPORTANT: boto3/urllib3 defaults can appear to "hang forever" under certain network
        # failure modes (stalled DNS, stalled TCP, etc). In production backtests we prefer bounded
        # waits + retries so a single cache upload cannot wedge an entire backtest.
        #
        # These defaults are conservative for intra-region S3 (small parquet chunks, many calls):
        # - 5s connect timeout, 60s read timeout per request/part
        # - standard retry with capped attempts
        client_config = Config(
            connect_timeout=5,
            read_timeout=60,
            retries={"max_attempts": 8, "mode": "standard"},
            max_pool_connections=50,
        )

        session = boto3.session.Session(
            aws_access_key_id=self._settings.access_key_id,
            aws_secret_access_key=self._settings.secret_access_key,
            aws_session_token=self._settings.session_token,
            region_name=self._settings.region,
        )
        return session.client("s3", config=client_config)

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
