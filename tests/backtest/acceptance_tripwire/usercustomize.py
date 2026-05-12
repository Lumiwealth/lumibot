"""
Acceptance backtests tripwire (test-only).

Goal
----
The 7 Strategy Library acceptance demos are expected to run with a *fully warm* S3 cache.
If anything tries to hit the Data Downloader during these tests, that is a cache regression and
the test should fail immediately.

How it works
------------
Python imports `usercustomize` at interpreter startup (after `site` + `sitecustomize`), if it is
present on `sys.path`. The acceptance test harness prepends this directory to `PYTHONPATH` for
the subprocess that runs each demo script.

We intentionally use `usercustomize` (not `sitecustomize`) so we do **not** shadow Homebrew's
own `sitecustomize.py`, which configures the interpreter's prefix and global site-packages.

When `LUMIBOT_ACCEPTANCE_TRIPWIRE=1` and `DATADOWNLOADER_BASE_URL` is set, this module patches
common HTTP entry points and aborts the subprocess as soon as a request targets the downloader.

Notes
-----
- This must not change production LumiBot behavior. It only applies to the subprocesses spawned
  by `tests/backtest/test_acceptance_backtests_ci.py`.
- The exit message is careful to avoid printing secret headers (API keys).
"""

from __future__ import annotations

import json
import os
import sys
from urllib.parse import urlparse


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_downloader_base_url(base_url: str) -> str:
    """Best-effort normalization to match LumiBot's downloader client behavior."""
    normalized = (base_url or "").strip().rstrip("/")
    if not normalized:
        return ""

    if "://" not in normalized:
        normalized = f"http://{normalized}"

    parsed = urlparse(normalized)
    host = (parsed.hostname or "").strip()
    if not host:
        return normalized

    return normalized


def _matches_downloader(url: str, normalized_base_url: str) -> bool:
    if not url:
        return False
    url = str(url)
    base = normalized_base_url.rstrip("/")
    return url.startswith(base)


def _install_tripwire() -> None:
    if not _truthy(os.environ.get("LUMIBOT_ACCEPTANCE_TRIPWIRE")):
        return

    raw_base_url = os.environ.get("DATADOWNLOADER_BASE_URL", "")
    normalized_base_url = _normalize_downloader_base_url(raw_base_url)
    if not normalized_base_url:
        return

    def _raise(method: str, url: str, *, payload: object | None = None) -> None:
        parsed = urlparse(url)
        safe_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            safe_url = f"{safe_url}?{parsed.query}"
        message = (
            "[ACCEPTANCE][TRIPWIRE] Attempted to call Data Downloader "
            f"({method} {safe_url}). Expected fully warm S3 cache; downloader usage is forbidden in acceptance tests."
        )
        if payload is not None:
            try:
                rendered = json.dumps(payload, sort_keys=True, default=str)
            except Exception:
                rendered = repr(payload)
            if len(rendered) > 2000:
                rendered = rendered[:2000] + "…"
            message = f"{message}\n[ACCEPTANCE][TRIPWIRE] request_payload={rendered}"
        # Fail-fast in the subprocess.
        #
        # Some Strategy Library demos (and occasionally framework code) use broad exception handling.
        # Even `SystemExit` can be swallowed by a bare `except Exception:`; use a hard process-exit so the
        # acceptance subprocess cannot "continue with missing data" and produce misleading results.
        sys.stderr.write(message + "\n")
        try:
            sys.stderr.flush()
        except Exception:
            pass
        os._exit(86)  # non-zero exit code so the harness fails reliably

    # Patch requests (used by the downloader client).
    try:
        import requests

        original_request = requests.sessions.Session.request

        def patched_request(self, method, url, *args, **kwargs):  # type: ignore[no-untyped-def]
            if isinstance(url, str) and _matches_downloader(url, normalized_base_url):
                body = kwargs.get("json")
                if isinstance(body, dict):
                    # Queue submissions are usually of the form {"path": "...", "params": {...}, ...}.
                    # Only include request-shape metadata (never headers) so the failure is actionable.
                    body = {
                        k: body.get(k)
                        for k in ("path", "query_params", "params", "correlation", "position")
                        if k in body
                    }
                _raise(str(method), url, payload=body)
            return original_request(self, method, url, *args, **kwargs)

        requests.sessions.Session.request = patched_request  # type: ignore[assignment]
    except Exception:
        pass

    # Patch stdlib urllib as a fallback.
    try:
        import urllib.request as urllib_request

        original_urlopen = urllib_request.urlopen

        def patched_urlopen(url, *args, **kwargs):  # type: ignore[no-untyped-def]
            real_url = getattr(url, "full_url", url)
            if isinstance(real_url, str) and _matches_downloader(real_url, normalized_base_url):
                _raise("urlopen", real_url)
            return original_urlopen(url, *args, **kwargs)

        urllib_request.urlopen = patched_urlopen  # type: ignore[assignment]
    except Exception:
        pass


_install_tripwire()
