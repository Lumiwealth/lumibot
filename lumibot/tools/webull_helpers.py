"""Webull OpenAPI transport and wire conventions."""
import logging
import os
import stat
import time
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from threading import Lock

PAPER_HOST = "api.sandbox.webull.com"
LIVE_HOST = "api.webull.com"
_SDK_INIT_LOCK = Lock()


class WebullAPIError(RuntimeError):
    """A redacted failure at the provider boundary."""

    def __init__(self, operation, *, status=None, code=None):
        self.status = status
        # The provider's free-form messages and response bodies can contain private data.
        self.code = (code if code and code.isascii() and code.isupper() and code.replace("_", "").isalnum()
                     and len(code) <= 100 else None)
        metadata = ", ".join(str(value) for value in (status, self.code) if value is not None)
        super().__init__(f"Webull {operation} failed" + (f" ({metadata})" if metadata else ""))

    @property
    def write_outcome_unknown(self):
        return self.status in (None, 408, 429) or self.status >= 500


def paper_mode(value):
    if value is None or value is True or str(value).lower().strip() in {"true", "yes", "on", "1"}:
        return True
    if value is False or str(value).lower().strip() in {"false", "no", "off", "0"}:
        return False
    raise ValueError("WEBULL_PAPER must be true or false")


def make_client(config, *, trading):
    try:
        from webull.core.client import ApiClient
        from webull.data.data_client import DataClient
        from webull.trade.trade_client import TradeClient
    except ImportError:
        raise ImportError("Install the 'webull' LumiBot extra to use Webull OpenAPI") from None

    key, secret = config.get("APP_KEY"), config.get("APP_SECRET")
    if not key or not secret:
        raise ValueError("Webull requires an APP_KEY and APP_SECRET")
    paper = paper_mode(config.get("PAPER"))
    with _SDK_INIT_LOCK:
        api = ApiClient(key, secret, "us", connect_timeout=5, timeout=10, verify=True, auto_retry=False)
        api.add_endpoint("us", PAPER_HOST if paper else LIVE_HOST)
        api.set_token_dir(str(_token_directory(config.get("TOKEN_DIR"), paper, key)))
        # The pinned SDK serializes request details on error. Silence only the
        # SDK's logger and disable its default creation of debug log files.
        sdk_logger = logging.getLogger("webull")
        sdk_logger.setLevel(logging.CRITICAL + 1)
        api.set_logger(sdk_logger)
        api.set_stream_logger(log_level=logging.CRITICAL + 1)
        try:
            return (TradeClient if trading else DataClient)(api)
        except Exception as error:
            raise _format_api_error("client initialization", error) from None


def _token_directory(configured, paper, key):
    if configured:
        path = Path(configured).expanduser()
    else:
        environment = "paper" if paper else "live"
        key_namespace = sha256(key.encode()).hexdigest()
        path = Path.home() / ".cache" / "lumibot" / "webull" / environment / key_namespace
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory = path.lstat()
    if not stat.S_ISDIR(directory.st_mode) or directory.st_mode & 0o077:
        raise ValueError("Webull SDK token directory must be a private directory")
    if hasattr(os, "getuid") and directory.st_uid != os.getuid():
        raise ValueError("Webull SDK token directory must be owned by the current user")
    path = path.resolve()
    for parent in path.parents:
        state = parent.stat()
        if state.st_mode & 0o022 and not state.st_mode & stat.S_ISVTX:
            raise ValueError("Webull SDK token directory must have protected parent directories")
        if hasattr(os, "getuid") and state.st_uid not in {0, os.getuid()}:
            raise ValueError("Webull SDK token directory must have protected parent directories")
    try:
        token = (path / "token.txt").lstat()
    except FileNotFoundError:
        pass
    else:
        if not stat.S_ISREG(token.st_mode) or token.st_nlink != 1:
            raise ValueError("Webull SDK token must be a private regular file")
    return path


def _format_api_error(operation, error):
    status = getattr(error, "get_http_status", lambda: None)()
    code = getattr(error, "get_error_code", lambda: None)()
    response = getattr(error, "response", None)
    if status is None and response is not None:
        status = getattr(response, "status_code", None)
    return WebullAPIError(operation, status=status if isinstance(status, int) else None,
                          code=code if isinstance(code, str) else None)


def request_json(operation, request, *args, **kwargs):
    try:
        response = request(*args, **kwargs)
        response.raise_for_status()
        payload = response.json() if getattr(response, "status_code", None) != 204 else {}
    except Exception as error:
        raise _format_api_error(operation, error) from None
    if isinstance(payload, dict) and (payload.get("error_code") or payload.get("success") is False):
        code = payload.get("error_code")
        raise WebullAPIError(operation, status=400, code=code if isinstance(code, str) else None)
    return payload


def read_json(operation, request, *args, **kwargs):
    """Retry an idempotent read when Webull throttles the application's shared quota."""
    for delay in (2, 6, 30):
        try:
            return request_json(operation, request, *args, **kwargs)
        except WebullAPIError as error:
            if error.status != 429:
                raise
            time.sleep(delay)
    return request_json(operation, request, *args, **kwargs)


def require_number(value, field):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"Webull did not supply a valid {field}") from None
    if not result.is_finite():
        raise ValueError(f"Webull did not supply a finite {field}")
    return result


def positive_value(value, field):
    result = require_number(value, field)
    if result <= 0:
        raise ValueError(f"Webull requires positive {field}")
    return format(result.normalize(), "f")
