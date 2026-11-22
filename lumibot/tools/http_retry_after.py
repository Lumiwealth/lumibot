"""Parse the HTTP ``Retry-After`` header (RFC 9110 section 10.2.3).

The header is either a number of seconds (``"2"``) or an HTTP date
(``"Wed, 21 Oct 2026 07:28:00 GMT"``). Brokers that rate limit with HTTP 429
use it to say when the next request may be sent.
"""

import time
from email.utils import parsedate_to_datetime
from typing import Optional


def _header_value(headers, name: str):
    if not headers:
        return None
    getter = getattr(headers, "get", None)
    if getter is None:
        return None
    value = getter(name)
    if value is None:
        value = getter(name.lower())
    if value is None and isinstance(headers, dict):
        for key, candidate in headers.items():
            if str(key).lower() == name.lower():
                return candidate
    return value


def retry_after_seconds(headers, *, wall_now: Optional[float] = None) -> Optional[float]:
    """Return the wait in seconds requested by ``Retry-After``, or ``None``.

    ``None`` means the header is missing or unreadable, so the caller picks its
    own backoff. A date in the past returns ``0.0``. The caller bounds the value.
    """
    value = _header_value(headers, "Retry-After")
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError):
        return None
    if when is None:
        return None
    now = time.time() if wall_now is None else float(wall_now)
    return max(0.0, when.timestamp() - now)


class RateLimitedError(ConnectionError):
    """A provider request was refused or skipped because of a rate limit.

    Raised for an HTTP 429 and for requests skipped while a ``Retry-After`` wait
    or a local request budget is still in effect. ``retry_after_seconds`` is the
    remaining wait when known. Callers must not split the request and retry it
    piece by piece: that multiplies the requests the limit is counting.
    """

    def __init__(self, message: str, *, retry_after_seconds: Optional[float] = None):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds
