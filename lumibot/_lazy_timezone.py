"""Lazy timezone helpers for startup-sensitive modules."""

from datetime import tzinfo


class LazyPytzTimezone(tzinfo):
    """tzinfo proxy that imports pytz and builds the timezone on first use."""

    __isabstractmethod__ = False

    def __init__(self, timezone_name: str):
        self._timezone_name = timezone_name
        self._timezone = None

    def _load(self):
        if self._timezone is None:
            import pytz

            self._timezone = pytz.timezone(self._timezone_name)
        return self._timezone

    @property
    def zone(self):
        return self._timezone_name

    @property
    def key(self):
        return self._timezone_name

    def utcoffset(self, dt):
        timezone = self._load()
        if dt is not None and dt.tzinfo is self:
            dt = dt.replace(tzinfo=timezone)
        return timezone.utcoffset(dt)

    def dst(self, dt):
        timezone = self._load()
        if dt is not None and dt.tzinfo is self:
            dt = dt.replace(tzinfo=timezone)
        return timezone.dst(dt)

    def tzname(self, dt):
        timezone = self._load()
        if dt is not None and dt.tzinfo is self:
            dt = dt.replace(tzinfo=timezone)
        return timezone.tzname(dt)

    def fromutc(self, dt):
        timezone = self._load()
        if dt.tzinfo is self:
            dt = dt.replace(tzinfo=timezone)
        return timezone.fromutc(dt)

    def localize(self, *args, **kwargs):
        return self._load().localize(*args, **kwargs)

    def normalize(self, *args, **kwargs):
        return self._load().normalize(*args, **kwargs)

    @property
    def __class__(self):
        return self._load().__class__

    def __str__(self):
        return str(self._load())

    def __repr__(self):
        return repr(self._load())

    def __eq__(self, other):
        if isinstance(other, LazyPytzTimezone):
            other = other._load()
        return self._load() == other

    def __hash__(self):
        return hash(self._load())

    def __getattr__(self, name):
        return getattr(self._load(), name)
