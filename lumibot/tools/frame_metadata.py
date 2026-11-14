"""Keep DataFrame metadata cheap to carry through pandas operations.

pandas deep-copies ``DataFrame.attrs`` every time a column, row or slice is taken
from a frame whose ``attrs`` is not empty.  A nested mapping there (for example a
ledger with one entry per corporate action) is therefore copied element by element
on every price lookup of a backtest.  The containers below hold the same content
but are read-only and return themselves when deep-copied, so derived frames share
one metadata object instead of rebuilding it.
"""

from __future__ import annotations

from typing import Any


def _read_only(self, *args, **kwargs):
    raise TypeError("Shared frame metadata is read-only; assign a new value to frame.attrs[key] instead")


class SharedFrameMapping(dict):
    """A read-only ``dict`` that pandas can propagate without copying."""

    __slots__ = ()

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = __ior__ = _read_only

    def __deepcopy__(self, memo):
        return self

    def __reduce__(self):
        # Pickle and copy.copy() produce an ordinary, independent dict.
        return (dict, (dict(self),))


class SharedFrameSequence(list):
    """A read-only ``list`` that pandas can propagate without copying."""

    __slots__ = ()

    __setitem__ = __delitem__ = clear = pop = append = extend = insert = remove = _read_only
    reverse = sort = __iadd__ = __imul__ = _read_only

    def __deepcopy__(self, memo):
        return self

    def __reduce__(self):
        return (list, (list(self),))


def _shared(value: Any) -> Any:
    if isinstance(value, (SharedFrameMapping, SharedFrameSequence)):
        return value
    if isinstance(value, dict):
        return SharedFrameMapping((key, _shared(item)) for key, item in value.items())
    if isinstance(value, list):
        return SharedFrameSequence(_shared(item) for item in value)
    return value


def share_frame_attrs(frame):
    """Make every nested container in ``frame.attrs`` shareable; return the frame.

    Values compare equal to the originals and still serialize as JSON.  Call this
    once on a long-lived frame that is sliced repeatedly (a preloaded bar series).
    """
    attrs = getattr(frame, "attrs", None)
    if not attrs:
        return frame
    for key, value in list(attrs.items()):
        shared = _shared(value)
        if shared is not value:
            attrs[key] = shared
    return frame
