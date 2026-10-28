"""Registry of Indicator v2 classes by ``key`` and immutable ``version``.

``version`` pins the math: once ``(key, version)`` is registered it can never
point at a different class. Change the formula, bump the version, and saved
charts or strategies that pinned the old version keep getting the old math.
"""

from __future__ import annotations

import threading

from lumibot.indicators.base import Indicator


class UnknownIndicatorError(KeyError):
    """No indicator is registered under this key."""


class UnknownIndicatorVersionError(KeyError):
    """The key exists but not at the requested version."""


class IndicatorRegistry:
    """Maps ``key -> {version: Indicator subclass}``. Thread-safe."""

    def __init__(self):
        self._classes: dict[str, dict[int, type]] = {}
        self._lock = threading.Lock()

    def register(self, indicator_cls: type) -> type:
        """Register an Indicator subclass. Returns it, so it works as a decorator."""
        if not isinstance(indicator_cls, type) or not issubclass(indicator_cls, Indicator):
            raise TypeError(f"register() needs an Indicator subclass, got {indicator_cls!r}")
        key, version = indicator_cls.key, indicator_cls.version
        if not isinstance(key, str) or not key:
            raise ValueError(f"{indicator_cls.__name__} needs a non-empty string `key`")
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            raise ValueError(f"{indicator_cls.__name__} needs an integer `version` >= 1")
        if not indicator_cls.outputs:
            raise ValueError(f"{indicator_cls.__name__} declares no outputs")
        indicator_cls.describe()  # fail now if the schema or defaults are invalid
        with self._lock:
            versions = self._classes.setdefault(key, {})
            existing = versions.get(version)
            if existing is not None and existing is not indicator_cls:
                raise ValueError(
                    f"indicator {key!r} v{version} is already registered to {existing.__name__}; "
                    "versions are immutable, so register the new math as a new version"
                )
            versions[version] = indicator_cls
        return indicator_cls

    def unregister(self, key: str, version: int | None = None) -> None:
        """Remove one version, or every version of ``key`` when ``version`` is None."""
        with self._lock:
            if key not in self._classes:
                raise UnknownIndicatorError(key)
            if version is None:
                del self._classes[key]
                return
            if version not in self._classes[key]:
                raise UnknownIndicatorVersionError(f"{key} v{version}")
            del self._classes[key][version]
            if not self._classes[key]:
                del self._classes[key]

    def get(self, key: str, version: int | None = None) -> type:
        """Indicator class for ``key``; latest version unless ``version`` is pinned."""
        versions = self._classes.get(key)
        if not versions:
            raise UnknownIndicatorError(
                f"unknown indicator {key!r}; available: {sorted(self._classes)}"
            )
        if version is None:
            return versions[max(versions)]
        if version not in versions:
            raise UnknownIndicatorVersionError(
                f"indicator {key!r} has no version {version!r}; available: {sorted(versions)}"
            )
        return versions[version]

    def create(self, key: str, version: int | None = None, **inputs) -> Indicator:
        """Instantiate ``key`` (optionally pinned to ``version``) with ``inputs``."""
        return self.get(key, version)(**inputs)

    def list(self, all_versions: bool = False) -> list[dict]:
        """``describe()`` of every indicator (latest version only by default)."""
        out = []
        for key in sorted(self._classes):
            versions = self._classes[key]
            chosen = sorted(versions) if all_versions else [max(versions)]
            out.extend(versions[v].describe() for v in chosen)
        return out

    def __contains__(self, key) -> bool:
        return key in self._classes


registry = IndicatorRegistry()


def register(indicator_cls: type) -> type:
    """Register on the shared default registry. Usable as a class decorator."""
    return registry.register(indicator_cls)


def _register_builtins() -> None:
    from lumibot.indicators.builtin import BUILTIN_INDICATORS

    for cls in BUILTIN_INDICATORS:
        registry.register(cls)


_register_builtins()

__all__ = [
    "IndicatorRegistry",
    "UnknownIndicatorError",
    "UnknownIndicatorVersionError",
    "register",
    "registry",
]
