from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import date, datetime, timedelta
from threading import Lock
from typing import Any, TypeAlias, cast

StrikeMap: TypeAlias = dict[str, list[float]]  # noqa: UP040 - keep Python 3.11 parser compatibility.
ChainsSection: TypeAlias = dict[str, StrikeMap]  # noqa: UP040
StrikeLoader: TypeAlias = Callable[[str], Iterable[object] | None]  # noqa: UP040


class OptionsDataFormatError(ValueError):
    """Raised when option chain payloads contain unsupported expiry formats."""


class _LazyStrikeMap(dict[str, list[float]]):
    """Expiration->strikes map that can populate missing strikes on demand."""

    def __init__(self, initial: StrikeMap, parent: Chains, option_type: str) -> None:
        super().__init__(initial)
        self._parent = parent
        self._option_type = option_type.upper()

    def _maybe_load(self, expiry_key: str) -> None:
        try:
            self._parent.ensure_strikes_loaded(expiry_key)
        except Exception:
            return

    def __getitem__(self, key: object) -> list[float]:
        try:
            expiry_key = _normalise_expiry_key(key)
        except OptionsDataFormatError:
            expiry_key = str(key)

        value = super().get(expiry_key)
        if value is None or value == []:
            self._maybe_load(expiry_key)
            value = super().get(expiry_key)
        if value is None:
            return []
        return value

    def get(self, key: object, default: Any = None) -> Any:
        try:
            expiry_key = _normalise_expiry_key(key)
        except OptionsDataFormatError:
            expiry_key = str(key)

        value = super().get(expiry_key)
        if value is None or value == []:
            self._maybe_load(expiry_key)
            value = super().get(expiry_key)
        if value is None:
            return default
        return value


class Chains(dict[str, Any]):
    """Dictionary-like container for option chains.

    Behaves exactly like the raw dict previously returned by ``get_chains`` but
    also exposes convenience helpers and rich ``repr``. Because it subclasses
    ``dict`` the old code paths that index into the structure continue to work.
    """

    def __init__(self, data: Mapping[str, Any]) -> None:
        super().__init__(data)
        self.multiplier: int | None = cast(int | None, data.get("Multiplier"))
        self.exchange: str | None = cast(str | None, data.get("Exchange"))
        self.underlying_symbol: str | None = cast(str | None, data.get("UnderlyingSymbol"))
        self._strike_loader: StrikeLoader | None = None
        self._strike_loader_lock: Lock = Lock()
        self._strike_loaded: set[str] = set()

    def _chains_section(self) -> ChainsSection:
        chains_section = self.get("Chains")
        if isinstance(chains_section, dict):
            return cast(ChainsSection, chains_section)
        return {}

    def _side_map(self, option_type: str) -> StrikeMap:
        side_map = self._chains_section().get(option_type.upper())
        if isinstance(side_map, dict):
            return side_map
        return {}

    def calls(self) -> StrikeMap:
        """Return the CALL side of the chain {expiration (YYYY-MM-DD): [strikes]}."""
        return self._side_map("CALL")

    def puts(self) -> StrikeMap:
        """Return the PUT side of the chain {expiration (YYYY-MM-DD): [strikes]}."""
        return self._side_map("PUT")

    def enable_lazy_strikes(self, loader: StrikeLoader) -> None:
        """Enable on-demand strike fetching for expirations that have empty/missing strike lists."""
        self._strike_loader = loader
        chains_section = self._chains_section()
        if not chains_section:
            return

        call_map = chains_section.get("CALL")
        put_map = chains_section.get("PUT")
        if isinstance(call_map, dict) and not isinstance(call_map, _LazyStrikeMap):
            chains_section["CALL"] = _LazyStrikeMap(dict(call_map), self, "CALL")
        if isinstance(put_map, dict) and not isinstance(put_map, _LazyStrikeMap):
            chains_section["PUT"] = _LazyStrikeMap(dict(put_map), self, "PUT")

    def ensure_strikes_loaded(self, expiry_key: str) -> None:
        loader = self._strike_loader
        if loader is None:
            return

        with self._strike_loader_lock:
            if expiry_key in self._strike_loaded:
                return
            self._strike_loaded.add(expiry_key)

        try:
            strikes_norm = sorted(set(_copy_strike_values(loader(expiry_key))))
        except (TypeError, ValueError):
            strikes_norm = []

        call_map = self._side_map("CALL")
        put_map = self._side_map("PUT")
        call_map[expiry_key] = strikes_norm
        put_map[expiry_key] = list(strikes_norm)

    def _ensure_strikes_loaded(self, expiry_key: str) -> None:
        self.ensure_strikes_loaded(expiry_key)

    def expirations(self, option_type: str = "CALL") -> list[str]:
        """List available expiration strings (YYYY-MM-DD) for the specified option type."""
        return sorted(self._side_map(option_type).keys())

    def strikes(self, expiration: str | date | datetime, option_type: str = "CALL") -> list[float]:
        """Return the strikes list for a given expiration (accepts string YYYY-MM-DD or date)."""
        side_map = self._side_map(option_type)

        try:
            expiry_key = _normalise_expiry_key(expiration)
        except OptionsDataFormatError:
            expiry_key = str(expiration)

        strikes = side_map.get(expiry_key)
        if strikes is not None:
            return strikes

        try:
            expiry_dt = _normalise_expiry(expiration)
        except OptionsDataFormatError:
            return []

        fallback_dates: list[date] = []
        if expiry_dt.weekday() == 4:
            fallback_dates.append(expiry_dt + timedelta(days=1))
        elif expiry_dt.weekday() == 5:
            fallback_dates.append(expiry_dt - timedelta(days=1))
        elif expiry_dt.weekday() == 6:
            fallback_dates.extend([expiry_dt - timedelta(days=2), expiry_dt - timedelta(days=1)])

        for candidate in fallback_dates:
            candidate_key = candidate.strftime("%Y-%m-%d")
            candidate_strikes = side_map.get(candidate_key)
            if candidate_strikes is not None:
                return candidate_strikes

        return []

    def to_dict(self) -> dict[str, Any]:
        """Return a shallow copy of the underlying dict."""
        return dict(self)

    def expirations_as_dates(self, option_type: str = "CALL") -> list[date]:
        """List expiration dates for internal use."""
        return sorted(_normalise_expiry(expiration) for expiration in self._side_map(option_type).keys())

    def get_option_chain_by_date(self, expiry_date: date, option_type: str = "CALL") -> list[float]:
        """Get strikes for a date object."""
        expiry_str = expiry_date.strftime("%Y-%m-%d")
        return self._side_map(option_type).get(expiry_str, [])

    def __repr__(self) -> str:
        expiry_cnt = len(self.expirations("CALL"))
        call_cnt = sum(len(strikes) for strikes in self.calls().values())
        put_cnt = sum(len(strikes) for strikes in self.puts().values())
        return (
            f"<Chains exchange={self.exchange} multiplier={self.multiplier} "
            f"expirations={expiry_cnt} calls={call_cnt} puts={put_cnt}>"
        )

    def __bool__(self) -> bool:
        return bool(self.calls()) or bool(self.puts())


def _normalise_expiry(expiry: Any) -> date:
    """Convert various expiry representations into a ``datetime.date``."""
    if isinstance(expiry, datetime):
        return expiry.date()
    if isinstance(expiry, date):
        return expiry
    if isinstance(expiry, str):
        cleaned = expiry.strip()
        if not cleaned:
            raise OptionsDataFormatError("Empty option expiry string encountered")
        digits_only = cleaned.replace("-", "")
        if len(digits_only) != 8 or not digits_only.isdigit():
            raise OptionsDataFormatError(f"Unsupported option expiry format: {expiry!r}")
        try:
            return datetime.strptime(digits_only, "%Y%m%d").date()
        except ValueError as exc:
            raise OptionsDataFormatError(f"Could not parse option expiry value {expiry!r}") from exc
    raise OptionsDataFormatError(f"Unsupported option expiry type: {type(expiry).__name__}")


def _normalise_expiry_key(expiry: Any) -> str:
    """Convert expiry to canonical YYYY-MM-DD string format."""
    return _normalise_expiry(expiry).strftime("%Y-%m-%d")


def _copy_strike_values(strikes: object) -> list[float]:
    if strikes is None:
        return []
    if isinstance(strikes, Iterable) and not isinstance(strikes, (str, bytes)):
        values = cast(Iterable[object], strikes)
    else:
        values = (strikes,)
    return [float(cast(Any, strike)) for strike in values if strike is not None]


def _copy_strike_map(strike_map: Any) -> StrikeMap:
    """Return a shallow copy of the expiration->strikes mapping with ISO string keys."""
    if not isinstance(strike_map, dict):
        return {}

    copied: StrikeMap = {}
    for expiry, strikes in cast(dict[Any, Any], strike_map).items():
        expiry_key = _normalise_expiry_key(expiry)
        existing = copied.setdefault(expiry_key, [])
        existing.extend(_copy_strike_values(strikes))

    for expiry_key, strike_values in copied.items():
        copied[expiry_key] = sorted(set(strike_values))

    return copied


def normalize_option_chains(data: Any) -> Chains:
    """Normalise arbitrary option-chain payloads into the standard structure."""
    if isinstance(data, Chains):
        base = data.to_dict()
    elif isinstance(data, dict):
        base = dict(cast(dict[str, Any], data))
    else:
        base = {}

    chains_section_obj = base.get("Chains")
    chains_section = cast(dict[str, Any], chains_section_obj) if isinstance(chains_section_obj, dict) else {}

    normalized: dict[str, Any] = {
        "Multiplier": base.get("Multiplier"),
        "Exchange": base.get("Exchange"),
        "UnderlyingSymbol": base.get("UnderlyingSymbol"),
        "Chains": {
            "CALL": _copy_strike_map(chains_section.get("CALL")),
            "PUT": _copy_strike_map(chains_section.get("PUT")),
        },
    }

    return Chains(normalized)
