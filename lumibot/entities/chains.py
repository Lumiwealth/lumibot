from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Union


class OptionsDataFormatError(ValueError):
    """Raised when option chain payloads contain unsupported expiry formats."""


class Chains(dict):
    """Dictionary-like container for option chains.

    Behaves exactly like the raw dict previously returned by ``get_chains`` but
    also exposes convenience helpers and rich ``repr``.  Because it subclasses
    ``dict`` the old code paths that index into the structure (e.g.
    ``chains["Chains"]["PUT"]`` or ``chains.get("Chains")``) continue to work
    unchanged.
    """

    def __init__(self, data: Dict[str, Any]):
        # preserve original mapping
        super().__init__(data)
        # Keep commonly accessed fields as attributes for quick access
        self.multiplier: int | None = data.get("Multiplier")
        self.exchange: str | None = data.get("Exchange")

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------
    def calls(self) -> Dict[str, List[float]]:
        """Return the CALL side of the chain {expiration (YYYY-MM-DD): [strikes]}"""
        return self.get("Chains", {}).get("CALL", {})

    def puts(self) -> Dict[str, List[float]]:
        """Return the PUT side of the chain {expiration (YYYY-MM-DD): [strikes]}"""
        return self.get("Chains", {}).get("PUT", {})

    def expirations(self, option_type: str = "CALL") -> List[str]:
        """List available expiration strings (YYYY-MM-DD) for the specified option type."""
        opts = self.get("Chains", {}).get(option_type.upper(), {})
        return sorted(opts.keys())

    def strikes(self, expiration: Union[str, date, datetime], option_type: str = "CALL") -> List[float]:
        """Return the strikes list for a given expiration (accepts string YYYY-MM-DD or date)."""
        if isinstance(expiration, (date, datetime)):
            expiration = _normalise_expiry_key(expiration)
        return self.get("Chains", {}).get(option_type.upper(), {}).get(expiration, [])

    def to_dict(self) -> Dict[str, Any]:
        """Return a shallow copy of the underlying dict."""
        return dict(self)

    # Internal helpers for date-based access
    def expirations_as_dates(self, option_type: str = "CALL") -> List[date]:
        """List expiration dates for internal use."""
        opts = self.get("Chains", {}).get(option_type.upper(), {})
        return sorted([_normalise_expiry(exp) for exp in opts.keys()])

    def get_option_chain_by_date(self, expiry_date: date, option_type: str = "CALL") -> List[float]:
        """Get strikes for a date object (internal helper)."""
        expiry_str = expiry_date.strftime("%Y-%m-%d")
        return self.get("Chains", {}).get(option_type.upper(), {}).get(expiry_str, [])

    # ------------------------------------------------------------------
    # Niceties
    # ------------------------------------------------------------------
    def __repr__(self) -> str:  # type: ignore[override]
        expiry_cnt = len(self.expirations("CALL"))
        call_cnt = sum(len(v) for v in self.calls().values())
        put_cnt = sum(len(v) for v in self.puts().values())
        return (
            f"<Chains exchange={self.exchange} multiplier={self.multiplier} "
            f"expirations={expiry_cnt} calls={call_cnt} puts={put_cnt}>"
        )

    def __bool__(self) -> bool:  # type: ignore[override]
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
            raise OptionsDataFormatError(
                f"Could not parse option expiry value {expiry!r}"
            ) from exc
    raise OptionsDataFormatError(
        f"Unsupported option expiry type: {type(expiry).__name__}"
    )


def _normalise_expiry_key(expiry: Any) -> str:
    """Convert expiry to canonical YYYY-MM-DD string format."""
    return _normalise_expiry(expiry).strftime("%Y-%m-%d")


def _copy_strike_map(strike_map: Any) -> Dict[str, List[float]]:
    """Return a shallow copy of the expiration->strikes mapping with ISO string keys."""
    if not isinstance(strike_map, dict):
        return {}

    copied: Dict[str, List[float]] = {}
    for expiry, strikes in strike_map.items():
        expiry_key = _normalise_expiry_key(expiry)
        if isinstance(strikes, Iterable) and not isinstance(strikes, (str, bytes)):
            strike_list = [float(s) for s in strikes]
        elif strikes is None:
            strike_list = []
        else:
            strike_list = [float(strikes)]
        existing = copied.setdefault(expiry_key, [])
        existing.extend(strike_list)

    for expiry_key, strike_values in copied.items():
        copied[expiry_key] = sorted(set(strike_values))

    return copied


def normalize_option_chains(data: Any) -> Chains:
    """Normalise arbitrary option-chain payloads into the standard structure."""
    if isinstance(data, Chains):
        base: Dict[str, Any] = data.to_dict()
    elif isinstance(data, dict):
        base = dict(data)
    else:
        base = {}

    chains_section = base.get("Chains")
    if not isinstance(chains_section, dict):
        chains_section = {}

    normalized = {
        "Multiplier": base.get("Multiplier"),
        "Exchange": base.get("Exchange"),
        "Chains": {
            "CALL": _copy_strike_map(chains_section.get("CALL")),
            "PUT": _copy_strike_map(chains_section.get("PUT")),
        },
    }

    return Chains(normalized)
