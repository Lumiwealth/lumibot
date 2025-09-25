from typing import Any, Dict, Iterable, List


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
        """Return the CALL side of the chain {expiration: [strikes]}"""
        return self.get("Chains", {}).get("CALL", {})

    def puts(self) -> Dict[str, List[float]]:
        """Return the PUT side of the chain {expiration: [strikes]}"""
        return self.get("Chains", {}).get("PUT", {})

    def expirations(self, option_type: str = "CALL") -> List[str]:
        """List available expiration strings for the specified option type."""
        opts = self.get("Chains", {}).get(option_type.upper(), {})
        return sorted(opts.keys())

    def strikes(self, expiration: str, option_type: str = "CALL") -> List[float]:
        """Return strikes list for a given expiration date string."""
        return self.get("Chains", {}).get(option_type.upper(), {}).get(expiration, [])

    def to_dict(self) -> Dict[str, Any]:
        """Return a shallow copy of the underlying dict."""
        return dict(self)

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


def _copy_strike_map(strike_map: Any) -> Dict[str, List[float]]:
    """Return a shallow copy of the expiration->strikes mapping."""
    if not isinstance(strike_map, dict):
        return {}

    copied: Dict[str, List[float]] = {}
    for expiry, strikes in strike_map.items():
        if isinstance(strikes, Iterable) and not isinstance(strikes, (str, bytes)):
            copied[expiry] = list(strikes)
        elif strikes is None:
            copied[expiry] = []
        else:
            copied[expiry] = [strikes]
    return copied


def normalize_option_chains(data: Any) -> Chains:
    """Normalize arbitrary option-chain payloads into the standard structure."""
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
