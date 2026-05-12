"""Lazy exports for optional component packages."""
# pyright: reportUnsupportedDunderAll=false

from importlib import import_module as _import_module
from typing import Any

_AGENT_EXPORTS = {
    "AgentManager",
    "AgentRunResult",
    "BuiltinTools",
    "MCPServer",
    "agent_tool",
}

__all__: list[str] = sorted(_AGENT_EXPORTS)


def __getattr__(name: str) -> Any:
    if name in _AGENT_EXPORTS:
        agents = _import_module(f"{__name__}.agents")
        value = getattr(agents, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
