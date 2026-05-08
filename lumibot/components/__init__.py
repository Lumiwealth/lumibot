"""Lazy exports for optional component packages."""

from importlib import import_module as _import_module

_AGENT_EXPORTS = {
    "AgentManager",
    "AgentRunResult",
    "BuiltinTools",
    "MCPServer",
    "agent_tool",
}
_SUBMODULES = {"agents"}

__all__ = sorted(_AGENT_EXPORTS)


def __getattr__(name):
    if name in _SUBMODULES:
        module = _import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module

    if name in _AGENT_EXPORTS:
        agents = _import_module(f"{__name__}.agents")
        value = getattr(agents, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(__all__) | _SUBMODULES)
