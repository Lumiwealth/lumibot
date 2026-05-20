"""Lazy exports for agent runtime components."""

from importlib import import_module as _import_module

_NAME_TO_MODULE = {
    "AgentHandle": "manager",
    "AgentManager": "manager",
    "AgentRunResult": "schemas",
    "AgentTraceEvent": "schemas",
    "BuiltinTools": "builtins",
    "GoogleADKRuntime": "runtime",
    "MCPServer": "schemas",
    "StubAgentRuntime": "runtime",
    "agent_tool": "tools",
}
_SUBMODULES = {
    "asset_resolution",
    "builtins",
    "docs_tools",
    "duckdb_tools",
    "manager",
    "replay_cache",
    "runtime",
    "schemas",
    "tools",
}

__all__ = sorted(_NAME_TO_MODULE)


def __getattr__(name):
    if name in _SUBMODULES:
        module = _import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module

    module_name = _NAME_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = _import_module(f"{__name__}.{module_name}")
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__) | _SUBMODULES)
