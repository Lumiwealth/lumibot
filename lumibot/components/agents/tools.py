import inspect
import logging
import textwrap
from typing import Any, Callable

from .schemas import BoundTool, ToolDefinition

logger = logging.getLogger(__name__)

# Warn if an @agent_tool function body exceeds this many lines
_SOURCE_WARNING_THRESHOLD = 100


def _get_clean_source(func: Callable[..., Any]) -> str | None:
    """Get the source code of a function, stripped of the decorator and self parameter."""
    try:
        source = inspect.getsource(func)
    except (OSError, TypeError):
        return None

    source = textwrap.dedent(source)
    lines = source.split("\n")

    # Strip @agent_tool decorator lines from the top
    clean_lines = []
    past_decorator = False
    for line in lines:
        stripped = line.strip()
        if not past_decorator:
            if stripped.startswith("@"):
                continue
            if stripped.startswith(")") and not stripped.startswith("def"):
                continue
            past_decorator = True
        clean_lines.append(line)

    if not clean_lines:
        return None

    body_line_count = len([ln for ln in clean_lines if ln.strip()])
    if body_line_count > _SOURCE_WARNING_THRESHOLD:
        logger.warning(
            f"Agent tool function has {body_line_count} lines of code. "
            f"This will be included in the tool description and may use many tokens. "
            f"Consider keeping @agent_tool functions short."
        )

    # Remove 'self' from the def signature for cleaner display
    result = "\n".join(clean_lines)
    result = result.replace("(self, ", "(", 1)
    result = result.replace("(self)", "()", 1)
    return result.strip()


def _build_description(func: Callable[..., Any], explicit_description: str | None) -> str:
    """Build the tool description, appending source code for AI context."""
    base = explicit_description or (func.__doc__ or "").strip() or func.__name__

    source = _get_clean_source(func)
    if source:
        return f"{base}\n\nSource code:\n{source}"

    # Fallback: at least show the signature
    try:
        sig = inspect.signature(func)
        params = []
        for name, param in sig.parameters.items():
            if name == "self":
                continue
            annotation = param.annotation
            type_str = annotation.__name__ if hasattr(annotation, "__name__") else str(annotation)
            if type_str == "<class 'inspect._empty'>":
                type_str = "any"
            default = f", default={param.default!r}" if param.default is not inspect.Parameter.empty else ""
            params.append(f"{name} ({type_str}{default})")
        if params:
            return f"{base}\n\nParameters: {', '.join(params)}"
    except (TypeError, ValueError):
        pass

    return base


def agent_tool(*, name: str | None = None, description: str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        setattr(
            func,
            "_lumibot_agent_tool",
            {
                "name": name or func.__name__,
                "description": _build_description(func, description),
            },
        )
        return func

    return decorator


def is_agent_tool(value: Any) -> bool:
    return hasattr(value, "_lumibot_agent_tool")


def bind_callable_tool(callable_obj: Callable[..., Any]) -> ToolDefinition:
    metadata = getattr(callable_obj, "_lumibot_agent_tool", None) or {}
    tool_name = metadata.get("name") or getattr(callable_obj, "__name__", "tool")
    tool_description = metadata.get("description") or _build_description(callable_obj, None)

    def binder(strategy: Any, manager: Any) -> BoundTool:
        return BoundTool(
            name=tool_name,
            description=tool_description,
            function=callable_obj,
            source="local",
            metadata={"kind": "callable"},
        )

    return ToolDefinition(
        name=tool_name,
        description=tool_description,
        binder=binder,
        metadata={"kind": "callable"},
    )
