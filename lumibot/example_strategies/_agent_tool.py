import inspect
import textwrap
from typing import Any, Callable


def _get_clean_source(func: Callable[..., Any]) -> str | None:
    try:
        source = inspect.getsource(func)
    except (OSError, TypeError):
        return None

    source = textwrap.dedent(source)
    lines = source.split("\n")
    clean_lines = []
    past_decorator = False
    decorator_depth = 0
    for line in lines:
        stripped = line.strip()
        if not past_decorator:
            # Skip all decorator lines, including multi-line decorator arguments,
            # before reaching the def statement that belongs in the tool prompt.
            if stripped.startswith("@") or decorator_depth:
                decorator_depth += stripped.count("(") - stripped.count(")")
                decorator_depth = max(decorator_depth, 0)
                continue
            past_decorator = True
        clean_lines.append(line)
    if not clean_lines:
        return None
    return "\n".join(clean_lines).replace("(self, ", "(", 1).replace("(self)", "()").strip()


def _build_description(func: Callable[..., Any], explicit_description: str | None) -> str:
    base = explicit_description or (func.__doc__ or "").strip() or func.__name__
    source = _get_clean_source(func)
    if source:
        return f"{base}\n\nSource code:\n{source}"
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        return base

    params = []
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        annotation = param.annotation
        if annotation is inspect.Parameter.empty:
            type_str = "any"
        elif hasattr(annotation, "__name__"):
            type_str = annotation.__name__
        else:
            type_str = str(annotation)
        default = f", default={param.default!r}" if param.default is not inspect.Parameter.empty else ""
        params.append(f"{name} ({type_str}{default})")
    if params:
        return f"{base}\n\nParameters: {', '.join(params)}"
    return base


def _requests():
    import requests

    return requests


def agent_tool(*, name: str | None = None, description: str | None = None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Lightweight agent tool decorator for import-cheap example strategies."""

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
