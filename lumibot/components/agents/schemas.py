from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True)
class MCPServer:
    name: str
    url: str | None = None
    transport: str | None = None
    command: str | None = None
    args: list[str] = field(default_factory=list)
    env: dict[str, str] | None = None
    cwd: str | None = None
    exposed_tools: list[str] | None = None
    allowed_tools: list[str] | None = None
    headers: dict[str, str] | None = None
    auth_token_env: str | None = None
    timeout_seconds: float = 30.0
    sse_read_timeout_seconds: float = 300.0
    terminate_on_close: bool = True

    def __post_init__(self) -> None:
        resolved_transport = (self.transport or ("stdio" if self.command else "http")).lower().replace("-", "_")
        resolved_tools = self.exposed_tools if self.exposed_tools is not None else self.allowed_tools
        if not resolved_tools:
            raise ValueError(f"MCP server {self.name!r} must expose at least one tool.")
        if resolved_transport == "stdio" and not self.command:
            raise ValueError(f"MCP server {self.name!r} uses stdio transport but no command was provided.")
        if resolved_transport in {"http", "streamable_http", "streamablehttp"} and not self.url:
            raise ValueError(f"MCP server {self.name!r} uses HTTP transport but no url was provided.")
        if resolved_transport not in {"stdio", "http", "streamable_http", "streamablehttp"}:
            raise ValueError(f"Unsupported MCP transport {resolved_transport!r} for server {self.name!r}.")
        object.__setattr__(self, "transport", resolved_transport)
        object.__setattr__(self, "exposed_tools", list(resolved_tools))
        object.__setattr__(self, "allowed_tools", list(resolved_tools))


@dataclass
class AgentTraceEvent:
    kind: str
    text: str | None = None
    tool_name: str | None = None
    payload: dict[str, Any] | None = None
    timestamp: str | None = None


@dataclass
class AgentRunResult:
    summary: str | None
    model: str
    events: list[AgentTraceEvent]
    cache_hit: bool = False
    cache_key: str | None = None
    usage: dict[str, Any] | None = None
    payload: dict[str, Any] | None = None
    warnings: list[dict[str, Any]] = field(default_factory=list)

    @property
    def text(self) -> str:
        parts: list[str] = []
        for event in self.events:
            if event.kind == "text" and event.text:
                parts.append(event.text)
        return "\n".join(parts).strip()

    @property
    def tool_calls(self) -> list[AgentTraceEvent]:
        return [event for event in self.events if event.kind == "tool_call"]

    @property
    def tool_results(self) -> list[AgentTraceEvent]:
        return [event for event in self.events if event.kind == "tool_result"]

    @property
    def warning_messages(self) -> list[str]:
        messages: list[str] = []
        for warning in self.warnings:
            if not isinstance(warning, dict):
                continue
            message = warning.get("message")
            if isinstance(message, str) and message.strip():
                messages.append(message.strip())
        return messages


@dataclass
class BoundTool:
    name: str
    description: str
    function: Callable[..., Any]
    source: str = "local"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    binder: Callable[[Any, Any], BoundTool]
    metadata: dict[str, Any] = field(default_factory=dict)
