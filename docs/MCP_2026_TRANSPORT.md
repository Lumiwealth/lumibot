# MCP 2026 Transport

One transport contract for LumiBot external agent tools.

Last Updated: 2026-08-12
Status: Implemented
Audience: LumiBot contributors and integrators

## Overview

LumiBot's external MCP runtime uses `mcp>=2,<3` and `httpx2`. HTTP and stdio
servers share one official `Client(mode="auto")` path. Modern peers negotiate
`2026-07-28` with `server/discover`; the v2 client owns request headers,
per-request metadata, response validation, and rolling fallback for older
conforming servers.

Modern HTTP behavior:

- POST-only JSON-RPC requests.
- `Mcp-Protocol-Version`, `Mcp-Method`, and tool-call `Mcp-Name` headers.
- Protocol version, client identity, and capabilities in request `_meta`.
- No `initialize`, `notifications/initialized`, `Mcp-Session-Id`, standalone
  GET event stream, or resumability state.

`MCPServer.headers` and `auth_token_env` still add application authentication
headers. `timeout_seconds`, `sse_read_timeout_seconds`, and
`terminate_on_close` retain their public meanings. No global TLS monkeypatch or
parallel raw JSON-RPC HTTP implementation remains.

## Verification

Focused transport coverage runs real MCP v2 stdio and Streamable HTTP servers,
plus a wire-level remote fixture that asserts discovery, modern headers,
request metadata, typed results, and absence of sessions or initialization.
