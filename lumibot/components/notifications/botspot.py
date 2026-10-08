import base64
import os
from typing import Any

from lumibot.components.agents.runtime import call_mcp_tool
from lumibot.components.agents.schemas import MCPServer

from .base import NotificationResult


class BotSpotNotificationProvider:
    """BotSpot-managed communications exposed through the owner-scoped MCP."""

    provider = "botspot"

    def __init__(
        self,
        *,
        url: str | None = None,
        token_env: str = "BOTSPOT_COMMUNICATIONS_MCP_TOKEN",
        renew_url: str | None = None,
    ) -> None:
        self.url = url or os.environ.get("BOTSPOT_COMMUNICATIONS_MCP_URL")
        self.token_env = token_env
        self.renew_url = renew_url or os.environ.get("BOTSPOT_COMMUNICATIONS_MCP_RENEW_URL")

    def _server(self) -> MCPServer:
        token = os.environ.get(self.token_env)
        if not self.url or not token:
            raise ValueError("BotSpot communications URL and token are required")
        return MCPServer(
            name="botspot_communications",
            transport="streamable_http",
            url=self.url,
            exposed_tools=[
                "upload_email_attachment",
                "send_email",
                "list_sent_emails",
                "get_sent_email",
                "get_email_status",
            ],
            auth_token_env=self.token_env,
            auth_token_refresh_url=self.renew_url,
        )

    @staticmethod
    def _structured(result: dict[str, Any]) -> dict[str, Any]:
        payload = result.get("structuredContent")
        if not isinstance(payload, dict):
            raise RuntimeError("BotSpot communications returned no structured content")
        return payload

    @staticmethod
    def _attachment_payload(attachment: dict[str, Any]) -> dict[str, str]:
        filename = str(attachment.get("filename") or "").strip()
        if not filename:
            raise ValueError("Email attachment filename is required")
        content = attachment.get("content")
        if isinstance(content, bytes):
            encoded = base64.b64encode(content).decode("ascii")
        elif isinstance(content, str):
            try:
                base64.b64decode(content, validate=True)
                encoded = content
            except (TypeError, ValueError):
                encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
        else:
            raise ValueError("Email attachment content must be bytes or a string")
        return {
            "filename": filename,
            "contentType": str(
                attachment.get("content_type")
                or attachment.get("contentType")
                or "application/octet-stream"
            ),
            "contentBase64": encoded,
        }

    def send_email(
        self,
        *,
        to: list[str],
        subject: str,
        text: str | None = None,
        html: str | None = None,
        attachments: list[dict[str, Any]] | None = None,
        idempotency_key: str | None = None,
        **kwargs: Any,
    ) -> NotificationResult:
        if kwargs:
            return NotificationResult(
                ok=False,
                provider=self.provider,
                title=subject,
                message=text or "",
                skipped=True,
                reason=f"unsupported BotSpot email field(s): {', '.join(sorted(kwargs))}",
            )
        if not idempotency_key:
            return NotificationResult(
                ok=False,
                provider=self.provider,
                title=subject,
                message=text or "",
                skipped=True,
                reason="BotSpot-managed email requires an idempotency key",
            )
        try:
            server = self._server()
            handles: list[str] = []
            for attachment in attachments or []:
                uploaded = self._structured(
                    call_mcp_tool(
                        server,
                        "upload_email_attachment",
                        self._attachment_payload(attachment),
                    )
                )
                handle = uploaded.get("handle")
                if not isinstance(handle, str) or not handle:
                    raise RuntimeError("BotSpot attachment upload returned no handle")
                handles.append(handle)
            payload = self._structured(
                call_mcp_tool(
                    server,
                    "send_email",
                    {
                        "to": to,
                        "subject": subject,
                        "text": text,
                        "html": html,
                        "attachmentHandles": handles,
                        "idempotencyKey": idempotency_key,
                    },
                )
            )
        except (RuntimeError, ValueError) as exc:
            return NotificationResult(
                ok=False,
                provider=self.provider,
                title=subject,
                message=text or "",
                reason=f"BotSpot communications request failed: {type(exc).__name__}",
            )
        return NotificationResult(
            ok=True,
            provider=self.provider,
            title=subject,
            message=text or "",
            payload=payload,
        )

    def notify(self, *, title: str, message: str, severity: str = "info", **kwargs: Any) -> NotificationResult:
        to = kwargs.pop("to", None)
        if isinstance(to, str):
            to = [to]
        if not to:
            return NotificationResult(
                ok=False,
                provider=self.provider,
                title=title,
                message=message,
                severity=severity,
                skipped=True,
                reason="email recipient is required",
            )
        return self.send_email(to=to, subject=title, text=message, **kwargs)

    def list_sent(self, *, limit: int = 20, **_kwargs: Any) -> dict[str, Any]:
        return self._structured(call_mcp_tool(self._server(), "list_sent_emails", {"limit": limit}))

    def get_sent(self, email_id: str) -> dict[str, Any]:
        return self._structured(
            call_mcp_tool(self._server(), "get_sent_email", {"messageId": email_id})
        )

    def get_status(self, email_id: str) -> dict[str, Any]:
        return self._structured(
            call_mcp_tool(self._server(), "get_email_status", {"messageId": email_id})
        )
