import os
from typing import Any

import requests

from .base import NotificationResult


class ResendNotificationProvider:
    """Resend email provider with outbound and inbound read operations."""

    provider = "resend"
    base_url = "https://api.resend.com"

    def __init__(self, *, api_key: str | None = None, from_address: str | None = None) -> None:
        self.api_key = api_key or os.environ.get("RESEND_API_KEY")
        self.from_address = from_address or os.environ.get("RESEND_FROM_EMAIL")

    def _headers(self) -> dict[str, str]:
        if not self.api_key:
            raise ValueError("RESEND_API_KEY is required")
        return {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

    def _get(self, path: str, **params: Any) -> dict[str, Any]:
        response = requests.get(
            f"{self.base_url}{path}",
            headers=self._headers(),
            params={key: value for key, value in params.items() if value is not None},
            timeout=20,
        )
        response.raise_for_status()
        return response.json() if response.content else {}

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
        if not self.api_key or not self.from_address:
            return NotificationResult(
                ok=False,
                provider=self.provider,
                title=subject,
                message=text or "",
                skipped=True,
                reason="RESEND_API_KEY and RESEND_FROM_EMAIL are required",
            )
        payload: dict[str, Any] = {"from": self.from_address, "to": to, "subject": subject}
        if text is not None:
            payload["text"] = text
        if html is not None:
            payload["html"] = html
        if attachments:
            payload["attachments"] = attachments
        reserved = {"from", "to", "subject"}.intersection(kwargs)
        if reserved:
            return NotificationResult(
                ok=False,
                provider=self.provider,
                title=subject,
                message=text or "",
                skipped=True,
                reason=f"reserved Resend field(s) cannot be overridden: {', '.join(sorted(reserved))}",
            )
        payload.update(kwargs)
        headers = self._headers()
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        try:
            response = requests.post(f"{self.base_url}/emails", json=payload, headers=headers, timeout=20)
            response.raise_for_status()
            response_payload = response.json() if response.content else {}
        except (requests.RequestException, ValueError) as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            reason = "resend request failed"
            if status_code is not None:
                reason += f" with status {status_code}"
            return NotificationResult(
                ok=False,
                provider=self.provider,
                title=subject,
                message=text or "",
                reason=reason,
            )
        return NotificationResult(
            ok=True,
            provider=self.provider,
            title=subject,
            message=text or "",
            payload=response_payload,
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

    def list_sent(self, *, limit: int = 20, after: str | None = None, before: str | None = None) -> dict[str, Any]:
        return self._get("/emails", limit=limit, after=after, before=before)

    def get_sent(self, email_id: str) -> dict[str, Any]:
        return self._get(f"/emails/{email_id}")

    def get_status(self, email_id: str) -> dict[str, Any]:
        """Return the provider's current sent-message record and delivery state."""
        return self.get_sent(email_id)

    def list_received(self, *, limit: int = 20, after: str | None = None, before: str | None = None) -> dict[str, Any]:
        return self._get("/emails/receiving", limit=limit, after=after, before=before)

    def get_received(self, email_id: str) -> dict[str, Any]:
        return self._get(f"/emails/receiving/{email_id}")

    def list_received_attachments(self, email_id: str) -> dict[str, Any]:
        return self._get(f"/emails/receiving/{email_id}/attachments")

    def get_received_attachment(self, email_id: str, attachment_id: str) -> dict[str, Any]:
        return self._get(f"/emails/receiving/{email_id}/attachments/{attachment_id}")
