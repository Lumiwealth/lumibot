import os
from typing import Any

import requests

from .base import NotificationResult


class TelegramNotificationProvider:
    """Telegram Bot API notification provider."""

    provider = "telegram"

    def __init__(
        self,
        *,
        bot_token: str | None = None,
        chat_id: str | None = None,
        parse_mode: str | None = None,
    ) -> None:
        self.bot_token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id or os.environ.get("TELEGRAM_CHAT_ID")
        self.parse_mode = parse_mode

    def notify(self, *, title: str, message: str, severity: str = "info", **kwargs: Any) -> NotificationResult:
        if not self.bot_token or not self.chat_id:
            return NotificationResult(
                ok=False,
                provider=self.provider,
                title=title,
                message=message,
                severity=severity,
                skipped=True,
                reason="TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required",
            )
        text = f"[{severity.upper()}] {title}\n\n{message}".strip()
        payload: dict[str, Any] = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if self.parse_mode:
            payload["parse_mode"] = self.parse_mode
        payload.update(kwargs)
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        try:
            response = requests.post(url, json=payload, timeout=20)
            response.raise_for_status()
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            reason = f"telegram request failed"
            if status_code is not None:
                reason = f"{reason} with status {status_code}"
            return NotificationResult(
                ok=False,
                provider=self.provider,
                title=title,
                message=message,
                severity=severity,
                skipped=False,
                reason=reason,
            )
        try:
            response_payload = response.json() if response.content else {}
        except ValueError:
            response_payload = {}
        return NotificationResult(
            ok=True,
            provider=self.provider,
            title=title,
            message=message,
            severity=severity,
            payload=response_payload,
        )
