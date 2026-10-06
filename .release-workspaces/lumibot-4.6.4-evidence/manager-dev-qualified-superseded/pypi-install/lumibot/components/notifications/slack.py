import os
import time
from typing import Any

import requests

from .base import NotificationResult


class SlackNotificationProvider:
    """Slack Web API provider for sending and reading messages."""

    provider = "slack"
    base_url = "https://slack.com/api"

    def __init__(self, *, bot_token: str | None = None, default_channel: str | None = None) -> None:
        self.bot_token = bot_token or os.environ.get("SLACK_BOT_TOKEN")
        self.default_channel = default_channel or os.environ.get("SLACK_CHANNEL_ID")

    def _headers(self) -> dict[str, str]:
        if not self.bot_token:
            raise ValueError("SLACK_BOT_TOKEN is required")
        return {"Authorization": f"Bearer {self.bot_token}", "Content-Type": "application/json; charset=utf-8"}

    @staticmethod
    def _payload(response: requests.Response) -> dict[str, Any]:
        response.raise_for_status()
        payload = response.json() if response.content else {}
        if not payload.get("ok", False):
            raise ValueError(payload.get("error") or "slack request failed")
        return payload

    @staticmethod
    def _retry_delay(response: requests.Response) -> float:
        raw = (getattr(response, "headers", {}) or {}).get("Retry-After", "0")
        try:
            return min(max(float(raw), 0.0), 60.0)
        except (TypeError, ValueError):
            return 0.0

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        for attempt in range(2):
            request = requests.get if method == "GET" else requests.post
            response = request(f"{self.base_url}/{path}", headers=self._headers(), timeout=20, **kwargs)
            if response.status_code != 429:
                return response
            if attempt == 1:
                return response
            time.sleep(self._retry_delay(response))
        raise AssertionError("unreachable")

    def send_message(
        self,
        text: str,
        *,
        channel: str | None = None,
        thread_ts: str | None = None,
        blocks: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        target = channel or self.default_channel
        if not target:
            raise ValueError("Slack channel is required")
        payload: dict[str, Any] = {"channel": target, "text": text}
        if thread_ts:
            payload["thread_ts"] = thread_ts
        if blocks:
            payload["blocks"] = blocks
        response = self._request(
            "POST",
            "chat.postMessage",
            json=payload,
        )
        return self._payload(response)

    def _read(self, method: str, *, channel: str | None = None, **params: Any) -> dict[str, Any]:
        target = channel or self.default_channel
        if not target:
            raise ValueError("Slack channel is required")
        response = self._request(
            "GET",
            method,
            params={"channel": target, **{key: value for key, value in params.items() if value is not None}},
        )
        return self._payload(response)

    def list_messages(
        self,
        *,
        channel: str | None = None,
        limit: int = 15,
        cursor: str | None = None,
        oldest: str | None = None,
        latest: str | None = None,
    ) -> dict[str, Any]:
        return self._read(
            "conversations.history",
            channel=channel,
            limit=limit,
            cursor=cursor,
            oldest=oldest,
            latest=latest,
        )

    def list_channels(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        types: str = "public_channel,private_channel",
    ) -> dict[str, Any]:
        response = self._request(
            "GET",
            "conversations.list",
            params={
                "limit": limit,
                "types": types,
                **({"cursor": cursor} if cursor else {}),
            },
        )
        return self._payload(response)

    def get_message(self, message_ts: str, *, channel: str | None = None) -> dict[str, Any]:
        payload = self._read(
            "conversations.history",
            channel=channel,
            oldest=message_ts,
            latest=message_ts,
            inclusive=True,
            limit=1,
        )
        messages = payload.get("messages") or []
        if not messages or messages[0].get("ts") != message_ts:
            raise LookupError("Slack message was not found in the requested conversation")
        return messages[0]

    def list_thread(
        self,
        thread_ts: str,
        *,
        channel: str | None = None,
        limit: int = 15,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        return self._read(
            "conversations.replies",
            channel=channel,
            ts=thread_ts,
            limit=limit,
            cursor=cursor,
        )

    def notify(self, *, title: str, message: str, severity: str = "info", **kwargs: Any) -> NotificationResult:
        try:
            payload = self.send_message(f"[{severity.upper()}] {title}\n\n{message}".strip(), **kwargs)
        except (requests.RequestException, ValueError) as exc:
            return NotificationResult(
                ok=False,
                provider=self.provider,
                title=title,
                message=message,
                severity=severity,
                reason=f"slack request failed: {type(exc).__name__}",
            )
        return NotificationResult(
            ok=True,
            provider=self.provider,
            title=title,
            message=message,
            severity=severity,
            payload=payload,
        )
