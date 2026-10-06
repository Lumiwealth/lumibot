import base64
import hashlib
import json
from dataclasses import dataclass
from typing import Any


@dataclass
class NotificationResult:
    ok: bool
    provider: str
    title: str
    message: str
    severity: str = "info"
    skipped: bool = False
    reason: str | None = None
    payload: dict[str, Any] | None = None


class NotificationManager:
    """Per-strategy notification fanout with backtest-safe defaults."""

    def __init__(self, strategy: Any, *, enabled: bool | None = None) -> None:
        self.strategy = strategy
        self.providers: list[Any] = []
        self.is_backtesting = bool(getattr(strategy, "is_backtesting", False))
        if enabled is None:
            enabled = not self.is_backtesting
        self.enabled = bool(enabled) and not self.is_backtesting

    def _should_send(self, enabled: bool | None) -> bool:
        if self.is_backtesting:
            return False
        return self.enabled if enabled is None else bool(enabled)

    def configure_telegram(
        self,
        *,
        bot_token: str | None = None,
        chat_id: str | None = None,
        parse_mode: str | None = None,
    ) -> Any:
        from .telegram import TelegramNotificationProvider

        provider = TelegramNotificationProvider(bot_token=bot_token, chat_id=chat_id, parse_mode=parse_mode)
        self.providers.append(provider)
        return provider

    def configure_resend(
        self,
        *,
        api_key: str | None = None,
        from_address: str | None = None,
    ) -> Any:
        from .resend import ResendNotificationProvider

        provider = ResendNotificationProvider(api_key=api_key, from_address=from_address)
        self.providers.append(provider)
        return provider

    def configure_slack(
        self,
        *,
        bot_token: str | None = None,
        default_channel: str | None = None,
    ) -> Any:
        from .slack import SlackNotificationProvider

        provider = SlackNotificationProvider(bot_token=bot_token, default_channel=default_channel)
        self.providers.append(provider)
        return provider

    def configure_botspot(
        self,
        *,
        url: str | None = None,
        token_env: str = "BOTSPOT_COMMUNICATIONS_MCP_TOKEN",
        renew_url: str | None = None,
    ) -> Any:
        from .botspot import BotSpotNotificationProvider

        provider = BotSpotNotificationProvider(url=url, token_env=token_env, renew_url=renew_url)
        self.providers.append(provider)
        return provider

    def _provider(self, name: str) -> Any | None:
        return next((provider for provider in self.providers if getattr(provider, "provider", None) == name), None)

    # Fields that may carry personal data or account content (email addresses,
    # balances, positions). They never reach strategy logs, which are shipped to
    # log sinks; only counts, lengths, and short stable hashes are logged so an
    # operator can still correlate a log line with the returned payload.
    _LOG_REDACTED_TEXT_FIELDS = ("subject", "text", "html")

    @staticmethod
    def _short_hash(value: Any) -> str:
        encoded = json.dumps(value, sort_keys=True, default=str, ensure_ascii=False).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()[:16]

    @classmethod
    def _log_safe_communication(cls, payload: dict[str, Any]) -> dict[str, Any]:
        safe: dict[str, Any] = {}
        for key, value in payload.items():
            if key == "to":
                recipients = [value] if isinstance(value, str) else list(value or [])
                safe["recipient_count"] = len(recipients)
                safe["recipients_sha256"] = cls._short_hash(sorted(str(item) for item in recipients))
            elif key in cls._LOG_REDACTED_TEXT_FIELDS:
                if value is not None:
                    safe[f"{key}_length"] = len(str(value))
                    safe[f"{key}_sha256"] = cls._short_hash(str(value))
            elif key == "blocks":
                if value is not None:
                    safe["block_count"] = len(value) if isinstance(value, (list, tuple)) else 1
            elif key == "attachments":
                safe["attachment_count"] = len(value or [])
            else:
                safe[key] = value
        return safe

    def _record_communication(self, payload: dict[str, Any]) -> None:
        logger = getattr(self.strategy, "logger", None)
        if logger is not None:
            logger.info(
                "COMMUNICATION %s",
                json.dumps(self._log_safe_communication(payload), sort_keys=True, default=str),
            )

    @staticmethod
    def _fingerprint_value(value: Any) -> Any:
        if isinstance(value, bytes):
            return {
                "bytes_sha256": hashlib.sha256(value).hexdigest(),
                "size_bytes": len(value),
            }
        if isinstance(value, dict):
            return {
                str(key): NotificationManager._fingerprint_value(item)
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            }
        if isinstance(value, (list, tuple)):
            return [NotificationManager._fingerprint_value(item) for item in value]
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        return str(value)

    @classmethod
    def _email_fingerprint(
        cls,
        *,
        provider: str,
        recipients: list[str],
        subject: str,
        text: str | None,
        html: str | None,
        attachments: list[dict[str, Any]] | None,
        kwargs: dict[str, Any],
    ) -> str:
        canonical = cls._fingerprint_value(
            {
                "provider": provider,
                "to": recipients,
                "subject": subject,
                "text": text,
                "html": html,
                "attachments": attachments or [],
                "options": kwargs,
            }
        )
        encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _idempotency_state(self, provider: str, idempotency_key: str) -> tuple[Any, str]:
        strategy_vars = getattr(self.strategy, "vars", None)
        state_key = (
            "communications.email.idempotency." + hashlib.sha256(f"{provider}\0{idempotency_key}".encode()).hexdigest()
        )
        return strategy_vars, state_key

    @staticmethod
    def _attachment_evidence(attachments: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
        evidence: list[dict[str, Any]] = []
        for item in attachments or []:
            row: dict[str, Any] = {"filename": item.get("filename")}
            content = item.get("content")
            if isinstance(content, str):
                try:
                    raw = base64.b64decode(content, validate=True)
                except (ValueError, TypeError):
                    raw = content.encode("utf-8")
                row.update({"size_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
            elif isinstance(content, bytes):
                row.update({"size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()})
            elif item.get("artifact"):
                row["artifact"] = item.get("artifact")
            evidence.append(row)
        return evidence

    def send_email(
        self,
        *,
        to: list[str] | str,
        subject: str,
        text: str | None = None,
        html: str | None = None,
        attachments: list[dict[str, Any]] | None = None,
        idempotency_key: str | None = None,
        enabled: bool | None = None,
        provider: str = "resend",
        **kwargs: Any,
    ) -> NotificationResult:
        recipients = [to] if isinstance(to, str) else list(to)
        should_send = self._should_send(enabled)
        if not should_send:
            payload = {
                "action": "send_email",
                "provider": provider,
                "status": "simulated_not_sent",
                "to": recipients,
                "subject": subject,
                "text": text,
                "html": html,
                "attachments": self._attachment_evidence(attachments),
                "idempotency_key": idempotency_key,
            }
            self._record_communication(payload)
            return NotificationResult(
                ok=True,
                provider=provider,
                title=subject,
                message=text or "",
                skipped=True,
                reason="simulated_not_sent",
                payload=payload,
            )
        configured_provider = self._provider(provider)
        if configured_provider is None:
            return NotificationResult(
                ok=False,
                provider=provider,
                title=subject,
                message=text or "",
                skipped=True,
                reason=f"{provider} provider not configured",
            )

        fingerprint = None
        strategy_vars = None
        state_key = None
        if idempotency_key:
            fingerprint = self._email_fingerprint(
                provider=provider,
                recipients=recipients,
                subject=subject,
                text=text,
                html=html,
                attachments=attachments,
                kwargs=kwargs,
            )
            strategy_vars, state_key = self._idempotency_state(provider, idempotency_key)
            existing = strategy_vars.get(state_key) if strategy_vars is not None else None
            if isinstance(existing, dict):
                if existing.get("fingerprint") != fingerprint:
                    self._record_communication(
                        {
                            "action": "send_email",
                            "provider": provider,
                            "status": "idempotency_conflict",
                            "idempotency_key": idempotency_key,
                        }
                    )
                    return NotificationResult(
                        ok=False,
                        provider=provider,
                        title=subject,
                        message=text or "",
                        skipped=True,
                        reason="idempotency key was already used for different email content",
                    )
                if existing.get("status") == "accepted":
                    provider_message_id = existing.get("provider_message_id")
                    replay_payload = {"id": provider_message_id} if provider_message_id else {}
                    self._record_communication(
                        {
                            "action": "send_email",
                            "provider": provider,
                            "status": "idempotent_replay",
                            "idempotency_key": idempotency_key,
                            "provider_message_id": provider_message_id,
                        }
                    )
                    return NotificationResult(
                        ok=True,
                        provider=provider,
                        title=subject,
                        message=text or "",
                        skipped=True,
                        reason="idempotent_replay",
                        payload=replay_payload,
                    )
        result = configured_provider.send_email(
            to=recipients,
            subject=subject,
            text=text,
            html=html,
            attachments=attachments,
            idempotency_key=idempotency_key,
            **kwargs,
        )
        if result.ok and idempotency_key and strategy_vars is not None and state_key and fingerprint:
            provider_payload = result.payload or {}
            provider_message_id = provider_payload.get("id") or provider_payload.get("messageId")
            strategy_vars.set(
                state_key,
                {
                    "fingerprint": fingerprint,
                    "status": "accepted",
                    "provider_message_id": provider_message_id,
                },
            )
        self._record_communication(
            {
                "action": "send_email",
                "provider": provider,
                "status": "accepted" if result.ok else "failed",
                "to": recipients,
                "subject": subject,
                "text": text,
                "html": html,
                "attachments": self._attachment_evidence(attachments),
                "idempotency_key": idempotency_key,
                "provider_message_id": (result.payload or {}).get("id"),
            }
        )
        return result

    def _fixture(self, key: str) -> Any:
        parameters = getattr(self.strategy, "parameters", {}) or {}
        fixtures = parameters.get("communication_fixtures", {}) if isinstance(parameters, dict) else {}
        if key not in fixtures:
            raise RuntimeError(
                f"Backtests require parameters['communication_fixtures']['{key}']; "
                "live communication reads are disabled."
            )
        return fixtures[key]

    def _read(self, provider_name: str, method: str, fixture_key: str, **kwargs: Any) -> Any:
        if bool(getattr(self.strategy, "is_backtesting", False)):
            payload = self._fixture(fixture_key)
            self._record_communication(
                {
                    "action": method,
                    "provider": provider_name,
                    "status": "fixture_read",
                    "fixture_key": fixture_key,
                }
            )
            return payload
        provider = self._provider(provider_name)
        if provider is None:
            raise RuntimeError(f"{provider_name} provider not configured")
        return getattr(provider, method)(**kwargs)

    def list_sent_emails(self, *, provider: str = "resend", **kwargs: Any) -> dict[str, Any]:
        return self._read(provider, "list_sent", f"{provider}.sent", **kwargs)

    def get_sent_email(self, email_id: str, *, provider: str = "resend") -> dict[str, Any]:
        return self._read(provider, "get_sent", f"{provider}.sent.{email_id}", email_id=email_id)

    def get_email_status(self, email_id: str, *, provider: str = "resend") -> dict[str, Any]:
        return self._read(provider, "get_status", f"{provider}.sent.{email_id}", email_id=email_id)

    def list_received_emails(self, **kwargs: Any) -> dict[str, Any]:
        return self._read("resend", "list_received", "resend.received", **kwargs)

    def get_received_email(self, email_id: str) -> dict[str, Any]:
        return self._read("resend", "get_received", f"resend.received.{email_id}", email_id=email_id)

    def list_received_email_attachments(self, email_id: str) -> dict[str, Any]:
        return self._read(
            "resend",
            "list_received_attachments",
            f"resend.received.{email_id}.attachments",
            email_id=email_id,
        )

    def get_received_email_attachment(self, email_id: str, attachment_id: str) -> dict[str, Any]:
        return self._read(
            "resend",
            "get_received_attachment",
            f"resend.received.{email_id}.attachment.{attachment_id}",
            email_id=email_id,
            attachment_id=attachment_id,
        )

    def send_slack_message(
        self,
        text: str,
        *,
        channel: str | None = None,
        thread_ts: str | None = None,
        blocks: list[dict[str, Any]] | None = None,
        enabled: bool | None = None,
    ) -> dict[str, Any]:
        should_send = self._should_send(enabled)
        if not should_send:
            payload = {
                "action": "send_slack_message",
                "provider": "slack",
                "status": "simulated_not_sent",
                "channel": channel,
                "thread_ts": thread_ts,
                "text": text,
                "blocks": blocks,
            }
            self._record_communication(payload)
            return payload
        provider = self._provider("slack")
        if provider is None:
            raise RuntimeError("slack provider not configured")
        payload = provider.send_message(text, channel=channel, thread_ts=thread_ts, blocks=blocks)
        self._record_communication(
            {
                "action": "send_slack_message",
                "provider": "slack",
                "status": "accepted",
                "channel": payload.get("channel") or channel,
                "message_ts": payload.get("ts"),
            }
        )
        return payload

    def list_slack_messages(self, **kwargs: Any) -> dict[str, Any]:
        return self._read("slack", "list_messages", "slack.messages", **kwargs)

    def list_slack_channels(self, **kwargs: Any) -> dict[str, Any]:
        return self._read("slack", "list_channels", "slack.channels", **kwargs)

    def get_slack_message(self, message_ts: str, **kwargs: Any) -> dict[str, Any]:
        channel = kwargs.get("channel") or "default"
        return self._read(
            "slack",
            "get_message",
            f"slack.message.{channel}.{message_ts}",
            message_ts=message_ts,
            **kwargs,
        )

    def list_slack_thread(self, thread_ts: str, **kwargs: Any) -> dict[str, Any]:
        return self._read(
            "slack",
            "list_thread",
            f"slack.thread.{thread_ts}",
            thread_ts=thread_ts,
            **kwargs,
        )

    def notify(
        self,
        title: str,
        message: str,
        *,
        severity: str = "info",
        enabled: bool | None = None,
        **kwargs: Any,
    ) -> list[NotificationResult]:
        should_send = self._should_send(enabled)
        if not should_send:
            return [
                NotificationResult(
                    ok=True,
                    provider="none",
                    title=title,
                    message=message,
                    severity=severity,
                    skipped=True,
                    reason="notifications disabled",
                )
            ]
        if not self.providers:
            return [
                NotificationResult(
                    ok=False,
                    provider="none",
                    title=title,
                    message=message,
                    severity=severity,
                    skipped=True,
                    reason="no notification providers configured",
                )
            ]
        results = []
        for provider in self.providers:
            results.append(provider.notify(title=title, message=message, severity=severity, **kwargs))
        return results
