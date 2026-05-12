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
        if enabled is None:
            enabled = not bool(getattr(strategy, "is_backtesting", False))
        self.enabled = bool(enabled)

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

    def notify(
        self,
        title: str,
        message: str,
        *,
        severity: str = "info",
        enabled: bool | None = None,
        **kwargs: Any,
    ) -> list[NotificationResult]:
        should_send = self.enabled if enabled is None else bool(enabled)
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
