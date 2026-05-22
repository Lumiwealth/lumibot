import stat
import sys
from datetime import datetime, timezone

import pytest
import requests

from lumibot.components.memory import MemoryStore
from lumibot.components.notifications import NotificationManager


class _Strategy:
    is_backtesting = True
    name = "Unit Memory Strategy"

    def get_datetime(self):
        return datetime(2026, 1, 2, 15, 30, tzinfo=timezone.utc)


class _Response:
    content = b"{}"

    def raise_for_status(self):
        return None

    def json(self):
        return {"ok": True}


def test_notifications_are_disabled_by_default_in_backtests():
    manager = NotificationManager(_Strategy())

    result = manager.notify("Decision", "Bought nothing")

    assert result[0].ok is True
    assert result[0].skipped is True
    assert result[0].reason == "notifications disabled"


def test_telegram_notification_provider_posts_payload(monkeypatch):
    calls = []

    def fake_post(url, json, timeout):
        calls.append({"url": url, "json": json, "timeout": timeout})
        return _Response()

    monkeypatch.setattr("lumibot.components.notifications.telegram.requests.post", fake_post)
    manager = NotificationManager(_Strategy(), enabled=True)
    manager.configure_telegram(bot_token="token", chat_id="chat")

    result = manager.notify("Trade decision", "Bought AAPL", severity="info")

    assert result[0].ok is True
    assert calls[0]["url"] == "https://api.telegram.org/bottoken/sendMessage"
    assert calls[0]["json"]["chat_id"] == "chat"
    assert "Bought AAPL" in calls[0]["json"]["text"]


def test_telegram_notification_provider_does_not_return_token_on_failure(monkeypatch):
    def fake_post(url, json, timeout):
        response = requests.Response()
        response.status_code = 401
        raise requests.HTTPError(f"401 Client Error for url: {url}", response=response)

    monkeypatch.setattr("lumibot.components.notifications.telegram.requests.post", fake_post)
    manager = NotificationManager(_Strategy(), enabled=True)
    manager.configure_telegram(bot_token="super-secret-token", chat_id="chat")

    result = manager.notify("Trade decision", "Bought AAPL", severity="warning")

    assert result[0].ok is False
    assert result[0].skipped is False
    assert result[0].reason == "telegram request failed with status 401"
    assert "super-secret-token" not in result[0].reason


def test_memory_store_records_and_searches_decisions(tmp_path):
    store = MemoryStore(_Strategy(), root_dir=tmp_path)

    decision = store.remember_decision("Bought AAPL because filings and indicators were strong.", symbol="AAPL", action="buy")
    lesson = store.remember_lesson("Do not chase entries after earnings gaps.", symbol="AAPL")
    thesis = store.open_thesis("AAPL long thesis based on services margin resilience.", symbol="AAPL")

    result = store.search("AAPL margin", limit=10)

    assert decision["kind"] == "decision"
    assert lesson["kind"] == "lesson"
    assert thesis["kind"] == "thesis"
    assert result["count"] >= 2
    assert any("services margin" in row["text"] for row in result["results"])


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions not supported on Windows")
def test_memory_store_uses_private_posix_permissions(tmp_path):
    store = MemoryStore(_Strategy(), root_dir=tmp_path)

    store.remember_decision("Bought AAPL because filings and indicators were strong.", symbol="AAPL", action="buy")
    store.remember_lesson("Do not chase entries after earnings gaps.", symbol="AAPL")
    store.open_thesis("AAPL long thesis based on services margin resilience.", symbol="AAPL")

    strategy_dir = tmp_path / "Unit_Memory_Strategy"
    assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o700
    assert stat.S_IMODE(strategy_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE((strategy_dir / "decisions.jsonl").stat().st_mode) == 0o600
    assert stat.S_IMODE((strategy_dir / "lessons.jsonl").stat().st_mode) == 0o600
    assert stat.S_IMODE((strategy_dir / "theses.jsonl").stat().st_mode) == 0o600
    assert stat.S_IMODE((strategy_dir / "memories.jsonl").stat().st_mode) == 0o600
