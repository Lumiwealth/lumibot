from datetime import datetime, timezone

import requests

from lumibot.components.agents.builtins import BuiltinTools
from lumibot.components.memory import MemoryStore
from lumibot.components.notifications import NotificationManager


class _Strategy:
    is_backtesting = True
    name = "Unit Memory Strategy"

    def get_datetime(self):
        return datetime(2026, 1, 2, 15, 30, tzinfo=timezone.utc)


class _LiveStrategy(_Strategy):
    is_backtesting = False


class _Response:
    content = b"{}"
    status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return {"ok": True}


class _JsonResponse:
    def __init__(self, payload, *, status_code=200, content=b"{}"):
        self._payload = payload
        self.status_code = status_code
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            response = requests.Response()
            response.status_code = self.status_code
            raise requests.HTTPError(response=response)

    def json(self):
        return self._payload


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
    manager = NotificationManager(_LiveStrategy(), enabled=True)
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
    manager = NotificationManager(_LiveStrategy(), enabled=True)
    manager.configure_telegram(bot_token="super-secret-token", chat_id="chat")

    result = manager.notify("Trade decision", "Bought AAPL", severity="warning")

    assert result[0].ok is False
    assert result[0].skipped is False
    assert result[0].reason == "telegram request failed with status 401"
    assert "super-secret-token" not in result[0].reason


def test_resend_provider_sends_idempotent_email_with_attachments(monkeypatch):
    calls = []

    def fake_post(url, json, headers, timeout):
        calls.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return _JsonResponse({"id": "email-123"})

    monkeypatch.setattr("lumibot.components.notifications.resend.requests.post", fake_post)
    manager = NotificationManager(_LiveStrategy(), enabled=True)
    manager.configure_resend(api_key="re-secret", from_address="Bot <bot@example.com>")

    result = manager.send_email(
        to=["rob@example.com"],
        subject="Christy's Picks",
        text="The report is attached.",
        attachments=[{"filename": "picks.csv", "content": "c3ltYm9sXG5WRVJB"}],
        idempotency_key="christy/2026-09-20",
    )

    assert result.ok is True
    assert result.payload == {"id": "email-123"}
    assert calls[0]["url"] == "https://api.resend.com/emails"
    assert calls[0]["json"]["from"] == "Bot <bot@example.com>"
    assert calls[0]["json"]["attachments"][0]["filename"] == "picks.csv"
    assert calls[0]["headers"]["Idempotency-Key"] == "christy/2026-09-20"
    assert "re-secret" not in repr(result)


def test_resend_provider_does_not_allow_per_message_sender_override(monkeypatch):
    manager = NotificationManager(_LiveStrategy(), enabled=True)
    manager.configure_resend(api_key="re-secret", from_address="Bot <bot@example.com>")

    result = manager.send_email(
        to=["rob@example.com"],
        subject="Christy's Picks",
        text="The report is attached.",
        **{"from": "forged@example.com"},
    )

    assert result.ok is False
    assert result.skipped is True
    assert "reserved Resend field" in result.reason


def test_resend_provider_reads_sent_received_and_attachments(monkeypatch):
    calls = []

    def fake_get(url, headers, params, timeout):
        calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        if url.endswith("/emails"):
            return _JsonResponse({"data": [{"id": "sent-1"}], "has_more": False})
        if url.endswith("/emails/receiving"):
            return _JsonResponse({"data": [{"id": "received-1"}], "has_more": False})
        if url.endswith("/emails/receiving/received-1"):
            return _JsonResponse({"id": "received-1", "subject": "Reply"})
        if url.endswith("/emails/receiving/received-1/attachments"):
            return _JsonResponse({"data": [{"id": "attachment-1", "download_url": "https://download.test/a"}]})
        if url.endswith("/emails/receiving/received-1/attachments/attachment-1"):
            return _JsonResponse({"id": "attachment-1", "download_url": "https://download.test/a"})
        if url.endswith("/emails/sent-1"):
            return _JsonResponse({"id": "sent-1", "last_event": "delivered"})
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.components.notifications.resend.requests.get", fake_get)
    manager = NotificationManager(_LiveStrategy(), enabled=True)
    provider = manager.configure_resend(api_key="re-secret", from_address="Bot <bot@example.com>")

    assert provider.list_sent(limit=10)["data"][0]["id"] == "sent-1"
    assert manager.get_sent_email("sent-1")["id"] == "sent-1"
    assert provider.get_status("sent-1")["last_event"] == "delivered"
    assert provider.list_received(limit=10)["data"][0]["id"] == "received-1"
    assert provider.get_received("received-1")["subject"] == "Reply"
    assert provider.list_received_attachments("received-1")["data"][0]["id"] == "attachment-1"
    assert provider.get_received_attachment("received-1", "attachment-1")["id"] == "attachment-1"
    assert all(call["headers"]["Authorization"] == "Bearer re-secret" for call in calls)


def test_slack_provider_posts_and_reads_messages(monkeypatch):
    calls = []

    def fake_post(url, json, headers, timeout):
        calls.append({"method": "POST", "url": url, "json": json, "headers": headers})
        return _JsonResponse({"ok": True, "channel": "C123", "ts": "1.2"})

    def fake_get(url, headers, params, timeout):
        calls.append({"method": "GET", "url": url, "params": params, "headers": headers})
        if url.endswith("/conversations.list"):
            return _JsonResponse({"ok": True, "channels": [{"id": "C123", "name": "trading"}]})
        return _JsonResponse({"ok": True, "messages": [{"ts": "1.2", "text": "Done"}]})

    monkeypatch.setattr("lumibot.components.notifications.slack.requests.post", fake_post)
    monkeypatch.setattr("lumibot.components.notifications.slack.requests.get", fake_get)
    manager = NotificationManager(_LiveStrategy(), enabled=True)
    provider = manager.configure_slack(bot_token="xoxb-secret", default_channel="C123")

    sent = manager.send_slack_message(
        "Done",
        thread_ts="1.0",
        blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": "Done"}}],
    )
    channels = provider.list_channels(limit=100)
    history = provider.list_messages(limit=15)
    message = provider.get_message("1.2")
    thread = provider.list_thread("1.0", limit=15)

    assert sent["ok"] is True
    assert channels["channels"][0]["id"] == "C123"
    assert history["messages"][0]["text"] == "Done"
    assert message["text"] == "Done"
    assert thread["messages"][0]["ts"] == "1.2"
    assert calls[0]["url"].endswith("/chat.postMessage")
    assert calls[0]["json"]["blocks"][0]["type"] == "section"
    assert calls[1]["url"].endswith("/conversations.list")
    assert calls[2]["url"].endswith("/conversations.history")
    assert calls[3]["url"].endswith("/conversations.history")
    assert calls[4]["url"].endswith("/conversations.replies")


def test_backtest_email_records_simulation_without_network(monkeypatch, caplog):
    manager = NotificationManager(_Strategy())
    manager.configure_resend(api_key="re-secret", from_address="Bot <bot@example.com>")

    def fail_post(*args, **kwargs):
        raise AssertionError("backtest must not use the network")

    monkeypatch.setattr("lumibot.components.notifications.resend.requests.post", fail_post)
    result = manager.send_email(
        to=["rob@example.com"],
        subject="Christy's Picks",
        text="Dry run",
        idempotency_key="christy/backtest",
    )

    assert result.ok is True
    assert result.skipped is True
    assert result.reason == "simulated_not_sent"
    assert result.payload["status"] == "simulated_not_sent"


def test_backtest_email_log_preserves_content_and_attachment_evidence(caplog):
    manager = NotificationManager(_Strategy())
    result = manager.send_email(
        to=["rob@example.com"],
        subject="Christy's Picks",
        text="Three files are attached.",
        html="<p>Three files are attached.</p>",
        attachments=[{"filename": "picks.csv", "content": "c3ltYm9sXG5WRVJB"}],
        idempotency_key="christy/backtest",
    )

    assert result.payload["text"] == "Three files are attached."
    assert result.payload["html"] == "<p>Three files are attached.</p>"
    assert result.payload["attachments"] == [
        {
            "filename": "picks.csv",
            "size_bytes": 12,
            "sha256": "74ac32db0f7e53033d018f16b69ff2f41472f20fac10c07da23861371ad913dd",
        }
    ]


def test_backtest_communication_cannot_be_force_enabled(monkeypatch):
    manager = NotificationManager(_Strategy(), enabled=True)
    manager.configure_resend(api_key="re-secret", from_address="Bot <bot@example.com>")
    manager.configure_slack(bot_token="xoxb-secret", default_channel="C123")

    monkeypatch.setattr(
        "lumibot.components.notifications.resend.requests.post",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("backtest must not send email")),
    )
    monkeypatch.setattr(
        "lumibot.components.notifications.slack.requests.post",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("backtest must not send Slack")),
    )

    email = manager.send_email(
        to="rob@example.com",
        subject="Never sent",
        text="Backtest proof",
        enabled=True,
    )
    slack = manager.send_slack_message("Never sent", enabled=True)

    assert email.skipped is True
    assert email.reason == "simulated_not_sent"
    assert slack["status"] == "simulated_not_sent"


def test_backtest_reads_use_timestamped_fixtures_without_network():
    strategy = _Strategy()
    strategy.parameters = {
        "communication_fixtures": {
            "resend.received": {"data": [{"id": "received-fixture"}], "captured_at": "2026-09-20T12:00:00Z"},
            "slack.messages": {"messages": [{"ts": "1.2", "text": "fixture"}], "captured_at": "2026-09-20T12:00:00Z"},
        }
    }
    manager = NotificationManager(strategy)

    assert manager.list_received_emails()["data"][0]["id"] == "received-fixture"
    assert manager.list_slack_messages()["messages"][0]["text"] == "fixture"


def test_agents_expose_scopable_email_and_slack_read_write_tools():
    names = {tool.name for tool in BuiltinTools.all()}

    assert {
        "send_email",
        "list_sent_emails",
        "get_sent_email",
        "get_email_status",
        "list_received_emails",
        "get_received_email",
        "list_received_email_attachments",
        "get_received_email_attachment",
        "send_slack_message",
        "list_slack_channels",
        "list_slack_messages",
        "get_slack_message",
        "list_slack_thread",
    }.issubset(names)


def test_memory_store_records_and_searches_decisions(tmp_path):
    store = MemoryStore(_Strategy(), root_dir=tmp_path)

    decision = store.remember_decision(
        "Bought AAPL because filings and indicators were strong.",
        symbol="AAPL",
        action="buy",
    )
    lesson = store.remember_lesson("Do not chase entries after earnings gaps.", symbol="AAPL")
    thesis = store.open_thesis("AAPL long thesis based on services margin resilience.", symbol="AAPL")

    result = store.search("AAPL margin", limit=10)

    assert decision["kind"] == "decision"
    assert lesson["kind"] == "lesson"
    assert thesis["kind"] == "thesis"
    assert result["count"] >= 2
    assert any("services margin" in row["text"] for row in result["results"])
