from types import SimpleNamespace

import pytest

from lumibot.components.agents.manager import AgentManager
from lumibot.components.notifications.resend import ResendNotificationProvider
from lumibot.components.notifications.slack import SlackNotificationProvider


class _Vars:
    def __init__(self):
        self.data = {}

    def get(self, key, default=None):
        return self.data.get(key, default)

    def set(self, key, value):
        self.data[key] = value


class _Strategy:
    def __init__(self):
        self.vars = _Vars()
        self.parameters = {}

    def log_message(self, *_args, **_kwargs):
        return None


class _Response:
    def __init__(self, payload, status_code=200, headers=None):
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.content = b"{}"

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            response = requests.Response()
            response.status_code = self.status_code
            raise requests.HTTPError(response=response)

    def json(self):
        return self.payload


def _tool_names(handle):
    return {getattr(tool, "name", None) for tool in handle._tool_inputs}


def test_agent_communication_permissions_are_separate_and_default_off():
    manager = AgentManager(_Strategy())
    runtime = SimpleNamespace()
    neither = manager.create(name="neither", model="openai/test", include_builtin_skills=False, _runtime=runtime)
    reads = manager.create(
        name="reads",
        model="openai/test",
        allow_communication_reads=True,
        include_builtin_skills=False,
        _runtime=runtime,
    )
    writes = manager.create(
        name="writes",
        model="openai/test",
        allow_communication_writes=True,
        include_builtin_skills=False,
        _runtime=runtime,
    )

    assert "send_email" not in _tool_names(neither)
    assert "list_received_emails" not in _tool_names(neither)
    assert "send_email" not in _tool_names(reads)
    assert "list_received_emails" in _tool_names(reads)
    assert "send_email" in _tool_names(writes)
    assert "list_received_emails" not in _tool_names(writes)


def test_actual_bound_agent_email_tool_calls_the_strategy_provider_path():
    strategy = _Strategy()
    calls = []

    def send_email(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(
            ok=True,
            provider="resend",
            title=kwargs["subject"],
            message=kwargs.get("text") or "",
            skipped=False,
            reason=None,
            payload={"id": "email-1"},
        )

    strategy.send_email = send_email
    handle = AgentManager(strategy).create(
        name="writer",
        model="openai/test",
        allow_communication_writes=True,
        include_builtin_skills=False,
        _runtime=SimpleNamespace(),
    )
    tools = {tool.name: tool for tool in handle._ensure_bound_tools()}

    result = tools["send_email"].function(
        to=["owner@example.com"],
        subject="Daily summary",
        text="Attached.",
        attachments=[{"filename": "source.csv", "content": "c291cmNl"}],
        idempotency_key="daily-summary/2026-09-20",
    )

    assert result["ok"] is True
    assert result["payload"] == {"id": "email-1"}
    assert calls == [
        {
            "to": ["owner@example.com"],
            "subject": "Daily summary",
            "text": "Attached.",
            "html": None,
            "attachments": [{"filename": "source.csv", "content": "c291cmNl"}],
            "idempotency_key": "daily-summary/2026-09-20",
            "provider": "resend",
        }
    ]


def test_resend_requires_body_and_valid_idempotency_key():
    provider = ResendNotificationProvider(api_key="secret", from_address="Bot <bot@example.com>")

    no_body = provider.send_email(to=["owner@example.com"], subject="No body")
    oversized_key = provider.send_email(
        to=["owner@example.com"], subject="Body", text="hello", idempotency_key="x" * 257
    )

    assert no_body.ok is False
    assert no_body.reason == "email text or html content is required"
    assert oversized_key.ok is False
    assert "256" in oversized_key.reason


@pytest.mark.parametrize("attachment_count", [0, 1, 3])
@pytest.mark.parametrize(
    ("text", "html"),
    [
        ("Plain text", None),
        (None, "<p>HTML</p>"),
        ("Plain text", "<p>HTML</p>"),
    ],
)
def test_resend_supports_zero_one_three_attachments_and_body_modes(monkeypatch, attachment_count, text, html):
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return _Response({"id": "email-1"})

    monkeypatch.setattr("lumibot.components.notifications.resend.requests.post", post)
    provider = ResendNotificationProvider(api_key="secret", from_address="Bot <bot@example.com>")
    attachments = [{"filename": f"file-{index}.csv", "content": "c3ltYm9sCg=="} for index in range(attachment_count)]

    result = provider.send_email(
        to=["owner@example.com"],
        subject="Daily summary",
        text=text,
        html=html,
        attachments=attachments,
        idempotency_key=f"daily-summary/{attachment_count}/{bool(text)}/{bool(html)}",
    )

    assert result.ok is True
    payload = calls[0][1]["json"]
    assert payload.get("text") == text
    assert payload.get("html") == html
    if attachment_count:
        assert payload["attachments"] == attachments
    else:
        assert "attachments" not in payload


def test_resend_read_operations_use_provider_endpoints_and_pagination(monkeypatch):
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return _Response({"data": []})

    monkeypatch.setattr("lumibot.components.notifications.resend.requests.get", get)
    provider = ResendNotificationProvider(api_key="secret", from_address="Bot <bot@example.com>")

    provider.list_sent(limit=7, after="sent-cursor")
    provider.get_sent("sent-1")
    provider.list_received(limit=9, before="received-cursor")
    provider.get_received("received-1")
    provider.list_received_attachments("received-1")
    provider.get_received_attachment("received-1", "attachment-1")

    assert [url.removeprefix("https://api.resend.com") for url, _kwargs in calls] == [
        "/emails",
        "/emails/sent-1",
        "/emails/receiving",
        "/emails/receiving/received-1",
        "/emails/receiving/received-1/attachments",
        "/emails/receiving/received-1/attachments/attachment-1",
    ]
    assert calls[0][1]["params"] == {"limit": 7, "after": "sent-cursor"}
    assert calls[2][1]["params"] == {"limit": 9, "before": "received-cursor"}


def test_resend_retries_transient_idempotent_send_once(monkeypatch):
    responses = [
        _Response({}, status_code=503, headers={"Retry-After": "0"}),
        _Response({"id": "email-1"}),
    ]
    calls = []

    def post(url, **kwargs):
        calls.append(SimpleNamespace(url=url, kwargs=kwargs))
        return responses.pop(0)

    monkeypatch.setattr("lumibot.components.notifications.resend.requests.post", post)
    provider = ResendNotificationProvider(api_key="secret", from_address="Bot <bot@example.com>")

    result = provider.send_email(
        to=["owner@example.com"],
        subject="Daily summary",
        text="hello",
        idempotency_key="daily-summary/2026-09-20",
    )

    assert result.ok is True
    assert result.payload == {"id": "email-1"}
    assert len(calls) == 2
    assert calls[0].kwargs["headers"]["Idempotency-Key"] == "daily-summary/2026-09-20"


def test_resend_does_not_retry_non_idempotent_send(monkeypatch):
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return _Response({}, status_code=503)

    monkeypatch.setattr("lumibot.components.notifications.resend.requests.post", post)
    provider = ResendNotificationProvider(api_key="secret", from_address="Bot <bot@example.com>")

    result = provider.send_email(to=["owner@example.com"], subject="Daily summary", text="hello")

    assert result.ok is False
    assert len(calls) == 1


def test_notification_manager_replays_same_idempotency_payload_without_resending(monkeypatch):
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return _Response({"id": "email-1"})

    monkeypatch.setattr("lumibot.components.notifications.resend.requests.post", post)
    strategy = _Strategy()
    strategy.is_backtesting = False
    from lumibot.components.notifications.base import NotificationManager

    manager = NotificationManager(strategy)
    manager.configure_resend(api_key="secret", from_address="Bot <bot@example.com>")
    message = {
        "to": ["owner@example.com"],
        "subject": "Daily summary",
        "text": "hello",
        "idempotency_key": "daily-summary/2026-09-20",
    }

    first = manager.send_email(**message)
    restarted_manager = NotificationManager(strategy)
    restarted_manager.configure_resend(api_key="secret", from_address="Bot <bot@example.com>")
    replay = restarted_manager.send_email(**message)

    assert first.ok is True
    assert replay.ok is True
    assert replay.skipped is True
    assert replay.reason == "idempotent_replay"
    assert replay.payload == {"id": "email-1"}
    assert len(calls) == 1


def test_notification_manager_rejects_changed_payload_for_existing_idempotency_key(monkeypatch):
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return _Response({"id": "email-1"})

    monkeypatch.setattr("lumibot.components.notifications.resend.requests.post", post)
    strategy = _Strategy()
    strategy.is_backtesting = False
    from lumibot.components.notifications.base import NotificationManager

    manager = NotificationManager(strategy)
    manager.configure_resend(api_key="secret", from_address="Bot <bot@example.com>")
    first = manager.send_email(
        to=["owner@example.com"],
        subject="Daily summary",
        text="hello",
        idempotency_key="daily-summary/2026-09-20",
    )
    restarted_manager = NotificationManager(strategy)
    restarted_manager.configure_resend(api_key="secret", from_address="Bot <bot@example.com>")
    conflict = restarted_manager.send_email(
        to=["owner@example.com"],
        subject="Daily summary",
        text="changed",
        idempotency_key="daily-summary/2026-09-20",
    )

    assert first.ok is True
    assert conflict.ok is False
    assert conflict.skipped is True
    assert conflict.reason == "idempotency key was already used for different email content"
    assert len(calls) == 1


def test_slack_retries_429_once(monkeypatch):
    responses = [
        _Response({"ok": False, "error": "ratelimited"}, status_code=429, headers={"Retry-After": "0"}),
        _Response({"ok": True, "channel": "C123", "ts": "1.2"}),
    ]
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return responses.pop(0)

    monkeypatch.setattr("lumibot.components.notifications.slack.requests.post", post)
    provider = SlackNotificationProvider(bot_token="xoxb-secret", default_channel="C123")

    result = provider.send_message("hello")

    assert result["ok"] is True
    assert len(calls) == 2


def test_slack_send_read_thread_and_cursor_contract(monkeypatch):
    post_calls = []
    get_calls = []

    def post(url, **kwargs):
        post_calls.append((url, kwargs))
        return _Response({"ok": True, "channel": "C123", "ts": "1.2"})

    def get(url, **kwargs):
        get_calls.append((url, kwargs))
        if url.endswith("conversations.list"):
            return _Response({"ok": True, "channels": [], "response_metadata": {"next_cursor": "next"}})
        if kwargs["params"].get("oldest") == "1.2":
            return _Response({"ok": True, "messages": [{"ts": "1.2", "text": "hello"}]})
        return _Response({"ok": True, "messages": [], "response_metadata": {"next_cursor": "next"}})

    monkeypatch.setattr("lumibot.components.notifications.slack.requests.post", post)
    monkeypatch.setattr("lumibot.components.notifications.slack.requests.get", get)
    provider = SlackNotificationProvider(bot_token="xoxb-secret", default_channel="C123")

    provider.send_message("hello")
    provider.send_message("reply", thread_ts="1.2")
    provider.list_channels(limit=12, cursor="channel-cursor")
    provider.list_messages(limit=15, cursor="message-cursor")
    assert provider.get_message("1.2") == {"ts": "1.2", "text": "hello"}
    provider.list_thread("1.2", limit=8, cursor="thread-cursor")

    assert post_calls[1][1]["json"]["thread_ts"] == "1.2"
    assert get_calls[0][1]["params"]["cursor"] == "channel-cursor"
    assert get_calls[1][1]["params"]["cursor"] == "message-cursor"
    assert get_calls[3][1]["params"]["cursor"] == "thread-cursor"
