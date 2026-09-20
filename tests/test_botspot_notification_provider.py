import base64

from lumibot.components.notifications.botspot import BotSpotNotificationProvider


def test_botspot_provider_uploads_arbitrary_attachments_then_sends(monkeypatch):
    monkeypatch.setenv("BOTSPOT_COMMUNICATIONS_MCP_TOKEN", "scoped-token")
    calls = []

    def call(_server, name, arguments):
        calls.append((name, arguments))
        if name == "upload_email_attachment":
            return {"structuredContent": {"handle": f"handle-{len(calls)}"}}
        return {"structuredContent": {"messages": [{"id": "message-1"}]}}

    monkeypatch.setattr("lumibot.components.notifications.botspot.call_mcp_tool", call)
    provider = BotSpotNotificationProvider(url="https://example.test/mcp")
    result = provider.send_email(
        to=["owner@example.com"],
        subject="Attachments",
        text="Three files",
        attachments=[
            {"filename": "one.csv", "content": b"a,b\n", "content_type": "text/csv"},
            {"filename": "two.xlsx", "content": base64.b64encode(b"xlsx").decode("ascii")},
            {"filename": "three.txt", "content": "plain text", "content_type": "text/plain"},
        ],
        idempotency_key="capture-1:owner@example.com",
    )

    assert result.ok is True
    assert [name for name, _arguments in calls] == [
        "upload_email_attachment",
        "upload_email_attachment",
        "upload_email_attachment",
        "send_email",
    ]
    assert calls[-1][1]["attachmentHandles"] == ["handle-1", "handle-2", "handle-3"]
    assert calls[-1][1]["idempotencyKey"] == "capture-1:owner@example.com"


def test_botspot_provider_requires_stable_idempotency_key(monkeypatch):
    monkeypatch.setenv("BOTSPOT_COMMUNICATIONS_MCP_TOKEN", "scoped-token")
    provider = BotSpotNotificationProvider(url="https://example.test/mcp")

    result = provider.send_email(to=["owner@example.com"], subject="Missing key", text="No key")

    assert result.ok is False
    assert result.skipped is True
    assert "idempotency" in (result.reason or "")


def test_notification_manager_can_select_botspot_provider(monkeypatch):
    from lumibot.components.notifications import NotificationManager

    class Strategy:
        is_backtesting = False
        parameters = {}
        logger = None

    monkeypatch.setenv("BOTSPOT_COMMUNICATIONS_MCP_TOKEN", "scoped-token")
    monkeypatch.setattr(
        "lumibot.components.notifications.botspot.call_mcp_tool",
        lambda _server, _name, _arguments: {
            "structuredContent": {"messages": [{"id": "message-1"}]}
        },
    )
    manager = NotificationManager(Strategy(), enabled=True)
    manager.configure_botspot(url="https://example.test/mcp")

    result = manager.send_email(
        to="owner@example.com",
        subject="Managed",
        text="Body",
        idempotency_key="managed-1",
        provider="botspot",
    )

    assert result.ok is True
    assert result.provider == "botspot"
