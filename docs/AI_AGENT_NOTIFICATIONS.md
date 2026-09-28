# AI Agent Notifications

Lumibot has native strategy notifications through:

```python
self.notify("Trade decision", "Bought AAPL because...")
self.notifications.notify("Trade decision", "Bought AAPL because...")
```

Telegram, Resend email, and Slack are native providers:

```python
self.notifications.configure_telegram(
    bot_token=os.environ["TELEGRAM_BOT_TOKEN"],
    chat_id=os.environ["TELEGRAM_CHAT_ID"],
)

self.notifications.configure_resend(
    api_key=os.environ["RESEND_API_KEY"],
    from_address=os.environ["RESEND_FROM_EMAIL"],
)

self.notifications.configure_slack(
    bot_token=os.environ["SLACK_BOT_TOKEN"],
    default_channel=os.environ["SLACK_CHANNEL_ID"],
)

# BotSpot-hosted strategies use a short-lived, deployment-bound capability.
# Bot Manager injects the URL/token/renewal URL; strategy code never receives
# an SES credential or chooses a system sender.
self.notifications.configure_botspot()
```

Environment variables:

```bash
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
RESEND_API_KEY=...
RESEND_FROM_EMAIL=Bot <bot@example.com>
SLACK_BOT_TOKEN=...
SLACK_CHANNEL_ID=...
BOTSPOT_COMMUNICATIONS_MCP_URL=...
BOTSPOT_COMMUNICATIONS_MCP_TOKEN=...
BOTSPOT_COMMUNICATIONS_MCP_RENEW_URL=...
```

For a hosted BotSpot deployment, communication authority is opt-in. Add only
the saved setting keys needed by that deployment:

```text
LUMIBOT_BOTSPOT_EMAIL_SEND
LUMIBOT_BOTSPOT_EMAIL_SENT_READ
LUMIBOT_BOTSPOT_EMAIL_RECEIVED_READ
```

The values are not credentials; the presence of a key asks BotSpot for the
corresponding narrow scope. BotSpot chooses the owner, environment, sender, and
provider connection server-side. Direct Resend and Slack providers continue to
use owner-supplied credentials and do not pass through BotSpot.

Strategies can send and read communications directly:

```python
self.send_email(
    to=["owner@example.com"],
    subject="Daily picks",
    text="Three files are attached.",
    attachments=[{"filename": "picks.csv", "content": base64_text}],
    idempotency_key="daily-picks/2026-09-20",
)
self.list_sent_emails(limit=20)
self.get_sent_email("sent-email-id")
self.get_email_status("sent-email-id")
self.list_received_emails(limit=20)
self.get_received_email("email-id")
self.list_received_email_attachments("email-id")
self.get_received_email_attachment("email-id", "attachment-id")
self.send_slack_message("Capture completed")
self.list_slack_channels(limit=100)
self.list_slack_messages(limit=15)
self.get_slack_message("message-ts", channel="C123")
self.list_slack_thread("parent-message-ts")
```

Backtests never send communications. They append a structured `COMMUNICATION`
log record with `status=simulated_not_sent`. Read operations require timestamped
fixtures under `parameters["communication_fixtures"]`, so a historical backtest
cannot silently read today's inbox or Slack workspace.

Agent tools (each can be independently included or omitted from an agent's tool
allowlist):

- `notify_user`
- `send_email`
- `list_sent_emails`
- `get_sent_email`
- `get_email_status`
- `list_received_emails`
- `get_received_email`
- `list_received_email_attachments`
- `get_received_email_attachment`
- `send_slack_message`
- `list_slack_channels`
- `list_slack_messages`
- `get_slack_message`
- `list_slack_thread`

BotSpot-hosted strategies can use the same surface with server-issued,
owner/deployment-scoped credentials. BotSpot remains responsible for selecting
the managed sender and enforcing channel-specific permissions; a strategy must
not be able to override another owner's sender or message history.
