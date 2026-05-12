# AI Agent Notifications

Lumibot has native strategy notifications through:

```python
self.notify("Trade decision", "Bought AAPL because...")
self.notifications.notify("Trade decision", "Bought AAPL because...")
```

Telegram is the first native provider:

```python
self.notifications.configure_telegram(
    bot_token=os.environ["TELEGRAM_BOT_TOKEN"],
    chat_id=os.environ["TELEGRAM_CHAT_ID"],
)
```

Environment variables:

```bash
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

Backtests disable notifications by default. Enable them explicitly when testing notification behavior.

Agent tool:

- `notify_user`

BotSpot can later add a richer notification provider that fans out to email, Telegram, Discord, SMS, Slack, or other linked channels.
