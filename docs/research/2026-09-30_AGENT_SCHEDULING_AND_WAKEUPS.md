# Agent Scheduling And Wake-Ups: How Other Harnesses Do It

How coding agents and always-on assistants decide when an AI runs, and what that means for LumiBot.

- **Last Updated:** 2026-09-30
- **Status:** Research for discussion. Nothing is built yet. Rob wants to agree on the design first.
- **Audience:** LumiBot maintainers and AI agents working on `lumibot/components/agents/`.

## Overview

Why this came up: the old 1DTE SPY iron condor (Lumiwealth-Strategies/options_condor_martingale) only made
money with an intraday stop that was checked every minute. An AI bot cannot call a model every minute,
especially in a multi-year backtest. Today LumiBot has one clock: `sleeptime` drives
`on_trading_iteration()`, and an agent only runs when strategy code calls it. The agent `cadence=` argument is
informational only (`lumibot/components/agents/manager.py`, `AgentManager.create`).

Rob's direction (2026-09-30): the agent sets its own schedule from a plain-English prompt; tools must be
generic, never built for one strategy; learn from Claude Code, Codex, Cursor, OpenClaw, Grok, Muse and dots;
decide how agent timers relate to `sleeptime` before building anything.

Every claim below was checked against the linked page on 2026-09-30 unless marked "not verified".

## What each product does

**Claude Code**
- `/loop` with an interval runs a prompt on a cron timer (1 minute minimum). `/loop` with no interval lets the
  agent pick its own next wake-up (1 minute to 1 hour) with `ScheduleWakeup`, or stop. A run that forgets to
  reschedule gets one fallback wake-up. https://code.claude.com/docs/en/scheduled-tasks
- `CronCreate` / `CronList` / `CronDelete`: the agent creates its own schedules. About 50 jobs; fire only when
  the session is idle; a missed job fires once, never once per missed slot; recurring jobs expire after 7 days.
- `Monitor`: the agent writes a small watch script; each line it prints wakes the agent. No model call until
  something prints. https://code.claude.com/docs/en/tools-reference
- Hooks: 32 events. A hook with `asyncRewake: true` wakes the agent when it exits with code 2.
  https://code.claude.com/docs/en/hooks
- Channels push outside events into a running session (research preview). https://code.claude.com/docs/en/channels
- Routines: cloud runs on a schedule, API call or GitHub event; the agent can create them.
  https://code.claude.com/docs/en/routines
- Desktop scheduled tasks can run every minute, get one catch-up run, and can change their own schedule.
  https://code.claude.com/docs/en/desktop-scheduled-tasks

**OpenAI Codex, ChatGPT, dots**
- Codex scheduled tasks can return to the same chat with its context, and the agent creates and updates them
  itself (heartbeat tasks with an RRULE). https://learn.chatgpt.com/docs/automations.md
- Known problems: one heartbeat replayed 128K to 146K tokens per call (over 1M in a minute), and heartbeats
  sometimes repeat an old answer without checking. GitHub openai/codex issues #42702 and #47864.
- Codex background hooks do not start a new turn (the opposite of Claude's `asyncRewake`).
  https://learn.chatgpt.com/docs/hooks.md
- dots (launched 2026-09-29): always-on agents with their own cloud computer and notes as memory; a dot
  "can decide when to pause and wake up to continue work", plus schedules and event monitoring.
  https://learn.chatgpt.com/docs/dots.md

**Cursor**
- Automations: cloud agents started by cron, repo events, Slack, webhooks, Linear, Sentry, PagerDuty, with a
  memory file between runs; a local agent can create one. https://cursor.com/docs/cloud-agent/automations.md
- IDE `stop` hook can auto-continue with `followup_message` (5 loops by default). https://cursor.com/docs/hooks.md

**OpenClaw**
- Heartbeat: a system timer runs a turn every 30 minutes by default. The normal answer is "nothing, stay
  quiet" (`NO_REPLY`). No model call when the checklist is empty. Can use a cheaper model and a small fresh
  context. Event wakes have a 30-second minimum gap and a flood guard of 5 wakes in 60 seconds. Overdue cron
  jobs are rescheduled on restart, not replayed.
  https://github.com/openclaw/openclaw/blob/main/docs/gateway/heartbeat.md

**xAI Grok Bot** (2026-08-11): always-on bots with routines (schedules or events, up to 50) and background
tasks that copy Claude Code (`/loop`, monitors, 7-day expiry, 50-task cap).
https://docs.x.ai/build/features/background-tasks

**Meta Muse** (2026-09-08): keeps working after the app closes and "comes back when something changes or when
it needs approval". https://about.fb.com/news/2026/09/introducing-muse-personal-ai-agent/

**Others**
- Hermes Agent: the agent creates its own cron jobs; a script can run first and print `{"wakeAgent": false}` to
  skip the model entirely. https://hermes-agent.nousresearch.com/docs/user-guide/features/cron
- QuantConnect: scheduled events and bar consolidators run on the data loop in backtests and on a real-time
  thread live. https://www.quantconnect.com/docs/v2/writing-algorithms/scheduled-events
- Not verified: Jules, Devin, Manus. OpenAI Pulse reported retired (third-party source only).

## Patterns they share

1. Timers (cron or fixed times).
2. Self-scheduled wake-ups: the agent decides when it comes back.
3. Heartbeats where "nothing to do" is the normal answer.
4. Event hooks (fills, webhooks, repo events) that start a run.
5. Agent-written watchers: the agent writes cheap code that watches for it; only code output wakes the model.
6. A fast code loop plus a slow AI loop. Every mature product ends up here.

Guardrails they share: about 50 jobs, expiries, a missed run fires once (or is skipped), cooldowns and flood
guards, a small fresh context on each wake instead of the whole history, token caps.

## What is different for LumiBot

The clock can be simulated. Cron and wall-clock monitors mean nothing in a five-year backtest, so:

- Agent timers should be separate from `sleeptime` but run on the same strategy clock. The engine steps to
  whichever comes first: the next `sleeptime` tick, the next agent timer, or the next event.
- A 3:45 PM action in a backtest needs data at 3:45 PM, never that day's close (look-ahead).
- Watchers run as plain Python on every bar the engine already has, seeing only data up to now.
- Heartbeats fit backtests worst: every tick costs a model call.
- LumiBot already has lifecycle events that can become wake events: `before_market_closes`,
  `on_filled_order`, `on_partially_filled_order`, `on_canceled_order` (`lumibot/strategies/strategy.py`).

## Options under discussion

1. **Timers on the strategy clock.** The agent sets "every trading day at 15:45", "in 30 minutes", once or
   repeating. Simple and repeatable; blind between timers.
2. **Watchers plus event hooks.** The agent writes a small check (price, profit/loss, VIX, anything the data
   API offers) that LumiBot runs every bar at no token cost, plus built-in events (fill, reject, market
   open/close). Cheap and fast; agent-written checks need a safe, limited language.
3. **Heartbeat with cheap triage.** Every N minutes a code check or small model decides whether to call the
   main model. Catches the unexpected; cost grows with backtest length.

Current lean (not agreed): 1 + 2 as the core, 3 opt-in. One wake queue on the simulated clock, a small fresh
context on each wake (the note the agent left itself plus what happened), missed wakes fire once inside a
grace window, cooldowns, a token cap.

Open questions for Rob:
- Is the agent allowed to write small checks that LumiBot runs every bar?
- For AI-only bots, should `sleeptime` and `on_trading_iteration` go away, with LumiBot running the agent clock?
- How small should the first version be?
