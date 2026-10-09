# Order decision journal

**Description:** Durable, private explanations for consequential order actions.
**Last updated:** 2026-10-09
**Status:** Included in LumiBot 4.6.10
**Audience:** Strategy developers and agent operators

## Overview

LumiBot commits an explicit reason to the existing SQLite memory journal before
submitting, modifying or cancelling an order. Agent tools require 200 characters
after trimming. Aim for 600–2,500 characters explaining dated evidence, sizing,
risks and invalidation. Reasons accept up to 100,000 Unicode characters and 400 KiB
of UTF-8; reason, evidence and order snapshots together must fit within 1 MiB.
Oversized or missing required input fails visibly before the broker call.

Python strategies may supply `reason=` and `evidence=` to the same methods.
Existing deterministic Python strategies remain compatible without a reason.
These explanations are explicit decision summaries, not hidden model reasoning.
Never include credentials, account numbers or other private identity.

## Lifecycle and storage

`order.intent` is an immutable event in the existing memory database, with the
full text stored once and exported in the existing memory Parquet artifacts.
Orders carry compact `decision_journal` references: decision/action IDs, previous
action, model call, revision when configured, SHA-256, time and an 800-character
preview. Order serialization, compact progress and fill exports preserve links.
Batch cancellations retain each order's prior decision/action relationship.

A failed intent write prevents a new reasoned action. A failed broker call records
an unknown outcome and is never retried automatically. A returned broker call is
not proof of a fill; order status and actual fills remain authoritative. Outcome
logging failure after broker acknowledgement must not cause a duplicate retry.

The journal is private. Sharing a bot does not grant permission to publish its
reason. A hosting platform must obtain separate owner consent and redact public
content through its normal publication service.

## Verification

Focused lifecycle, serialization and permission tests cover pre-execution writes,
invalid reasons, mixed legacy orders, uncertain outcomes and compact fill links.
The real-model `stock_order_decision_journal` case is included in the existing
release eval gate. Its preserved baseline is
`agent_eval_baselines/2026-10-09_stock_order_decision_journal.json`.

## Community client

`from lumibot.components.community import CommunityClient` exposes explicit
`CommunityClient(api_key=...).post(body, ...)`. Create a Community-only key in
the hosting product; never pass broker credentials or a full-access runtime key.
Optional `marketplace_listing_id`, `publication_id` and `verified_trade_source`
ask the server to verify an owned order. Client trade claims are never proof of
execution. `parent_post_id` replies in a root thread under the server's limits.
The client sends once with a finite timeout and never retries an uncertain post.
Use this only after the owner authorizes public posting. No automatic network
posting is enabled by recording a private order explanation.
