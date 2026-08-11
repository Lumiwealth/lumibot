# One-Shot Broker Read Latency

One-shot live broker readers paid for unrelated account synchronization before every requested read.

Last Updated: 2026-08-12
Status: Implemented on version/4.5.84
Audience: LumiBot and downstream runtime maintainers

## Overview

Live `Strategy` construction intentionally fetches balances and positions so a normal trading strategy starts with complete account state. Short-lived read-only runtimes then performed another fresh provider call for their requested operation. Positions were fetched twice; account snapshots fetched positions unnecessarily and balances up to three times.

`synchronize_broker_on_start=False` now provides an explicit narrow opt-out. Default behavior remains unchanged. Callers using the opt-out must make a fresh broker-backed call for requested state before reading it. This keeps provider behavior inside LumiBot while avoiding unrelated network traffic.

Local fresh-process profiling put full one-shot reader imports near 300 ms p50. Multi-second downstream latency therefore came from provider round trips and runtime orchestration, not import serialization or CPU work.
