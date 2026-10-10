# Strategy Asset Persistence

Typed instrument restoration across scheduled files and database backups.

Last Updated: 2026-10-11
Status: Implemented; deployment verification is separate
Audience: Strategy authors and maintainers

## Overview

The generic JSON encoder calls `Asset.to_dict()`, which discards its Python
type. Passing that restored dictionary into position or chart APIs fails their
instrument contract. The variable backup boundary now wraps actual assets with
the existing `__lumibot_type__` envelope and restores them with `Asset.from_dict`.
Both persistence backends use the same serializer and decoder. Nested assets,
option underlyings, leverage, and precision use the entity's canonical contract.
Sets keep the existing list restoration contract while retaining nested asset types.

`Asset.to_dict()` and `Asset.from_dict()` preserve `auto_expiry`, including nested
underlyings and the Asset payloads used by orders and positions. Deferred futures
retain `expiration=None`; explicit expirations are never materialized or changed.
Legacy dictionaries may omit `auto_expiry` and/or `leverage`, but all other schema
fields remain required and unknown keys are rejected during initialized-path
restoration. A missing selector remains `None`: neither a symbol nor an
initialized selector can establish the saved instrument's missing roll rule.

Database legacy date-string coercion runs after entity decoding so an option's
expiration is not converted before `Asset.from_dict` consumes it. Scheduled
plain strings retain their existing behavior. Malformed typed data fails before
any loaded variables replace initialized state. Repeated saves compare the same
serialized representation as the restore fingerprint.

Literal dictionaries matching the reserved type-envelope shape are escaped,
including nested escape envelopes, so they are not interpreted as instruments
or other typed values. Serialization failures leave the prior backup intact and
are handled by each backend's existing backup error log.

## Compatibility and verification

Old untagged dictionaries remain dictionaries unless the same initialized variable
path contains an Asset: shape alone cannot distinguish instrument state from
metadata. Initialized paths supply the type while saved contract details remain
authoritative. Scheduled migration preserves the original bytes in a restricted
`.legacy-<sha256>.bak` sibling before applying restored variables. Older runtimes
cannot restore the new asset tag. Release and rollback
planning must account for that format boundary.

`tests/test_strategy_asset_backup.py` exercises real scheduled files and an
isolated SQLite database, repeated restarts, position lookup and OHLC creation,
plain/empty state, malformed backups, and existing date-string behavior. Broker
reads are isolated; no orders, customer state changes, or production recovery are
proved by these checks.

`tests/test_futures_asset_persistence.py` additionally checks selector equality and
hash after JSON roundtrip, both persistence backends through the actual IBKR
expiry preflight, all legacy optional-field combinations, explicit expirations,
and nested order/position assets. The IBKR check stops at exchange resolution
before provider access; it proves preflight acceptance, not prices or execution.
