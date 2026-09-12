# Strategy Asset Persistence

Typed instrument restoration across scheduled files and database backups.

Last Updated: 2026-09-12
Status: Implemented; deployment verification is separate
Audience: Strategy authors and maintainers

## Overview

The generic JSON encoder calls `Asset.to_dict()`, which discards its Python
type. Passing that restored dictionary into position or chart APIs fails their
instrument contract. The variable backup boundary now wraps actual assets with
the existing `__lumibot_type__` envelope and restores them with `Asset.from_dict`.
Both persistence backends use the same serializer and decoder. Nested assets,
option underlyings, leverage, and precision use the entity's canonical contract.

Database legacy date-string coercion runs after entity decoding so an option's
expiration is not converted before `Asset.from_dict` consumes it. Scheduled
plain strings retain their existing behavior. Malformed typed data fails before
any loaded variables replace initialized state. Repeated saves compare the same
serialized representation as the restore fingerprint.

## Compatibility and verification

Old untagged dictionaries remain dictionaries: shape alone cannot distinguish
instrument state from metadata. Known instrument variables require an explicit
strategy migration after restoration; this fix does not rewrite existing customer
backups. Older runtimes cannot restore the new asset tag. Release and rollback
planning must account for that format boundary.

`tests/test_strategy_asset_backup.py` exercises real scheduled files and an
isolated SQLite database, repeated restarts, position lookup and OHLC creation,
plain/empty state, malformed backups, and existing date-string behavior. Broker
reads are isolated; no orders, customer state changes, or production recovery are
proved by these checks.
