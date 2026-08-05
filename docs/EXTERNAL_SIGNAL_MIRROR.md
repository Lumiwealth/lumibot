# External Signal Mirror

Reusable LumiBot component for exact structured option signals.

Last Updated: 2026-08-05

Status: Implemented

Audience: Strategy authors and BotSpot runtime engineers

## Overview

`ExternalSignalMirror` consumes an immutable normalized batch prepared by the BotSpot control plane. It supports BUY, SELL, and HOLD for exact CALL and PUT contracts. BUY maps to `BUY_TO_OPEN`, SELL maps to `SELL_TO_CLOSE`, and HOLD submits no order. Orders use LumiBot SMART_LIMIT and do not add stops, profit targets, substitutions, or retries.

All account positions, quotes, contract validation, order submission, status, and fills remain inside the LumiBot strategy and broker adapter path. The component does not call a broker API directly.

## Runtime contract

The BotSpot launcher supplies a private S3 bucket, key, and SHA-256. The component verifies the hash before parsing JSON. The normalized batch must include `batchId`, `contentSha256`, and at least one record.

Every action appends a structured `external_signal_audit.jsonl` event containing the batch identity, exact contract, intended quantity, quote, SMART_LIMIT inputs, broker order ID and status, and reported fill comparison when available. Strategy lifecycle callbacks can call `record_order_event` to append fill, rejection, or cancellation evidence.

## Safety behavior

- Invalid or incomplete records fail visibly.
- A SELL larger than the exact held contract position is rejected without clipping the quantity.
- A missing or unactionable quote does not fall back to a market order.
- HOLD never submits an order.
- Reference and reported fill prices are audit fields only. They do not become autonomous limits.
