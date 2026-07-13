# IBKR REST Gateway Lifecycle

Architecture and safety notes for LumiBot's Interactive Brokers Client Portal REST transport.

Last Updated: 2026-07-13

Status: Paper proof of concept

Audience: LumiBot contributors and downstream runtime integrators

## Overview

`InteractiveBrokersRESTData` now consumes two replaceable boundaries:

- a gateway lifecycle that supplies a REST base URL and owns start/stop behavior;
- an HTTP client that sends REST requests and can later carry OAuth request signing.

This keeps broker, order, position, contract, and market-data behavior independent from authentication. Local individual-account testing can use IBeam temporarily. Approved third-party integrations can later inject an OAuth-capable HTTP session without rewriting broker methods.

## Supported Transport Shapes

### External Gateway

Set `IB_API_URL` when Client Portal Gateway or another approved transport is already managed outside LumiBot. The URL may include `/v1/api`; LumiBot will not append that path twice.

`RUNNING_ON_SERVER=true` without `IB_API_URL` preserves the sidecar shape at `https://localhost:<IB_GATEWAY_PORT>/v1/api`.

### Local IBeam Proof Of Concept

When neither `IB_API_URL` nor `RUNNING_ON_SERVER=true` is set, LumiBot starts one IBeam Docker container. Current safeguards:

- versioned default image `voyz/ibeam:0.5.12`;
- unique instance-scoped container name;
- gateway port published only on `127.0.0.1`;
- paper mode enabled by default;
- credentials forwarded by environment-variable name, never embedded in Docker command arguments;
- configuration file mode `0600` and cleanup on stop;
- bounded authentication wait;
- owned-container cleanup only.

IBeam remains a third-party wrapper. IBKR does not support automated individual Client Portal Gateway authentication. Docker also retains container environment values. Do not use this transport as a customer credential-collection or production authentication design.

## OAuth Migration Boundary

Future OAuth transport should:

1. implement the no-op or managed `IbkrGateway` lifecycle;
2. provide an HTTP session that signs requests;
3. use the approved `https://api.ibkr.com/v1/api` base URL;
4. preserve existing REST broker methods and tests;
5. avoid IBKR username, password, and 2FA material entirely.

## Configuration

- `IB_GATEWAY_PORT`: host port for a local or sidecar gateway; default `4234`.
- `IB_GATEWAY_INSTANCE_ID`: non-secret identifier used in local container naming.
- `IB_USE_PAPER_ACCOUNT`: IBeam paper toggle; defaults to `true`.
- `IBEAM_DOCKER_TAG`: versioned IBeam tag; defaults to `0.5.12`.
- `IB_AUTH_TIMEOUT`: maximum authentication wait in seconds; default `300`.
- `IB_AUTH_POLL_INTERVAL`: authentication polling interval in seconds; default `5`.
- `IB_REQUEST_TIMEOUT`: individual HTTP request timeout in seconds; default `30`.
- `IB_VERIFY_SSL`: explicit TLS verification override. Defaults off for localhost gateways and on for non-local hosts.

## Verification

Deterministic coverage:

```bash
python -m pytest \
  tests/test_ibkr_gateway.py \
  tests/test_interactive_brokers_rest_gateway_lifecycle.py \
  tests/test_broker_initialization.py -q
```

Real broker smoke testing must use a dedicated paper username, verify the paper account before any order action, and record account identifiers only in masked form. A passing paper smoke does not prove production or OAuth readiness.
