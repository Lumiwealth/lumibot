# Release telemetry: codex-release-lumibot-4.6.4-promotion

Current stage: closeout (passed)

## Release clocks

- Inventory to frozen candidate: not available
- Dev lock to unlock: not available / 60m target
- Dev deployment to production readback: 28m 24s / 30m target
- Failed attempts: 3
- Retry attempts: 3
- Executed terminal attempts: 43
- Reused terminal attempts: 1

## Stages

| Stage | Latest status | Attempts | Recorded time | Failures |
| --- | --- | ---: | ---: | ---: |
| local-preflight | passed | 3 | 8s | 1 |
| deterministic-tests | passed | 1 | 42s | 0 |
| dev-deployment | passed | 12 | 35m 4s | 0 |
| dev-readback | passed | 3 | 3s | 0 |
| production-readiness | passed | 7 | 2m 32s | 0 |
| production-authorization | passed | 1 | 0s | 0 |
| production-deployment | passed | 10 | 19m 30s | 0 |
| production-readback | passed | 5 | 8m 2s | 2 |
| main-reconciliation | passed | 1 | 9m 2s | 0 |
| closeout | passed | 1 | 4s | 0 |

## Rework causes

- environment: 3m 40s
- unknown: 1s

## Improvement signals

- Largest recorded rework cause: environment.
- Strengthen the bounded local environment preflight before the complete Playwright tests.
