# LumiBot 4.5.40 Release Verification

Date: 2026-05-29

## Release

- Released LumiBot `4.5.40` from `dev` through the GitHub Actions release workflow.
- PyPI verified with the JSON API and a no-cache `pip download lumibot==4.5.40`.
- GitHub release workflow completed successfully and created the next branch `version/4.5.41`.
- Local canonical checkout was moved to `version/4.5.41` after the release.

## BotManager Rollout

- BotManager repository variable `LUMIBOT_VERSION` was set to `4.5.40`.
- Development workflow succeeded: run `26658172875`, job `78573626674`, completed in 26m10s.
- Production workflow succeeded: run `26658172863`, job `78573625762`, completed in 21m18s.
- Both workflow logs showed `LUMIBOT_VERSION=4.5.40`.
- Both dependency-image builds installed `lumibot==4.5.40`.
- Production image build log showed `LumiBot v4.5.40 starting`.

## Production MCP Smoke

Required post-deploy smoke was run through BotSpot MCP.

- First canary attempt used `TQQQMedian` revision 5 and failed due strategy code:
  `AttributeError: 'numpy.float64' object has no attribute 'median_200'`.
  This did not produce `settings.json`, so it was not valid as the release gate.
- Second canary used `ModelBench gpt-5.5 spy_simple 2026-05-15` revision 1.
- Backtest ID: `e468a6ba-fcf7-4d9e-8832-28cb137980b3`.
- Backtest status: `completed_no_trades`.
- `settings.json` asserted `lumibot_version: 4.5.40`.
- MCP account status, recent backtests, recent deployments, and artifact query paths responded.
- `stats.csv` resolved to `stats.parquet`; `SELECT COUNT(*)` returned 36 rows.

## Notes

- A GitHub Actions Node.js 20 deprecation warning appeared on both BotManager workflows. It is not release-blocking, but the workflow actions should be updated before GitHub forces Node 24 defaults.
- Agent 1 live Schwab testing before release verified account ending 4364 against read-only Schwab smoke, Titus-style 4-second option cancel, stale market-order reconciliation, and manual cancel reconciliation. The option replace/cancel smoke filled immediately, so Schwab correctly refused cancellation because the order was already filled.
