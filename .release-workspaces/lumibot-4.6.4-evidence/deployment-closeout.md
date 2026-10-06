# LumiBot 4.6.4 fully deployed, 2026-10-03

Rob authorized PyPI, Bot Manager Development and Bot Manager Production deployment in the current release request. All three destinations are deployed and production runtime validation passed.

## Published source and package

- LumiBot [PR 1187](https://github.com/Lumiwealth/lumibot/pull/1187) merged into `dev` at `4280719f6ab3a8ca63510c94e0447cff01931050`. Annotated `v4.6.4` points to that merge.
- [Release workflow 37097198069](https://github.com/Lumiwealth/lumibot/actions/runs/37097198069) passed and published [PyPI 4.6.4](https://pypi.org/project/lumibot/4.6.4/).
- Public wheel SHA256: `8604a31f76ce5b32fc2cc7890352f237ba8934aa1bc4f3b5cb9cac1241bfad1d`. Normal pip installation and a standalone installed-package runtime readback both passed.
- The canonical checkout advanced to clean, pushed `version/4.6.5` before Manager deployment. The excluded historical research spend counter was preserved byte-for-byte on that next-version branch. No shared work was discarded.
- Official documentation deployment and served-content readback passed.

## Qualification

- LumiBot portable suite: 3,539 passed, 44 skipped, 2 xfailed, 4 xpassed. Affected runtime, quote/bar and skill/harness tests passed. Wheel and Sphinx builds passed.
- Final [real-model CI gate 37095760226](https://github.com/Lumiwealth/lumibot/actions/runs/37095760226): 16 cases, 48/48 repetitions passed, 0 failed/errors/missing/skipped/resumed. Acting and judge model: `openai/gpt-6-luna`. External writes: 48 fixture repetitions, 0 real external writes.
- That gate took 1,340.943 seconds wall time. Summed case timing: queue 0, setup 89.396, model 1,369.034, judge 151.022 seconds. Lock time was not separately reported. Input 5,986,887 tokens, cached input 5,147,369, uncached input 839,518, output 76,174, thinking 36,048. Incremental/cumulative estimated gate cost: $0.191536. The tag gate reused all 16 fresh cases without new paid calls.
- Whole release attempt model budget: $6. Deduplicated estimated settled cost $1.2737854, conservative unsettled reservations $2.3240096, total estimated commitment $3.597795. Reservations include interrupted/rejected calls and are not confirmed provider charges. Aggregate input 42,090,687, cached input 36,863,120, output 516,608, thinking 248,187 tokens. Local runs resumed only missing work.
- Bot Manager exact source `deec5696b41f12f332d337561080e0748930da5e`, tree `59fd80e2d37d4effe32ffcc04977932054be7dbb`: all 1,822 local unit tests passed, Ruff/format/type checks, locked audit/export, package build, 13 Lambda archives, runtime bundle, backend-free Terraform validation, and 4 mocked safety plans passed.

## Development and exact production promotion

The first green Development candidate `32f2c506` was superseded by the concurrent normal [Development PR 551](https://github.com/Lumiwealth/bot_manager/pull/551). Its only source change clears stale deleted schedule status during a revision restart, with regression assertions. A fresh isolated candidate was qualified. The prior sealed candidate was preserved as superseded and was not silently changed or promoted.

- [Development 37099985602](https://github.com/Lumiwealth/bot_manager/actions/runs/37099985602): successful deployment, 12/12 integrations in 793.34 seconds, authenticated served-manifest readback passed. Synthetic placement cleanup returned a timeout, but owner-scoped status readback confirmed the test bot stopped.
- Develop SHA: `330bd1095c61852b4c7534a79ff387b2f6d9bed3`.
- Dev manifest: `330bd1095c61-37099985602-1`, SHA256 `a9ca4293dfc5a58abc4c32eb636167bc2fee0112ac01693c8677ec303f7db669`.
- [Production Readiness 37100549152](https://github.com/Lumiwealth/bot_manager/actions/runs/37100549152): all gates passed, including 1,822 unit tests.
- [Production PR 552](https://github.com/Lumiwealth/bot_manager/pull/552) received the authorized owner review and merged normally. Production SHA `13edd12075d7f95ea205323c2805439c3b944716` has the exact develop SHA as its second parent and the same tree.
- [Production 37101252446](https://github.com/Lumiwealth/bot_manager/actions/runs/37101252446): passed. It resolved the exact Dev pointer, promoted immutable artifacts without source rebuilds, passed Terraform safety/apply and authenticated served-manifest readback. Production state-mutating integration tests remained disabled.
- Prod manifest: `13edd12075d7-37101252446-1-promoted`, SHA256 `21dbdc4476f5e7f02b7db531721a08ef7c0487f675742593cfbe77c7164765bc`.
- Runtime bundle SHA256 in both environments: `48a35b66bd0d7cb40ff399c33e28c8156b74e987a9a66687a95aff9075dfa042`. All 13 local Lambda archive hashes match the Dev packages. Promotion verifies bytes and digests before apply.

| Image | Identical Dev/Prod digest |
| --- | --- |
| Base | `sha256:38195d77128db9793518dd316cd34c92a48e47aa345d9a500e6301d19d92f748` |
| Backtest | `sha256:13ca1d8b2077a52b259ea36afe8d085ba66a2cf8159b23bf5b8c8357d374c64b` |
| Single trade | `sha256:dd4c7a58105f42edff17bf35e40a663ad8d692f51a8f3c2ccd8a0919b0ed9fd7` |

All three fresh image install logs prove LumiBot 4.6.4. `LUMIBOT_VERSION=4.6.4`; temporary `FORCE_REBUILD_IMAGES=true` was restored to false after its input was frozen into the original fresh-image Dev run. The NAT qualification module remains disabled. No customer bots were manually stopped, restarted, edited or redeployed.

## Authoritative production smoke

Existing owner-account Buy Hold TQQQ v1, revision `339b6b66-7cd9-4a04-a22b-b18032a3db97`, completed the September 1-15 historical window in production. Backtest `e69b8716-c0a2-407b-9754-ec07c3acfab0`:

- `settings.json.lumibot_version == "4.6.4"` and bootstrap version agree.
- Completed terminal status, bootstrap and upload exit 0, no error/traceback/exception log matches.
- Parquet trade ledger queried successfully: 2 lifecycle rows, 1 filled buy of 1,376 TQQQ shares at $71.44. This is a historical simulated fill, not a live order.
- All four MCP checks passed: account, recent history containing this completed run, active/scheduled inventory, and DuckDB trade queries.
- The 3 sampled pre-existing schedules remain enabled and healthy with populated performance data and unchanged task definitions, last-run timestamps and performance snapshots. This is a bounded sample, not an all-account inventory claim. No live bots were restarted to force a package upgrade.

The first probe, existing TQQQMedian v5 (`62aa21ea-7621-439d-a566-a3822956dbe2`), failed because saved code reads `.median_200` on the scalar returned by a Series custom indicator. The indicator module is byte-identical in 4.6.3 and 4.6.4 (SHA256 `f6c2bf7fbaa19dac40854c0ed3820b6fd6f571ec0c1410764cfd9e5171bc11fc`). The failure and startup 4.6.4 proof were preserved. No saved strategies were changed. A different existing simple strategy supplied the completed-run release gate.

## Evidence and caveats

Compatible telemetry records local gates, GitHub queues/jobs, artifact publication, Terraform plan/apply, runtime reconciliation, deployed synthetic placement, readbacks, failures and retries separately. This was an independent Manager release; no coordinated platform Dev lock or unlock was claimed. The fresh candidate sealed at approximately 05:37:48 UTC; all production MCP checks were recorded passed at 06:11:58 UTC. This exceeded the 30-minute seal-to-final-readback target by approximately 4 minutes; no gate was weakened. The maintained helper's Dev-terminal-to-readback clock is a different interval.

Local artifacts and per-call eval ledgers are retained under the release evidence archive. GitHub links above bind the durable public run evidence to the source and artifact identities.

A Dev credential argument and a stale local production credential appeared in private tool output. Local telemetry was scrubbed, exception handling was corrected, and its failure path verified credential-safe. Neither value was committed or uploaded; no secrets were edited or rotated. The stale local production credential returned HTTP 401. Production identity was instead verified through the successful authenticated GitHub workflow readback and the independently authenticated owner MCP completed run.
