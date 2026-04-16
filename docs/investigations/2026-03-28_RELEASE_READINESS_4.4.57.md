# Release Readiness 4.4.57

One-line description: March 28, 2026 pre-deploy audit for the next LumiBot release after `v4.4.56`.

**Last Updated:** 2026-03-28
**Status:** Draft
**Audience:** Release captain, BotManager deploy operator, reviewers

## Overview

As of **March 28, 2026**, LumiBot `origin/dev` contains two merged PRs that are **not** part of the published
`v4.4.56` tag:

- `#976` Tradier stock shorting support
- `#981` backtest console logging fix

Because `v4.4.56` is already tagged and published, the next deployable LumiBot version must be **greater than
`4.4.56`**. The practical candidate is `4.4.57`.

## North Star / OKRs

**North Star metric**

- Production and development BotManager environments run the intended LumiBot version with zero emergency rollback.

**Release OKRs**

- Objective: Ship the next LumiBot release with the exact intended commit set.
  - KR: PyPI version matches the tagged Git commit and includes PRs `#976` and `#981`.
  - KR: BotManager `LUMIBOT_VERSION` is updated only after PyPI installability is confirmed.
- Objective: Keep release risk explicit and measurable.
  - KR: Targeted tests covering Tradier side mapping and logger behavior pass on a clean snapshot.
  - KR: Dependency/security scan results are documented before rollout.

## Exact commit map

Release base:

- `d2b01580` `Merge pull request #980 from Lumiwealth/version/4.4.56`
- tag: `v4.4.56`

Additional commits now present on `origin/dev`:

- `372559f4` `2026-03-27T23:11:41-07:00` `Merge pull request #976 from Lumiwealth/be-enable-tradier-stock-shorting`
- `489d34ff` `2026-03-27T23:12:22-07:00` `Merge pull request #981 from Lumiwealth/backtest_console_logging`

Underlying PR commits in the release range:

- `43157417` `enable shorting stocks on tradier`
- `8b4c7e89` `use new side in stock trades`
- `bcefeea6` `fix syntax error`
- `064ecc58` `updates to tradier order mappings...`
- `8c4c0913` `backtest: fix console print settings being overwritten`

## What changed

### PR #976

Files changed in effective release range:

- `lumibot/brokers/tradier.py`
- `tests/test_tradier.py`
- `tests/backtest/test_shorting.py`

Behavioral intent:

- Submit stock orders to Tradier using `self._lumi_side2tradier(order)` instead of raw `order.side`.
- Map LumiBot stock-side variants to Tradier-accepted values:
  - `buy_to_open -> buy`
  - `sell_to_close -> sell`
  - `buy_to_close` / `buy_to_cover -> buy_to_cover`
  - `sell_to_open` / `sell_short -> sell_short`

Release impact:

- This is a runtime broker fix and should be included in the next release.
- It directly affects live/paper Tradier stock shorting behavior.

### PR #981

Files changed in effective release range:

- `lumibot/tools/lumibot_logger.py`

Behavioral intent:

- Add `skip_if_configured` to `_ensure_handlers_configured(...)`.
- Avoid overwriting already-configured console/file logger levels on repeated setup calls.
- Preserve BotManager backtest verbosity settings once configured.

Release impact:

- This is operationally important for BotManager because backtests explicitly set `BACKTESTING_QUIET_LOGS=False`.
- It should reduce cases where console logging gets reset unexpectedly during backtests.

## Targeted validation

Validation was run against a clean snapshot created from `origin/dev`, not against the dirty local `version/4.4.56`
working tree.

### Targeted pytest

Command:

```bash
pytest -q \
  tests/test_tradier.py \
  tests/test_lumibot_logger.py \
  tests/test_unified_logger.py \
  tests/test_logger_env_vars.py \
  tests/test_quiet_logs_requirements.py \
  tests/backtest/test_shorting.py \
  -m "not apitest and not downloader"
```

Result:

- `40 passed`
- `2 skipped`
- `2 deselected`
- runtime: `32.96s`

Notes:

- The skipped backtest shorting coverage is credential-gated when Alpaca test credentials are unavailable.
- Logger and Tradier unit coverage for the merged changes passed.

## Security / dependency audit

### Diff-level code audit

Reviewed the release range from `v4.4.56..origin/dev`.

Findings:

- No new subprocess execution paths
- No new secret handling paths
- No new network destinations
- No workflow or packaging changes introduced by PRs `#976` or `#981`

### Bandit

Command:

```bash
bandit -q -r lumibot/brokers/tradier.py lumibot/tools/lumibot_logger.py -f txt
```

Result:

- No high- or medium-severity findings
- Low-severity findings were existing broad exception-swallowing patterns in Tradier/logger code
- No Bandit finding was specific to the new logic added by PRs `#976` or `#981`

### pip-audit

Command:

```bash
pip-audit -r requirements.txt
```

Result:

- `pygments 2.19.2` flagged with `CVE-2026-4539`

Release note:

- This is not introduced by PRs `#976` or `#981`.
- It is still relevant for release readiness because `setup.py` currently includes broad runtime dependencies
  including `pytest`, which can pull `pygments` into installed environments.
- Before production rollout, either:
  - identify a safe upgraded dependency set that clears the advisory, or
  - explicitly accept/document the risk with scope and mitigation.

## Deploy sequence once release window opens

1. Cut the next LumiBot release version from current `origin/dev` and bump `setup.py` above `4.4.56`.
2. Update `CHANGELOG.md` with the full range from `v4.4.56..new_tag`, explicitly including PRs `#976` and `#981`.
3. Tag and publish the new LumiBot version to PyPI.
4. Verify `python3 -m pip install --no-deps "lumibot==X.Y.Z"` succeeds.
5. Update BotManager `LUMIBOT_VERSION` to that published version.
6. Trigger BotManager dev deploy, verify, then prod deploy.
7. Confirm the deployed environments are running the intended LumiBot version.

## Go / no-go

### Ready

- Deployment path is understood end to end:
  - LumiBot tag and PyPI publish
  - BotManager `LUMIBOT_VERSION` bump
  - GitHub Actions deploy to dev then prod
- PRs `#976` and `#981` are merged on `origin/dev`
- Targeted tests covering the changed behavior passed
- No new high-risk security behavior was found in the merged diffs

### Not ready yet

- No new LumiBot version has been cut after `v4.4.56`
- Therefore BotManager cannot deploy these two PRs yet by only changing `LUMIBOT_VERSION`
- Dependency audit still shows `pygments 2.19.2` with `CVE-2026-4539`

## Recommendation

Do **not** deploy BotManager against `4.4.56` expecting PRs `#976` and `#981` to be present. Prepare the next LumiBot
release as `4.4.57` (or higher), carry these two PRs into that release, resolve or explicitly accept the `pygments`
advisory, then perform the normal BotManager rollout.
