# RELEASE 4.5.25 PYPI BLOCKER

> Release status for the 4.5.25 dotenv/lazy-import startup release and the remaining package-publish blocker.

**Last Updated:** 2026-05-19
**Status:** Active
**Audience:** Developers + AI Agents

---

## Summary

PR #1047 merged `version/4.5.25` into `dev` with green CI, including docs build, lint, unit shards, backtest shards, aggregate `LintAndTest`, and CodeRabbit.

The code is on `dev`, but the normal Lumibot package release is blocked before BotManager rollout because PyPI is rejecting new Lumibot uploads with the project storage quota error:

```text
400 Project size too large. Limit for project 'lumibot' total size is 10 GB.
```

Do not update BotManager to a new LumiBot version until a published, installable LumiBot artifact exists and `python3 -m pip install --no-deps "lumibot==X.Y.Z"` succeeds.

---

## Verified

- `version/4.5.25` PR: https://github.com/Lumiwealth/lumibot/pull/1047
- `dev` merge commit: `e35e2a314e4258a445268cc58afdf48fffbb4c7c`
- Deploy marker on version branch: `8ae3baece0ddea2971f292bfa25e1744101c3483`
- CI on deploy marker: green
- Existing remote tag `v4.5.25`: points to older merge commit `a29e3eba01ccad44aa04d56223210ae539ac350f`
- Existing `v4.5.25` release workflow run: https://github.com/Lumiwealth/lumibot/actions/runs/25943116545
- PyPI index on 2026-05-19 still listed latest Lumibot as `4.5.23`, not `4.5.25`.

The older `v4.5.25` release workflow validated, tested, and built successfully, then failed during `Publish to PyPI` because of the PyPI project size limit. No GitHub Release was created.

---

## Risk Notes

- Do not silently move or reuse the existing `v4.5.25` tag without explicitly deciding how to handle the failed prior release run.
- Do not point BotManager at `LumiBot dev`. BotManager's deployment docs say `LUMIBOT_VERSION` must select a published wheel.
- BotManager currently has `LUMIBOT_VERSION` set to a GitHub archive URL, but its Docker templates still render `lumibot==LUMIBOT_VERSION_PLACEHOLDER`; that is not the documented package install path and should not be treated as a verified deploy path.

---

## Next Options

1. Resolve PyPI storage: delete old unnecessary release files or request a project size increase, then publish a new Lumibot version from the current `dev` merge.
2. If choosing to preserve `4.5.25`, intentionally retarget `v4.5.25` to `e35e2a31` only after confirming no package or GitHub Release was published from the old tag.
3. If avoiding tag movement, bump and release `4.5.26` instead once PyPI can accept uploads.
4. Only after PyPI installability is confirmed, update BotManager `LUMIBOT_VERSION`, trigger dev/prod workflows, and run the required BotSpot backtest version smoke.
