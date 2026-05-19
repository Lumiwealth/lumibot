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

PyPI public JSON on 2026-05-19 showed Lumibot package files total approximately `10.71 GB` decimal. PyPI's documented default project limit is `10.0 GB`, so the project needs cleanup before another upload can succeed.

Official PyPI storage docs:
- Project storage settings page: `https://pypi.org/manage/project/lumibot/settings/`
- Release management page: `https://pypi.org/manage/project/lumibot/releases/`
- Deleting or yanking are different: yanking does not free storage; deleting files does free storage but is permanent and can break downstream pinned installs.

---

## Risk Notes

- Do not silently move or reuse the existing `v4.5.25` tag without explicitly deciding how to handle the failed prior release run.
- Do not point BotManager at `LumiBot dev`. BotManager's deployment docs say `LUMIBOT_VERSION` must select a published wheel.
- BotManager currently has `LUMIBOT_VERSION` set to a GitHub archive URL, but its Docker templates still render `lumibot==LUMIBOT_VERSION_PLACEHOLDER`; that is not the documented package install path and should not be treated as a verified deploy path.

---

## Recommended PyPI Cleanup

Prefer deleting old source distributions only when the same version still has a universal wheel. Do not delete whole releases unless there is a separate deprecation decision.

First cleanup target:

- Delete individual `.tar.gz` source distribution files for `4.4.45` through `4.5.22`.
- Keep the matching `py3-none-any.whl` files for those versions.
- Expected reclaimed storage from this first target: about `1.47 GB` decimal across `39` source distribution files.
- Expected result: enough headroom for the blocked wheel-only Lumibot release and several follow-up releases without breaking normal wheel installs for those pinned versions.

If more headroom is needed later, the broader cleanup target is all `.tar.gz` source distributions that have a matching wheel. Public JSON showed `581` such files totaling about `6.77 GB` decimal.

Chrome DevTools MCP was attempted on 2026-05-19 to drive the PyPI UI with Rob logging in, but `list_pages` timed out. Do not launch a separate browser or touch personal browser profiles as a workaround. If Chrome MCP is restored, the safe UI flow is:

1. Rob logs into PyPI and completes 2FA.
2. Navigate to `https://pypi.org/manage/project/lumibot/releases/`.
3. For each target release, use `Options` -> `Manage`.
4. Delete only the target `lumibot-X.Y.Z.tar.gz` file.
5. Confirm the matching `lumibot-X.Y.Z-py3-none-any.whl` remains.
6. Re-check `https://pypi.org/manage/project/lumibot/settings/` for current project size.

Do not automate deletion unless the target file list is visible and confirmed. PyPI file deletion is permanent.

---

## Next Options

1. Resolve PyPI storage: delete old unnecessary release files or request a project size increase, then publish a new Lumibot version from the current `dev` merge.
2. If choosing to preserve `4.5.25`, intentionally retarget `v4.5.25` to `e35e2a31` only after confirming no package or GitHub Release was published from the old tag.
3. If avoiding tag movement, bump and release `4.5.26` instead once PyPI can accept uploads.
4. Only after PyPI installability is confirmed, update BotManager `LUMIBOT_VERSION`, trigger dev/prod workflows, and run the required BotSpot backtest version smoke.
