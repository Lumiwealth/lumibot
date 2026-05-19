# RELEASE 4.5.25 PYPI BLOCKER

> Release status for the 4.5.25 dotenv/lazy-import startup release, PyPI storage cleanup, and 4.5.26 follow-up release.

**Last Updated:** 2026-05-19
**Status:** Resolved for LumiBot package release; BotManager rollout still requires its own deployment verification.
**Audience:** Developers + AI Agents

---

## Summary

PR #1047 merged `version/4.5.25` into `dev` with green CI, including docs build, lint, unit shards, backtest shards, aggregate `LintAndTest`, and CodeRabbit.

The first package publish attempt was blocked because PyPI rejected new Lumibot uploads with the project storage quota error:

```text
400 Project size too large. Limit for project 'lumibot' total size is 10 GB.
```

PyPI storage was cleaned up on 2026-05-19 by deleting old `.tar.gz` source distribution files while keeping wheels. A follow-up release `v4.5.26` was published successfully and verified installable from PyPI.

Do not update BotManager to a new LumiBot version until its documented deployment workflow is followed and the required BotSpot smoke tests pass.

---

## Verified

- `version/4.5.25` PR: https://github.com/Lumiwealth/lumibot/pull/1047
- `dev` merge commit: `e35e2a314e4258a445268cc58afdf48fffbb4c7c`
- Deploy marker on version branch: `8ae3baece0ddea2971f292bfa25e1744101c3483`
- CI on deploy marker: green
- Existing remote tag `v4.5.25`: points to older merge commit `a29e3eba01ccad44aa04d56223210ae539ac350f`
- Existing `v4.5.25` release workflow run: https://github.com/Lumiwealth/lumibot/actions/runs/25943116545
- Follow-up release PR: https://github.com/Lumiwealth/lumibot/pull/1049
- Follow-up release tag: `v4.5.26`
- Follow-up `dev` merge commit: `5420f11843898cdf993b26ea2686effa3a3a2a4d`
- Successful release workflow: https://github.com/Lumiwealth/lumibot/actions/runs/26114733341
- GitHub Release: https://github.com/Lumiwealth/lumibot/releases/tag/v4.5.26
- PyPI install check: clean venv, run from `/tmp`, `python -m pip install --no-deps lumibot==4.5.26`, then `importlib.metadata.version("lumibot") == "4.5.26"`.

The older `v4.5.25` release workflow validated, tested, and built successfully, then failed during `Publish to PyPI` because of the PyPI project size limit. No GitHub Release was created.

PyPI public JSON before cleanup on 2026-05-19 showed Lumibot package files total approximately `10.71 GB` decimal. PyPI's documented default project limit is `10.0 GB`.

After cleanup, PyPI public JSON showed:

- total package file size: approximately `3.94 GB` decimal
- source distributions remaining: `0`
- wheel files remaining: `584`

Official PyPI storage docs:
- Project storage settings page: `https://pypi.org/manage/project/lumibot/settings/`
- Release management page: `https://pypi.org/manage/project/lumibot/releases/`
- Deleting or yanking are different: yanking does not free storage; deleting files does free storage but is permanent and can break downstream pinned installs.

---

## Risk Notes

- Do not silently move or reuse the existing `v4.5.25` tag without explicitly deciding how to handle the failed prior release run.
- Do not point BotManager at `LumiBot dev`. BotManager's deployment docs say `LUMIBOT_VERSION` must select a published wheel.
- BotManager currently has `LUMIBOT_VERSION` set to a GitHub archive URL, but its Docker templates still render `lumibot==LUMIBOT_VERSION_PLACEHOLDER`; that is not the documented package install path and should not be treated as a verified deploy path.
- Running `importlib.metadata.version("lumibot")` from inside the repo can read stale local `lumibot.egg-info` metadata. Run package install verification from `/tmp` or another directory outside the checkout.

---

## PyPI Cleanup Performed

Rob approved destructive cleanup in the PyPI UI on 2026-05-19. The cleanup deleted old source distributions only where wheels remained. Whole releases were not deleted.

The final authenticated PyPI management page check found no release rows that still had both a source distribution and a wheel. The public JSON check also showed zero remaining source distributions.

Normal wheel installs for pinned versions should continue to work. Source-only installs for deleted `.tar.gz` filenames will not.

If future PyPI storage cleanup is needed, use this safe UI flow:

1. Rob logs into PyPI and completes 2FA.
2. Navigate to `https://pypi.org/manage/project/lumibot/releases/`.
3. For each target release, use `Options` -> `Manage`.
4. Delete only the target `lumibot-X.Y.Z.tar.gz` file.
5. Confirm the matching `lumibot-X.Y.Z-py3-none-any.whl` remains.
6. Re-check `https://pypi.org/manage/project/lumibot/settings/` for current project size.

Do not automate deletion unless the target file list is visible and confirmed. PyPI file deletion is permanent.

---

## Next Actions

1. Keep the canonical Lumibot checkout on `version/4.5.27`; the release workflow created it and local verification confirmed `setup.py` is `4.5.27`.
2. Before BotManager rollout, inspect BotManager deployment docs/workflows and update only through its documented version path.
3. After BotManager deploy, run the required BotSpot production smoke and confirm the running deployment reports `settings.json.lumibot_version == "4.5.26"`.
