# Indicator v2 carry-forward

Where the Indicator v2 code stands after the 4.6.30 release, and the only version that may come back.

**Last Updated:** 2026-10-24
**Status:** On hold, waiting for design approval
**Audience:** Release captains and agents working on LumiBot indicators

## Overview

Indicator v2 (one-bar updates, standalone runner, registry) was excluded from
both the 4.6.29 and the 4.6.30 releases. In 4.6.30 it was removed with one
normal revert commit (`39738d81`) of `3b0b628b`. No history was rewritten.

Rules for bringing it back:

- Do **not** reapply `1eca9bff` or `3b0b628b` anywhere. Those commits include a
  built-in indicator derived from private third-party code that must never ship
  in LumiBot.
- The only version that may be carried is the cleaned branch at commit
  `f9104d4f` on `agent/claude-cassius-indicators-v2`, which removes that
  built-in, its tests and its docs.
- Carry it only after the Indicator v2 design is approved. Until then nothing is
  reapplied on any version branch.
- When it does come back, it changes `lumibot/` runtime files, so the full
  real-model agent qualification must rerun on the new candidate
  (`docs/AGENT_EVALS.md`).
- The 4.6.30 revert commit message says the code "carries over to 4.6.31". That
  line is superseded by this note.
