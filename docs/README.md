# Documentation

> LumiBot internal documentation: architecture, operations, investigations, and cross-session handoffs.

This folder contains **human-authored** documentation for the LumiBot trading and backtesting framework. AI assistants should start here to understand documentation structure.

---

## Backtesting Definitions (Accuracy + Speed)

**Accuracy (gold standard):** a backtest is “accurate” if we can replay a period that was traded live and reproduce the broker’s realized behavior (fills + PnL) within defined tolerances (tick size, fees model). Vendor parity (e.g., DataBento artifact baselines) is a regression signal, not “truth”.

### Accuracy validation ladder (Tier 3 is the real gold standard)

- **Tier 1 (regression):** vendor parity / stored artifact baselines (e.g., DataBento-era runs) to detect drift.
- **Tier 2 (audit):** manual reviews around known hard edges (session gaps, holidays/early closes, rolls, rounding).
- **Tier 3 (gold):** **live replay baseline** — replay an interval that was traded live and reproduce broker fills + realized PnL within tolerances.

**Speed:** a backtest is “fast” when warm-cache runs are queue-free and complete in bounded wall time, with evidence (request counts, cache hit rate, iterations/sec, and wall-time split: data wait vs compute vs artifacts).

**Resilience:** a backtest is “resilient” when:
- simulation completion is not masked by post-processing failures (tearsheets/stats/plots),
- artifacts are as complete as possible even after failures (e.g., `trades.csv` and `stats.csv` still upload),
- failure modes are classified (simulation vs postprocess vs upload), and
- run metadata makes debugging easy (include `lumibot_version` in `settings.json` / `completion.json` whenever possible).

If you’re coordinating IBKR speed + crash hardening work, start with:
- `docs/handoffs/2026-01-26_IBKR_SPEED_RESILIENCE_MASTER_HANDOFF.md`

If you’re assessing whether backtests should be sped up via parallelism, start with:
- `docs/investigations/2026-03-28_BACKTEST_PARALLELISM_ASSESSMENT.md`

---

## File Index

### Core Documentation

| File | Purpose | When to Read |
|------|---------|--------------|
| `BACKTESTING_ARCHITECTURE.md` | **START HERE** - Data flow diagrams, component relationships, how backtests execute | Before modifying any backtesting code |
| `BACKTESTING_ACCURACY_VALIDATION.md` | Accuracy validation ladder (Tier 1/2/3) + how to build live replay baselines | When defining “accuracy” for a project |
| `BACKTESTING_PERFORMANCE.md` | How to measure and improve backtest speed (startup, downloader, caching, parity, cost) | When investigating slowness or production/local parity |
| `BACKTESTING_SPEED_PLAYBOOK.md` | Step-by-step SOP for performance work (router-mode, evidence, tests, ledgers) | When doing speed improvements (Theta/IBKR/etc.) |
| `BACKTESTING_SECOND_LEVEL_ROADMAP.md` | Roadmap for “seconds-level” backtesting (fills magnifier, event-driven clock); implementation notes in `investigations/bot_manager.md` | When planning second-level support |
| `ENV_VARS.md` | Complete environment variable reference with defaults and examples | When adding/changing env vars or debugging config issues |
| `ACCEPTANCE_BACKTESTS.md` | Release gate criteria - what must pass before deployment | Before any release or version bump |
| `BROKER_ORDER_SEMANTICS.md` | What live brokers allow/reject (extended hours, order types, etc.) | When matching live broker behavior |
| `BACKTESTING_SESSION_GAPS_AND_DATA_GAPS.md` | How we handle missing bars, session gaps, early closes, and multi-asset markets | When debugging “fills during closed market” issues |
| `BACKTESTING_TESTS.md` | Test suite organization and how to run backtest-related tests | When writing or debugging tests |
| `THETADATA_CACHE_VALIDATION.md` | How ThetaData caching works, cache invalidation, version bumping | When debugging stale data or cache issues |
| `REMOTE_CACHE.md` | S3 remote cache architecture and configuration | When debugging cache sync or S3 issues |
| `AI_TRADING_AGENTS.md` | Architecture, roadmap, prompt model, observability model, external MCP strategy, and acceptance-test plan for LumiBot's AI trading agent runtime | When documenting or planning LumiBot's AI trading agent feature set |
| `AI_AGENT_RUNTIME_PLAN.md` | Research summary, implementation notes, and follow-on plan for agentic strategies | When planning runtime internals, replay caching, memory, and tool plumbing |
| `TEARSHEET_METRICS.md` | Machine-readable tearsheet artifacts, custom tearsheet metrics, and usage rules | When working on tearsheet metrics, custom metrics, or agent consumption of tearsheet summaries |
| `PRODLIKE_LOCAL_BACKTEST_RUNS.md` | How to run production-like backtests locally | When replicating prod behavior locally |
| `DEPLOYMENT.md` | Release workflow (version branches, changelog, tags) | When deploying a new version |
| `FUTURES_ROLL_POLICY.md` | Futures contract rolling logic and configuration | When working with futures strategies |
| `DATABENTO_POLARS_OVERVIEW.md` | Databento integration and Polars dataframe usage | When working with Databento data source |

### Root-Level Files

| File | Purpose |
|------|---------|
| `../CHANGELOG.md` | **Deployment history** - MUST be updated every release |
| `../AGENTS.md` | Cross-tool AI assistant instructions (Codex, Cursor, etc.) |
| `../CLAUDE.md` | Claude Code-specific instructions |

---

## Directory Structure

```
docs/
├── README.md                    # This file - documentation index
├── [TOPIC_NAME].md              # Core documentation files
├── handoffs/                    # Cross-session coordination
│   ├── README.md                # Handoff conventions
│   └── YYYY-MM-DD_TOPIC.md      # Individual handoff notes
└── investigations/              # Deep dives and root-cause analyses
    └── YYYY-MM-DD_TOPIC.md      # Investigation reports
```

### Subdirectories

| Directory | Purpose | When to Use |
|-----------|---------|-------------|
| `handoffs/` | Cross-session coordination notes between AI agents or developers | When pausing work mid-task, switching contexts, or coordinating with others |
| `investigations/` | Deep dives, root-cause analyses, accuracy audits | When debugging complex issues or documenting findings for future reference |

Recommended investigation for backtest scaling questions:

- `docs/investigations/2026-03-28_BACKTEST_PARALLELISM_ASSESSMENT.md`

---

## Creating New Documentation

### File Header Template (REQUIRED)

Every new documentation file MUST start with this header:

```markdown
# TITLE

> Brief one-line description of what this document covers.

**Last Updated:** YYYY-MM-DD
**Status:** [Draft | Active | Deprecated]
**Audience:** [Developers | AI Agents | Both]

---

## Overview

Brief 2-3 sentence summary of the document's purpose and key points.

---

[Document content follows...]
```

### File Naming Convention (MANDATORY)

**All documentation files MUST use UPPERCASE names.**

Exceptions:
- Some local/private coordination notes or legacy files may not follow this (e.g., `docs/investigations/bot_manager.md`).
- When promoting an investigation into long-lived documentation, prefer the standard date-first investigation format:
  `docs/investigations/YYYY-MM-DD_TOPIC.md`.

| Location | Pattern | Example |
|----------|---------|---------|
| `docs/` | `TOPIC_NAME.md` | `BACKTESTING_ARCHITECTURE.md` |
| `handoffs/` | `YYYY-MM-DD_TOPIC_NAME.md` | `2026-01-04_THETADATA_HANDOFF.md` |
| `investigations/` | `YYYY-MM-DD_TOPIC_NAME.md` | `2026-01-02_ACCURACY_AUDIT.md` |

### Rules

1. **UPPERCASE** - All letters must be uppercase (except date digits)
2. **Underscores** - Use underscores `_` to separate words (not hyphens)
3. **Date prefix** - Handoffs and investigations use `YYYY-MM-DD_` prefix for chronological sorting
4. **Descriptive names** - Use clear, specific topic names

### Examples

```
✅ CORRECT:
  docs/BACKTESTING_ARCHITECTURE.md
  docs/handoffs/2026-01-04_THETADATA_HANDOFF.md
  docs/investigations/2026-01-02_ACCURACY_AUDIT.md

❌ WRONG:
  docs/backtesting_architecture.md      (lowercase)
  docs/Backtesting-Architecture.md      (mixed case, hyphens)
  docs/handoffs/thetadata_handoff.md    (missing date, lowercase)
```

---

## Changelog Maintenance (MANDATORY)

**`../CHANGELOG.md` MUST be updated with every deployment.**

### When to Update

- Deployments to production
- Version bumps / releases
- Bug fixes (especially data source, split, dividend fixes)
- New features
- Breaking changes (mark with ⚠️)

### Format

```markdown
## X.Y.Z - YYYY-MM-DD

### Added / Changed / Fixed / Deprecated / Removed / Security
- Description of change
```

### Pre-Deployment Checklist

- [ ] Changelog entry added with current date
- [ ] Version number updated
- [ ] Breaking changes clearly marked

See `../AGENTS.md` and `../CLAUDE.md` for full requirements.

---

## Public Documentation Site

- `docsrc/` contains the Sphinx source for the public docs site
- `generated-docs/` is local build output (gitignored)
- GitHub Actions builds + deploys Pages on pushes to `dev`
- **User-facing changes** should update both internal docs (`docs/`) AND public docs (`docsrc/`)
