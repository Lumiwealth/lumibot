# Documentation

This folder contains **human-authored** documentation (architecture, investigations, handoffs, ops notes).

Start here:
- `docs/BACKTESTING_ARCHITECTURE.md`

ThetaData docs:
- Handoffs: `docs/thetadata/handoffs/`
- Investigations: `docs/thetadata/investigations/`

Public documentation site:
- `docsrc/` contains the Sphinx source.
- `generated-docs/` is local build output from `docsrc/` (gitignored).
- GitHub Actions should build + deploy Pages on pushes to `dev`.
