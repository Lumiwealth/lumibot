# Contributing to LumiBot

Help developers build reliable strategies, backtests, and AI trading teams.
Small reproducible fixes, complete examples, and provider contract tests are welcome.

## Before coding

Search existing issues and pull requests. For a new API or broker, describe the
user problem, current workaround, and compatibility requirements first. Existing
`Strategy` subclasses are a supported public contract.

## A useful bug report

Include LumiBot/Python versions, operating system, broker/data source, expected
and observed behavior, and the smallest reproducing strategy. Remove credentials,
account identifiers, and personal data from logs. Never submit `.env` files.

## Tests and pull requests

Reproduce the failure before fixing it. Run the entire affected test files and
tests for consumers of any changed contract. Distinguish fixture tests, real
backtest-engine integration, real-model evals, and external-provider tests.
Do not submit fabricated model trajectories as real-model evidence.

```sh
LUMIBOT_DISABLE_DOTENV=1 LUMIBOT_DISABLE_DOTENV_LOCAL=1 python -m pytest tests/test_growth_entrypoints.py -q
```

Select the relevant files for your change; this example command is not the full
test suite. External-service tests need separately configured test credentials.
Do not run untrusted contributions with broker or provider secrets.

Open a focused PR targeting `dev`. Explain the problem, final behavior, exact
test commands/results, compatibility, and relevant docs. Maintain both `docsrc/`
for public docs and `docs/` for engineering context. A maintainer review is
separate from an automated review. See [maintainer triage](docs/MAINTAINER_TRIAGE.md).
