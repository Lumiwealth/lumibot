# Agentic tools and strategy implementation receipt

Date: 2026-09-20

## Scope and source

- Repository: `Lumiwealth/lumibot`
- Branch: `version/4.5.92`
- Starting commit: `58fb3781158cea21399bf65eb51d379d48b7a04d`
- Test host: macOS ARM64, Python 3.10.19
- Relevant runtime versions: HTTPX 0.28.1, Patchright 1.62.3
- Test isolation: deterministic suites used `LUMIBOT_DISABLE_DOTENV=1` and
  `LUMIBOT_DISABLE_DOTENV_LOCAL=1` unless a public API check explicitly needed
  the repository's API-test credential preflight.

This receipt distinguishes implemented source, local proof, and external gates.
It does not treat a source edit, documentation render, or selected green test as
proof of a production deployment.

## Implemented behavior

### Point-in-time SEC data

- Mutable live ticker, submissions, and company-facts responses use a bounded
  TTL. Backtests retain deterministic cache behavior.
- Immutable filing documents remain indefinitely cacheable.
- Company facts, filing lists, searches, documents, and sections accept the
  strategy's `as_of` boundary and reject information not publicly available at
  that time.
- All nine agent-facing SEC tools declare
  `temporal=published_at_as_of`. The previously reported tenth tool was an
  internal lookup path, not a tenth public tool.

### Full HTTP and RSS tools

- `http_request` supports GET, HEAD, OPTIONS, POST, PUT, PATCH, and DELETE;
  query parameters; JSON, form, multipart, and raw bodies; downloads; redirects;
  cookies; and structured errors.
- Credential profiles support bearer, basic, API-key/custom-header, cookies,
  and client certificates. Credentials are host-scoped and redacted.
- Public internet access remains available while loopback, private, link-local,
  reserved, metadata, and redirect-to-private targets are rejected unless the
  operator explicitly configures a trusted target.
- `rss_fetch` reuses the transport/session layer and handles RSS/Atom,
  deduplication, ETag, Last-Modified, 304 responses, and malformed feeds.

### Disclosure strategies

- The Congress example uses public report availability, not transaction date,
  for point-in-time visibility. It handles amendments, duplicates, invalid
  tickers, stale disclosures, and one-time processing.
- The SEC example parses Form 4 open-market versus other transaction codes,
  derivative status, direct/indirect ownership, amendments, and acceptance time.
- Both examples use a non-trading researcher and a dedicated trading/risk agent.
  This is the documented recommendation, not a framework restriction.
- Documentation states the congressional reporting lag and commercial-data
  licensing boundary. Frozen fixtures are packaged for deterministic examples.

### Stateful browser capability and showcase

- `BrowserSessionManager` exposes persistent profiles, JavaScript navigation,
  observation, actions, login, multi-tab control, uploads, downloads, storage
  state, screenshots, and action receipts through optional Patchright support.
- Browser credentials are host-scoped. Uploads are restricted to the configured
  managed root.
- Closing the last session now shuts down the Patchright runtime, fixing the
  event-loop/profile lifecycle failure found by the combined acceptance and soak
  tests.
- The showcase separates browser research, trading/risk, and optional publishing
  roles. Publishing is disabled by default and uses an idempotency receipt when
  enabled.

## TDD and automated proof

Red cases were observed before their fixes for disclosure amendment/ticker
handling, structured transport failures, and Patchright runtime reuse. The
following final commands are green:

```text
pytest tests/test_sec_fundamentals.py tests/test_agent_web_tools.py
       tests/test_agent_browser_tools.py tests/test_disclosure_strategies.py
       tests/test_browser_research_showcase.py
       tests/test_agent_capability_docs.py -q
45 passed

pytest tests/ --ignore=tests/backtest/
       -m 'not apitest and not downloader' -q
2798 passed, 37 skipped, 73 deselected, 2 xfailed, 4 xpassed,
83 subtests passed

pytest tests/test_agent_web_public_apitest.py -m apitest -q
1 passed

pytest tests/backtest/test_agent_runtime_backtest.py
       tests/backtest/test_ai_committee_builtin_tools_backtest.py
       tests/backtest/test_researcher_trader_example.py
       -m 'not apitest and not downloader' -q
16 passed

pytest tests/test_agent_browser_patchright_apitest.py -m browsertest -q
2 passed, including the 100-cycle lifecycle soak
```

The first complete run exposed a corrupt SciPy 1.15.3 native wheel on this host.
Reinstalling the same wheel reproduced the loader error; installing the supported
SciPy 1.14.1 wheel restored the import. The 34 previously blocked tearsheet,
statistics, and data-source tests then passed, followed by the complete green
non-network run above. No dependency pin was added to the repository.

Static and packaging proof:

```text
ruff check <all new Python modules and tests>
ruff check --select I001,W291,W293 <modified legacy Python modules>
git diff --check
All checks passed

python setup.py bdist_wheel
success; wheel includes browser/web modules and both frozen disclosure fixtures

sphinx-build -M html docsrc docsrc/_build -a -E
success with 15 existing documentation warnings
```

## Browser acceptance evidence

The owned local fixture proved authenticated login, JavaScript state, persistent
cookies and local storage, three tabs, managed upload, download contents,
idempotent publication, storage-state export, and screenshots. A separate
100-open/close profile soak passed without deadlock. The same browser acceptance
tests also ran inside the complete non-network suite.

- Authenticated screenshot:
  `artifacts/browser_acceptance/showcase-pytest/test_patchright_stateful_login0/artifacts/dded434d11d446c0ad3fabffc0da1b1d/authenticated-dashboard.png`
  SHA-256 `fb24e581aaed9be14cf57cea0d87d3df1dced27f9b8418c300f5236ef5971237`
- Published receipt screenshot:
  `artifacts/browser_acceptance/showcase-pytest/test_patchright_stateful_login0/artifacts/dded434d11d446c0ad3fabffc0da1b1d/published-trade-receipt.png`
  SHA-256 `4ba417a388c92b075e5bd64036a089eb64f0050ef607faf23848b660cfe5078d`

## Artwork and documentation proof

Every replacement was generated as an independent approved Image Generator
output, copied without overlays or post-processing, and inspected at its native
1672x941 resolution. The dense first batch was rejected and not added. The final
direction uses a light background, Spot-style robot roles, few arrows, and very
little text, following the successful Citadel and Ray Dalio examples. Those two
existing reference assets were preserved.

New simple workflow images:

- Congress disclosures
- SEC insider filings
- Browser research showcase
- Iron Condor
- Credit Spread
- Opening Range Breakout
- VWAP
- SPX zero-DTE bear-call team

Simplified replacements:

- Bill Ackman concentrated
- Bull/Bear large-cap stocks
- Bull/Bear leveraged ETF
- Warren Buffett value

Committed-output SHA-256 checksums:

```text
76e2b458c54c637374a32f7c076b1d288f60829a62f439e11fd861b4ba4432d0  ai-browser-research-showcase.png
6395078862405201fac6591fea7c7eead664289aa98f34eb0a45f78ee29decab  ai-congress-disclosures.png
27d56126a96b98f114a5d3d03b2510e2439dc876241b732144dd5be40d273db7  ai-credit-spread.png
96e0cc5131f06948b4d9cf1815fc4b038cac14ab476516cbccfea4992e2af030  ai-iron-condor.png
ddcb61b8c1b2ec643bc5ad0d17d2a3ecd37e27dd79d5c90037e64bd542ec14d3  ai-opening-range-breakout.png
0c3db37b07a92fd4f15b6538f69e743104ba2e6b0c7f1eea921cc3ad7d7cf86a  ai-sec-insider-filings.png
290b11837c1561852ae64742c5e119bca03378867f0fba90c4721409d261adf9  ai-spx-zero-dte-bear-call-team.png
c49df13be72d6014d23eaab9b9732cdfa55ceabc198d5063481d5ff721bb3c3f  ai-vwap.png
53a884df95b4a68f2c29eb39e971712b17da717eab7fec7d83eb92eb402d75d9  bill-ackman-concentrated.png
d69e90ead7eabae56c2a44835ffc3a8463baa34ae87cfb59eb81bf09f31f7df2  bull-bear-large-cap-stocks.png
d7829d6d88fad0ff8d8b07cf530e639dc8c128de0bb9f6a0a1c3559892d37f8b  bull-bear-leveraged-etf.png
1e8381974d3153f6b406be373ac681787268175cde3204de9d94277f2ebfb5a7  warren-buffett-value.png
```

Sphinx completed successfully, every documented agent example resolves to an
inspected PNG, and desktop plus 390-pixel mobile layouts were visually checked.

## External gates not represented as completed

- The exact Linux ARM64 Bot Manager/Fargate image, memory, cold-start, crash-rate,
  and anti-detection benchmark was not run. This Mac has no container runtime,
  and this task did not authorize a cloud deployment or new cloud spend.
- No third-party account was logged into and nothing was posted to a real social
  network. The consequential-action proof uses an owned deterministic fixture.
- Congress marketplace publication remains gated on commercial data rights or
  written authorization. Frozen fixtures and the engineering example are ready.
- No MetaMask integration, Slack message, marketplace publication, release, or
  Development/production deployment was performed.
- The separate historical backtest inventory was not green: three acceptance
  cases passed, two were skipped, and
  `test_acceptance_backdoor_butterfly` hit its own 1,500-second subprocess hard
  timeout. The run was stopped after that terminal failure rather than spending
  additional external-data time on a candidate already known to be red. Its
  preserved run directory is
  `tests/backtest/_acceptance_runs/backdoor_butterfly_full_year_20260920_025501_fbf00e52`.
  This is reported as an unresolved legacy acceptance-data/runtime failure, not
  as a green full-backtest suite and not as a failure in the new feature tests.
