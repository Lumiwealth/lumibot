# Agentic tools and strategy implementation receipt

Date: 2026-09-20

## Scope and source

- Repository: `Lumiwealth/lumibot`
- Branch: `version/4.5.92`
- Starting commit: `58fb3781158cea21399bf65eb51d379d48b7a04d`
- Test host: macOS ARM64, Python 3.10.19
- Relevant runtime versions: HTTPX 0.28.1, Patchright 1.62.3,
  Camoufox 0.5.6 (qualification-only optional engine)
- Test isolation: deterministic suites used `LUMIBOT_DISABLE_DOTENV=1` and
  `LUMIBOT_DISABLE_DOTENV_LOCAL=1` unless a public API check explicitly needed
  the repository's API-test credential preflight.

This receipt distinguishes implemented source, local proof, and external gates.
It does not treat a source edit, documentation render, or selected green test as
proof of a production deployment.

## Implemented behavior

### Point-in-time SEC data

- Mutable live ticker, submissions, and company-facts responses use a bounded
  TTL and persist ETag/Last-Modified validators for conditional revalidation.
  A 304 refreshes cache provenance without replacing the payload. Backtests
  retain deterministic cache behavior.
- Immutable filing documents remain indefinitely cacheable.
- Company facts, filing lists, searches, documents, and sections accept the
  strategy's `as_of` boundary and reject information not publicly available at
  that time.
- External web and disclosure results carry stable identity plus `source`,
  `published_at`, and `fetched_at` provenance. SEC cache metadata is persisted
  in sidecars rather than inferred from a cache filename.
- All nine agent-facing SEC tools declare
  `temporal=published_at_as_of`. The previously reported tenth tool was an
  internal lookup path, not a tenth public tool.
- An enumeration test fails if an external-data agent tool omits its temporal
  behavior declaration.

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
  automatic-plan transactions, derivative status, direct/indirect ownership,
  amendments, duplicates, and acceptance time. Amendments supersede the
  original transaction by stable transaction identity.
- Both examples use a non-trading researcher and a dedicated trading/risk agent.
  This is the documented recommendation, not a framework restriction.
- Documentation states the congressional reporting lag and commercial-data
  licensing boundary. Frozen fixtures are packaged for deterministic examples.
- A deterministic replay produces an agent trace, order CSV, summary, and HTML
  tear sheet. It refuses pre-publication records and proves that orders can
  originate only from the trading/risk role.

### Stateful browser capability and showcase

- `BrowserSessionManager` exposes persistent profiles, JavaScript navigation,
  observation, actions, login, multi-tab control, uploads, downloads, storage
  state, screenshots, recovery, and append-only redacted action receipts.
- Browser credentials are host-scoped. Uploads are restricted to the configured
  managed root.
- Locator waits explicitly support attached, detached, visible, and hidden
  states. Closing the last session shuts down the engine runtime.
- Patchright and Camoufox are selectable optional engines. Neither is declared
  the hosted default: Patchright failed the local basic anti-detection probe;
  Camoufox passed it but exceeded the current 1 GiB Fargate memory shape.
- The showcase separates browser research, trading/risk, and optional publishing
  roles. Publishing is disabled by default and uses an idempotency receipt when
  enabled.
- The real showcase backtest uses LumiBot's Pandas backtesting engine and proves
  browser evidence reaches the risk role, a real simulated order is filled, and
  the publisher receives the exact order identifier.

### Email and Slack communication tools

- Strategies can send email through Resend and Slack messages through Slack,
  list/read sent and received email, inspect attachment metadata/content, list
  Slack channels/messages/threads, and retrieve individual messages.
- The capabilities are exposed as independent agent tools, so an operator can
  grant only the exact communication surface a strategy needs.
- Backtests cannot force a live send, including by passing `enabled=True`.
  Writes become structured `simulated_not_sent` receipts and reads require
  explicit timestamped fixtures, preventing a historical simulation from
  reading today's inbox or workspace.
- Provider secrets remain inside configured providers and are not returned in
  tool output. Hosted BotSpot ownership and permission enforcement remains a
  server responsibility rather than a client-side credential convention.

## TDD and automated proof

Red cases were observed before their fixes for disclosure amendment/ticker
handling, structured transport failures, Patchright runtime reuse, SEC
conditional revalidation, and attempts to override the no-send backtest
boundary. The following final commands are green:

```text
pytest tests/test_sec_fundamentals.py tests/test_agent_web_tools.py
       tests/test_agent_browser_tools.py tests/test_disclosure_strategies.py
       tests/test_disclosure_replay.py tests/test_browser_research_showcase.py
       tests/test_browser_benchmark.py tests/test_agent_capability_docs.py
       tests/test_thetadata_queue_client.py
       tests/test_notifications_and_memory.py
       tests/backtest/test_browser_showcase_backtest.py -q
112 passed

pytest tests/ --ignore=tests/backtest/
       -m 'not apitest and not downloader' -q
2823 passed, 37 skipped, 73 deselected, 2 xfailed, 4 xpassed,
83 subtests passed

pytest tests/test_agent_web_public_apitest.py -m apitest -q
1 passed

pytest tests/backtest/test_agent_runtime_backtest.py
       tests/backtest/test_ai_committee_builtin_tools_backtest.py
       tests/backtest/test_researcher_trader_example.py
       tests/backtest/test_browser_showcase_backtest.py
       -m 'not apitest and not downloader' -q
17 passed

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
git diff --check
All checks passed

python setup.py bdist_wheel
success; wheel includes browser/web modules and both frozen disclosure fixtures

sphinx-build -M html docsrc docsrc/_build -a -E
success with 15 existing documentation warnings
```

The broader legacy-only lint probe still reports 20 existing import/whitespace
findings in `lumibot/strategies/_strategy.py` and `tests/conftest.py`, all
outside the changed hunks. They were not silently reformatted as part of this
feature change.

The final complete non-network output is preserved at
`docs/research/artifacts/test-logs/non-network-suite.log`; the matching node-id
inventory is preserved at
`docs/research/artifacts/test-logs/non-network-inventory.log` (2,862 selected
from 2,935 collected, 73 deselected). The separate
Backdoor Butterfly acceptance command reported one skip in 0.20 seconds because
the current isolated shell intentionally had no ThetaData, downloader, or S3
acceptance credentials. Its exact skip output is preserved at
`docs/research/artifacts/test-logs/backdoor-butterfly-acceptance.log`. It is
recorded as incomplete, not green.

Raw-log SHA-256 receipts:

```text
21355f2128de7cc06bb05abaeaff0051f8640982d2af406244a49f249596005b  non-network-suite.log
08b1358d9855140fba5489302c568b40a766432f2bd5910dbd6d82ccfccda52a  non-network-inventory.log
63e06332973d8aaf92533adc813617b7d8f4a0ecf25419b84546f0066062c709  focused-feature-suite.log
570bf02a40927881bc23b5fc4de3b79e9b66ef707407a7192ce016ee29f732ff  backtest-consumers.log
9961703b63397cd7ef051bb67f6998938f4fe9b1c759ee6b7c245c1738ce4dff  public-http-apitest.log
253521a1fc5643c2529a0a756e39c9cb5cf62e09dedddcec96d09c7e08469994  browser-patchright-acceptance.log
cfa1506f61f535333df7ed0b97886196d61a9eef402a803f40bc884f3b816fd2  backdoor-butterfly-acceptance.log
e10183cbd254f3184fc220a83cbd32ae34624bbe6cbd450f32a84d2ee67ff7ac  sphinx-build.log
590b07fee3a9fedab0859359db88757b19c3adc3bf6fe90a3b644e8a9757ae63  wheel-build.log
```

## Browser acceptance evidence

The owned local fixture proved authenticated login, JavaScript state, persistent
cookies and local storage, three tabs, managed upload, download contents,
idempotent publication, storage-state export, and screenshots. A separate
100-open/close profile soak passed without deadlock. The same browser acceptance
tests also ran inside the complete non-network suite.

The local Patchright/Camoufox benchmark results, fingerprints, memory, latency,
and read-only production task-shape/cost analysis are in
`docs/research/2026-09-20_browser-engine-qualification.md`; its two raw JSON
receipts are committed under `artifacts/browser_benchmark/`.

- Authenticated screenshot:
  `docs/research/artifacts/browser-acceptance/authenticated-dashboard.png`
  SHA-256 `fb24e581aaed9be14cf57cea0d87d3df1dced27f9b8418c300f5236ef5971237`
- Published receipt screenshot:
  `docs/research/artifacts/browser-acceptance/published-trade-receipt.png`
  SHA-256 `4ba417a388c92b075e5bd64036a089eb64f0050ef607faf23848b660cfe5078d`

The deterministic Congress replay proof is committed under
`docs/research/artifacts/congress-disclosure-replay/`:

- summary SHA-256 `7d4749aeae3ca802e96c700f4ac8ed8b2ff0452237689c85879e4e86faaff669`
- agent trace SHA-256 `987b3caaa22a16df4cfc575daa5d1c7469a6356c1597fd1108d2fb165e5e9d84`
- trade CSV SHA-256 `dd51f01a3daed54e20736d42098ca9ca67a325f6d9e46290117d7dfc10091de4`
- rendered tear sheet SHA-256 `ec3190cefdd10652f205cf5f7bc2188db5769a27364e9112da0e8247daa196a0`

## Artwork and documentation proof

Every replacement was generated as an independent GPT Image 2.5 Sunburst
approved-generator output, copied without overlays or post-processing, and
inspected at full resolution. The dense first batch was rejected and not added.
The final direction uses a light background, Spot-style robot roles, few arrows,
and very little text. Ray Dalio and Citadel were regenerated in the same simpler
system rather than preserving their older arrow artifacts.

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
- Ray Dalio idea meritocracy
- Citadel sector pods

The exact topology prompt, model/quality record, raw output path, dimensions,
and output hash for every asset are recorded in
`docs/research/2026-09-20_sunburst-workflow-artwork-receipt.md`.

Committed-output SHA-256 checksums:

```text
62062d5628845cd319f98240a1753869783835795181cc5d4c36f3c7fdd2a09d  ai-browser-research-showcase.png
9dc423d24e7adbb9ce59fface65bcf85ef9cc11ced4c29784724a95f38f05eba  ai-congress-disclosures.png
4e1ac9cf4ffcc00c4ab42440b231b8076119cf49b3602b6a1df6b595b813a0ca  ai-credit-spread.png
639a3bf19a14c9566002609682c210a115ef47f2c1d4a161415e436b7d010fd1  ai-iron-condor.png
e929328cff462b76709b12a5592dbf04a9b8d88b4a987afe651f540976006dca  ai-opening-range-breakout.png
82add84230c36659c28c63390c5b538613c03514d93df56af257df600c0d5da6  ai-sec-insider-filings.png
ab74d18a0471137303e6c7aa4d2bfc91425e6dbf9f23b4936c5ca679be32d118  ai-spx-zero-dte-bear-call-team.png
1e405a3e97fd0f8654164698cdf5206f5b8d003e5ba44a1cbac389b88ab5b932  ai-vwap.png
d115acd09e12d8663b520e06f806fe88d57d6eb7802c0b3b30150467fe9af39b  bill-ackman-concentrated.png
50267f7b324bc37a2859cb5406982458e3b1dcdaa384a486f3d3fb747015640b  bull-bear-large-cap-stocks.png
701d80d1a6b3cce20539147d5c031c545beaf223d3d20f7e71a42c35321ce519  bull-bear-leveraged-etf.png
3316eb0df31351353f4b4e691ea08404ad586f2e90f307619a6a80d4bf235d85  citadel-sector-pods.png
08880ebef98df090d1bd185e38ed521ba3b416069eb7361659ba93860104a560  ray-dalio-idea-meritocracy.png
c6cb5fc1fbbb1eb8ad198b06506e255828a91a85e7b948859bc5c821be96015b  warren-buffett-value.png
```

Sphinx completed successfully, every documented agent example resolves to an
inspected PNG, and desktop plus 390-pixel mobile layouts were visually checked.

## External gates not represented as completed

- The exact Linux ARM64 Bot Manager/Fargate image, memory, cold-start, crash-rate,
  and anti-detection benchmark was not run. This Mac has no container runtime,
  and this task did not authorize a cloud deployment or new cloud spend.
- No third-party account was logged into and nothing was posted to a real social
  network. The consequential-action proof uses an owned deterministic fixture.
- A frozen-fixture Congress engineering demo or free educational listing is
  ready. A live-data product still needs a source-terms review because inclusion
  in a subscription product may be commercial use even when the listing itself
  has no separate price.
- No MetaMask integration, external MetaMask Slack message, marketplace
  publication, release, or Development/production deployment was performed.
- The separate historical backtest inventory was not green: three acceptance
  cases passed, two were skipped, and
  `test_acceptance_backdoor_butterfly` hit its own 1,500-second subprocess hard
  timeout. The preserved output repeatedly records the upstream terminal cause
  as `ThetaTerminal session invalid (status=403)`. The run was stopped after
  that terminal failure rather than spending additional external-data time on
  a candidate already known to be red. Its
  preserved run directory is
  `tests/backtest/_acceptance_runs/backdoor_butterfly_full_year_20260920_025501_fbf00e52`.
  This is reported as an unresolved legacy acceptance-data/runtime failure, not
  as a green full-backtest suite and not as a failure in the new feature tests.
