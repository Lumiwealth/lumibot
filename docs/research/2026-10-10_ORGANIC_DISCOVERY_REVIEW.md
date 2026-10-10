# Organic discovery and developer activation

Evidence-based follow-up to the September growth review.

Last Updated: 2026-10-10
Status: Local implementation and research; publication pending
Audience: Maintainers, documentation contributors, developer education

## Overview

Improve the existing path from discovery to a successful, inspectable backtest.
The current repository already has a CLI, eight translated READMEs, comparison
pages, migration guidance, AI examples and a separate hosted continuation through
BotSpot. Rebuilding those surfaces is not the next growth task.

This review supersedes the September 18 report's claims about missing features,
adoption proxies, peer review and causal growth. Its historical snapshot remains
available, but its recommendations require revalidation against current source.

## Verified corrections

| Earlier claim | Current evidence and implication |
|---|---|
| Lumibot has no CLI. | `lumibot demo`, `init`, `backtest` and `run` already exist. Verify the first run and recovery paths before expanding onboarding. |
| Fork/star ratio measures use; downloads count people running the package. | These are different events without a matched cohort. Package retrieval, cloning, forking and starring do not establish a successful backtest. PyPI also changed download logging on August 24, 2026; annotate that measurement break. [PyPI announcement](https://blog.pypi.org/posts/2026-08-31-download-counts/). |
| TradingAgents has never been reviewed. | It appears on the [AAAI 2025 MARW accepted-paper list](https://sites.google.com/view/marw-ai-agents/accepted-papers). The [AAAI workshop policy](https://aaai.org/conference/aaai/aaai-25/workshop-list/) describes peer review. Workshop acceptance is distinct from journal publication. |
| A paper caused TradingAgents' star count. | A paper, workshop, code, demonstrations and community channels exist; no channel attribution or causal experiment establishes that explanation. |
| Other projects cannot connect agents to execution. | [Hummingbot Condor](https://hummingbot.org/condor/) explicitly connects agents to execution. [FinRL](https://github.com/AI4Finance-Foundation/FinRL) also describes Alpaca support and successor projects. Show Lumibot's actual workflow rather than exclusivity. |
| Broker backlinks must start from zero. | [Alpaca already discusses Lumibot](https://alpaca.markets/learn/how-i-use-ai-to-research-and-test-trading-ideas-with-alpaca), and [Tradier lists it](https://docs.tradier.com/docs/libraries). Offer a maintained lesson and technical review. Listings are not endorsements. |
| A large educational repo proves university-led growth. | [UChicago's Autumn 2026 syllabus](https://mpcs-courses.cs.uchicago.edu/2026-27/autumn/courses/mpcs-52560-1) names QuantConnect. This verifies teaching use, not its contribution to stars or the identity of an unnamed 60k-star project. |

## Local fixes

- Sphinx and the custom template emitted competing Open Graph descriptions,
  images and URLs. One shared metadata context now preserves the authored
  description, and the extension emits one Open Graph set.
- The homepage now uses `/` consistently in canonical, Open Graph and sitemap
  URLs. The sitemap is generated after a successful HTML build from Sphinx's
  document inventory, including nested API pages. Source fragments, copied
  redirects and search/index utility pages are excluded. Optional `lastmod`
  dates are omitted because source-file history alone misses included code and
  shallow CI history can invent freshness. This follows Google's guidance to
  submit canonical URLs and accurate dates; inclusion does not guarantee
  indexing. [Sitemap guidance](https://developers.google.com/search/docs/crawling-indexing/sitemaps/build-sitemap).
- Indicator documentation now links to actual marker, line and OHLC API pages.
- README and homepage comparisons now acknowledge Condor and avoid the unsupported
  “IB only” Backtrader claim. Vectorbt's license label includes Commons Clause.
  ThetaData coverage no longer includes CME futures, consistent with its
  [FAQ](https://www.thetadata.net/faq).
- All nine READMEs distinguish the free demo from billed AI calls and Alpaca
  paper-account credentials. Point-in-time tool controls are no longer described
  as a guarantee against all model-training or external-data leakage.
- The synthetic installation example now supplies `risk_free_rate=0.0` so saving
  its settings does not fetch Yahoo's rate. This change is limited to the example;
  its prices and result remain synthetic, not historical performance evidence.

Regression coverage checks built HTML, escaped descriptions, fallback descriptions,
the chosen share image, canonical URLs, sitemap membership and the absence of the
offline example's external rate lookup. Existing docs and navigation tests remain
applicable. Building with optional imports mocked can still produce pre-existing
autodoc/RST warnings; a successful build is not proof of warning-free API content.

## Next useful work

1. Review and release the local fixes through the normal repository workflow;
   then verify deployed metadata, sitemap, links and the installation command.
2. Rehearse one small deterministic lesson with an independent participant.
   Preserve settings, orders, failures and the learner's explanation of assumptions.
   Model calls and paper accounts belong in optional follow-on steps.
3. Offer that lesson to one existing broker education channel or one relevant
   student society. Publication, contact, rights and integration validation require
   their own owners; no outreach or partner agreement is implied here.
4. Verify the existing web identity and acquisition contract before reporting
   docs → signup → successful backtest → payment conversion. Keep local library
   use separate; do not add package telemetry to manufacture a denominator.
5. Pursue academic work only after a novelty and methods review. Audit temporal
   validity and reproducibility with neutral baselines; publish null findings and
   failures. A protocol is not a study result or a submission-ready paper.

Do not prioritize fake activity, paid backlinks, another CLI, another generic
landing page, an unvalidated dashboard or trading-performance claims. Search
rankings, causal growth, partner interest and academic acceptance remain unproven.
