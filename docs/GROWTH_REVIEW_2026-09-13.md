# LumiBot growth implementation and review

## What changed and why

The goal is faster library adoption and GitHub-star growth, with useful education leading interested developers into the free challenge and bootcamp. Visual polish alone does not establish a growth rate. No claim of doubled stars, improved conversion, or settled revenue is made by this change.

The headline is now **Turn trading ideas into working strategies.** This describes a developer outcome. It replaces internal workflow labels that neither explain the product nor apply to every strategy. AI remains prominent, while Python strategies and examples are direct entry points. The existing Strategy lifecycle remains intact.

| Surface | Exact files | Result |
| --- | --- | --- |
| README | `README.md` | Benefit headline and four concrete reasons to use LumiBot; runnable AI command, source and recorded-run links; traditional Python route; retained clickable free-challenge banner |
| Homepage | `docsrc/index.rst` | Benefit copy, explicit AI/Python paths, concrete quickstart, centered character-free hero, reasons to choose the library, tracked education banner after useful content |
| AI examples | `docsrc/agents_examples.rst` | Stocks/macro/options illustration matching the gallery, examples remain linked; new Rob free-challenge banner |
| AI quickstart | `docsrc/agents_quickstart.rst` | Backtesting benefit illustration, “See the example backtest” heading, preserved old anchor, bootcamp banner after tutorial |
| Python setup | `docsrc/getting_started.rst` | Traditional setup retained; new Rob free-challenge banner |
| Reusable components | `docsrc/standalone_components.rst` | FRED/SEC illustration matching the actual examples; bootcamp learning link |
| Agent entry | `docsrc/agent_start_here.rst` | Download link points to the version branch containing the example |
| Traditional examples | `docsrc/examples.rst` | Existing examples remain; obsolete model identifier updated |
| SEO/social | `docsrc/_templates/base.html` | One description meta tag; page-specific social images |
| Click measurement | `docsrc/_html/posthog.js` | Traditional Python example route gets its own start-route classification |
| Agent-readable docs | `llms.txt`, `llms-full.txt` | Regenerated from current documentation |

The current set has six character-free editorial illustrations and four creator banners. Generated mascot variants were rejected and replaced; the repository instructions now prohibit regenerating them. The user-approved free-challenge asset remains. See [asset provenance](assets/ai-trading/README.md). These are conceptual illustrations and education promotions; actual backtest results remain separately identified.

## What we learned from successful projects

- [TradingAgents](https://github.com/TauricResearch/TradingAgents) makes its framework recognizable, shows how to install and run it, and provides both CLI and package usage. Apply the recognizable identity and executable start; do not claim every LumiBot strategy uses a fixed debate workflow.
- [AI Hedge Fund](https://github.com/virattt/ai-hedge-fund) leads with a plain explanation and a short installation/run path. Apply that short path and clear requirements. Its popularity does not prove that replacing LumiBot's Strategy API would increase adoption.
- [OpenBB](https://github.com/OpenBB-finance/OpenBB) communicates a clear platform purpose and developer entry points. Apply concise use cases, useful components, and code that fits an existing project.
- [Google's SEO starter guide](https://developers.google.com/search/docs/fundamentals/seo-starter-guide) emphasizes useful content, organization, descriptive links and clear page descriptions. This supports the navigation, metadata and working-example work; repeating “AI” or adding decorative images is not a ranking strategy.

These are observed presentation patterns, not a causal explanation of their star totals. LumiBot should borrow useful structure and keep its own brand, broker integration and backtesting strengths.

## Tracked education placements

All image links are real anchors. Documentation image links use `utm_source=documentation`, `utm_medium=docs`, `utm_campaign=lumibot_ai_trading`.

| Placement | Destination path | utm_content |
| --- | --- | --- |
| Homepage | `/challenges` | `home_challenge_image` |
| Setup | `/challenges` | `setup_challenge_image` |
| AI examples | `/challenges` | `examples_challenge_image` |
| AI quickstart | `/courses/ai-trading-bootcamp` | `quickstart_bootcamp_image` |
| Components | `/courses/ai-trading-bootcamp` | `components_bootcamp_image` |
| README | `/challenges` | `free_challenge_image` (source github, medium readme) |

Firefox verified the homepage image click redirects to the current September 16 free challenge with all four tags retained. The bootcamp destination also loads with its tags. This proves navigation, not registration, CRM attribution, booking or purchase. No form was submitted and no payment was made.

## Validation and screenshots

[Complete screenshot review](assets/ai-trading/review/complete/README.md) contains 40 full documentation pages, focused views, mobile-width navigation and destination evidence. The captures include text, code, menus and images. They are local build evidence, not a Dev deployment.

The following command passed **22 tests**:

```bash
.venv/bin/python -m pytest tests/test_growth_entrypoints.py tests/test_public_docs_community_links.py tests/test_public_docs_tracking.py tests/test_public_docs_discovery.py tests/backtest/test_researcher_trader_example.py tests/backtest/test_first_backtest_example.py -q
```

Sphinx HTML and agent-document builds succeeded. All 40 rendered page audits found one description tag and no broken article images. Three 390px Firefox frame layouts had no document-level horizontal overflow. This is browser-width evidence, not an iPhone-device test.

New tracking and benefit assertions first failed before their fixes. No trading runtime was modified in this visual follow-up. Earlier real AI run evidence is in [the recorded SPY run](assets/ai-trading/spy-20260913/README.md); new AI decisions are not promised to reproduce its returns. The current visual commit is not full broker or release qualification.

## Next actions, in priority order

1. **Measure the now-clear first-success paths.** Preserve the separate Python and AI routes. Compare rolling 7/28-day net stars, GitHub visitors/clones and documentation start clicks with a dated baseline. These are distinct measures, not interchangeable users. Use opt-in completion evidence or explicit user feedback to learn where setup fails; never add hidden strategy telemetry. Lowest engineering cost and no Rob attention beyond reviewing findings. Success: evidence of more completed starts, not simply more clicks.
2. **Turn one working example into a repeatable public tutorial and demonstration.** Reuse the source, dependencies, run dates and recorded outputs already linked. Show the installation and actual execution, including uncertainty in fresh AI output. This makes the library easier to recommend and gives distribution a useful object. Prepare drafts before any separately authorized public sends. Measure referred starts and stars; paid generation has a separate cap.
3. **Connect useful learning to collected revenue.** The new contextual creator banners provide the route. Verify consented registration-to-booking-to-payment attribution through approved analytics before claiming revenue lift. Report settled payments less refunds and fulfillment costs by placement where attribution supports it. Conversion rates and ROI are currently unknown; do not invent them. Rob's time should be limited to qualified booked calls; prospect follow-up belongs to the sales owner.
4. **Improve contributor turnaround.** Use the existing triage inventory, issue forms and contribution instructions to classify old PRs by actionable next step. Review merits and ask for missing reproduction evidence; do not bulk-close by age. Track time to first useful response and time to resolution. This supports trust and retained contributors, with an indirect and uncertain cash effect.
5. **Expand proven use cases, not API surface by guesswork.** Prioritize failures observed in real setup attempts and common broker/data questions. Add complete examples and narrow integration coverage before offering another programming model. A function API remains unproven as a growth lever and is not part of this change.

The largest remaining growth work is distribution and evidence-driven iteration. No redesign can promise 105,000 stars. The smallest useful decision is whether more visitors can run a strategy and whether interested learners reach a qualified, retained education outcome.

## Release boundary

Source changes belong to `version/4.5.92`. Dev PR [#1165](https://github.com/Lumiwealth/lumibot/pull/1165) is open and requires review; it was BLOCKED at this audit. This work does not bypass that review, deploy the public documentation site, publish a package, or claim the complete wider growth program is finished.
