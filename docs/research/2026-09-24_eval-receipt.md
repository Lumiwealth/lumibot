# Agent eval receipt, 2026-09-24

First measured run of the full agent eval suite. Everything here is read from
`artifacts/agent_evals/2026-09-24-full-suite*/summary.json`, not estimated.

## Result

| | Run 1 | Run 2 |
|---|---|---|
| Cases | 15 | 15 |
| Repetitions | 45 | 45 |
| Passed | 42 | 44 |
| Failed | 3 | 1 |
| Wall time | 317s | 315s |
| Cost | $0.1787 | $0.1660 |

**All 15 cases are now established**, meaning each has three consecutive
passes recorded against its current fingerprint. `fresh_case_count` 15 of 15.

**Total spend for both runs: $0.345.** Against the $30 budget that is 1.1%.

## What this settles

**Cost is a non-issue.** The whole suite costs about 17 cents a run on
`openai/gpt-6-luna`. Earlier estimates in
`2026-09-24_agent-eval-and-capability-audit.md` guessed "low single-digit
dollars" and were roughly 20x too high. Treat that audit's cost section as
superseded by this file.

**The slowness was never the repeat count.** It was
`scripts/agent_eval_rate_pacing.py`, which defaulted to 200,000 input tokens
per minute, a leftover from Gemini's free tier. Measured against the
`botspot-dev-ci-evals` OpenAI project on 2026-09-24, `gpt-6-luna` reports
`x-ratelimit-limit-tokens: 180000000` per minute. The suite was throttled about
900x below what the account allows.

Before and after, same suite, same machine:

```
old pacer (200k/min):   3 repetitions in ~600s
new pacer (20M/min):   18 repetitions in  150s
full run:              45 repetitions in  317s
```

Roughly 20x. That is the single change that makes running evals often
practical, and it is why coverage stayed thin.

## Flakiness, which is the real finding

Four single-repetition failures across 90 repetitions, about a 4.4% flake
rate, spread over four different cases:

- `options_iron_condor_limit_between_bid_ask` (run 1)
- `researcher_trader_evidence_handoff` (run 1)
- `stock_price_before_order` (run 1)
- `stock_pending_exit_no_duplicate` (run 2)

No two runs failed the same case, and none carried an `error`, so these are
judge verdicts on agent behaviour rather than crashes. That is exactly the
behaviour the three-consecutive-passes rule exists to surface: a single pass
would have accepted any of them.

It also means a one-repetition gate will report a false failure roughly 4% of
the time per case. With 15 cases that is a meaningful chance of a red gate on a
healthy build, so the gate needs a retry policy on a single failure before
this suite grows much larger. Worth fixing before the suite reaches 60 cases.

## Two operational facts worth keeping

**The eval key.** `OPENAI_API_KEY` must come from the OpenAI project
`botspot-dev-ci-evals`. The copy in `lumibot/.env` is empty or dead and returns
`401 Unauthorized` from `https://api.openai.com/v1/responses`. The working key
lives in `botspot_agent/.env`. Nothing in this repo should hold the value.

**Stale output roots block a rerun.** `artifacts/agent_evals/` carries a
33-row `ledger.jsonl` with no `model_calls.jsonl`, so the runner refuses to
resume: "This old run has no per-call spending ledger." Use a dated output
root, which is what previous runs did.

## Reproduce

```bash
cd /Users/robertgrzesik/Development/lumibot
export OPENAI_API_KEY=<key from the botspot-dev-ci-evals project>
python3 scripts/run_agent_evals.py \
  --max-cost-usd 30 \
  --output-root artifacts/agent_evals/$(date +%F)-full-suite
```

## Next

The numbers above were assembled by hand from the summary JSON. The runner
should emit this file itself at the end of a run, the way the Playwright
receipt works, so nobody has to ask what a run cost again.
