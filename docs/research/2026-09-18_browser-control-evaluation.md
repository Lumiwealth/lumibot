# Browser control for LumiBot agents: which browser, and why

Research date: September 18, 2026. Benchmarks cited are other people's published
measurements, not ours. Where something is our own measurement or a fact read
out of this codebase, it says so.

## The question

Give a LumiBot agent the ability to read any page, including ones with no API:
earnings call transcripts, filings, exchange notices, broker portals, niche data
sources. It has to run headless on the same infrastructure that already runs
scheduled strategies, and it has to not be blocked as a bot.

## The constraints we actually have

Read out of `bot_manager` today:

- **Base image:** `python:3.13-slim-trixie` (Debian trixie). No browser in it.
- **Scheduled custom Fargate default shape:** `cpu = 512` (0.5 vCPU),
  `memory = 1024` MiB. From `terraform/main.tf`,
  `scheduled_custom_fargate_cpu` and `scheduled_custom_fargate_memory`.
- **Backtest tasks** reserve `cpu = 2048` and `memory = 3700` MiB, so the larger
  shape exists but is not what scheduled strategies get.
- Fargate is Linux and headless. There is no display.

Half a vCPU and one gigabyte is the number that decides this. A Chromium process
with one page is comfortable in about 300 to 500 MiB, so one browser fits, but
not with much room, and not two at once.

## What the 2026 benchmarks actually found

Two independent studies, different methods, different winners. Both are worth
reading because the disagreement is the finding.

**Ian Paterson, May 18 2026.** Seven tools, 31 Cloudflare-protected live
targets, three sweeps, 651 verdicts, headed, single residential IP.

| Tool | OK | Gated | Blocked |
|---|---:|---:|---:|
| nodriver (system Chrome 148) | 28 | 3 | **0** |
| CloakBrowser | 26 | 3 | 2 |
| curl_cffi (no browser at all) | 26 | 3 | 2 |
| Patchright (channel=chrome) | 25 | 3 | 3 |
| Camoufox (Firefox 135) | 25 | 3 | 3 |
| vanilla Playwright | 24 | 2 | 5 |
| rebrowser-playwright | 24 | 2 | 5 |

**The Web Scraping Club, July 23 2026.** 15 libraries against a pure JS
fingerprinting referee. Only four passed: Camoufox, CloakBrowser, RayoBrowse and
scrapling.

They disagree because they measure different layers. The first measures live
targets, where the dominant signal is **automation-protocol fingerprinting**:
anti-bot gates watching for the `Runtime.enable` and `Target.setAutoAttach` CDP
calls that Playwright issues at startup, before any page JavaScript runs. The
second measures JS fingerprinting in a lab, where engine-level spoofing wins.

Two conclusions that survive both studies:

1. **A JavaScript stealth plugin cannot fix the protocol leak.** It runs too
   late. `playwright-stealth` and `playwright-extra` are a floor, not a solution.
2. **The tool is one layer and the IP is another, and neither compensates for
   the other.** A clean fingerprint from a marked datacenter IP still gets
   blocked, and a Linux server behind a residential proxy advertising a Linux
   browser is a contradiction that gates flag on its own. Fargate tasks come
   from AWS datacenter IPs. That is the layer we are weakest on, and no browser
   choice fixes it.

## Licences, and this decides more than the benchmark does

| Tool | PyPI licence | Implication for BotSpot |
|---|---|---|
| Camoufox | MIT (package; the Firefox build is MPL-2.0) | Fine |
| Patchright | Apache-2.0, following Playwright | Fine |
| curl_cffi | MIT | Fine |
| **nodriver** | **AGPL-3.0** | **Problem** |

nodriver won the live benchmark outright and it is the one we should not adopt.
AGPL-3.0's network clause reaches software offered to users over a network,
which is exactly what BotSpot is. Using it inside the hosted product would put
an obligation on BotSpot's own source that Rob has not agreed to. LumiBot itself
is GPL-3.0 so the open-source side is arguable, but the hosted side is where the
risk lands, and the two share code.

This is a question for counsel before anyone writes an import statement, not an
engineering preference.

## Recommendation

**Default: Patchright, with `channel=chrome`.**

- Drop-in replacement for Playwright. `from patchright.sync_api import ...`
  instead of `from playwright.sync_api import ...`. No rewrite.
- It closes the `Runtime.enable` protocol leak, which is the layer that actually
  blocks people, and it is the only actively maintained patch fork that tracks
  upstream Playwright closely. rebrowser-patches has not shipped code since
  September 2024 and benchmarked identically to vanilla, which is what a stale
  fork looks like.
- Permissive licence.
- Cost: Chrome or Chromium in the image, roughly 400 MiB. Fits the current
  shape with one page open, but only just.

**For anything that does not need JavaScript: curl_cffi, and skip the browser.**
It tied a patched Chromium fork at 26 of 31 targets in a 21-line wrapper. Filings,
RSS, JSON endpoints and most static pages need no browser at all, and this is
the difference between a container that fits in 1 GiB and one that does not.
Try it first, every time.

**Escalation for hard targets: Camoufox.** MIT, engine-level Firefox spoofing,
the strongest measured result on pure JS fingerprinting. Costs 200+ MiB per
instance on top of a ~150 MiB custom Firefox download, so it needs a bigger
Fargate shape than the scheduled default. Make it opt-in per strategy, not the
default.

**Not nodriver**, despite it winning, for the licence reason above. It also
prefers headed mode, which on Fargate means adding Xvfb, and its async-only
object model is not a Playwright drop-in.

**Hosted browsers such as Browserbase** solve the IP reputation problem, which is
the layer we are genuinely weakest on, and they cost money per session. That is
a spending decision for Rob with a real monthly number attached, not something
to adopt quietly.

## The order to do this in

1. Add a `browser_fetch` agent tool that tries `curl_cffi` first and only starts
   a browser when the page needs JavaScript. Most targets stop here.
2. Add Patchright behind that, with Chrome in the bot_manager image. Measure
   the container size increase and the task memory headroom before merging.
3. Only then look at whether any real target still fails, and only then consider
   Camoufox with a raised Fargate shape.
4. Treat proxying as a separate decision from browser choice. It is the bigger
   lever for sustained workloads and it has its own cost and policy questions.

Raising `scheduled_custom_fargate_memory` above its current 1024 MiB default is a
cost increase across every scheduled task and needs Rob's explicit approval with
the incremental monthly number in front of him.

## For the Christy engagement

If browser control lands inside Christy's project, it funds a capability LumiBot
wants anyway, which is the cheapest way to build it. Two things to establish on
the call before scoping:

- **Which sites, specifically.** The recommendation above changes completely
  depending on whether the targets are static filings or a Cloudflare-protected
  broker portal. Ask for three real URLs.
- **Whether a login is involved.** A browser that has to hold a session is a
  different, harder problem than one that reads public pages, and it brings
  credential handling rules with it.

Do not promise a browser capability with a date attached until those two answers
exist.
