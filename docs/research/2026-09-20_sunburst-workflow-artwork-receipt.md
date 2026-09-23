# Sunburst workflow artwork receipt

Date: 2026-09-20

All fourteen assets below are independent, raw approved-generator outputs. They
were regenerated after a code-topology audit, copied into the documentation
without cropping, retouching, compositing, overlays, or text repair, and
inspected at full resolution.

## Generation contract

- Model: GPT Image 2.5 Sunburst
- Quality: maximum
- Purpose: website/documentation
- Shared prompt: Create a clean LumiBot workflow diagram on a pale warm
  background. Only actual agents receive the supplied official LumiBot head and
  an explicit ``AGENT`` tag. Inputs use source-specific icons, and broker orders
  use a generic order-ticket icon plus an explicit ``OUTPUT`` tag; neither gets
  an agent head. Use large typography, exact public strategy titles, exact code
  roles, and only the arrows needed to show the audited topology. Render every
  label exactly and add no explanatory prose.

The following asset-specific prompt contracts record the exact topology and
visible labels used for regeneration. The generator invocation also referenced
the previous asset and/or the canonical Spot close-up where useful.

| Asset | Asset-specific prompt contract | Raw output | SHA-256 |
|---|---|---|---|
| `ai-congress-disclosures.png` | `Congressional Disclosure Agent`; INPUT `Public Disclosure` -> AGENT `Disclosure Researcher` (`Checks timing and evidence`) -> AGENT `Trading & Risk` -> OUTPUT `Broker Order`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-685da486-bf4f-4b54-940b-327d0d4d1f95.png` | `0704203c035132f205d5402302f1a8346bace941bd8ebec8f51f4ca62fcc2977` |
| `ai-sec-insider-filings.png` | `SEC Form 4 Insider-Filing Agent`; INPUT `SEC Form 4` -> AGENT `Form 4 Researcher` (`Classifies public filings`) -> AGENT `Trading & Risk` -> OUTPUT `Broker Order`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-19fed1db-3b6e-4333-964e-cfc27627ce98.png` | `77e2c2e3fff8dd46e29410695454a4dee232f46330d278c64267f91921765d01` |
| `ai-browser-research-showcase.png` | `Authenticated Browser Research Showcase`; INPUT `Authorized Portal` -> AGENT `Browser Researcher` (`Logs in and extracts evidence`) -> AGENT `Trading & Risk` -> OUTPUT `Broker Order` -> optional AGENT `Receipt Publisher`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-2df1b373-8b0d-4476-86e5-2677143593cd.png` | `e5b0ed490925cc2ce8b26ca804ab81e755db25051c2c1c1ef3828f43c5b687e0` |
| `ai-credit-spread.png` | `AI Credit Spread`; INPUT `Market + Option Chain` -> AGENT `Spread Researcher` (`Finds exact two-leg spread`) -> AGENT `Trading & Risk` -> OUTPUT `Broker Order`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-01251ac3-85e0-4e53-8e7c-3b6665e4bd3e.png` | `adb760ee1c4998277bda9a0f0f63dde4ac5850aef70bdb9ea3f8efee9f907509` |
| `ai-iron-condor.png` | `AI Iron Condor`; INPUT `Market + Option Chain` -> AGENT `Condor Researcher` (`Finds exact four-leg spread`) -> AGENT `Trading & Risk` -> OUTPUT `Broker Order`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-37628ed7-7186-4807-a63d-14868220d401.png` | `0637c691fb262847d2d05666e37d5c61784c49e1da94a2b4f547169aef20c7ae` |
| `ai-opening-range-breakout.png` | `AI Opening Range Breakout`; INPUT `Opening Range Data` -> AGENT `ORB Researcher` (`Builds signal evidence`) -> AGENT `Trading & Risk` -> OUTPUT `Broker Order`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-22b5bec0-ff43-4c7e-9431-631219757369.png` | `575666c971312cb44ae11e22817b32618d20289bf6d6c1597e7d93a10152c5fb` |
| `ai-vwap.png` | `AI VWAP Strategy`; INPUT `Price + Volume` -> AGENT `VWAP Researcher` (`Computes signal evidence`) -> AGENT `Trading & Risk` -> OUTPUT `Broker Order`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-7e90122b-21dd-4322-8e4a-22bdc9b690e8.png` | `20f5401380ed3fcd33773120ae2d3a08f196edf97e4056aaced7de1fb67d74f7` |
| `ai-spx-zero-dte-bear-call-team.png` | `Two-Agent SPX 0 DTE Bear Call Experiment`; INPUT `SPX + Option Chain` -> AGENT `SPX Researcher` -> AGENT `Trading & Risk` -> OUTPUT `Broker Order`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-9e0270c3-3480-497f-8d5c-e13f0091c461.png` | `1c428e544d2a46ec78c3c8185b4e17a865b2b28fb87ee334d3d624aaacf2f624` |
| `bill-ackman-concentrated.png` | `Bill Ackman Concentrated AI Trading Team`; AGENT `Quality Researcher` -> AGENT `Activist Bull` -> AGENT `Short Seller` -> AGENT `Portfolio Manager` -> OUTPUT `Broker Order`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-85ed0ff9-c6be-4325-b27a-4d8f4a68cbfa.png` | `53d8e7a3ea0e46bbd6b1e1d31f0dca3ee3da12a2c4b3ed095f8d409a90f991c9` |
| `bull-bear-large-cap-stocks.png` | `Bull/Bear Large-Cap Stocks AI Trading Team`; AGENT `Stock Researcher` -> AGENT `Bull Agent` -> AGENT `Bear Agent` -> AGENT `Trader & Risk` -> OUTPUT `Broker Order`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-2046543e-e4aa-4279-9eee-b7586b7c12aa.png` | `fccfd9e60612b2974f5a9164b59ad634cfd017363e8c511d29c2cdefae1c7486` |
| `bull-bear-leveraged-etf.png` | `Bull/Bear Leveraged ETF AI Trading Team`; AGENT `ETF Researcher` -> AGENT `Bull Agent` -> AGENT `Bear Agent` -> AGENT `Trader & Risk` -> OUTPUT `Broker Order`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-74b905a1-bc1e-44df-9806-43aac7ffce97.png` | `4e347a611f475d71c79aed720592b2d987c3a3f652b1d175dd42e7036f8807ad` |
| `warren-buffett-value.png` | `Warren Buffett Value AI Trading Team`; AGENT `Report Reader` -> AGENT `Valuation Skeptic` -> AGENT `Portfolio Manager` -> OUTPUT `Broker Order`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-d5263a3d-ff38-4d53-940b-3d01c949a214.png` | `d41083cf18d10e723b905b2baac2d6aebbf1076e1ea62c6dca19066a2bc87bf4` |
| `ray-dalio-idea-meritocracy.png` | `Ray Dalio Idea Meritocracy AI Trading Team`; parallel AGENT peers `Growth Agent`, `Inflation Agent`, and `Debt & Liquidity Agent` converge into AGENT `Disagreement Agent` -> AGENT `Trader & Risk` -> OUTPUT `Broker Orders`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-1bb817f3-18c4-47bc-a864-276c160ee5e8.png` | `53bf2289f7952b4db667466fffac25b85ac35441ad31bf86a46499c998126662` |
| `citadel-sector-pods.png` | `Citadel Sector Pods AI Trading Team`; five parallel AGENT peers `Technology & Comms`, `Financials`, `Healthcare`, `Energy`, and `Consumer` converge into AGENT `Risk Manager` -> AGENT `Portfolio Manager` -> OUTPUT `Broker Orders`. | `.codex/generated_images/01a0bb50-0e4b-7f02-b19b-59727f45df79/exec-4b799c7f-f72f-4e5e-87df-85e067014fec.png` | `dbc88c9a18a6a70ab9963851eae8e4f3e17c14706ce791644ab083b6f03e4509` |

Dimensions are generator-controlled and vary from 1536 x 1024 through 2073 x
758. They were not cropped, resized, or otherwise altered after generation.
