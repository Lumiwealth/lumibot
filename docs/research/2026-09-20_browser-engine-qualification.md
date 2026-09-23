# Stateful browser engine qualification

Date: 2026-09-20

## Decision

No engine is approved as the hosted default yet.

- Patchright 1.62.3 is fast and fits the current memory shape locally, but its
  headless fingerprint exposed `HeadlessChrome` and zero plugins. It fails the
  mandatory anti-detection acceptance criterion.
- Camoufox 0.5.6 passed the same basic fingerprint probe and completed the
  restart soak, but peaked at 1,449,820,160 bytes (about 1.35 GiB). It cannot fit
  the currently running 1 GiB Fargate task shape.
- Browser Use informed the session/profile/tool design. It is an orchestration
  layer and optional hosted browser service, not a third local rendering engine;
  nesting its agent runtime inside LumiBot would duplicate the existing LumiBot
  agent runtime.

The remaining launch gate is the complete acceptance suite in the exact Linux
ARM64 Bot Manager image. No AWS task, task definition, service, or paid browser
resource was created during this qualification.

## Local macOS ARM64 bakeoff

| Criterion | Patchright | Camoufox |
|---|---:|---:|
| Package | 1.62.3, Apache-2.0 | 0.5.6, MPL-2.0 |
| Restart soak | 100/100 | 100/100 |
| Crash rate | 0% | 0% |
| Open p50 | 0.270739 s | 1.415180 s |
| Open p95 | 0.280367 s | 1.453151 s |
| Peak process-tree RSS | 465,911,808 B | 1,449,820,160 B |
| `navigator.webdriver` | false | false |
| User agent | exposed `HeadlessChrome` | normal Firefox 152 string |
| Plugins | 0 | 5 |
| Basic anti-detection verdict | fail | pass |

Raw benchmark artifacts:

- `artifacts/browser_benchmark/local-macos-arm64-patchright.json`
  SHA-256 `87d42ed64a8037450c487293d6432fdf9ef888f91b040d3040290040e90ce547`
- `artifacts/browser_benchmark/local-macos-arm64-camoufox.json`
  SHA-256 `97e024aad19e4e677d8fca28775f4ff1ef3477c2099e8572157a415ad6bb97cd`

The local extracted browser caches measured approximately 554 MiB for the
Patchright Chromium/headless-shell/ffmpeg set used by the benchmark and 657 MiB
for Camoufox. Those are local filesystem measurements, not compressed container
layer sizes; exact image-size impact remains part of the Linux ARM64 gate.

## Current hosted runtime

The hosted runtime is a mix of shared EC2 hosts and Fargate tasks with small
CPU and memory shapes. Exact inventory and task sizes are kept in private
BotSpot operations notes.

This proves production is not "all Fargate" and that a browser cannot be added
indiscriminately to every current workload.

## Cost boundary

AWS's current US East (N. Virginia) Linux/ARM example price is
`$0.0000089944` per vCPU-second and `$0.0000009889` per GB-second. At 730 hours
per month:

- Current 0.5 vCPU / 1 GiB continuously running task: about `$14.42/month`.
- Smallest plausible Camoufox candidate, 0.5 vCPU / 2 GiB: about
  `$17.02/month`.
- Incremental memory cost versus the current task shape: about `$2.60/month`
  for every task left running continuously, or `$0.00356` per task-hour.

These figures exclude logs, data transfer, public IPv4, and any extra storage.
They are an estimate, not spending authorization. A browser-capable workload
must remain opt-in and default-off. Rollback is to stop launching that workload
class and return new launches to the current non-browser task definition; no
existing customer task needs to be changed.

Sources:

- https://aws.amazon.com/fargate/pricing/
- https://github.com/Kaliiiiiiiiii-Vinyzu/patchright
- https://github.com/daijro/camoufox
- https://github.com/browser-use/browser-use
