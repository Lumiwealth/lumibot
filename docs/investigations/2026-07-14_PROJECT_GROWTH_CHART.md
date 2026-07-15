# Project Growth README Chart

Last updated: 2026-07-14

## Incident

The README's Project Growth section was blank because it embedded a live Star
History image backed by an encrypted GitHub access token. Once that credential
or the upstream request failed, GitHub's image proxy could not render the chart.

## Repair

The README now embeds the repository-owned asset at:

```text
docs/assets/readme/star_history.svg
```

The chart is generated from GitHub's authenticated stargazer timestamps by:

```text
python3 scripts/update_star_history_chart.py
```

The generated SVG contains only monthly aggregate counts. It contains no GitHub
usernames, access tokens, or individual stargazer records.

## Verification

- GitHub returned 1,811 stargazer timestamps with no missing timestamps.
- The generated SVG passed local SVG rendering and visual inspection.
- The README no longer contains `sealed_token` or a Star History image URL.

## Maintenance

Regenerate the asset when a release needs a refreshed Project Growth chart. The
README should continue to reference the checked-in asset so chart rendering is
not coupled to an external service or long-lived credential.
