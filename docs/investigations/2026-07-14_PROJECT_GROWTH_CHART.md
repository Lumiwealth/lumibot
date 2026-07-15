# Project Growth README Chart

Last updated: 2026-07-14

## Incident

The README's Project Growth section was blank because it embedded a live Star
History image backed by an encrypted GitHub access token. Once that credential
or the upstream request failed, GitHub's image proxy could not render the chart.

## Root cause

GitHub restricted stargazer-list access in July 2026 to repository admins and
collaborators. Star History's public chart service therefore introduced sealed
personal tokens for repository charts. The sealed token previously embedded in
the README expired or was revoked, and Star History's chart endpoint also began
returning request timeouts. Refreshing another long-lived personal token would
repeat the same failure mode.

## Repair

The README now embeds a live chart at:

```text
https://lumibot.lumiwealth.com/_static/star_history.svg
```

The documentation workflow generates that endpoint from authenticated GitHub
stargazer timestamps during every documentation deployment and on a daily
schedule. It uses the repository-scoped, short-lived GitHub Actions token rather
than a personal token. The generator is:

```text
python3 scripts/update_star_history_chart.py
```

The generated SVG contains only monthly aggregate counts. It contains no GitHub
usernames, access tokens, or individual stargazer records.

## Verification

- GitHub returned 1,811 stargazer timestamps with no missing timestamps.
- The generated SVG passed local SVG rendering and visual inspection.
- The README no longer contains `sealed_token` or a Star History image URL.
- The README keeps linking to Star History's interactive repository view while
  embedding the repository-owned, automatically refreshed SVG.

## Maintenance

The Documentation workflow refreshes and deploys the chart daily at 04:17 UTC.
It can also be refreshed immediately with the workflow's manual-dispatch action.
