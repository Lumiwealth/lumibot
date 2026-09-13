# Partnerships and AI discovery

Small documentation improvements for developers and prospective partners.

Last Updated: 2026-09-08
Status: Source changes; public-site publication follows the normal independent docs process
Audience: LumiBot maintainers

## Overview

The README and documentation homepage now offer explicit backtest, AI, and
options routes while retaining existing examples and visuals. The new
`docsrc/PARTNERSHIPS.rst` page explains funded integration work, maintenance,
joint developer education, and confidential strategic collaboration. It uses
the package's existing public maintainer contact.

The sector-pod and idea-meritocracy pages link directly to the regular and
leveraged Data-On marketplace variants. They distinguish these variants from
the earlier example source and make no live-performance claims.

## License labels

The LICENSE file's GPLv3 text dates to commit `f99df933` (2021-01-12). The
README and setup.py MIT labels disagreed with that file. This change corrects
the labels without modifying LICENSE or claiming new commercial license rights.
Already-published package metadata is outside this source change.

## Search metadata and first run

The shared template uses page-specific descriptions for priority landing pages
and a title-based fallback for other reference pages. It escapes descriptions
before writing HTML attributes. Existing canonical URL behavior is unchanged.
The AI quick start adds prerequisites and a full daily-data research runner.
It explicitly explains model charges and the limits of historical LLM research.

## Verification scope

Build Sphinx locally and inspect generated titles, descriptions, internal links,
the partnership contact, and marketplace destinations. Use the existing public
documentation tests. No model-backed backtest or performance claim is implied
by syntax checks or a successful documentation build.

The GitHub funding configuration points to the partnership source page on the
active version branch, which becomes available with the source push. This
replaces the inactive GitHub Sponsors enrollment destination without depending
on public-site publication. Once the documentation page is live, use its
canonical site URL instead. GitHub Sponsors enrollment and any grant application
remain separate from this partnership information.
