# Documentation favicon
Transparent circular artwork for browser tabs.

Last Updated: 2026-10-09
Status: Active
Audience: Documentation maintainers

## Overview

`docsrc/conf.py` uses `_html/lumibot_favicon_transparent.png`, the approved
transparent mascot badge, unchanged. The distinct filename lets browsers fetch
the replacement instead of reusing the old opaque icon from their favicon cache.

Keep the PNG's alpha channel and transparent corners. The regression check is
`tests/docs/test_favicon.py`. The custom HTML head uses Sphinx's `favicon_url`
for both icon and touch-icon links so it cannot override the configured artwork
with the old opaque files. Publish through the normal documentation workflow;
committing the source does not update the hosted documentation.
