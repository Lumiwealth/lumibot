# LumiBot live star history

This Cloudflare Worker renders the live GitHub star history used by the main
README. It keeps the GitHub token out of the public repository and refreshes the
chart from GitHub's API every five minutes.

The token must be configured as the `GITHUB_TOKEN` Worker secret. It only needs
read access to repository metadata for `Lumiwealth/lumibot`.

```bash
npx wrangler secret put GITHUB_TOKEN
npx wrangler deploy
node --test test/*.test.mjs
```

Endpoints:

- `/chart.svg` renders the light chart.
- `/chart.svg?theme=dark` renders the dark chart.
- `/healthz` reports service readiness without revealing the secret.
