# LumiBot live star history

This Cloudflare Worker renders the live GitHub star history used by the main
README. It keeps the GitHub token out of the public repository and refreshes the
chart from GitHub's API every five minutes.

The token must be configured as the `GITHUB_TOKEN` Worker secret. Create a
fine-grained GitHub personal access token with these settings:

- Resource owner: `Lumiwealth`
- Repository access: only `Lumiwealth/lumibot`
- Metadata: read-only
- Contents: read and write
- Expiration: the longest period allowed by the organization

The resource owner matters. A token owned by a personal account can read the
public repository metadata but GitHub will reject the restricted stargazers
endpoint with `403 Resource not accessible by personal access token`.

```bash
npx wrangler secret put GITHUB_TOKEN
npx wrangler deploy
node --test test/*.test.mjs
```

After rotating the secret, verify the actual chart rather than relying only on
the configuration-only health response:

```bash
curl --fail --silent --show-error \
  "https://lumibot-star-history.lumiwealth.workers.dev/chart.svg?theme=light" \
  | grep -F '<title id="title">LumiBot live star history</title>'
```

Endpoints:

- `/chart.svg` renders the light chart.
- `/chart.svg?theme=dark` renders the dark chart.
- `/healthz` reports that the Worker and secret binding are configured. The
  scheduled `Star History chart health` workflow checks the rendered chart and
  catches invalid, expired, or unauthorized tokens.
