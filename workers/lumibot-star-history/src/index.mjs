const GITHUB_API = "https://api.github.com";
const REPOSITORY = "Lumiwealth/lumibot";
const CACHE_SECONDS = 300;

const THEMES = {
  light: {
    background: "#ffffff",
    border: "#d0d7de",
    grid: "#d8dee4",
    text: "#1f2328",
    muted: "#59636e",
    line: "#0969da",
    area: "#ddf4ff",
  },
  dark: {
    background: "#0d1117",
    border: "#30363d",
    grid: "#30363d",
    text: "#f0f6fc",
    muted: "#8b949e",
    line: "#58a6ff",
    area: "#13233a",
  },
};

function githubHeaders(token) {
  return {
    Accept: "application/vnd.github.star+json",
    Authorization: `Bearer ${token}`,
    "User-Agent": "lumibot-live-star-history",
    "X-GitHub-Api-Version": "2022-11-28",
  };
}

async function readJson(response, label) {
  if (!response.ok) {
    const body = (await response.text()).slice(0, 300);
    throw new Error(`${label} returned ${response.status}: ${body}`);
  }
  return response.json();
}

export function parseLastPage(linkHeader) {
  if (!linkHeader) return 1;
  for (const part of linkHeader.split(",")) {
    if (!/rel="last"/.test(part)) continue;
    const match = part.match(/[?&]page=(\d+)/);
    if (match) return Number.parseInt(match[1], 10);
  }
  return 1;
}

export async function fetchStarHistory(token, fetchImpl = fetch) {
  if (!token) throw new Error("GITHUB_TOKEN is not configured");

  const headers = githubHeaders(token);
  const [repoResponse, firstPageResponse] = await Promise.all([
    fetchImpl(`${GITHUB_API}/repos/${REPOSITORY}`, { headers }),
    fetchImpl(
      `${GITHUB_API}/repos/${REPOSITORY}/stargazers?per_page=100&page=1`,
      { headers },
    ),
  ]);

  const [repository, firstPage] = await Promise.all([
    readJson(repoResponse, "GitHub repository API"),
    readJson(firstPageResponse, "GitHub stargazers API page 1"),
  ]);

  const lastPage = parseLastPage(firstPageResponse.headers.get("link"));
  const remainingPages = await Promise.all(
    Array.from({ length: Math.max(0, lastPage - 1) }, async (_, index) => {
      const page = index + 2;
      const response = await fetchImpl(
        `${GITHUB_API}/repos/${REPOSITORY}/stargazers?per_page=100&page=${page}`,
        { headers },
      );
      return readJson(response, `GitHub stargazers API page ${page}`);
    }),
  );

  const starredAt = [firstPage, ...remainingPages]
    .flat()
    .map((entry) => entry.starred_at)
    .filter(Boolean)
    .map((value) => new Date(value).getTime())
    .filter(Number.isFinite)
    .sort((left, right) => left - right);

  return {
    repository: repository.full_name,
    repositoryUrl: repository.html_url,
    createdAt: repository.created_at,
    generatedAt: new Date().toISOString(),
    totalStars: repository.stargazers_count,
    starredAt,
  };
}

function formatCount(value) {
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(value >= 10_000_000 ? 0 : 1)}m`;
  }
  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(value >= 10_000 ? 0 : 1)}k`;
  }
  return String(Math.round(value));
}

function formatDate(timestamp) {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  }).format(new Date(timestamp));
}

function roundedMaximum(value) {
  if (value <= 10) return 10;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const normalized = value / magnitude;
  const step = normalized <= 2 ? 0.5 : normalized <= 5 ? 1 : 2;
  return Math.ceil(normalized / step) * step * magnitude;
}

function escapeXml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

export function buildChart(history, requestedTheme = "light") {
  const themeName = requestedTheme === "dark" ? "dark" : "light";
  const colors = THEMES[themeName];
  const width = 900;
  const height = 460;
  const margin = { top: 82, right: 38, bottom: 56, left: 72 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;

  const created = new Date(history.createdAt).getTime();
  const firstStar = history.starredAt[0] ?? created;
  const start = Math.min(created, firstStar);
  const generated = new Date(history.generatedAt).getTime();
  const lastStar = history.starredAt.at(-1) ?? start;
  const end = Math.max(generated, lastStar, start + 86_400_000);
  const yMaximum = roundedMaximum(Math.max(1, history.totalStars));

  const x = (timestamp) =>
    margin.left + ((timestamp - start) / (end - start)) * plotWidth;
  const y = (count) =>
    margin.top + plotHeight - (count / yMaximum) * plotHeight;

  const points = [[start, 0]];
  history.starredAt.forEach((timestamp, index) => {
    points.push([timestamp, index + 1]);
  });
  if (history.totalStars > history.starredAt.length) {
    points.push([lastStar, history.totalStars]);
  }
  points.push([end, history.totalStars]);

  const linePath = points
    .map(([timestamp, count], index) =>
      `${index === 0 ? "M" : "L"}${x(timestamp).toFixed(2)},${y(count).toFixed(2)}`,
    )
    .join(" ");
  const areaPath = `${linePath} L${x(end).toFixed(2)},${y(0).toFixed(2)} L${x(start).toFixed(2)},${y(0).toFixed(2)} Z`;

  const horizontalTicks = Array.from({ length: 5 }, (_, index) => {
    const ratio = index / 4;
    const value = yMaximum * (1 - ratio);
    const tickY = margin.top + plotHeight * ratio;
    return `<line x1="${margin.left}" y1="${tickY}" x2="${width - margin.right}" y2="${tickY}" stroke="${colors.grid}" stroke-width="1" />\n      <text x="${margin.left - 14}" y="${tickY + 5}" text-anchor="end" fill="${colors.muted}" font-size="14">${formatCount(value)}</text>`;
  }).join("\n      ");

  const verticalTicks = Array.from({ length: 5 }, (_, index) => {
    const ratio = index / 4;
    const timestamp = start + (end - start) * ratio;
    const tickX = margin.left + plotWidth * ratio;
    return `<line x1="${tickX}" y1="${margin.top}" x2="${tickX}" y2="${height - margin.bottom}" stroke="${colors.grid}" stroke-width="1" />\n      <text x="${tickX}" y="${height - margin.bottom + 30}" text-anchor="middle" fill="${colors.muted}" font-size="14">${escapeXml(formatDate(timestamp))}</text>`;
  }).join("\n      ");

  const updated = new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "UTC",
  }).format(new Date(history.generatedAt));

  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="title description">
  <title id="title">LumiBot live star history</title>
  <desc id="description">GitHub star growth for ${escapeXml(history.repository)}, currently ${history.totalStars} stars.</desc>
  <rect x="1" y="1" width="${width - 2}" height="${height - 2}" rx="16" fill="${colors.background}" stroke="${colors.border}" stroke-width="2" />
  <g font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Helvetica, Arial, sans-serif">
    <text x="${margin.left}" y="38" fill="${colors.text}" font-size="24" font-weight="700">LumiBot Project Growth</text>
    <text x="${margin.left}" y="62" fill="${colors.muted}" font-size="14">Live GitHub star history · refreshed ${escapeXml(updated)}</text>
    <g>
      ${horizontalTicks}
      ${verticalTicks}
    </g>
    <path d="${areaPath}" fill="${colors.area}" />
    <path d="${linePath}" fill="none" stroke="${colors.line}" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" />
    <circle cx="${x(end).toFixed(2)}" cy="${y(history.totalStars).toFixed(2)}" r="6" fill="${colors.line}" stroke="${colors.background}" stroke-width="3" />
    <text x="${x(end).toFixed(2)}" y="${Math.max(margin.top + 16, y(history.totalStars) - 14).toFixed(2)}" text-anchor="end" fill="${colors.text}" font-size="17" font-weight="700">${escapeXml(formatCount(history.totalStars))}</text>
  </g>
</svg>`;
}

function errorChart(message, requestedTheme) {
  const colors = THEMES[requestedTheme === "dark" ? "dark" : "light"];
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 900 180" role="img" aria-label="Live star history temporarily unavailable">
  <rect x="1" y="1" width="898" height="178" rx="16" fill="${colors.background}" stroke="${colors.border}" stroke-width="2" />
  <g font-family="-apple-system, BlinkMacSystemFont, Segoe UI, Helvetica, Arial, sans-serif">
    <text x="40" y="70" fill="${colors.text}" font-size="24" font-weight="700">LumiBot Project Growth</text>
    <text x="40" y="108" fill="${colors.muted}" font-size="16">Live GitHub data is temporarily unavailable. Refresh shortly.</text>
    <text x="40" y="140" fill="${colors.muted}" font-size="12">${escapeXml(message).slice(0, 120)}</text>
  </g>
</svg>`;
}

function svgResponse(svg, cacheControl) {
  return new Response(svg, {
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": cacheControl,
      "Content-Type": "image/svg+xml; charset=utf-8",
      "X-Content-Type-Options": "nosniff",
    },
  });
}

export default {
  async fetch(request, env, context) {
    const url = new URL(request.url);

    if (url.pathname === "/healthz") {
      return Response.json(
        { ok: true, repository: REPOSITORY, tokenConfigured: Boolean(env.GITHUB_TOKEN) },
        { headers: { "Cache-Control": "no-store" } },
      );
    }

    if (url.pathname !== "/chart.svg") {
      return new Response("Not found", { status: 404 });
    }

    const theme = url.searchParams.get("theme") === "dark" ? "dark" : "light";
    const cache = caches.default;
    const cacheKey = new Request(url.toString(), { method: "GET" });
    const cached = await cache.match(cacheKey);
    if (cached) return cached;

    try {
      const history = await fetchStarHistory(env.GITHUB_TOKEN);
      const response = svgResponse(
        buildChart(history, theme),
        `public, max-age=${CACHE_SECONDS}, s-maxage=${CACHE_SECONDS}, stale-while-revalidate=86400`,
      );
      context.waitUntil(cache.put(cacheKey, response.clone()));
      return response;
    } catch (error) {
      console.error("Unable to render live star history", error);
      return svgResponse(errorChart(error.message, theme), "no-store");
    }
  },
};
