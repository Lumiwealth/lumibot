#!/usr/bin/env python3
"""Generate a README star-history SVG from GitHub stargazer timestamps."""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html import escape
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPOSITORY = "Lumiwealth/lumibot"
DEFAULT_OUTPUT = REPO_ROOT / "docs/assets/readme/star_history.svg"
GITHUB_API = "https://api.github.com"


@dataclass(frozen=True)
class Stargazer:
    starred_at: datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=DEFAULT_REPOSITORY, help="GitHub repository as owner/name")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="SVG output path")
    parser.add_argument("--token", default=None, help="GitHub token. Prefer GH_TOKEN or GITHUB_TOKEN.")
    return parser.parse_args()


def resolve_token(explicit_token: str | None) -> str | None:
    if explicit_token:
        return explicit_token
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if token:
        return token
    try:
        result = subprocess.run(
            ["gh", "auth", "token"],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def request_json(url: str, token: str | None) -> tuple[list[dict], str | None]:
    headers = {
        "Accept": "application/vnd.github.star+json",
        "User-Agent": "lumibot-star-history-chart",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            link = response.headers.get("Link")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API returned HTTP {exc.code}: {detail}") from exc

    return json.loads(body), next_link(link)


def next_link(link_header: str | None) -> str | None:
    if not link_header:
        return None
    for part in link_header.split(","):
        url_part, _, rel_part = part.strip().partition(";")
        if 'rel="next"' not in rel_part:
            continue
        return url_part.strip()[1:-1]
    return None


def fetch_stargazers(repository: str, token: str | None) -> list[Stargazer]:
    url = f"{GITHUB_API}/repos/{repository}/stargazers?per_page=100"
    stargazers: list[Stargazer] = []
    while url:
        page, url = request_json(url, token)
        for entry in page:
            starred_at = entry.get("starred_at")
            if not isinstance(starred_at, str):
                raise RuntimeError("GitHub response did not include starred_at timestamps")
            stargazers.append(Stargazer(datetime.fromisoformat(starred_at.replace("Z", "+00:00"))))
    stargazers.sort(key=lambda item: item.starred_at)
    return stargazers


def month_start(value: datetime) -> date:
    return date(value.year, value.month, 1)


def add_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def iter_months(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current = add_month(current)


def build_monthly_counts(stargazers: list[Stargazer]) -> list[tuple[date, int]]:
    if not stargazers:
        return []

    monthly_new = Counter(month_start(item.starred_at) for item in stargazers)
    start = min(monthly_new)
    end = month_start(datetime.now(timezone.utc))
    total = 0
    points: list[tuple[date, int]] = []
    for month in iter_months(start, end):
        total += monthly_new[month]
        points.append((month, total))
    return points


def nice_upper_bound(value: int) -> int:
    if value <= 10:
        return 10
    magnitude = 10 ** math.floor(math.log10(value))
    normalized = value / magnitude
    if normalized <= 2:
        nice = 2
    elif normalized <= 5:
        nice = 5
    else:
        nice = 10
    return int(nice * magnitude)


def x_for(index: int, total: int, left: int, width: int) -> float:
    if total <= 1:
        return left
    return left + (index / (total - 1)) * width


def y_for(value: int, max_value: int, top: int, height: int) -> float:
    return top + height - (value / max_value) * height


def format_count(value: int) -> str:
    if value >= 1000:
        return f"{value / 1000:.1f}k".replace(".0k", "k")
    return str(value)


def build_svg(repository: str, points: list[tuple[date, int]]) -> str:
    width = 900
    height = 420
    left = 80
    right = 36
    top = 70
    bottom = 70
    chart_width = width - left - right
    chart_height = height - top - bottom

    if not points:
        raise RuntimeError("Cannot build chart without stargazer data")

    max_value = nice_upper_bound(points[-1][1])
    path_points = [
        f"{x_for(index, len(points), left, chart_width):.1f},{y_for(count, max_value, top, chart_height):.1f}"
        for index, (_, count) in enumerate(points)
    ]

    area_points = " ".join(path_points)
    baseline = top + chart_height
    line_points = " ".join(path_points)
    latest_month, latest_count = points[-1]
    updated = datetime.now(timezone.utc).date().isoformat()

    y_ticks = [round(max_value * index / 4) for index in range(5)]
    y_grid = []
    for tick in y_ticks:
        y = y_for(tick, max_value, top, chart_height)
        y_grid.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{left + chart_width}" y2="{y:.1f}" stroke="#e5e7eb" />'
            f'<text x="{left - 14}" y="{y + 5:.1f}" text-anchor="end" fill="#6b7280" font-size="14">'
            f"{escape(format_count(tick))}</text>"
        )

    tick_count = min(6, len(points))
    x_grid = []
    if tick_count > 1:
        for tick_index in range(tick_count):
            point_index = round(tick_index * (len(points) - 1) / (tick_count - 1))
            month, _ = points[point_index]
            x = x_for(point_index, len(points), left, chart_width)
            x_grid.append(
                f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{baseline}" stroke="#f3f4f6" />'
                f'<text x="{x:.1f}" y="{baseline + 34}" text-anchor="middle" fill="#6b7280" font-size="14">'
                f"{month.strftime('%b %Y')}</text>"
            )

    latest_x = x_for(len(points) - 1, len(points), left, chart_width)
    latest_y = y_for(latest_count, max_value, top, chart_height)

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">
  <title id="title">{escape(repository)} star history</title>
  <desc id="desc">Cumulative GitHub stars for {escape(repository)} through {escape(latest_month.isoformat())}.</desc>
  <rect width="{width}" height="{height}" rx="16" fill="#ffffff" />
  <text x="{left}" y="34" fill="#111827" font-family="Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" font-size="24" font-weight="700">{escape(repository)} Star History</text>
  <text x="{left}" y="58" fill="#6b7280" font-family="Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" font-size="14">Updated {escape(updated)} - {escape(format_count(latest_count))} stars</text>
  <g font-family="Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif">
    {"".join(y_grid)}
    {"".join(x_grid)}
    <line x1="{left}" y1="{baseline}" x2="{left + chart_width}" y2="{baseline}" stroke="#d1d5db" />
    <line x1="{left}" y1="{top}" x2="{left}" y2="{baseline}" stroke="#d1d5db" />
    <polygon points="{left},{baseline} {area_points} {left + chart_width},{baseline}" fill="#dbeafe" opacity="0.75" />
    <polyline points="{line_points}" fill="none" stroke="#2563eb" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" />
    <circle cx="{latest_x:.1f}" cy="{latest_y:.1f}" r="6" fill="#2563eb" stroke="#ffffff" stroke-width="3" />
    <text x="{latest_x - 10:.1f}" y="{latest_y - 14:.1f}" text-anchor="end" fill="#111827" font-size="15" font-weight="700">{escape(format_count(latest_count))}</text>
  </g>
</svg>
"""


def main() -> int:
    args = parse_args()
    token = resolve_token(args.token)
    if not token:
        print(
            "GitHub authentication is required for stargazer timestamps. Set GH_TOKEN/GITHUB_TOKEN or run gh auth login.",
            file=sys.stderr,
        )
        return 1

    output = args.output
    if not output.is_absolute():
        output = REPO_ROOT / output

    stargazers = fetch_stargazers(args.repository, token)
    svg = build_svg(args.repository, build_monthly_counts(stargazers))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(svg, encoding="utf-8")
    print(f"Wrote {output.relative_to(REPO_ROOT)} with {len(stargazers)} stars.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
