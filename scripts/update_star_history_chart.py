#!/usr/bin/env python3
"""Generate the live README star-history SVG from authenticated GitHub data."""

from __future__ import annotations

import argparse
import calendar
import json
import math
import subprocess
from collections import Counter
from datetime import date, datetime, timezone
from html import escape
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPOSITORY = "Lumiwealth/lumibot"
DEFAULT_OUTPUT = REPO_ROOT / "docsrc/_html/star_history.svg"
DEFAULT_HISTORY = REPO_ROOT / "docs/data/star_history.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=DEFAULT_REPOSITORY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--history-file", type=Path, default=DEFAULT_HISTORY)
    parser.add_argument(
        "--refresh-history",
        action="store_true",
        help="Rebuild aggregate history from the restricted stargazer-list API.",
    )
    parser.add_argument(
        "--record-current",
        action="store_true",
        help="Append today's public repository star count to aggregate history.",
    )
    return parser.parse_args()


def fetch_starred_at(repository: str) -> list[datetime]:
    result = subprocess.run(
        [
            "gh",
            "api",
            "--paginate",
            "--slurp",
            "-H",
            "Accept: application/vnd.github.star+json",
            "-H",
            "X-GitHub-Api-Version: 2022-11-28",
            f"repos/{repository}/stargazers?per_page=100",
        ],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    pages = json.loads(result.stdout)
    values = [
        datetime.fromisoformat(entry["starred_at"].replace("Z", "+00:00"))
        for page in pages
        for entry in page
    ]
    if not values:
        raise RuntimeError("GitHub returned no stargazer timestamps")
    values.sort()
    return values


def fetch_current_count(repository: str) -> int:
    result = subprocess.run(
        ["gh", "api", f"repos/{repository}", "--jq", ".stargazers_count"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return int(result.stdout.strip())


def add_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def monthly_counts(values: list[datetime]) -> list[tuple[date, int]]:
    additions = Counter(date(value.year, value.month, 1) for value in values)
    current = min(additions)
    end = date(datetime.now(timezone.utc).year, datetime.now(timezone.utc).month, 1)
    total = 0
    points: list[tuple[date, int]] = []
    while current <= end:
        total += additions[current]
        today = datetime.now(timezone.utc).date()
        if current.year == today.year and current.month == today.month:
            point_date = today
        else:
            point_date = date(
                current.year,
                current.month,
                calendar.monthrange(current.year, current.month)[1],
            )
        points.append((point_date, total))
        current = add_month(current)
    return points


def load_history(path: Path, repository: str) -> list[tuple[date, int]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("repository") != repository:
        raise RuntimeError(
            f"History belongs to {payload.get('repository')}, not {repository}"
        )
    points = [
        (date.fromisoformat(point["date"]), int(point["count"]))
        for point in payload["points"]
    ]
    if not points:
        raise RuntimeError("Star history contains no aggregate points")
    return points


def write_history(
    path: Path, repository: str, points: list[tuple[date, int]]
) -> None:
    payload = {
        "repository": repository,
        "description": "Aggregate cumulative star counts; contains no user data.",
        "points": [
            {"date": point_date.isoformat(), "count": count}
            for point_date, count in points
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def record_current_count(
    points: list[tuple[date, int]], current_count: int
) -> list[tuple[date, int]]:
    today = datetime.now(timezone.utc).date()
    if points[-1][0] == today:
        points[-1] = (today, current_count)
    else:
        points.append((today, current_count))
    return points


def upper_bound(value: int) -> int:
    magnitude = 10 ** math.floor(math.log10(max(value, 1)))
    normalized = value / magnitude
    multiplier = 2 if normalized <= 2 else 5 if normalized <= 5 else 10
    return max(10, multiplier * magnitude)


def compact_count(value: int) -> str:
    if value >= 1000:
        return f"{value / 1000:.1f}k".replace(".0k", "k")
    return str(value)


def build_svg(repository: str, points: list[tuple[date, int]]) -> str:
    width, height = 900, 420
    left, right, top, bottom = 80, 36, 72, 70
    chart_width = width - left - right
    chart_height = height - top - bottom
    maximum = upper_bound(points[-1][1])

    first_ordinal = points[0][0].toordinal()
    last_ordinal = points[-1][0].toordinal()

    def x_at(point_date: date) -> float:
        elapsed = point_date.toordinal() - first_ordinal
        duration = max(1, last_ordinal - first_ordinal)
        return left + elapsed / duration * chart_width

    def y_at(value: int) -> float:
        return top + chart_height - value / maximum * chart_height

    line = " ".join(
        f"{x_at(point_date):.1f},{y_at(count):.1f}"
        for point_date, count in points
    )
    baseline = top + chart_height
    y_grid = []
    for index in range(5):
        tick = round(maximum * index / 4)
        y = y_at(tick)
        y_grid.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{left + chart_width}" y2="{y:.1f}" stroke="#dbe4ee" />'
            f'<text x="{left - 14}" y="{y + 5:.1f}" text-anchor="end" fill="#64748b" font-size="14">{compact_count(tick)}</text>'
        )

    x_grid = []
    for index in range(6):
        tick_ordinal = round(first_ordinal + index * (last_ordinal - first_ordinal) / 5)
        tick_date = date.fromordinal(tick_ordinal)
        x = x_at(tick_date)
        x_grid.append(
            f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{baseline}" stroke="#eef2f7" />'
            f'<text x="{x:.1f}" y="{baseline + 34}" text-anchor="middle" fill="#64748b" font-size="14">{tick_date.strftime("%b %Y")}</text>'
        )

    latest_date, latest_count = points[-1]
    latest_x = x_at(latest_date)
    latest_y = y_at(latest_count)
    updated = datetime.now(timezone.utc).date().isoformat()
    safe_repository = escape(repository)

    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">
  <title id="title">{safe_repository} star history</title>
  <desc id="desc">Cumulative GitHub stars for {safe_repository} through {latest_date.isoformat()}.</desc>
  <rect width="{width}" height="{height}" rx="16" fill="#ffffff" stroke="#dbe4ee" />
  <text x="{left}" y="34" fill="#0f172a" font-family="Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" font-size="24" font-weight="700">LumiBot Project Growth</text>
  <text x="{left}" y="59" fill="#64748b" font-family="Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" font-size="14">Updated {updated} from GitHub data - {compact_count(latest_count)} stars</text>
  <g font-family="Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif">
    {''.join(y_grid)}
    {''.join(x_grid)}
    <polygon points="{left},{baseline} {line} {left + chart_width},{baseline}" fill="#dbeafe" opacity="0.8" />
    <polyline points="{line}" fill="none" stroke="#2563eb" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" />
    <circle cx="{latest_x:.1f}" cy="{latest_y:.1f}" r="6" fill="#2563eb" stroke="#ffffff" stroke-width="3" />
    <text x="{latest_x - 10:.1f}" y="{latest_y - 14:.1f}" text-anchor="end" fill="#0f172a" font-size="15" font-weight="700">{compact_count(latest_count)}</text>
  </g>
</svg>
'''


def main() -> int:
    args = parse_args()
    output = args.output if args.output.is_absolute() else REPO_ROOT / args.output
    history_file = (
        args.history_file
        if args.history_file.is_absolute()
        else REPO_ROOT / args.history_file
    )
    if args.refresh_history:
        values = fetch_starred_at(args.repository)
        points = monthly_counts(values)
        write_history(history_file, args.repository, points)
    else:
        points = load_history(history_file, args.repository)

    if args.record_current:
        points = record_current_count(points, fetch_current_count(args.repository))
        write_history(history_file, args.repository, points)

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(build_svg(args.repository, points), encoding="utf-8")
    print(
        f"Wrote {output.relative_to(REPO_ROOT)} through {points[-1][0]} "
        f"with {points[-1][1]} stars"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
