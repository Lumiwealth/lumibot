#!/usr/bin/env python3
"""Tell IndexNow search engines which LumiBot docs pages changed in a deploy.

IndexNow (https://www.indexnow.org/) is how Bing, Yandex, Seznam, Naver, Yep and
Amazon learn about new or changed pages within minutes instead of waiting for a
recrawl. Bing in turn feeds Microsoft Copilot, DuckDuckGo and parts of ChatGPT
search, so new docs pages reach AI answers sooner.

The key is public by design: it lives at
``https://lumibot.lumiwealth.com/<key>.txt`` (``docsrc/_extra/<key>.txt``) so the
engines can confirm we own the host.

Usage (the docs workflow runs this after GitHub Pages deploys)::

    git diff --name-status BEFORE AFTER -- docsrc | \
        python scripts/submit_docs_indexnow.py --changes -

    python scripts/submit_docs_indexnow.py --all --dry-run

Changed ``docsrc/<page>.rst`` files map to their page URLs, including deleted
pages (IndexNow accepts removed URLs so engines drop them). A change to shared
build inputs (``conf.py``, templates, static assets, data or includes) can touch
every page, so it submits every URL from the live sitemap.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

HOST = "lumibot.lumiwealth.com"
BASE_URL = f"https://{HOST}/"
ENDPOINT = "https://api.indexnow.org/indexnow"
DOCSRC = Path(__file__).resolve().parents[1] / "docsrc"
SHARED_PREFIXES = (
    "docsrc/conf.py",
    "docsrc/_templates/",
    "docsrc/_html/",
    "docsrc/_includes/",
    "docsrc/_data/",
    "docsrc/Makefile",
)
MAX_URLS = 10_000  # IndexNow limit per request


def find_key(extra_dir: Path = DOCSRC / "_extra") -> str:
    """Return the IndexNow key: the one 32-hex-character .txt file whose body is its own name."""
    keys = []
    for path in extra_dir.glob("*.txt"):
        stem = path.stem
        if len(stem) == 32 and all(c in "0123456789abcdef" for c in stem):
            if path.read_text(encoding="utf-8").strip() == stem:
                keys.append(stem)
    if len(keys) != 1:
        raise SystemExit(f"Expected exactly one IndexNow key file in {extra_dir}, found {keys}")
    return keys[0]


def page_url(rst_path: str) -> str | None:
    """Map ``docsrc/foo.bar.rst`` to its public URL, or None for non-page files."""
    if not rst_path.startswith("docsrc/") or not rst_path.endswith(".rst"):
        return None
    name = rst_path[len("docsrc/"):-len(".rst")]
    if name.startswith("_") or "/_" in name:
        return None
    return BASE_URL if name == "index" else f"{BASE_URL}{name}.html"


def parse_changes(lines) -> tuple[set[str], bool]:
    """Read ``git diff --name-status`` output. Returns (page URLs, shared input changed)."""
    urls, shared = set(), False
    for line in lines:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 2:
            continue
        for path in parts[1:]:  # renames list both the old and the new path
            if path.startswith(SHARED_PREFIXES):
                shared = True
            url = page_url(path)
            if url:
                urls.add(url)
    return urls, shared


def sitemap_urls(xml_text: str) -> list[str]:
    root = ET.fromstring(xml_text)
    return [node.text.strip() for node in root.iter() if node.tag.endswith("loc") and node.text]


def payload(urls, key: str) -> dict:
    return {
        "host": HOST,
        "key": key,
        "keyLocation": f"{BASE_URL}{key}.txt",
        "urlList": sorted(u for u in urls if u.startswith(BASE_URL))[:MAX_URLS],
    }


def _get(url: str) -> tuple[int, str]:
    request = urllib.request.Request(url, headers={"User-Agent": "lumibot-docs-indexnow"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        return error.code, ""


def wait_for_live_key(key: str, attempts: int = 30, pause: float = 10.0) -> None:
    """GitHub Pages can take a minute to publish; engines reject a key they cannot read."""
    for attempt in range(attempts):
        status, body = _get(f"{BASE_URL}{key}.txt?check={int(time.time())}")
        if status == 200 and body.strip() == key:
            return
        if attempt < attempts - 1:
            time.sleep(pause)
    raise SystemExit(f"IndexNow key file is not live at {BASE_URL}{key}.txt (last HTTP {status}).")


def submit(body: dict) -> int:
    data = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        ENDPOINT,
        data=data,
        headers={"Content-Type": "application/json; charset=utf-8", "User-Agent": "lumibot-docs-indexnow"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", "replace")[:500]
        raise SystemExit(f"IndexNow rejected the submission: HTTP {error.code} {detail}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--changes", help="File with `git diff --name-status` output, or - for stdin")
    group.add_argument("--all", action="store_true", help="Submit every URL in the live sitemap")
    parser.add_argument("--dry-run", action="store_true", help="Print the payload without sending it")
    args = parser.parse_args(argv)

    key = find_key()
    urls, shared = set(), bool(args.all)
    if args.changes:
        stream = sys.stdin if args.changes == "-" else open(args.changes, encoding="utf-8")
        with stream:
            urls, shared = parse_changes(stream)
    if shared:
        status, text = _get(f"{BASE_URL}sitemap.xml")
        if status != 200:
            raise SystemExit(f"Could not read the live sitemap (HTTP {status}).")
        urls |= set(sitemap_urls(text))
    if not urls:
        print("No docs page changed; nothing to submit to IndexNow.")
        return 0

    body = payload(urls, key)
    if args.dry_run:
        print(json.dumps(body, indent=2))
        return 0
    wait_for_live_key(key)
    status = submit(body)
    print(f"IndexNow accepted {len(body['urlList'])} URLs (HTTP {status}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
