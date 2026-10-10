"""Check the HTML that crawlers receive, including Sphinx extension output."""

import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BASE_URL = "https://lumibot.lumiwealth.com/"


class Head(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.meta = {}
        self.canonicals = []
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "meta":
            key = attrs.get("name") or attrs.get("property")
            self.meta.setdefault(key, []).append(attrs.get("content"))
        if tag == "link" and attrs.get("rel") == "canonical":
            self.canonicals.append(attrs["href"])


@pytest.fixture(scope="module")
def built_docs(tmp_path_factory):
    for dependency in ("sphinx", "furo", "sphinx_llms_txt", "sphinxext.opengraph"):
        pytest.importorskip(dependency)
    root = tmp_path_factory.mktemp("docs-seo")
    source = root / "docsrc"
    source.mkdir()
    shutil.copy(ROOT / "docsrc/conf.py", source / "conf.py")
    shutil.copytree(ROOT / "docsrc/_templates", source / "_templates")
    (source / "_extra").mkdir()
    (source / "_extra/old-guide.html").write_text('<meta name="robots" content="noindex">')
    (source / "_includes").mkdir()
    (source / "_includes/fragment.rst").write_text("Not a standalone page\n=====================\n")
    (source / "strategy_methods.orders").mkdir()
    (source / "strategy_methods.orders/reference.rst").write_text(
        "Order reference\n===============\n\nA reference without a manual description.\n"
    )
    (source / "index.rst").write_text(
        'Home\n====\n\n.. meta::\n   :description: Python & AI: "backtest first".\n\n'
        ".. toctree::\n\n   guide\n   strategy_methods.orders/reference\n"
    )
    (source / "guide.rst").write_text(
        "Guide\n=====\n\n.. meta::\n   :description: Research & test before trading.\n\n"
        "Body text should not replace the editor's description.\n"
    )
    output = root / "html"
    result = subprocess.run(
        [sys.executable, "-m", "sphinx", "-b", "html", str(source), str(output)],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return output


@pytest.mark.parametrize("page", ["index", "guide", "strategy_methods.orders/reference"])
def test_crawlers_receive_one_consistent_metadata_set(built_docs, page):
    head = Head((built_docs / f"{page}.html").read_text())
    expected_url = BASE_URL if page == "index" else f"{BASE_URL}{page}.html"
    assert head.canonicals == [expected_url]
    for key in ("description", "og:title", "og:description", "og:url", "og:image", "twitter:card"):
        assert len(head.meta.get(key, [])) == 1, (page, key, head.meta.get(key))
    assert head.meta["og:url"] == [expected_url]
    assert head.meta["og:description"] == head.meta["description"]
    assert head.meta["twitter:description"] == head.meta["description"]
    if page == "index":
        assert head.meta["description"] == ['Python & AI: "backtest first".']
        assert head.meta["og:image"] == [f"{BASE_URL}_images/benefit-hero.png"]
        assert head.meta["twitter:image"] == head.meta["og:image"]
    if page == "guide":
        assert head.meta["description"] == ["Research & test before trading."]


def test_sitemap_lists_only_built_canonical_documents(built_docs):
    sitemap = ET.parse(built_docs / "sitemap.xml")
    urls = [node.text for node in sitemap.findall(".//{*}loc")]
    assert sorted(urls) == sorted(
        [
            BASE_URL,
            f"{BASE_URL}guide.html",
            f"{BASE_URL}strategy_methods.orders/reference.html",
        ]
    )
