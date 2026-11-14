"""People must be able to find LumiBot's free fundamental data.

The page existed but the sidebar called it something else, and neither the README nor the
docs homepage mentioned it. These tests fail if it drops off the entry pages again.
"""

import re
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
DOCSRC = ROOT / "docsrc"
URL = "https://lumibot.lumiwealth.com/fundamentals.html"


def test_sidebar_entry_matches_the_page_title():
    title = (DOCSRC / "fundamentals.rst").read_text(encoding="utf-8").splitlines()[0]
    entry = re.search(r"^\s+(.+?)\s+<fundamentals>\s*$", (DOCSRC / "index.rst").read_text(encoding="utf-8"), re.M)
    assert entry, "fundamentals page missing from the sidebar"
    assert title.startswith(entry.group(1)), (entry.group(1), title)


def test_homepage_body_links_fundamentals():
    index = (DOCSRC / "index.rst").read_text(encoding="utf-8")
    body = index[: index.index(".. toctree::")]
    assert "<fundamentals>`" in body


def test_readme_shows_fundamentals_with_an_example_and_botspot_pointers():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert URL in readme
    assert "self.fundamentals.get_metrics_table(" in readme
    section = readme[readme.index("## Screen stocks on fundamentals"):readme.index("## Run the same strategy with a paper broker")]
    links = [urlparse(link) for link in re.findall(r"\]\(([^)]+)\)", section)]
    assert any(link.hostname == "lumibot.lumiwealth.com" and link.path == "/botspot_mcp.html" for link in links)
    assert any(link.hostname == "botspot.trade" for link in links)


def test_every_localized_readme_links_fundamentals():
    for path in ROOT.glob("README.*.md"):
        assert URL in path.read_text(encoding="utf-8"), path.name
