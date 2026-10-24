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


# ---------------------------------------------------------------------------
# AI search discovery (2026-10-23): robots, sitemap lastmod, IndexNow, llms.txt
# Why: ChatGPT, Claude, Perplexity, Copilot and Cursor answer from search
# indexes (OpenAI's own crawl, Brave, Bing, Exa, Google). These checks keep the
# docs open to their crawlers and keep fresh pages flowing to them.
# ---------------------------------------------------------------------------

AI_CRAWLERS = (
    "Googlebot", "Bingbot", "Applebot", "OAI-SearchBot", "ChatGPT-User", "GPTBot",
    "Claude-SearchBot", "Claude-User", "ClaudeBot", "PerplexityBot", "Perplexity-User",
    "DuckAssistBot", "Meta-WebIndexer", "MistralAI-User",
)
GIT_IDENTITY = ("-c", "user.name=docs-test", "-c", "user.email=docs-test@example.invalid")


def _robots_groups(text):
    """Map each user-agent to the rules of its group."""
    groups, agents, rules = {}, [], []
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        field, value = (part.strip() for part in line.split(":", 1))
        if field.lower() == "user-agent":
            if rules:
                agents, rules = [], []
            agents.append(value.lower())
            groups[value.lower()] = rules
        elif agents:
            rules.append((field.lower(), value))
    return groups


def test_robots_allows_every_major_ai_and_search_crawler():
    text = (ROOT / "docsrc/_extra/robots.txt").read_text()
    groups = _robots_groups(text)
    for agent in ("*",) + AI_CRAWLERS:
        rules = groups.get(agent.lower())
        assert rules is not None, f"{agent} has no robots group"
        assert ("allow", "/") in rules, agent
        assert not any(field == "disallow" and value for field, value in rules), agent
    assert "Sitemap: https://lumibot.lumiwealth.com/sitemap.xml" in text


def _indexnow():
    sys.path.insert(0, str(ROOT / "scripts"))
    import submit_docs_indexnow

    return submit_docs_indexnow


def test_indexnow_key_file_is_published_and_self_describing():
    indexnow = _indexnow()
    key = indexnow.find_key()
    assert (ROOT / f"docsrc/_extra/{key}.txt").read_text().strip() == key
    body = indexnow.payload({f"{BASE_URL}a.html", "https://elsewhere.example/x"}, key)
    assert body["host"] == "lumibot.lumiwealth.com"
    assert body["keyLocation"] == f"{BASE_URL}{key}.txt"
    assert body["urlList"] == [f"{BASE_URL}a.html"]


def test_indexnow_maps_changed_and_deleted_pages_and_flags_shared_inputs():
    indexnow = _indexnow()
    urls, shared = indexnow.parse_changes([
        "A\tdocsrc/brokers.ccxt.bybit.rst\n",
        "D\tdocsrc/old_page.rst\n",
        "M\tdocsrc/index.rst\n",
        "R100\tdocsrc/a.rst\tdocsrc/b.rst\n",
        "M\tdocsrc/_includes/fragment.rst\n",
        "M\tdocsrc/_extra/robots.txt\n",
    ])
    assert urls == {
        f"{BASE_URL}brokers.ccxt.bybit.html",
        f"{BASE_URL}old_page.html",
        BASE_URL,
        f"{BASE_URL}a.html",
        f"{BASE_URL}b.html",
    }
    assert shared is True  # an include can change every page that uses it
    assert indexnow.parse_changes(["M\tdocsrc/guide.rst\n"]) == ({f"{BASE_URL}guide.html"}, False)
    sitemap = (
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url>'
        "<loc>https://lumibot.lumiwealth.com/</loc><lastmod>2026-10-01</lastmod></url></urlset>"
    )
    assert indexnow.sitemap_urls(sitemap) == [BASE_URL]


def test_docs_workflow_keeps_history_for_lastmod_and_pings_indexnow_after_deploy():
    workflow = (ROOT / ".github/workflows/docs.yml").read_text()
    assert "fetch-depth: 0" in workflow
    assert "needs: deploy" in workflow
    assert "scripts/submit_docs_indexnow.py" in workflow
    assert "make llms" in workflow  # llms.txt and llms-full.txt stay generated from the docs


def _load_config():
    import importlib.util

    spec = importlib.util.spec_from_file_location("lumibot_docs_conf_seo", ROOT / "docsrc/conf.py")
    module = importlib.util.module_from_spec(spec)
    original = sys.modules.copy()
    try:
        spec.loader.exec_module(module)
    finally:
        for name in set(sys.modules) - set(original):
            sys.modules.pop(name, None)
        sys.modules.update(original)
    return module


def test_every_exchange_page_is_a_built_document():
    """Exchange pages reach sitemap.xml and llms.txt only if Sphinx builds them."""
    from fnmatch import fnmatch

    config = _load_config()
    pages = sorted((ROOT / "docsrc").glob("brokers.ccxt.*.rst"))
    assert pages, "expected generated exchange pages"
    for page in pages:
        name = page.relative_to(ROOT / "docsrc").as_posix()
        assert not any(fnmatch(name, pattern) for pattern in config.exclude_patterns), name
    assert "sphinx_llms_txt" in config.extensions


def _git(repo, *args, date=None):
    import os

    env = dict(os.environ)
    if date:
        env.update(GIT_AUTHOR_DATE=date, GIT_COMMITTER_DATE=date)
    subprocess.run(["git", *GIT_IDENTITY, "-C", str(repo), *args], check=True, capture_output=True, env=env)


def test_sitemap_lastmod_uses_page_and_include_commit_dates(tmp_path):
    for dependency in ("sphinx", "furo", "sphinx_llms_txt", "sphinxext.opengraph"):
        pytest.importorskip(dependency)
    repo = tmp_path / "repo"
    source = repo / "docsrc"
    source.mkdir(parents=True)
    shutil.copy(ROOT / "docsrc/conf.py", source / "conf.py")
    shutil.copytree(ROOT / "docsrc/_templates", source / "_templates")
    (source / "_includes").mkdir()
    (source / "_includes/snippet.rst").write_text("Shared text.\n")
    (source / "index.rst").write_text("Home\n====\n\n.. toctree::\n\n   plain\n   uses_include\n   api\n")
    (source / "plain.rst").write_text("Plain\n=====\n\nText.\n")
    (source / "uses_include.rst").write_text("Uses\n====\n\n.. include:: _includes/snippet.rst\n")
    (source / "api.rst").write_text("API\n===\n\n.. automodule:: json\n")
    _git(repo, "init", "-q")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "one", date="2026-01-05T12:00:00+00:00")
    (source / "_includes/snippet.rst").write_text("Shared text, updated.\n")
    _git(repo, "commit", "-qam", "two", date="2026-03-09T23:30:00-05:00")  # 2026-03-10 in UTC
    (source / "untracked.rst").write_text("Untracked\n=========\n")

    output = tmp_path / "html"
    result = subprocess.run(
        [sys.executable, "-m", "sphinx", "-b", "html", "-q", str(source), str(output)],
        capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    entries = {}
    for url in ET.parse(output / "sitemap.xml").getroot():
        lastmod = url.find("{*}lastmod")
        entries[url.find("{*}loc").text] = lastmod.text if lastmod is not None else None
    assert entries[f"{BASE_URL}plain.html"] == "2026-01-05"
    assert entries[f"{BASE_URL}uses_include.html"] == "2026-03-10"  # include changed later
    assert entries[f"{BASE_URL}api.html"] is None  # autodoc: RST date is not honest
    assert entries[f"{BASE_URL}untracked.html"] is None  # no commit, no date


def test_shallow_clone_omits_lastmod_instead_of_inventing_freshness(tmp_path):
    config = _load_config()
    origin = tmp_path / "origin"
    (origin / "docsrc").mkdir(parents=True)
    (origin / "docsrc/page.rst").write_text("Page\n====\n")
    _git(origin, "init", "-q")
    _git(origin, "add", ".")
    _git(origin, "commit", "-qm", "a")
    (origin / "docsrc/page.rst").write_text("Page\n====\n\nMore.\n")
    _git(origin, "commit", "-qam", "b")
    clone = tmp_path / "clone"
    subprocess.run(["git", "clone", "-q", "--depth", "1", f"file://{origin}", str(clone)], check=True)
    assert config._git_commit_dates(clone) is None
    assert config._git_commit_dates(origin)["docsrc/page.rst"]


def test_bing_webmaster_verification_file_is_published():
    """Bing Webmaster Tools verifies lumibot.lumiwealth.com with this file (added 2026-10-23)."""
    text = (ROOT / "docsrc/_extra/BingSiteAuth.xml").read_text()
    assert "<user>4858E38F7A73B940D6A4FF548455514F</user>" in text
