from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_readme_promotes_reddit_before_discord_without_linking_to_itself():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    reddit = "https://www.reddit.com/r/BotSpotTrade/"
    discord = "https://discord.gg/4R9j6T3PN8"

    assert "cdn.simpleicons.org" not in readme
    assert "docs/assets/community/github.svg" not in readme
    assert "> GitHub</a>" not in readme
    assert "docs/assets/community/reddit.svg" in readme
    assert "docs/assets/community/discord.svg" in readme
    assert reddit in readme
    assert discord in readme
    assert readme.index(reddit) < readme.index(discord)


def test_docs_navigation_and_mobile_brand_stay_compact():
    index = (REPO_ROOT / "docsrc" / "index.rst").read_text(encoding="utf-8")
    conf = (REPO_ROOT / "docsrc" / "conf.py").read_text(encoding="utf-8")
    template = (
        REPO_ROOT / "docsrc" / "_templates" / "base.html"
    ).read_text(encoding="utf-8")

    github_label = "GitHub <https://github.com/Lumiwealth/lumibot>"
    reddit_label = (
        "Reddit Community <https://www.reddit.com/r/BotSpotTrade/>"
    )
    discord_label = "Discord Community <https://discord.gg/4R9j6T3PN8>"

    assert index.index(github_label) < index.index(reddit_label)
    assert index.index(reddit_label) < index.index(discord_label)
    assert 'html_title = "Lumibot"' in conf
    assert "bootstrap/css/bootstrap.css" not in conf
    assert "{% block htmltitle %}" in template
    assert "Lumibot: Python Algorithmic Trading and AI Agents" in template


def test_homepage_keeps_a_short_hero_and_places_image_above_supporting_copy():
    index = (REPO_ROOT / "docsrc" / "index.rst").read_text(encoding="utf-8")

    # September 13 user correction: AI trading pitch, compact image, task routes.
    assert index.startswith("LumiBot AI Trading\n==================")
    assert "Build AI-powered trading strategies in Python." in index
    assert ":width: 560px" in index
    assert index.index("Start the AI quickstart") < index.index("ai-trading-hero.png")
    assert index.index(".. _first-python-backtest:") < index.index("learn_with_rob.rst")


def test_docs_community_icons_are_local_static_assets():
    community = (
        REPO_ROOT / "docsrc" / "_html" / "community_links.html"
    ).read_text(encoding="utf-8")

    assert "cdn.simpleicons.org" not in community
    for name in ("github", "reddit", "discord"):
        assert f'_static/{name}.svg' in community
        assert (REPO_ROOT / "docs" / "assets" / "community" / f"{name}.svg").is_file()


def test_ai_gallery_uses_verified_public_listings():
    pages = ["agents_examples.rst", "agents_example_citadel_sector_pods.rst", "agents_example_ray_dalio_idea_meritocracy.rst"]
    text = "\n".join((REPO_ROOT / "docsrc" / page).read_text() for page in pages)
    # September 12 owner/public MCP audit: these four former listings return not_found.
    for missing in ("4fb6cf2f-272c-4a73-96e7-edd7383b1a33", "da83818b-f994-4163-8ef3-99ea346325b4", "b00c5f9c-beea-46fe-bdba-fc65c1315d5f", "362a50a1-d501-4b08-8d42-c7701a363731"):
        assert missing not in text
    assert "0b4576c7-f78b-4477-ba3a-630758fb0168" in text
    assert "81af73b8-7dec-4941-ba35-d5a06fee6863" in text
