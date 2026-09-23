import hashlib
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


def test_homepage_keeps_a_short_hero_and_places_runner_before_image():
    index = (REPO_ROOT / "docsrc" / "index.rst").read_text(encoding="utf-8")

    # September 13 user correction: AI trading pitch, compact image, task routes.
    assert index.startswith("LumiBot AI Trading\n==================")
    assert "Build AI-powered trading strategies in Python." in index
    assert ":width: 640px" in index
    assert index.index("python -m lumibot.example_strategies.ai_researcher_trader") < index.index("benefit-hero.png")
    assert index.index(".. _first-python-backtest:") < index.index("Want help building your first AI trading bot?")


def test_docs_community_icons_are_local_static_assets():
    community = (
        REPO_ROOT / "docsrc" / "_html" / "community_links.html"
    ).read_text(encoding="utf-8")

    assert "cdn.simpleicons.org" not in community
    for name in ("github", "reddit", "discord"):
        assert f'_static/{name}.svg' in community
        assert (REPO_ROOT / "docs" / "assets" / "community" / f"{name}.svg").is_file()


def test_ai_gallery_uses_verified_public_listings():
    pages = [
        "agents_examples.rst",
        "agents_example_citadel_sector_pods.rst",
        "agents_example_ray_dalio_idea_meritocracy.rst",
    ]
    text = "\n".join((REPO_ROOT / "docsrc" / page).read_text() for page in pages)
    # September 20 read-only production audit: these are the approved regular
    # and leveraged listings whose published main.py files own the docs source.
    for listing_id in (
        "4fb6cf2f-272c-4a73-96e7-edd7383b1a33",
        "da83818b-f994-4163-8ef3-99ea346325b4",
        "b00c5f9c-beea-46fe-bdba-fc65c1315d5f",
        "362a50a1-d501-4b08-8d42-c7701a363731",
    ):
        assert listing_id in text


def test_ray_and_citadel_examples_match_published_botspot_sources():
    # These files are the source the four listings above publish as main.py.
    # September 23: the default moved to GPT-6 Luna, so the listings must be
    # republished from exactly these bytes; the September 20 Gemini revisions
    # are superseded.
    expected = {
        "ai_trading_team_ray_dalio_idea_meritocracy.py": (
            "cdf995d11fe147ff44e93c89003ae559b680c16f5577030e126e348d32792950"
        ),
        "ai_trading_team_ray_dalio_idea_meritocracy_leveraged.py": (
            "40aa0c50be129442f91adf84d6a9aad3dfb5d0d7b5bbe542f6829785546bb10e"
        ),
        "ai_trading_team_citadel_sector_pods.py": (
            "083286662fdbe9cc41b1f82e1336f75996388feba4eb825ec252f57ba037d64f"
        ),
        "ai_trading_team_citadel_sector_pods_leveraged.py": (
            "401b6454166828894aa1d6ea506fdc73e15de062c7ea485746efe27dc429d10b"
        ),
    }
    root = REPO_ROOT / "lumibot" / "example_strategies"
    for filename, expected_sha256 in expected.items():
        assert hashlib.sha256((root / filename).read_bytes()).hexdigest() == expected_sha256
