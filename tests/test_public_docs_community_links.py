from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_readme_promotes_reddit_before_discord_with_brand_icons():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    reddit = "https://www.reddit.com/r/BotSpotTrade/"
    discord = "https://discord.gg/lumiwealth"

    assert "cdn.simpleicons.org/reddit" in readme
    assert "cdn.simpleicons.org/discord" in readme
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
    discord_label = "Discord Community <https://discord.gg/lumiwealth>"

    assert index.index(github_label) < index.index(reddit_label)
    assert index.index(reddit_label) < index.index(discord_label)
    assert 'html_title = "Lumibot"' in conf
    assert "bootstrap/css/bootstrap.css" not in conf
    assert "{% block htmltitle %}" in template


def test_homepage_image_precedes_supporting_intro_copy():
    index = (REPO_ROOT / "docsrc" / "index.rst").read_text(encoding="utf-8")

    image = ".. image:: ../docs/assets/home/lumibot_strategy_lifecycle_homepage.png"
    supporting_copy = ".. raw:: html\n   :file: _html/main.html"

    assert index.index(image) < index.index(supporting_copy)
