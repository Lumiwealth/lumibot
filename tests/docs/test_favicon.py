"""The documentation tab icon must keep its transparent circular silhouette."""

import ast
from html.parser import HTMLParser
from pathlib import Path

from jinja2 import DictLoader, Environment
from PIL import Image


ROOT = Path(__file__).resolve().parents[2]


def test_configured_favicon_has_transparent_corners():
    config = ast.parse((ROOT / "docsrc/conf.py").read_text())
    favicon = next(
        ast.literal_eval(node.value)
        for node in config.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "html_favicon"
                for target in node.targets)
    )
    with Image.open(ROOT / "docsrc" / favicon) as icon:
        assert icon.mode == "RGBA", "An opaque favicon creates a white box on dark tabs"
        width, height = icon.size
        for corner in ((0, 0), (width - 1, 0), (0, height - 1), (width - 1, height - 1)):
            assert icon.getpixel(corner)[3] == 0
        assert icon.getpixel((width // 2, height // 2))[3] == 255


def test_template_icon_links_use_the_configured_favicon():
    # The custom head formerly overrode Sphinx with two opaque PNG icons.
    template = (ROOT / "docsrc/_templates/base.html").read_text()
    env = Environment(loader=DictLoader({
        "base.html": template,
        "!base.html": "{% block extrahead %}{% endblock %}",
    }))
    expected = "_static/configured-transparent-favicon.png"
    html = env.get_template("base.html").render(
        favicon_url=expected, pathto=lambda path, *args: path,
        pagename="index", master_doc="index", title="LumiBot", metatags="",
    )

    class IconLinks(HTMLParser):
        def __init__(self):
            super().__init__()
            self.links = []

        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if tag == "link" and attrs.get("rel") in ("icon", "apple-touch-icon"):
                self.links.append(attrs)

    parser = IconLinks()
    parser.feed(html)
    assert {link["rel"] for link in parser.links} == {"icon", "apple-touch-icon"}
    assert all(link["href"] == expected for link in parser.links)
