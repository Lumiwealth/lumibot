# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath("../lumibot/"))
# sys.path.insert(0, os.path.abspath("../"))


# -- Project information -----------------------------------------------------

project = "Lumibot"
copyright = "2021, Lumiwealth"
author = "Lumiwealth Inc."

html_title = "Lumibot Documentation"

source_paths = ["lumibot.brokers", "backtesting"]


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
    "sphinx.ext.autodoc",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

html_theme_options = {
    "sidebar_hide_name": True,
    "light_logo": "Lumibot_Logo.webp",
    "dark_logo": "Lumibot_Logo.webp",
    'announcement': """
    <div class="footer-banner bg-warning text-dark p-3">
        <h5>Need Extra Help?</h5>
        <p>Visit <a href="https://www.lumiwealth.com/?utm_source=documentation&utm_medium=referral&utm_campaign=lumibot_footer_banner" target="_blank" class="text-dark"><strong>Lumiwealth</strong></a> for courses, community, and profitable pre-made trading bots.</p>
    </div>
    """
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_html"]
html_css_files = ["custom.css", "bootstrap/css/bootstrap.css"]

# html_theme_options = {
#     "announcement": """
#     <div class="important-note" style="margin-top: 20px; padding: 20px; background-color: #ffdd57; border-radius: 5px;">
#         <h3>Important!</h3>
#         <p>If you need extra help building your strategies and making them profitable, Lumiwealth has you covered. By visiting Lumiwealth, you not only learn how to use the Lumibot library but also gain access to a wealth of highly profitable algorithms.</p>
#         <p><strong>Our strategies have shown exceptional results, with some achieving over 100% annual returns and others reaching up to 1,000% in backtesting.</strong></p>
#         <p>Join our community of traders, take comprehensive courses, and access our library of profitable trading bots. Visit <a href="https://www.lumiwealth.com/?utm_source=documentation&utm_medium=referral&utm_campaign=lumibot_every_page" target="_blank">Lumiwealth</a> to learn more.</p>
#     </div>
#     """
# }

html_context = {
    'note': """
    <div class="important-note" style="margin-top: 20px; padding: 20px; background-color: #ffdd57; border-radius: 5px;">
        <h3>Important!</h3>
        <p>If you need extra help building your strategies and making them profitable, Lumiwealth has you covered. By visiting Lumiwealth, you not only learn how to use the Lumibot library but also gain access to a wealth of highly profitable algorithms.</p>
        <p><strong>Our strategies have shown exceptional results, with some achieving over 100% annual returns and others reaching up to 1,000% in backtesting.</strong></p>
        <p>Join our community of traders, take comprehensive courses, and access our library of profitable trading bots. Visit <a href="https://www.lumiwealth.com/?utm_source=documentation&utm_medium=referral&utm_campaign=lumibot_every_page" target="_blank">Lumiwealth</a> to learn more.</p>
    </div>
    """
}