# Steps to Generate Docs

## Prerequisites

In terminal enter

1. Install the project and development dependencies in your virtual environment.
2. `python -m pip install sphinx furo sphinx-llms-txt sphinxext-opengraph`

## Creating New Docs

In terminal enter

1. `cd docsrc`
2. `make github`

This will generate new files in `generated-docs/`. To see the docs, open `generated-docs/index.html`.

Note: The canonical documentation site should be built and deployed by GitHub Actions (on `dev`),
so contributors typically should not commit generated HTML output.

For a local preview, run `make html` from `docsrc`, then serve `_build/html` with
`python -m http.server --bind 127.0.0.1 --directory _build/html`. The successful
HTML build writes `sitemap.xml` from Sphinx's document inventory. It includes
nested API pages, uses `/` for the homepage, and excludes copied redirects and
utility pages. Do not add a second sitemap under `_extra` or edit build output.

Page `.. meta::` descriptions feed both search snippets and social cards. The
Open Graph extension owns `og:*` tags; the theme template adds Twitter fields.
Run `python -m pytest -o addopts='' tests/docs/test_docs_seo.py` from the repository
root to check the resulting HTML and sitemap.
