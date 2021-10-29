# Steps to Generate Docs

## Prerequisites

All in terminal type

1. `pip install sphinx`
2. `pip install sphinx_rtd_theme`

## Creating New Docs

All in terminal type

1. `cd docsrc`
2. `sphinx-apidoc -o . ../lumibot`
3. `make github`

This will have generated new files in `docs/_build/html`. To see the docs, open `index.html`
