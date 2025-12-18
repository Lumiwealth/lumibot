# Steps to Generate Docs

## Prerequisites

In terminal enter

1. `pip install sphinx`
2. `pip install furo`

## Creating New Docs

In terminal enter

1. `cd docsrc`
2. `make github`

This will generate new files in `generated-docs/`. To see the docs, open `generated-docs/index.html`.

Note: The canonical documentation site should be built and deployed by GitHub Actions (on `dev`),
so contributors typically should not commit generated HTML output.
