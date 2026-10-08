# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?= -a -E
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

 github:
	@make html
	@rm -rf ../generated-docs
	@mkdir -p ../generated-docs
	@cp -a _build/html/. ../generated-docs

# Generate llms-full.txt with COMPILED documentation (resolved autodoc directives)
# This uses Sphinx's text builder which extracts actual docstrings
llms-full:
	@echo "Building text output with resolved autodoc..."
	@$(SPHINXBUILD) -b text "$(SOURCEDIR)" "$(BUILDDIR)/text" $(SPHINXOPTS)
	@echo "Concatenating text files into llms-full.txt..."
	@echo "# Lumibot Documentation (Compiled)" > ../llms-full.txt
	@echo "# Generated: $$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> ../llms-full.txt
	@echo "" >> ../llms-full.txt
	@find _build/text -name "*.txt" -type f | sort | while read f; do \
		echo "" >> ../llms-full.txt; \
		echo "===== $$(basename $$f .txt) =====" >> ../llms-full.txt; \
		echo "" >> ../llms-full.txt; \
		sed 's/[[:blank:]]*$$//' "$$f" >> ../llms-full.txt; \
	done
	@echo "llms-full.txt generated at repo root ($$(wc -c < ../llms-full.txt | tr -d ' ') bytes)"

# Generate llms.txt as a proper index with critical rules and quick reference
llms-index:
	@echo "Generating llms.txt index..."
	@python3 generate_llms_txt.py

# Build HTML, generate compiled llms-full.txt, and generate index
# This is the target GitHub Actions should use
llms: html llms-full llms-index
	@echo "Docs built and llms files updated"

# Alias for backwards compatibility
html-llms: llms
