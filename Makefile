.PHONY: vendor tailwind css build dev test e2e wheels image clean

PY      ?= .venv/bin/python
PLUGINS := /Users/jeasoft/Projects/Tusk/cluster \
           /Users/jeasoft/Projects/Tusk/sec \
           /Users/jeasoft/Projects/Tusk/bi \
           /Users/jeasoft/Projects/Tusk/ci

# Download vendor JS/CSS assets (Alpine, HTMX, Lucide, MapLibre, proj4)
vendor:
	bash scripts/vendor.sh

# Tailwind 4 standalone CLI (no Node.js). Uses $TAILWINDCSS, else
# scripts/tailwindcss, else `tailwindcss` on PATH. `make tailwind` downloads it.
TAILWINDCSS ?= $(if $(wildcard scripts/tailwindcss),scripts/tailwindcss,tailwindcss)
tailwind:
	bash scripts/install-tailwind.sh

# Compile the CSS the UI ships (committed under static/vendor). Rerun after
# adding Tailwind classes to a template; tests/test_offline_assets.py fails
# when the compiled file lags the templates.
css:
	$(TAILWINDCSS) -i src/tusk/studio/static/tailwind.css \
	    -o src/tusk/studio/static/vendor/tailwind.min.css --minify

# Build wheel
build: vendor css
	uv build

# Run dev server (CDN mode)
dev:
	TUSK_CDN=1 .venv/bin/python -m tusk studio

# Run tests
test:
	PYTHONPATH=src $(PY) -m pytest tests/ -v --tb=short

# Run only the end-to-end HTTP suite
e2e:
	PYTHONPATH=src $(PY) -m pytest tests/test_e2e.py -v

# Build tuskdata + every plugin wheel into ./wheels/
wheels:
	mkdir -p wheels
	$(PY) -m build --wheel
	cp dist/tuskdata-*.whl wheels/
	@for p in $(PLUGINS); do \
		echo "==> building $$p" ; \
		( cd "$$p" && $(PY) -m build --wheel ) ; \
		cp "$$p"/dist/tusk_*-*.whl wheels/ ; \
	done
	@echo "==> wheels built:" ; ls -1 wheels/

# Build the production Docker image (needs wheels/)
image: wheels
	docker build -t tuskdata:local .

# Clean build artifacts
clean:
	rm -rf dist/ build/ *.egg-info
	rm -rf src/tusk/studio/static/vendor/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
