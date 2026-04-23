.PHONY: vendor tailwind css build dev test e2e wheels image clean

PY      ?= .venv/bin/python
PLUGINS := /Users/jeasoft/Projects/Tusk/cluster \
           /Users/jeasoft/Projects/Tusk/sec \
           /Users/jeasoft/Projects/Tusk/bi \
           /Users/jeasoft/Projects/Tusk/ci

# Download vendor JS/CSS assets (Alpine, HTMX, Lucide, MapLibre, proj4)
vendor:
	bash scripts/vendor.sh

# Install Tailwind CSS standalone CLI (no Node.js)
tailwind:
	bash scripts/install-tailwind.sh

# Build production CSS with Tailwind
css:
	scripts/tailwindcss -i src/tusk/studio/static/styles.css \
	    -o src/tusk/studio/static/vendor/tailwind.min.css \
	    --content "src/tusk/studio/templates/**/*.html" --minify

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
