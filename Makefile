.PHONY: vendor tailwind css build dev test clean

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
	PYTHONPATH=src .venv/bin/pytest tests/ -v --tb=short

# Clean build artifacts
clean:
	rm -rf dist/ build/ *.egg-info
	rm -rf src/tusk/studio/static/vendor/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
