#!/bin/bash
# Download vendor assets for offline/production use
# Run: bash scripts/vendor.sh
set -e

VENDOR_DIR="src/tusk/studio/static/vendor"
mkdir -p "$VENDOR_DIR"

echo "Downloading vendor assets..."

# Alpine.js 3.14.9
echo "  Alpine.js..."
curl -sL -o "$VENDOR_DIR/alpine.min.js" \
  "https://cdn.jsdelivr.net/npm/alpinejs@3.14.9/dist/cdn.min.js"

# HTMX 2.0.4
echo "  HTMX..."
curl -sL -o "$VENDOR_DIR/htmx.min.js" \
  "https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js"

# Lucide Icons 0.469.0
echo "  Lucide..."
curl -sL -o "$VENDOR_DIR/lucide.min.js" \
  "https://unpkg.com/lucide@0.469.0/dist/umd/lucide.min.js"

# MapLibre GL 4.1.0
echo "  MapLibre GL..."
curl -sL -o "$VENDOR_DIR/maplibre-gl.js" \
  "https://unpkg.com/maplibre-gl@4.1.0/dist/maplibre-gl.js"
curl -sL -o "$VENDOR_DIR/maplibre-gl.css" \
  "https://unpkg.com/maplibre-gl@4.1.0/dist/maplibre-gl.css"

# proj4js 2.15.0
echo "  proj4js..."
curl -sL -o "$VENDOR_DIR/proj4.min.js" \
  "https://cdnjs.cloudflare.com/ajax/libs/proj4js/2.15.0/proj4.min.js"

# Dagre.js 1.1.4 (graph layout for pipeline canvas)
echo "  Dagre.js..."
curl -sL -o "$VENDOR_DIR/dagre.min.js" \
  "https://cdn.jsdelivr.net/npm/@dagrejs/dagre@1.1.4/dist/dagre.min.js"

# Fonts (Inter + JetBrains Mono) — CSS only, fonts loaded from Google
echo "  Fonts CSS..."
curl -sL -o "$VENDOR_DIR/fonts.css" \
  "https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&family=Inter:wght@400;500;600&display=swap"

echo ""
echo "Vendor assets downloaded to $VENDOR_DIR"
ls -lh "$VENDOR_DIR"
