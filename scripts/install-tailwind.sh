#!/bin/bash
# Install Tailwind CSS standalone CLI (no Node.js required)
# Detects platform automatically (macOS arm64/x64, Linux x64/arm64)
# Run: bash scripts/install-tailwind.sh
set -e

case "$(uname -s)-$(uname -m)" in
    Darwin-arm64)  PLATFORM="macos-arm64" ;;
    Darwin-x86_64) PLATFORM="macos-x64" ;;
    Linux-x86_64)  PLATFORM="linux-x64" ;;
    Linux-aarch64) PLATFORM="linux-arm64" ;;
    *) echo "Unsupported platform: $(uname -s)-$(uname -m)"; exit 1 ;;
esac

echo "Downloading Tailwind CSS CLI for $PLATFORM..."
curl -sLo scripts/tailwindcss \
  "https://github.com/tailwindlabs/tailwindcss/releases/latest/download/tailwindcss-${PLATFORM}"
chmod +x scripts/tailwindcss

echo "Tailwind CLI installed: scripts/tailwindcss"
scripts/tailwindcss --help 2>/dev/null | head -1 || echo "(binary ready)"
