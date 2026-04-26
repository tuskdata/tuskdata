#!/usr/bin/env bash
# Release driver — bumps versions, tags, pushes. Idempotent: if a repo
# has no changes since its last tag, it's left alone.
#
# Usage:
#   scripts/release.sh                          # dry-run
#   scripts/release.sh --apply                  # actually do it
#   scripts/release.sh --apply --core-only      # skip plugins
#   scripts/release.sh --apply --tuskdata-ref v0.3.2 --bi-ref v0.2.2
#
# What it does:
#   1. For each repo (TuskData + 4 plugins) checks if there are commits
#      since the last tag.
#   2. If yes, builds + tags + pushes (when --apply).
#   3. Prints the build-arg map you'd paste into Coolify so the next
#      Docker build picks up the new tags.

set -euo pipefail

ROOT="${SCRIPT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
PLUGINS_DIR="${PLUGINS_DIR:-$(cd "$ROOT/.." && pwd)/Tusk}"
DRY=1
CORE_ONLY=0

# Manual ref overrides (plain vars — keeps the script bash 3 compatible
# so it runs unchanged on stock macOS and Linux).
OVERRIDE_tuskdata=""
OVERRIDE_bi=""
OVERRIDE_ci=""
OVERRIDE_sec=""
OVERRIDE_cluster=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply) DRY=0 ;;
    --core-only) CORE_ONLY=1 ;;
    --tuskdata-ref) OVERRIDE_tuskdata="$2"; shift ;;
    --bi-ref)       OVERRIDE_bi="$2"; shift ;;
    --ci-ref)       OVERRIDE_ci="$2"; shift ;;
    --sec-ref)      OVERRIDE_sec="$2"; shift ;;
    --cluster-ref)  OVERRIDE_cluster="$2"; shift ;;
    *) echo "unknown flag: $1" >&2 ; exit 2 ;;
  esac
  shift
done

override_for() {
  case "$1" in
    tuskdata) printf '%s' "$OVERRIDE_tuskdata" ;;
    bi)       printf '%s' "$OVERRIDE_bi" ;;
    ci)       printf '%s' "$OVERRIDE_ci" ;;
    sec)      printf '%s' "$OVERRIDE_sec" ;;
    cluster)  printf '%s' "$OVERRIDE_cluster" ;;
  esac
}

run() {
  if [[ $DRY -eq 1 ]]; then
    echo "  [dry-run] $*"
  else
    eval "$@"
  fi
}

last_tag() {
  git -C "$1" describe --tags --abbrev=0 2>/dev/null || echo ""
}

has_changes_since_tag() {
  local repo="$1" tag="$2"
  if [[ -z "$tag" ]]; then return 0; fi
  local n
  n=$(git -C "$repo" rev-list --count "$tag..HEAD")
  [[ "$n" -gt 0 ]]
}

bump_patch() {
  # Takes a tag like v1.2.3, returns v1.2.4
  local tag="$1"
  local stripped="${tag#v}"
  local major="${stripped%%.*}"
  local rest="${stripped#*.}"
  local minor="${rest%%.*}"
  local patch="${rest#*.}"
  echo "v${major}.${minor}.$((patch + 1))"
}

process() {
  local label="$1" path="$2"
  if [[ ! -d "$path/.git" ]]; then
    echo "[$label] skipped (not a git repo at $path)"
    return
  fi
  local tag override
  tag=$(last_tag "$path")
  override="$(override_for "$label")"

  if [[ -n "$override" ]]; then
    echo "[$label] forced ref → $override"
    echo "$label=$override" >> /tmp/release-refs.txt
    return
  fi

  if [[ -z "$tag" ]]; then
    echo "[$label] no tags yet — manual first release required"
    return
  fi

  if has_changes_since_tag "$path" "$tag"; then
    local next
    next=$(bump_patch "$tag")
    echo "[$label] $tag → $next ($(git -C "$path" rev-list --count "$tag..HEAD") new commits)"
    if [[ $DRY -eq 0 ]]; then
      run "git -C '$path' tag -a '$next' -m '$next'"
      run "git -C '$path' push origin main"
      run "git -C '$path' push origin '$next'"
    fi
    echo "$label=$next" >> /tmp/release-refs.txt
  else
    echo "[$label] up to date at $tag"
    echo "$label=$tag" >> /tmp/release-refs.txt
  fi
}

: > /tmp/release-refs.txt
process "tuskdata"  "$ROOT"
if [[ $CORE_ONLY -eq 0 ]]; then
  process "bi"      "$PLUGINS_DIR/bi"
  process "ci"      "$PLUGINS_DIR/ci"
  process "sec"     "$PLUGINS_DIR/sec"
  process "cluster" "$PLUGINS_DIR/cluster"
fi

echo
echo "=== Build args for Coolify / .env ==="
while read -r line; do
  case "${line%%=*}" in
    tuskdata) ;;  # core repo — pin via Coolify branch, not build-arg
    bi)       echo "TUSK_BI_REF=${line#*=}" ;;
    ci)       echo "TUSK_CI_REF=${line#*=}" ;;
    sec)      echo "TUSK_SEC_REF=${line#*=}" ;;
    cluster)  echo "TUSK_CLUSTER_REF=${line#*=}" ;;
  esac
done < /tmp/release-refs.txt
echo

if [[ $DRY -eq 1 ]]; then
  echo "(dry-run — re-run with --apply to publish)"
fi
