#!/usr/bin/env bash
# Container entrypoint for TuskData.
#
# Runs as root just long enough to (a) make sure the persistent volume at
# $HOME is writable by the `tusk` user (UID 1000), and (b) drop privileges
# via gosu so the actual app process is unprivileged.
#
# Honors:
#   TUSK_UID            uid to run as (default: 1000, the baked-in `tusk` user)
#   TUSK_SKIP_CHOWN     if "1", skip chown (use when host already owns it)

set -euo pipefail

HOME_DIR="${HOME:-/var/lib/tusk}"
TARGET_UID="${TUSK_UID:-1000}"

if [[ "${TUSK_SKIP_CHOWN:-0}" != "1" ]] && [[ -d "$HOME_DIR" ]]; then
  current_uid="$(stat -c '%u' "$HOME_DIR" 2>/dev/null || echo "0")"
  if [[ "$current_uid" != "$TARGET_UID" ]]; then
    echo "[entrypoint] chown -R $TARGET_UID $HOME_DIR (was $current_uid)"
    chown -R "$TARGET_UID:$TARGET_UID" "$HOME_DIR"
  fi
fi

# If we're already non-root (e.g. Kubernetes runAsUser), just exec.
if [[ "$(id -u)" != "0" ]]; then
  exec "$@"
fi

exec gosu "$TARGET_UID:$TARGET_UID" "$@"
