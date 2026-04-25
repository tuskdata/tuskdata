# TuskData — production image
#
# Two-stage build:
#   builder  → install dependencies + plugin wheels with uv
#   runtime  → slim image with just python3.12 + app code
#
# Expects the following build context layout:
#   Dockerfile
#   pyproject.toml, src/, README.md, LICENSE
#   wheels/                 # tuskdata + plugin wheels (built via `make wheels`)
#
# Runtime env:
#   TUSK_DEBUG              0|1
#   TUSK_LOG_LEVEL          debug|info|warning|error|critical
#   TUSK_LOG_FORMAT         console|json
#   TUSK_QUERY_TIMEOUT      seconds, default 300
#   TUSK_CLUSTER_SECRET     for tusk-cluster worker auth
#   TUSK_CLUSTER_TLS        1 to dial workers over grpc+tls
#   HOME                    overrides where ~/.tusk lives (use a volume!)
#
# Default port: 8000. Docker healthcheck hits /api/health.

FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_SYSTEM_PYTHON=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential git curl \
      libpq-dev postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY src/ ./src/
COPY wheels/ ./wheels/

# Build the TuskData wheel itself, then install tuskdata[all] + every plugin
# wheel from the wheels/ directory into /opt/tusk-venv. The plugin wheels
# ship their own dependencies but reference tuskdata via `>=` so we install
# them with --no-deps once the core is present.
#
# fail-fast: the install loop exits non-zero on the first failure. Earlier
# revisions had `|| true`, which masked silent plugin breakage and only
# surfaced when the tab didn't appear at runtime.
RUN uv venv /opt/tusk-venv \
    && uv pip install --python /opt/tusk-venv/bin/python --no-cache \
         "tuskdata[all] @ ." \
    && set -e \
    && for w in wheels/tusk_*-*.whl; do \
         echo "[plugin install] $w"; \
         uv pip install --python /opt/tusk-venv/bin/python --no-cache --no-deps "$w"; \
       done


FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/tusk-venv/bin:${PATH}" \
    HOME=/var/lib/tusk

# PostgreSQL client tools are needed at runtime for pg_dump / psql / pg_restore.
RUN apt-get update && apt-get install -y --no-install-recommends \
      postgresql-client libpq5 curl gosu \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --create-home --home-dir /var/lib/tusk --shell /usr/sbin/nologin tusk

COPY --from=builder /opt/tusk-venv /opt/tusk-venv
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint
RUN chmod +x /usr/local/bin/docker-entrypoint

# Stay as root for the entrypoint, which fixes volume ownership before
# dropping privileges to `tusk` via gosu. Coolify and friends often mount
# bind volumes with the host user's UID; running directly as `tusk` would
# fail with EACCES on `~/.tusk/.key` and the chmod silencer would hide it.
WORKDIR /var/lib/tusk

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=20s \
  CMD curl -fsS http://127.0.0.1:8000/api/health | grep -q '"status":"ok"' || exit 1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint"]

# Default command: serve Studio with Granian. Override to run workers,
# scheduler, or CLI tasks. For reverse proxies (Coolify), set
# `--host 0.0.0.0` so the health probe on localhost still reaches us.
CMD ["tusk", "studio", "--host", "0.0.0.0", "--port", "8000"]
