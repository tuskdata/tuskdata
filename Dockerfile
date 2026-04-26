# syntax=docker/dockerfile:1.7
#
# TuskData — production image
#
# Two-stage build. The builder stage clones plugin repos at the pinned refs
# (build args) and builds wheels for them on the fly, so the deployment
# repo only contains TuskData itself. No `make wheels` step required.
#
# Build secrets:
#   --secret id=gh_token,src=/path/to/pat   (HTTPS clone for private repos)
#   --ssh default                           (SSH clone with your agent)
# If neither is supplied, falls back to public HTTPS clone.
#
# Build args (override per-deploy in Coolify):
#   TUSK_BI_REF        e.g. v0.2.1
#   TUSK_CI_REF        e.g. v0.2.0
#   TUSK_SEC_REF       e.g. v0.2.0
#   TUSK_CLUSTER_REF   e.g. v0.2.1
#
# Runtime env:
#   TUSK_DEBUG              0|1
#   TUSK_LOG_LEVEL          debug|info|warning|error|critical
#   TUSK_LOG_FORMAT         console|json
#   TUSK_QUERY_TIMEOUT      seconds, default 300
#   TUSK_CLUSTER_SECRET     for tusk-cluster worker auth
#   TUSK_CLUSTER_TLS        1 to dial workers over grpc+tls
#   TUSK_PORT               internal listen port, default 8000
#   HOME                    where ~/.tusk lives (use a volume!)
#
# Default port: 8000 (host port mapped via Coolify or compose).
# Healthcheck hits /api/health — passes when status="ok".

FROM python:3.13-slim AS builder

ARG TUSK_BI_REF=v0.2.1
ARG TUSK_CI_REF=v0.2.0
ARG TUSK_SEC_REF=v0.2.0
ARG TUSK_CLUSTER_REF=v0.2.1
ARG TUSK_PLUGINS_ORG=tuskdata

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_SYSTEM_PYTHON=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential git curl ca-certificates openssh-client \
      libpq-dev postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv build

WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY src/ ./src/

# Build TuskData wheel + venv with the [all] extras in one shot.
RUN uv venv /opt/tusk-venv \
    && uv pip install --python /opt/tusk-venv/bin/python --no-cache "tuskdata[all] @ ."

# Clone every plugin at its pinned ref and build a wheel for each.
# `set -e` so a failed clone or build kills the image build instead of
# silently shipping a tab-less Studio.
RUN --mount=type=secret,id=gh_token \
    --mount=type=ssh \
    set -e ; \
    if [ -s /run/secrets/gh_token ]; then \
        TOKEN="$(cat /run/secrets/gh_token)"; \
        BASE="https://x-access-token:${TOKEN}@github.com/${TUSK_PLUGINS_ORG}"; \
        echo "[deploy] using https + gh_token" ; \
    elif [ -d /tmp/buildkit-ssh-agent ] || [ -n "${SSH_AUTH_SOCK:-}" ]; then \
        BASE="git@github.com:${TUSK_PLUGINS_ORG}"; \
        mkdir -p ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null ; \
        echo "[deploy] using ssh agent" ; \
    else \
        BASE="https://github.com/${TUSK_PLUGINS_ORG}"; \
        echo "[deploy] using public https (no auth)" ; \
    fi ; \
    mkdir -p /tmp/wheels ; \
    for spec in \
        "tusk-bi:${TUSK_BI_REF}" \
        "tusk-ci:${TUSK_CI_REF}" \
        "tusk-security:${TUSK_SEC_REF}" \
        "tusk-cluster:${TUSK_CLUSTER_REF}" ; \
    do \
        repo="${spec%%:*}" ; ref="${spec##*:}" ; \
        echo "[plugin] cloning ${repo}@${ref}" ; \
        git clone --depth 1 --branch "$ref" "${BASE}/${repo}.git" "/tmp/${repo}" ; \
        echo "[plugin] building wheel for ${repo}" ; \
        ( cd "/tmp/${repo}" && python -m build --wheel --outdir /tmp/wheels ) ; \
    done \
    && ls -la /tmp/wheels

# Install plugin wheels into the venv. fail-fast if any wheel breaks.
RUN set -e ; \
    for w in /tmp/wheels/tusk_*.whl; do \
        echo "[install] $w" ; \
        uv pip install --python /opt/tusk-venv/bin/python --no-cache --no-deps "$w" ; \
    done


FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/tusk-venv/bin:${PATH}" \
    HOME=/var/lib/tusk \
    TUSK_PORT=8000

# postgresql-client at runtime → pg_dump / psql / pg_restore for admin.
# gosu lets the entrypoint chown the persistent volume before dropping
# privileges to the unprivileged `tusk` user.
RUN apt-get update && apt-get install -y --no-install-recommends \
      postgresql-client libpq5 curl gosu \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --create-home --home-dir /var/lib/tusk --shell /usr/sbin/nologin tusk

COPY --from=builder /opt/tusk-venv /opt/tusk-venv
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint
RUN chmod +x /usr/local/bin/docker-entrypoint

WORKDIR /var/lib/tusk

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=30s \
  CMD curl -fsS "http://127.0.0.1:${TUSK_PORT:-8000}/api/health" | grep -q '"status":"ok"' || exit 1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint"]
CMD ["sh", "-c", "exec tusk studio --host 0.0.0.0 --port ${TUSK_PORT:-8000}"]
