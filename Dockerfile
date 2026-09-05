# syntax=docker/dockerfile:1.7
#
# TuskData — minimal community image
#
# This Dockerfile builds the core TuskData app, optionally bundled with
# the public tusk-cluster plugin. For the full suite (BI, CI, security)
# see https://github.com/tuskdata/tuskdata-compose.
#
# Build args:
#   TUSK_CLUSTER_REF   tusk-cluster ref (default: v0.2.1, public repo)
#   WITH_CLUSTER       1 | 0 — bake tusk-cluster into the image
#                      (default: 0 — the plugin is paused)
#   TUSK_POLARS        compat | avx — polars binary runtime. `compat` (default)
#                      runs on any x86-64 (no AVX2 needed); `avx` is faster
#                      but dies with SIGILL on CPUs without AVX2 (QEMU, old Xeons).
#
# Runtime env:
#   TUSK_DEBUG, TUSK_LOG_LEVEL, TUSK_LOG_FORMAT, TUSK_QUERY_TIMEOUT
#   TUSK_CLUSTER_SECRET, TUSK_CLUSTER_TLS, TUSK_PORT, HOME, TZ
#
# Default port: 8000. Healthcheck on /api/health.

FROM python:3.13-slim AS builder

ARG TUSK_CLUSTER_REF=v0.2.1
ARG WITH_CLUSTER=0
ARG TUSK_POLARS=compat

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_SYSTEM_PYTHON=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential git curl ca-certificates \
      libpq-dev postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv build

WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY src/ ./src/

# Build TuskData itself + the [all] extras into the venv.
RUN uv venv /opt/tusk-venv \
    && uv pip install --python /opt/tusk-venv/bin/python --no-cache "tuskdata[all] @ ."

# Polars ≥1.37 ships its binary in a separate runtime package; PyPI's default
# is `polars-runtime-32` (AVX2). On a CPU without AVX2 `import polars` aborts
# with "Illegal instruction" and the container restart-loops. Install the
# baseline runtime and drop the AVX one unless TUSK_POLARS=avx.
RUN set -e ; \
    PV=$(/opt/tusk-venv/bin/python -c "import importlib.metadata as m; print(m.version('polars'))") ; \
    if [ "${TUSK_POLARS}" != "avx" ]; then \
        echo "[polars] installing polars-runtime-compat==$PV" ; \
        uv pip install --python /opt/tusk-venv/bin/python --no-cache "polars-runtime-compat==$PV" ; \
        uv pip uninstall --python /opt/tusk-venv/bin/python polars-runtime-32 polars-runtime-64 2>/dev/null || true ; \
    fi ; \
    /opt/tusk-venv/bin/python -c "import polars, importlib.metadata as m; print('[polars]', polars.__version__, [d.metadata['Name'] for d in m.distributions() if d.metadata['Name'].startswith('polars-runtime')])"

# Optionally bundle the public tusk-cluster plugin. Skip with WITH_CLUSTER=0
# if you don't need distributed query support.
RUN set -e ; \
    if [ "${WITH_CLUSTER}" = "1" ]; then \
        echo "[plugin] cloning tusk-cluster@${TUSK_CLUSTER_REF}" ; \
        git clone --depth 1 --branch "${TUSK_CLUSTER_REF}" \
            https://github.com/tuskdata/tusk-cluster.git /tmp/tusk-cluster ; \
        ( cd /tmp/tusk-cluster && python -m build --wheel --outdir /tmp/wheels ) ; \
        uv pip install --python /opt/tusk-venv/bin/python --no-cache --no-deps \
            /tmp/wheels/tusk_cluster-*.whl ; \
    else \
        echo "[plugin] tusk-cluster skipped (WITH_CLUSTER=0)" ; \
    fi


FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/tusk-venv/bin:${PATH}" \
    HOME=/var/lib/tusk \
    TUSK_PORT=8000

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
