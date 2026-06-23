FROM ghcr.io/astral-sh/uv:0.10-python3.12-trixie-slim

# Compile Python bytecode for faster startup (.venv only)
ENV UV_COMPILE_BYTECODE=1
# Use copy mode to avoid hardlink issues across the uv cache mount
ENV UV_LINK_MODE=copy
# Prevent Python from writing .pyc files at runtime
ENV PYTHONDONTWRITEBYTECODE=1
ENV MPLCONFIGDIR=/tmp

# build-essential + cython3 + libgeos/libproj headers: cartopy ships no aarch64
#   wheel, so it builds from source when the image targets ARM64.
# libeccodes: the `eccodes` wheel is pure-Python and dlopens the system library
#   at runtime (it bundles nothing) on every architecture.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        git curl \
        build-essential cython3 \
        libgeos-dev libproj-dev libeccodes-dev

RUN groupadd --system ocs && useradd --system --gid ocs --create-home ocs

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./
COPY open_climate_service/ open_climate_service/
COPY README.md LICENSE ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --extra server && \
    python -m compileall -q open_climate_service/

RUN mkdir -p /app/data/pygeoapi /app/data/artifacts && \
    printf '[]\n' > /app/data/artifacts/records.json && \
    chown -R ocs:ocs /app/data

ENV PYGEOAPI_CONFIG=/app/data/pygeoapi/pygeoapi-config.yml
ENV PYGEOAPI_OPENAPI=/app/data/pygeoapi/pygeoapi-openapi.yml
ENV PORT=8000
ENV PATH="/app/.venv/bin:$PATH"

USER ocs

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# exec form (JSON) so uvicorn receives signals directly; `exec` replaces the
# shell that expands ${PORT} so no extra process sits between init and uvicorn.
CMD ["sh", "-c", "exec uvicorn open_climate_service.main:app --host 0.0.0.0 --port ${PORT}"]
