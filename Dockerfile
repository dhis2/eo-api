FROM ghcr.io/astral-sh/uv:0.10-python3.13-trixie-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git curl \
        build-essential cython3 \
        libgeos-dev libproj-dev libeccodes-dev && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd --system eo && useradd --system --gid eo --create-home eo

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./
COPY src/ src/

RUN uv sync --frozen --no-dev && \
    mkdir -p /app/.venv/lib/python3.13/site-packages/prefect/server/ui_build && \
    chown eo:eo /app/.venv/lib/python3.13/site-packages/prefect/server/ui_build

COPY pygeoapi-config.yml pygeoapi-openapi.yml ./

ENV PYGEOAPI_CONFIG=/app/pygeoapi-config.yml
ENV PYGEOAPI_OPENAPI=/app/pygeoapi-openapi.yml
ENV PORT=8000

USER eo

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

CMD /app/.venv/bin/uvicorn eo_api.main:app --host 0.0.0.0 --port ${PORT}
