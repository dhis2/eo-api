.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

sync: ## Install dependencies with uv
	uv sync

sync-gdal: ## Pin gdal override to the installed system libgdal version, then sync
	@GDAL_VERSION=$$(gdal-config --version 2>/dev/null) && \
		[ -n "$$GDAL_VERSION" ] || (echo "gdal-config not found — install libgdal-dev first" && exit 1) && \
		echo "System GDAL: $$GDAL_VERSION" && \
		sed -i.bak "s/\"gdal==[^\"]*\"/\"gdal==$$GDAL_VERSION\"/" pyproject.toml && \
		rm -f pyproject.toml.bak && \
		uv sync

run: ## Start the app with uvicorn
	uv run uvicorn open_climate_service.main:app --reload --reload-include "*.html" --reload-include "*.yaml" --reload-include "*.yml"

lint: ## Check linting, formatting, and types (no autofix)
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy open_climate_service/
	uv run pyright

fix: ## Autofix ruff lint and format issues
	uv run ruff check --fix .
	uv run ruff format .

test: ## Run tests with pytest
	uv run pytest tests/

openapi: ## Generate pygeoapi OpenAPI spec
	@set -a && . ./.env && set +a && \
		uv run python -c "from open_climate_service.publications.services import ensure_pygeoapi_base_config; ensure_pygeoapi_base_config()"

start: ## Start the Docker stack (builds images first)
	docker compose up --build

restart: ## Tear down, rebuild, and start the Docker stack from scratch
	docker compose down -v && docker compose build --no-cache && docker compose up
