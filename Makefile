.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

sync: ## Install dependencies with uv
	uv sync

run: openapi ## Start the app with uvicorn
	uv run uvicorn eo_api.main:app --reload

lint: ## Run ruff linting and formatting (autofix)
	uv run ruff check --fix .
	uv run ruff format .
	uv run mypy src/
	uv run pyright

test: ## Run tests with pytest
	uv run pytest tests/

openapi: ## Generate pygeoapi OpenAPI spec
	@set -a && . ./.env && set +a && \
		PYTHONPATH="$(PWD)/src" uv run pygeoapi openapi generate ./pygeoapi-config.yml > pygeoapi-openapi.yml

start: openapi ## Start the Docker stack (builds images first)
	docker compose up --build

restart: openapi ## Tear down, rebuild, and start the Docker stack from scratch
	docker compose down -v && docker compose build --no-cache && docker compose up
