.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

sync: ## Install dependencies with uv
	uv sync

run: ## Start the app with uvicorn
	uv run uvicorn eo_api.main:app --reload

lint: ## Run ruff linting and formatting (autofix)
	uv run ruff check --fix .
	uv run ruff format .

test: ## Run tests with pytest
	uv run pytest tests/

openapi: ## Generate pygeoapi OpenAPI spec
	PYTHONPATH="$(PWD)" uv run pygeoapi openapi generate ./pygeoapi-config.yml > pygeoapi-openapi.yml

docker-build: ## Full rebuild with docker compose
	docker compose build --no-cache

docker-run: ## Start containers with docker compose
	docker compose up
