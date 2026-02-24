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

docker-build: ## Build Docker image
	docker build -t eo-api .

docker-run: ## Run Docker container
	docker run --rm --env-file .env -p 8000:8000 eo-api