sync:
	uv sync

run:
	uv run uvicorn eo_api.main:app --reload

lint:
	uv run ruff check --fix .
	uv run ruff format .