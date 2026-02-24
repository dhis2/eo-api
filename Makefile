sync:
	uv sync

run:
	uv run uvicorn eo_api.main:app --reload