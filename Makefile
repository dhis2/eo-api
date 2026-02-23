sync:
	uv sync

run:
	uv run uvicorn main:app --reload

validate-datasets:
	uv run python scripts/validate_datasets.py

test:
	uv run --with pytest pytest -q
