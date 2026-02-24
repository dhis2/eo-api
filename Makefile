sync:
	uv sync

run:
	uv run uvicorn main:app --reload

openapi:
	PYTHONPATH="$(PWD)" uv run pygeoapi openapi generate ./pygeoapi-config.yml > pygeoapi-openapi.yml