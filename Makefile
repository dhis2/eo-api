.PHONY: sync run run-pygeoapi run-pygeoapi-example

sync:
	uv sync

run:
	uv run uvicorn main:app --reload

run-pygeoapi:
	@if [ -z "$(PYGEOAPI_CONFIG)" ]; then \
		echo "PYGEOAPI_CONFIG is required. Example:"; \
		echo "  make run-pygeoapi PYGEOAPI_CONFIG=/absolute/path/to/pygeoapi-config.yml"; \
		exit 1; \
	fi
	PYGEOAPI_CONFIG="$(PYGEOAPI_CONFIG)" uv run uvicorn main:app --reload

run-pygeoapi-example:
	@if [ ! -f "./pygeoapi-config.yml" ]; then \
		echo "Missing ./pygeoapi-config.yml"; \
		echo "Create one in the repo root or use:"; \
		echo "  make run-pygeoapi PYGEOAPI_CONFIG=/absolute/path/to/pygeoapi-config.yml"; \
		exit 1; \
	fi
	PYGEOAPI_CONFIG="$(PWD)/pygeoapi-config.yml" uv run uvicorn main:app --reload
