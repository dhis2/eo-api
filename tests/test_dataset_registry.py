from pathlib import Path

import pytest

from open_climate_service.data_registry.services import datasets


def test_dataset_registry_requires_sync_kind(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    registry_file = tmp_path / "missing_sync_kind.yaml"
    registry_file.write_text(
        """
- id: missing_sync_kind
  name: Missing sync kind
  variable: value
  period_type: daily
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="must define sync.kind"):
        datasets.list_datasets()


def test_dataset_registry_rejects_unsupported_sync_kind(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "invalid_sync_kind.yaml"
    registry_file.write_text(
        """
- id: invalid_sync_kind
  name: Invalid sync kind
  variable: value
  period_type: daily
  sync:
    kind: sometimes
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="unsupported sync.kind 'sometimes'"):
        datasets.list_datasets()


def test_dataset_registry_accepts_supported_sync_kind(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "valid.yaml"
    registry_file.write_text(
        """
- id: valid_temporal
  name: Valid temporal
  variable: value
  period_type: daily
  sync:
    kind: temporal
  ingestion:
    plugin: open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    assert datasets.list_datasets()[0]["id"] == "valid_temporal"


def test_dataset_registry_accepts_ingestion_plugin_without_function(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "valid_plugin.yaml"
    registry_file.write_text(
        """
- id: valid_plugin
  name: Valid plugin
  variable: value
  period_type: daily
  sync:
    kind: temporal
  ingestion:
    plugin: open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    assert datasets.list_datasets()[0]["ingestion"]["plugin"].endswith("CHIRPS3DailyPlugin")


def test_dataset_registry_rejects_unsupported_sync_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "invalid_sync_execution.yaml"
    registry_file.write_text(
        """
- id: invalid_sync_execution
  name: Invalid sync execution
  variable: value
  period_type: daily
  sync:
    kind: temporal
    execution: sometimes
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="unsupported sync.execution 'sometimes'"):
        datasets.list_datasets()


def test_dataset_registry_rejects_non_string_sync_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "invalid_sync_execution_type.yaml"
    registry_file.write_text(
        """
- id: invalid_sync_execution_type
  name: Invalid sync execution type
  variable: value
  period_type: daily
  sync:
    kind: temporal
    execution:
      - append
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="invalid sync.execution"):
        datasets.list_datasets()


def test_dataset_registry_accepts_supported_sync_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "valid_append.yaml"
    registry_file.write_text(
        """
- id: valid_append
  name: Valid append
  variable: value
  period_type: daily
  sync:
    kind: temporal
    execution: append
  ingestion:
    plugin: open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    assert datasets.list_datasets()[0]["sync"]["execution"] == "append"


def test_dataset_registry_rejects_invalid_sync_availability_function(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "invalid_sync_availability.yaml"
    registry_file.write_text(
        """
- id: invalid_sync_availability
  name: Invalid sync availability
  variable: value
  period_type: daily
  sync:
    kind: temporal
    availability:
      latest_available_function: 42
  ingestion:
    plugin: open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="invalid sync.availability.latest_available_function"):
        datasets.list_datasets()


def test_dataset_registry_accepts_sync_availability_function(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "valid_sync_availability.yaml"
    registry_file.write_text(
        """
- id: valid_sync_availability
  name: Valid sync availability
  variable: value
  period_type: daily
  sync:
    kind: temporal
    availability:
      latest_available_function: open_climate_service.providers.availability.lagged_latest_available
  ingestion:
    plugin: open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    assert datasets.list_datasets()[0]["sync"]["availability"]["latest_available_function"].endswith(
        "lagged_latest_available"
    )
