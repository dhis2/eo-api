from pathlib import Path

import pytest

from eoapi.processing.registry import clear_dataset_registry_cache, load_dataset_registry


def test_default_dataset_registry_contains_chirps3_provider() -> None:
    clear_dataset_registry_cache()
    datasets = load_dataset_registry()

    assert "chirps-daily" in datasets
    assert datasets["chirps-daily"].provider.name == "chirps3"
    assert "precip" in datasets["chirps-daily"].parameters


def test_dataset_registry_rejects_unknown_dataset_provider_mapping(tmp_path: Path) -> None:
    registry_path = tmp_path / "providers.yaml"
    registry_path.write_text(
        "\n".join(
            [
                'version: "1.0"',
                "providers:",
                "  unknown-dataset:",
                "    name: chirps3",
            ]
        ),
        encoding="utf-8",
    )

    clear_dataset_registry_cache()
    with pytest.raises(Exception, match="unknown dataset id"):
        load_dataset_registry(registry_path)
