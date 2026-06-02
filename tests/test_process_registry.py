from pathlib import Path

import pytest

from open_climate_service.data_registry.services import processes as process_registry


def test_builtin_processes_include_resample(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", None)
    monkeypatch.delenv("CLIMATE_SERVICE_CONFIG", raising=False)

    ids = {p["id"] for p in process_registry.list_processes()}
    assert "resample" in ids


def test_builtin_resample_has_execution_function(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", None)
    monkeypatch.delenv("CLIMATE_SERVICE_CONFIG", raising=False)

    resample = process_registry.get_process("resample")
    assert resample is not None
    assert resample["title"] == "Temporal resampling"
    assert resample["execution"]["function"] == "open_climate_service.processing.services.execute_resample"
    assert resample["jobControlOptions"] == ["sync-execute", "async-execute"]


def test_get_process_returns_none_for_unknown_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", None)
    monkeypatch.delenv("CLIMATE_SERVICE_CONFIG", raising=False)

    assert process_registry.get_process("does_not_exist") is None


def test_get_process_function_returns_callable_for_registered_process(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        process_registry,
        "get_process",
        lambda process_id: {
            "id": process_id,
            "title": "Callable process",
            "execution": {"function": "mypackage.execute.run"},
            "expose": True,
        },
    )
    monkeypatch.setattr(process_registry, "_get_dynamic_function", lambda path: {"path": path})

    resolved = process_registry.get_process_function("callable-process")

    assert resolved == {"path": "mypackage.execute.run"}


def test_process_registry_rejects_missing_title(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    processes_subdir = tmp_path / "processes"
    processes_subdir.mkdir()
    (processes_subdir / "no_name.yaml").write_text(
        "- id: no_name\n  execution:\n    function: mypackage.execute\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", processes_subdir)

    with pytest.raises(ValueError, match="must define title"):
        process_registry.list_processes()


def test_plugins_dir_processes_subdir_adds_to_builtin(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    processes_subdir = tmp_path / "plugins" / "processes"
    processes_subdir.mkdir(parents=True)
    (processes_subdir / "custom.yaml").write_text(
        """
- id: custom_process
  title: Custom process
  execution:
    function: mypackage.custom.execute
""",
        encoding="utf-8",
    )
    config_file = tmp_path / "climate-service.yaml"
    config_file.write_text(f"plugins_dir: {tmp_path / 'plugins'}\n", encoding="utf-8")

    monkeypatch.setattr(process_registry, "CONFIGS_DIR", None)
    monkeypatch.setenv("CLIMATE_SERVICE_CONFIG", str(config_file))

    ids = {p["id"] for p in process_registry.list_processes()}
    assert "custom_process" in ids
    assert "resample" in ids


def test_plugins_dir_process_overrides_builtin_by_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    processes_subdir = tmp_path / "plugins" / "processes"
    processes_subdir.mkdir(parents=True)
    (processes_subdir / "resample.yaml").write_text(
        """
- id: resample
  title: Custom resample override
  execution:
    function: mypackage.resample.execute
""",
        encoding="utf-8",
    )
    config_file = tmp_path / "climate-service.yaml"
    config_file.write_text(f"plugins_dir: {tmp_path / 'plugins'}\n", encoding="utf-8")

    monkeypatch.setattr(process_registry, "CONFIGS_DIR", None)
    monkeypatch.setenv("CLIMATE_SERVICE_CONFIG", str(config_file))

    processes = {p["id"]: p for p in process_registry.list_processes()}
    assert processes["resample"]["title"] == "Custom resample override"
    assert processes["resample"]["execution"]["function"] == "mypackage.resample.execute"


def test_process_registry_rejects_legacy_name_without_title(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    processes_subdir = tmp_path / "processes"
    processes_subdir.mkdir()
    (processes_subdir / "legacy.yaml").write_text(
        """
- id: legacy_process
  name: Legacy process
  execution_function: mypackage.legacy.execute
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", processes_subdir)

    with pytest.raises(ValueError, match="must define title"):
        process_registry.list_processes()


def test_process_registry_rejects_legacy_execution_function_without_execution_block(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    processes_subdir = tmp_path / "processes"
    processes_subdir.mkdir()
    (processes_subdir / "legacy_execution.yaml").write_text(
        """
- id: legacy_execution
  title: Legacy execution
  execution_function: mypackage.legacy.execute
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", processes_subdir)

    with pytest.raises(ValueError, match=r"must define execution\.function"):
        process_registry.list_processes()


def test_process_registry_rejects_empty_title_and_empty_execution_function(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    processes_subdir = tmp_path / "processes"
    processes_subdir.mkdir()
    (processes_subdir / "legacy_empty_preferred.yaml").write_text(
        """
- id: legacy_empty_preferred
  title: ""
  execution:
    function: ""
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", processes_subdir)

    with pytest.raises(ValueError, match="must define title"):
        process_registry.list_processes()


def test_process_registry_rejects_invalid_inputs_enum(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    processes_subdir = tmp_path / "processes"
    processes_subdir.mkdir()
    (processes_subdir / "invalid_inputs.yaml").write_text(
        """
- id: bad_inputs
  title: Bad inputs
  execution:
    function: mypackage.bad.execute
  inputs:
    frequency:
      type: string
      enum:
        - ok
        - 1
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", processes_subdir)

    with pytest.raises(ValueError, match=r"invalid inputs\.frequency\.enum"):
        process_registry.list_processes()


def test_process_registry_rejects_non_mapping_outputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    processes_subdir = tmp_path / "processes"
    processes_subdir.mkdir()
    (processes_subdir / "invalid_outputs.yaml").write_text(
        """
- id: bad_outputs
  title: Bad outputs
  execution:
    function: mypackage.bad.execute
  outputs:
    - not-a-mapping
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", processes_subdir)

    with pytest.raises(ValueError, match="has invalid outputs"):
        process_registry.list_processes()


def test_process_registry_defaults_null_expose(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    processes_subdir = tmp_path / "processes"
    processes_subdir.mkdir()
    (processes_subdir / "null_expose.yaml").write_text(
        """
- id: null_expose
  title: Null expose
  expose:
  execution:
    function: mypackage.execute.run
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", processes_subdir)

    process = process_registry.list_processes()[0]
    assert process["expose"] is True


def test_process_registry_defaults_null_job_control_options(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    processes_subdir = tmp_path / "processes"
    processes_subdir.mkdir()
    (processes_subdir / "null_job_control_options.yaml").write_text(
        """
- id: null_job_control_options
  title: Null job control options
  jobControlOptions:
  execution:
    function: mypackage.execute.run
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", processes_subdir)

    process = process_registry.list_processes()[0]
    assert process["jobControlOptions"] == ["sync-execute"]


def test_process_registry_rejects_empty_job_control_options(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    processes_subdir = tmp_path / "processes"
    processes_subdir.mkdir()
    (processes_subdir / "empty_job_control_options.yaml").write_text(
        """
- id: empty_job_control_options
  title: Empty job control options
  jobControlOptions: []
  execution:
    function: mypackage.execute.run
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(process_registry, "CONFIGS_DIR", processes_subdir)

    with pytest.raises(ValueError, match="invalid jobControlOptions"):
        process_registry.list_processes()
