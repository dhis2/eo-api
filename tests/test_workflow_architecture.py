from pathlib import Path


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_chirps_workflow_does_not_import_process_wrappers() -> None:
    root = Path(__file__).resolve().parents[1]
    source = _read(root / "src" / "eo_api" / "routers" / "ogcapi" / "plugins" / "processes" / "chirps3_workflow.py")

    forbidden = [
        "plugins.processes.chirps3",
        "plugins.processes.data_aggregate",
        "plugins.processes.feature_fetch",
        "plugins.processes.datavalue_build",
    ]
    for needle in forbidden:
        assert needle not in source


def test_worldpop_workflow_does_not_import_process_wrappers() -> None:
    root = Path(__file__).resolve().parents[1]
    source = _read(
        root / "src" / "eo_api" / "routers" / "ogcapi" / "plugins" / "processes" / "worldpop_dhis2_workflow.py"
    )

    forbidden = [
        "plugins.processes.chirps3",
        "plugins.processes.data_aggregate",
        "plugins.processes.feature_fetch",
        "plugins.processes.datavalue_build",
    ]
    for needle in forbidden:
        assert needle not in source
