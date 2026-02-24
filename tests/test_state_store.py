from eoapi.state_store import load_state_map, save_state_map


def test_state_store_roundtrip(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("EOAPI_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("EOAPI_STATE_PERSIST", "true")

    save_state_map("jobs", {"a": {"jobId": "a"}})
    loaded = load_state_map("jobs")

    assert loaded == {"a": {"jobId": "a"}}


def test_state_store_disabled(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("EOAPI_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("EOAPI_STATE_PERSIST", "false")

    save_state_map("jobs", {"a": {"jobId": "a"}})
    loaded = load_state_map("jobs")

    assert loaded == {}
