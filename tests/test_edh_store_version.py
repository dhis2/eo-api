"""Superseded Earth Data Hub stores must announce themselves (CLIM-955)."""

from __future__ import annotations

import logging
from typing import Any

import pytest
import xarray as xr

from open_climate_service.plugins.datasets import era5_land


@pytest.fixture(autouse=True)
def _capturable_logging() -> Any:
    """Let `caplog` see these records, and start each test with a cold probe cache.

    `startup.py` sets `propagate = False` on the `open_climate_service` logger so the package
    owns its own handler and does not double-log under uvicorn. That also means caplog, which
    attaches at the root, never sees a thing — the warning is emitted and the assertion still
    fails. Re-enabled for the duration of each test rather than weakened in the package.
    """
    package_logger = logging.getLogger("open_climate_service")
    previous = package_logger.propagate
    package_logger.propagate = True
    era5_land._edh_zarr_format_cache.clear()
    era5_land._edh_zarr_format_unknown_until.clear()
    era5_land._edh_superseded_warned.clear()
    yield
    package_logger.propagate = previous
    era5_land._edh_zarr_format_cache.clear()
    era5_land._edh_zarr_format_unknown_until.clear()
    era5_land._edh_superseded_warned.clear()


def _dataset_with_time(last: str = "2026-05-31T23:00") -> xr.Dataset:
    import numpy as np

    times = np.array(["2026-05-31T22:00", last], dtype="datetime64[ns]")
    return xr.Dataset({"t2m": (("valid_time",), [1.0, 2.0])}, coords={"valid_time": times})


def test_a_v2_store_warns_and_names_its_latest_timestamp(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The whole point: a superseded store stops advancing without failing.

    An ingest against it reports no new periods, which reads exactly like "nothing has been
    published yet". The warning is the only thing distinguishing the two.
    """
    monkeypatch.setattr(era5_land, "_edh_zarr_format", lambda _url: 2)

    with caplog.at_level(logging.WARNING, logger=era5_land.logger.name):
        era5_land._warn_if_superseded("https://example.invalid/store.zarr", _dataset_with_time())

    assert "Zarr v2, not v3" in caplog.text
    assert "no longer advances" in caplog.text
    assert "2026-05-31T23:00" in caplog.text, "the operator needs the gap, not just the version"
    assert "CLIM-955" in caplog.text


def test_a_v3_store_is_silent(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setattr(era5_land, "_edh_zarr_format", lambda _url: 3)

    with caplog.at_level(logging.WARNING, logger=era5_land.logger.name):
        era5_land._warn_if_superseded("https://example.invalid/store.zarr", _dataset_with_time())

    assert caplog.text == ""


def test_an_undeterminable_version_is_silent(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """A failed probe must not cry wolf: unknown is not the same as superseded."""
    monkeypatch.setattr(era5_land, "_edh_zarr_format", lambda _url: None)

    with caplog.at_level(logging.WARNING, logger=era5_land.logger.name):
        era5_land._warn_if_superseded("https://example.invalid/store.zarr", _dataset_with_time())

    assert caplog.text == ""


def test_a_store_with_no_time_axis_still_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(era5_land, "_edh_zarr_format", lambda _url: 2)

    with caplog.at_level(logging.WARNING, logger=era5_land.logger.name):
        era5_land._warn_if_superseded("https://example.invalid/store.zarr", xr.Dataset({"t2m": 1.0}))

    assert "Zarr v2, not v3" in caplog.text


@pytest.mark.parametrize(
    ("document", "probe", "expected"),
    [
        ({"zarr_format": 3}, "zarr.json", 3),
        ({"metadata": {".zgroup": {"zarr_format": 2}}}, ".zmetadata", 2),
    ],
)
def test_the_version_is_probed_from_the_store_not_the_url(
    monkeypatch: pytest.MonkeyPatch, document: dict[str, Any], probe: str, expected: int
) -> None:
    """Inferring from the URL would go stale exactly when a store is migrated in place."""
    import io
    import json

    class _Response:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        def read(self) -> bytes:
            return self._payload

        def __enter__(self) -> _Response:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_urlopen(request: Any, timeout: int = 0) -> _Response:
        if not request.full_url.endswith(probe):
            raise OSError("not this one")
        return _Response(json.dumps(document).encode())

    monkeypatch.setattr(era5_land, "urlopen", fake_urlopen)
    assert io  # keep the import meaningful for readers of the fake above

    assert era5_land._edh_zarr_format("https://example.invalid/store.zarr") == expected


def test_a_probe_that_fails_entirely_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed probe must never stop a working read — the store still opens."""

    def fake_urlopen(request: Any, timeout: int = 0) -> None:
        raise OSError("network down")

    monkeypatch.setattr(era5_land, "urlopen", fake_urlopen)

    assert era5_land._edh_zarr_format("https://example.invalid/store.zarr") is None


def test_the_probe_authenticates_from_netrc(monkeypatch: pytest.MonkeyPatch) -> None:
    """The gap the mocked tests missed, and that only a live run exposed.

    xarray reaches EDH through fsspec with `trust_env=True`, which reads netrc itself.
    `urlopen` does not, so without an explicit header the probe gets a 401, returns None, and
    the warning silently never fires — the exact failure it exists to prevent.
    """
    import json

    monkeypatch.delenv("EDH_API_KEY", raising=False)

    class _Netrc:
        @staticmethod
        def authenticators(host: str) -> tuple[str, str, str] | None:
            return ("edh", "", "secret-key") if host == "api.earthdatahub.destine.eu" else None

    monkeypatch.setattr(era5_land.netrc, "netrc", lambda: _Netrc())

    seen: dict[str, str] = {}

    class _Response:
        def read(self) -> bytes:
            return json.dumps({"zarr_format": 3}).encode()

        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_urlopen(request: Any, timeout: int = 0) -> _Response:
        seen.update(request.headers)
        return _Response()

    monkeypatch.setattr(era5_land, "urlopen", fake_urlopen)

    assert era5_land._edh_zarr_format("https://api.earthdatahub.destine.eu/era5/store.zarr") == 3
    assert any(k.lower() == "authorization" for k in seen), "the probe sent no credentials"


def test_no_credentials_anywhere_sends_no_header(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EDH_API_KEY", raising=False)
    monkeypatch.setattr(era5_land.netrc, "netrc", lambda: (_ for _ in ()).throw(OSError("no netrc")))

    assert era5_land._edh_auth_header("https://api.earthdatahub.destine.eu/x.zarr") == {}


def test_the_env_key_path_also_authenticates(monkeypatch: pytest.MonkeyPatch) -> None:
    """`urlopen` does not turn `user:pass@host` userinfo into Basic Auth the way fsspec does.

    Probing the authenticated URL therefore 401s for anyone using EDH_API_KEY — the whole
    documented environment-variable path — and the warning silently never fires. Verified
    against the live store: the probe returned None with the key set and 2 without it.
    """
    monkeypatch.setenv("EDH_API_KEY", "mytoken")

    header = era5_land._edh_auth_header("https://api.earthdatahub.destine.eu/era5/x.zarr")

    assert header["Authorization"].startswith("Basic ")
    import base64

    assert base64.b64decode(header["Authorization"].split()[1]).decode() == "edh:mytoken"


def test_the_probe_never_sends_credentials_in_the_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Userinfo in the probe URL is both useless to urlopen and a credential leak into logs."""
    import json

    monkeypatch.setenv("EDH_API_KEY", "mytoken")
    seen: list[str] = []

    class _Response:
        def read(self) -> bytes:
            return json.dumps({"zarr_format": 3}).encode()

        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_urlopen(request: Any, timeout: int = 0) -> _Response:
        seen.append(request.full_url)
        return _Response()

    monkeypatch.setattr(era5_land, "urlopen", fake_urlopen)
    era5_land._edh_zarr_format("https://api.earthdatahub.destine.eu/era5/x.zarr")

    assert seen and all("mytoken" not in url and "@" not in url for url in seen)


@pytest.mark.parametrize("document", [[], {"metadata": []}, "a string", None, {"metadata": {".zgroup": []}}])
def test_a_valid_json_response_of_the_wrong_shape_yields_none(monkeypatch: pytest.MonkeyPatch, document: Any) -> None:
    """A 200 of an unexpected shape must not raise past the probe.

    It would propagate out of `_edh_open_zarr` and take down a store that had already opened
    successfully — turning a diagnostic into an outage.
    """
    import json

    class _Response:
        def read(self) -> bytes:
            return json.dumps(document).encode()

        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(era5_land, "urlopen", lambda *a, **k: _Response())

    assert era5_land._edh_zarr_format("https://example.invalid/store.zarr") is None


def test_an_unknown_result_is_not_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    """A transient failure must not permanently disable the check in a long-running worker."""
    import json

    calls = {"n": 0}

    class _Response:
        def read(self) -> bytes:
            return json.dumps({"zarr_format": 2}).encode()

        def __enter__(self) -> "_Response":
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def flaky_urlopen(request: Any, timeout: int = 0) -> _Response:
        calls["n"] += 1
        if calls["n"] <= 2:  # both probes fail on the first attempt
            raise OSError("transient")
        return _Response()

    monkeypatch.setattr(era5_land, "urlopen", flaky_urlopen)

    assert era5_land._edh_zarr_format("https://example.invalid/store.zarr") is None
    # Within the TTL the failure is remembered, so the per-period call path does not re-probe.
    assert era5_land._edh_zarr_format("https://example.invalid/store.zarr") is None
    assert calls["n"] == 2, "the store was re-probed inside the negative-cache window"
    # Past it, the check recovers on its own rather than staying off for the process.
    era5_land._edh_zarr_format_unknown_until.clear()
    assert era5_land._edh_zarr_format("https://example.invalid/store.zarr") == 2


def test_the_warning_is_emitted_once_per_store(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """`_latest_available` reopens the store per period; a multi-year ingest would flood."""
    monkeypatch.setattr(era5_land, "_edh_zarr_format", lambda _url: 2)

    with caplog.at_level(logging.WARNING, logger=era5_land.logger.name):
        for _ in range(50):
            era5_land._warn_if_superseded("https://example.invalid/store.zarr", _dataset_with_time())

    assert caplog.text.count("no longer advances") == 1


def test_an_unknown_result_is_not_reprobed_on_every_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """The regression the negative-cache TTL exists to prevent (Copilot, second review).

    `_latest_available` reopens the store for every period. With no negative cache, a store
    fsspec can read while these separate requests time out costs up to 30 s *per period* —
    making an optional diagnostic the bottleneck during exactly the transient failure it was
    written to tolerate.
    """
    calls = {"n": 0}

    def always_fails(request: Any, timeout: int = 0) -> None:
        calls["n"] += 1
        raise OSError("timeout")

    monkeypatch.setattr(era5_land, "urlopen", always_fails)

    for _ in range(100):
        assert era5_land._edh_zarr_format("https://example.invalid/store.zarr") is None

    assert calls["n"] == 2, f"probed {calls['n']} times across 100 opens; expected one round of 2"


def test_the_negative_cache_expires(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bounded, not permanent: the check must come back without a process restart."""
    clock = {"t": 1000.0}
    monkeypatch.setattr(era5_land, "monotonic", lambda: clock["t"])

    def always_fails(request: Any, timeout: int = 0) -> None:
        raise OSError("timeout")

    monkeypatch.setattr(era5_land, "urlopen", always_fails)
    assert era5_land._edh_zarr_format("https://example.invalid/store.zarr") is None

    clock["t"] += era5_land._EDH_UNKNOWN_FORMAT_TTL_SECONDS + 1
    probed = {"n": 0}

    def counts(request: Any, timeout: int = 0) -> None:
        probed["n"] += 1
        raise OSError("still down")

    monkeypatch.setattr(era5_land, "urlopen", counts)
    assert era5_land._edh_zarr_format("https://example.invalid/store.zarr") is None
    assert probed["n"] > 0, "the negative cache never expired"


def test_a_failing_timestamp_read_still_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A diagnostic must not turn a usable store into a failed ingest (Copilot, second review).

    Reading the latest timestamp is a lazy remote fetch that can fail on its own. It only
    sharpens the message, so losing it must cost the timestamp and nothing else.
    """
    import numpy as np

    monkeypatch.setattr(era5_land, "_edh_zarr_format", lambda _url: 2)

    class _Exploding:
        def isel(self, *args: object, **kwargs: object) -> Any:
            raise OSError("coordinate chunk could not be fetched")

    ds = xr.Dataset(
        {"t2m": (("valid_time",), [1.0])},
        coords={"valid_time": np.array(["2026-05-31"], dtype="datetime64[ns]")},
    )
    monkeypatch.setitem(ds._variables, "valid_time", _Exploding())  # type: ignore[arg-type]

    with caplog.at_level(logging.WARNING, logger=era5_land.logger.name):
        era5_land._warn_if_superseded("https://example.invalid/store.zarr", ds)

    assert "Zarr v2, not v3" in caplog.text, "the warning was lost with the timestamp"
    assert "latest timestamp" not in caplog.text
