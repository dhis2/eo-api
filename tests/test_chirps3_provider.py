from pathlib import Path

from eoapi.processing.providers.base import RasterFetchRequest
from eoapi.processing.providers.chirps3 import Chirps3Provider


def test_chirps3_provider_uses_cache_before_download(monkeypatch, tmp_path: Path) -> None:
    provider = Chirps3Provider(cache_dir=tmp_path)
    request = RasterFetchRequest(
        dataset_id="chirps-daily",
        parameter="precip",
        start="2026-01-15",
        end="2026-01-15",
        bbox=(-10.0, -10.0, 10.0, 10.0),
    )

    target_dir = tmp_path / "chirps3" / "chirps-daily" / "precip"
    target_dir.mkdir(parents=True, exist_ok=True)
    prefix = "precip_20260115_20260115"
    cached_file = target_dir / f"{prefix}_cached.nc"
    cached_file.write_text("cached", encoding="utf-8")

    def fail_download(**kwargs):
        raise AssertionError("download() should not be called when cache is available")

    monkeypatch.setattr("eoapi.processing.providers.chirps3._download_chirps_files", fail_download)

    result = provider.fetch(request)
    assert result.from_cache is True
    assert result.asset_paths == [str(cached_file)]


def test_chirps3_provider_downloads_when_cache_empty(monkeypatch, tmp_path: Path) -> None:
    provider = Chirps3Provider(cache_dir=tmp_path)
    request = RasterFetchRequest(
        dataset_id="chirps-daily",
        parameter="precip",
        start="2026-01-16",
        end="2026-01-16",
        bbox=(-10.0, -10.0, 10.0, 10.0),
    )

    def fake_download(*, dirname: str, prefix: str, **kwargs):
        output = Path(dirname) / f"{prefix}_downloaded.nc"
        output.write_text("downloaded", encoding="utf-8")
        return [str(output)]

    monkeypatch.setattr("eoapi.processing.providers.chirps3._download_chirps_files", fake_download)

    result = provider.fetch(request)
    assert result.from_cache is False
    assert len(result.asset_paths) == 1
    assert result.asset_paths[0].endswith("_downloaded.nc")
