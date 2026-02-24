"""CHIRPS3 provider with cache-first retrieval semantics."""

import os
from pathlib import Path

from eoapi.processing.providers.base import RasterFetchRequest, RasterFetchResult


def _download_chirps_files(
    *,
    start: str,
    end: str,
    bbox: tuple[float, float, float, float],
    dirname: str,
    prefix: str,
    var_name: str,
) -> list[str]:
    """Isolated download call to enable easy monkeypatching in tests."""

    from dhis2eo.data.chc.chirps3 import daily as chirps3_daily

    return chirps3_daily.download(
        start=start,
        end=end,
        bbox=bbox,
        dirname=dirname,
        prefix=prefix,
        var_name=var_name,
        overwrite=False,
    )


class Chirps3Provider:
    """Fetch CHIRPS files from local cache or remote provider."""

    provider_id = "chirps3"

    def __init__(self, *, cache_dir: str | Path | None = None, variable: str = "precip") -> None:
        cache_root = cache_dir or os.getenv("EOAPI_PROVIDER_CACHE_DIR", ".cache/providers")
        self._cache_dir = Path(cache_root) / self.provider_id
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._variable = variable

    def fetch(self, request: RasterFetchRequest) -> RasterFetchResult:
        """Return dataset assets for the requested date range and bbox."""

        start = request.start
        end = request.end
        target_dir = self._cache_dir / request.dataset_id / request.parameter
        target_dir.mkdir(parents=True, exist_ok=True)

        prefix = f"{request.parameter}_{start.isoformat()}_{end.isoformat()}".replace("-", "")
        # Cache hit shortcut: skip remote download when matching files exist.
        cached_paths = sorted(path for path in target_dir.glob(f"{prefix}*") if path.is_file())
        if cached_paths:
            return RasterFetchResult(
                provider=self.provider_id,
                asset_paths=[str(path) for path in cached_paths],
                from_cache=True,
            )

        files = _download_chirps_files(
            start=start.isoformat(),
            end=end.isoformat(),
            bbox=request.bbox,
            dirname=str(target_dir),
            prefix=prefix,
            var_name=self._variable,
        )
        asset_paths = [str(Path(path)) for path in files if Path(path).exists()]
        if not asset_paths:
            raise RuntimeError("CHIRPS3 provider returned no files")

        return RasterFetchResult(
            provider=self.provider_id,
            asset_paths=asset_paths,
            from_cache=False,
        )

    def implementation_details(self) -> dict[str, str]:
        """Describe concrete provider libraries used by this implementation."""

        return {
            "adapter": "dhis2eo.data.chc.chirps3.daily.download",
            "raster_io": "rioxarray.open_rasterio (inside dhis2eo adapter)",
            "local_cache": "eoapi cache-first file lookup (.cache/providers/chirps3)",
        }
