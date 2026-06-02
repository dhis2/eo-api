import numpy as np
import xarray as xr

from open_climate_service.transforms import kelvin_to_celsius, metres_to_mm


def _ds(varname: str, values: list[float], time_steps: int = 1) -> xr.Dataset:
    if time_steps > 1:
        data = np.array(values, dtype=float).reshape(time_steps, -1)
        return xr.Dataset({varname: xr.DataArray(data, dims=["time", "x"])})
    return xr.Dataset({varname: xr.DataArray(np.array(values, dtype=float))})


class TestKelvinToCelsius:
    def test_converts_values(self):
        ds = _ds("t2m", [273.15, 293.15, 313.15])
        result = kelvin_to_celsius(ds, {"variable": "t2m"})
        np.testing.assert_allclose(result["t2m"].values, [0.0, 20.0, 40.0])

    def test_sets_units_attr(self):
        ds = _ds("t2m", [300.0])
        result = kelvin_to_celsius(ds, {"variable": "t2m"})
        assert result["t2m"].attrs["units"] == "degC"

    def test_preserves_existing_attrs(self):
        ds = xr.Dataset({"t2m": xr.DataArray([300.0], attrs={"long_name": "temperature", "units": "K"})})
        result = kelvin_to_celsius(ds, {"variable": "t2m"})
        assert result["t2m"].attrs["long_name"] == "temperature"


class TestMetresToMm:
    def test_converts_values(self):
        ds = _ds("tp", [0.001, 0.005])
        result = metres_to_mm(ds, {"variable": "tp"})
        np.testing.assert_allclose(result["tp"].values, [1.0, 5.0])

    def test_sets_units_attr(self):
        ds = _ds("tp", [0.001])
        result = metres_to_mm(ds, {"variable": "tp"})
        assert result["tp"].attrs["units"] == "mm"


class TestRunTransformsPipeline:
    def test_pipeline_via_dotted_path(self):
        ds = _ds("t2m", [273.15])
        dataset = {
            "variable": "t2m",
            "transforms": ["open_climate_service.transforms.kelvin_to_celsius"],
        }
        from open_climate_service.data_manager.services.downloader import _run_transforms

        result = _run_transforms(ds, dataset)
        np.testing.assert_allclose(result["t2m"].values, [0.0])

    def test_empty_transforms_is_noop(self):
        ds = _ds("x", [1.0, 2.0])
        from open_climate_service.data_manager.services.downloader import _run_transforms

        result = _run_transforms(ds, {"variable": "x", "transforms": []})
        np.testing.assert_array_equal(result["x"].values, ds["x"].values)

    def test_no_transforms_key_is_noop(self):
        ds = _ds("x", [1.0])
        from open_climate_service.data_manager.services.downloader import _run_transforms

        result = _run_transforms(ds, {"variable": "x"})
        np.testing.assert_array_equal(result["x"].values, ds["x"].values)

    def test_dict_entry_with_params(self):
        ds = _ds("t2m", [273.15])
        dataset = {
            "variable": "t2m",
            "transforms": [{"function": "open_climate_service.transforms.kelvin_to_celsius"}],
        }
        from open_climate_service.data_manager.services.downloader import _run_transforms

        result = _run_transforms(ds, dataset)
        np.testing.assert_allclose(result["t2m"].values, [0.0])
