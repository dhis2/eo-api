
import xarray as xr

from datasets import registry, cache
from datasets.cache import get_time_dim
from . import preprocess
from . import temporal
from . import spatial
from . import units

def get_aggregate(
    dataset_id,
    features,
    period_type,
    start,
    end,
):
    # get dataset metadata
    dataset = registry.get_dataset(dataset_id)
    varname = dataset['variable']

    # load xarray from cache
    print('Accessing dataset')
    files = cache.get_cache_files(dataset)
    ds = xr.open_mfdataset(
        files,
        data_vars="minimal",
        coords="minimal",
        compat="override"
    )

    # subset time dim
    time_dim = get_time_dim(ds)
    ds = ds.sel(**{time_dim: slice(start, end)})

    # preprocess if needed
    for prep_name in dataset.get('preProcess', []):
        prep_func = getattr(preprocess, prep_name)
        ds = prep_func(ds)

    # aggregate to period type
    print(f'Aggregating period type from {dataset["periodType"]} to {period_type}')
    ds = temporal.aggregate(ds, dataset, period_type, start, end)

    # aggregate to geojson features
    print('Aggregating to org units')
    df = spatial.aggregate(ds, dataset, features)

    # convert to units
    if dataset.get('convertUnits'):
        units.convert_units(df, varname, from_units=dataset['units'], to_units=dataset['convertUnits'])

    # convert to json
    js = df.to_dict(orient='records')
    return js
