
import json

import geopandas as gpd
from earthkit import transforms

def aggregate(ds, dataset, features, statistic):
    # load geojson as geopandas
    gdf = gpd.read_file(json.dumps(features))

    # aggregate
    ds = transforms.spatial.reduce(
        ds,
        gdf,
        mask_dim="id", # TODO: DONT HARDCODE
        how=statistic,
    )

    # convert to df
    df = ds.to_dataframe().reset_index()

    # return
    return df
