
import json

import geopandas as gpd
from earthkit import transforms

def aggregate(ds, dataset, features):
    # load geojson as geopandas
    gdf = gpd.read_file(json.dumps(features))

    # aggregate
    agg_method = dataset['aggregation']['spatial']
    ds = transforms.spatial.reduce(
        ds,
        gdf,
        mask_dim="id", # TODO: DONT HARDCODE
        how=agg_method,
    )

    # convert to df
    df = ds.to_dataframe().reset_index()

    # return
    return df
