from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


# https://docs.pygeoapi.io/en/stable/publishing/ogcapi-processes.html
# https://docs.pygeoapi.io/en/stable/plugins.html#example-custom-pygeoapi-processing-plugin
# https://pavics-weaver.readthedocs.io/

def _to_serializable_time(value):
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    try:
        import numpy as np
        if isinstance(value, np.datetime64):
            return np.datetime_as_string(value, unit='s')
    except Exception:
        pass
    return str(value)


def _resolve_spatial_dims(data_array):
    candidates = [
        ('lat', 'lon'),
        ('latitude', 'longitude'),
        ('y', 'x'),
    ]
    dims = set(data_array.dims)
    for y_dim, x_dim in candidates:
        if y_dim in dims and x_dim in dims:
            return y_dim, x_dim
    raise ProcessorExecuteError(
        'Could not resolve spatial dimensions. Expected one of: '
        '(lat, lon), (latitude, longitude), or (y, x).'
    )


def _build_affine(x_values, y_values):
    try:
        import numpy as np
        from rasterio.transform import from_origin
    except Exception as exc:
        raise ProcessorExecuteError(
            'rasterio and numpy are required for geometry/raster alignment'
        ) from exc

    if len(x_values) < 2 or len(y_values) < 2:
        raise ProcessorExecuteError('Spatial coordinates must have at least 2 values each')

    x_res = float(abs(np.median(np.diff(x_values))))
    y_res = float(abs(np.median(np.diff(y_values))))

    west = float(x_values.min() - (x_res / 2.0))
    north = float(y_values.max() + (y_res / 2.0))
    return from_origin(west, north, x_res, y_res)

# https://docs.pygeoapi.io/en/stable/plugins.html#example-custom-pygeoapi-processing-plugin
# https://github.com/geopython/pygeoapi/tree/master/pygeoapi/process
# https://dive.pygeoapi.io/publishing/ogcapi-processes/

PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'aggregation',
    'title': {
        'en': 'Time series zonal aggregation',
    },
    'description': {
        'en': 'Aggregate NetCDF values by GeoJSON features for each time point.'
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['aggregation', 'netcdf', 'geojson', 'time-series', 'zonal-statistics'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'netcdf': {
            'title': 'NetCDF URI or path',
            'description': 'Filesystem path or URI to a NetCDF dataset with a time dimension.',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'keywords': ['netcdf', 'dataset']
        },
        'geojson': {
            'title': 'GeoJSON FeatureCollection',
            'description': 'FeatureCollection used as aggregation zones.',
            'schema': {
                'type': 'object'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'keywords': ['geojson', 'featurecollection', 'zones']
        },
        'variable': {
            'title': 'Variable name',
            'description': 'Variable/band name in the NetCDF dataset.',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'keywords': ['variable', 'band']
        },
        'time_dimension': {
            'title': 'Time dimension name',
            'description': 'Name of time dimension in the NetCDF variable.',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 0,
            'maxOccurs': 1,
            'keywords': ['time']
        },
        'feature_id_property': {
            'title': 'Feature ID property',
            'description': 'Property name to use as feature id when id is not set.',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 0,
            'maxOccurs': 1,
            'keywords': ['id', 'feature']
        },
        'aggregation': {
            'title': 'Aggregation statistic',
            'description': 'Statistic to compute for each feature and time point.',
            'schema': {
                'type': 'string',
                'enum': ['mean', 'sum', 'min', 'max']
            },
            'minOccurs': 0,
            'maxOccurs': 1,
            'keywords': ['mean', 'sum', 'min', 'max']
        },
        'all_touched': {
            'title': 'All touched',
            'description': 'If true, include all raster cells touched by each geometry.',
            'schema': {
                'type': 'boolean'
            },
            'minOccurs': 0,
            'maxOccurs': 1,
            'keywords': ['mask']
        },
        'output_property': {
            'title': 'Output property name',
            'description': 'Name of property where time-series aggregation values are attached.',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 0,
            'maxOccurs': 1,
            'keywords': ['output']
        }
    },
    'outputs': {
        'features': {
            'title': 'Aggregated FeatureCollection',
            'description': 'GeoJSON FeatureCollection with per-time aggregated values attached to each feature.',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'netcdf': 'tests/data/chirps.nc',
            'geojson': {
                'type': 'FeatureCollection',
                'features': []
            },
            'variable': 'precip',
            'time_dimension': 'time',
            'feature_id_property': 'id',
            'aggregation': 'mean',
            'all_touched': False,
            'output_property': 'aggregated_values'
        }
    }
}


class AggregationProcessor(BaseProcessor):
    """Time-series zonal aggregation process."""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: plugins.aggregation.AggregationProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True

    def execute(self, data, outputs=None):
        try:
            import numpy as np
            import xarray as xr
            from rasterio.features import geometry_mask
        except Exception as exc:
            raise ProcessorExecuteError(
                'xarray, rasterio, and numpy are required for aggregation processing'
            ) from exc

        mimetype = 'application/json'
        netcdf_path = data.get('netcdf')
        geojson = data.get('geojson')
        variable = data.get('variable')
        time_dimension = data.get('time_dimension', 'time')
        feature_id_property = data.get('feature_id_property', 'id')
        aggregation = data.get('aggregation', 'mean')
        all_touched = bool(data.get('all_touched', False))
        output_property = data.get('output_property', 'aggregated_values')

        if not netcdf_path:
            raise ProcessorExecuteError('Cannot process without netcdf input')
        if not isinstance(geojson, dict):
            raise ProcessorExecuteError('geojson must be a GeoJSON FeatureCollection object')
        if geojson.get('type') != 'FeatureCollection':
            raise ProcessorExecuteError('geojson.type must be FeatureCollection')
        if not isinstance(geojson.get('features'), list):
            raise ProcessorExecuteError('geojson.features must be a list')
        if not variable:
            raise ProcessorExecuteError('Cannot process without variable input')
        if aggregation not in {'mean', 'sum', 'min', 'max'}:
            raise ProcessorExecuteError('aggregation must be one of: mean, sum, min, max')

        with xr.open_dataset(netcdf_path) as dataset:
            if variable not in dataset.variables:
                raise ProcessorExecuteError(
                    f"Variable '{variable}' not found in dataset"
                )

            data_array = dataset[variable]
            if time_dimension not in data_array.dims:
                raise ProcessorExecuteError(
                    f"Time dimension '{time_dimension}' not found in variable '{variable}'"
                )

            y_dim, x_dim = _resolve_spatial_dims(data_array)

            data_array = data_array.sortby(dataset[x_dim])
            data_array = data_array.sortby(dataset[y_dim], ascending=False)

            x_coords = data_array.coords[x_dim].values
            y_coords = data_array.coords[y_dim].values
            transform = _build_affine(x_coords, y_coords)

            output_features = []
            time_values = [
                _to_serializable_time(value)
                for value in data_array[time_dimension].values.tolist()
            ]

            reducer = getattr(np, f'nan{aggregation}')

            for index, feature in enumerate(geojson['features']):
                geometry = feature.get('geometry')
                if not geometry:
                    raise ProcessorExecuteError(f'Feature at index {index} is missing geometry')

                properties = dict(feature.get('properties') or {})
                feature_id = feature.get('id', properties.get(feature_id_property, index))

                mask = geometry_mask(
                    [geometry],
                    out_shape=(len(y_coords), len(x_coords)),
                    transform=transform,
                    invert=True,
                    all_touched=all_touched,
                )

                values = []
                for position, time_value in enumerate(time_values):
                    time_slice = data_array.isel({time_dimension: position}).values
                    masked = np.where(mask, time_slice, np.nan)
                    if np.isnan(masked).all():
                        aggregate_value = None
                    else:
                        aggregate_value = float(reducer(masked))
                    values.append({
                        'time': time_value,
                        'value': aggregate_value,
                    })

                properties[output_property] = values

                output_features.append({
                    'type': 'Feature',
                    'id': feature_id,
                    'geometry': geometry,
                    'properties': properties,
                })

            value = {
                'type': 'FeatureCollection',
                'features': output_features,
            }

        produced_outputs = {}
        if not bool(outputs) or 'features' in outputs:
            produced_outputs = {
                'id': 'features',
                'value': value
            }

        return mimetype, produced_outputs

    def __repr__(self):
        return f'<AggregationProcessor> {self.name}'