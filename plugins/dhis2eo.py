from pygeoapi.provider.base import BaseProvider


class DHIS2EOProvider(BaseProvider):
    """DHIS2 EO Provider"""

    def __init__(self, provider_def):
        """Inherit from parent class"""

        super().__init__(provider_def)
        self._coverage_properties = self._get_coverage_properties()
        self.axes = self._coverage_properties['axes']
        self.crs = self._coverage_properties['bbox_crs']
        self.num_bands = self._coverage_properties['num_bands']
        self.get_fields()

    def _get_coverage_properties(self) -> dict:
        return {
            'bbox': [-180.0, -60.0, 180.0, 60.0],
            'bbox_crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
            'crs_type': 'GeographicCRS',
            'bbox_units': 'deg',
            'x_axis_label': 'Long',
            'y_axis_label': 'Lat',
            'width': 2400,
            'height': 800,
            'resx': 0.15,
            'resy': 0.15,
            'num_bands': 1,
            'tags': {},
            'axes': ['Long', 'Lat'],
        }

    def get_fields(self):
        # generate a JSON Schema of coverage band metadata
        self._fields = {
            'testfield': {
                'type': 'number'
            }
        }
        return self._fields

    def query(self, bands=None, subsets=None, format_='json', **kwargs):
        # process bands and subsets parameters
        # query/extract coverage data

        if bands is None:
            bands = []
        if subsets is None:
            subsets = {}

        # optionally specify the output filename pygeoapi can use as part of the response (HTTP Content-Disposition header)
        self.filename = 'my-cool-filename.dat'

        if format_ == 'json':
            # return a CoverageJSON representation
            return {
                'type': 'Coverage',
                'domain': {
                    'type': 'Domain',
                    'domainType': 'Grid',
                    'axes': {
                        'Long': {'start': -180.0, 'stop': 180.0, 'num': 2},
                        'Lat': {'start': -60.0, 'stop': 60.0, 'num': 2},
                    },
                    'referencing': [{
                        'coordinates': ['Long', 'Lat'],
                        'system': {
                            'type': 'GeographicCRS',
                            'id': self.crs,
                        },
                    }],
                },
                'parameters': {
                    'testfield': {
                        'type': 'Parameter',
                        'description': {'en': 'Test field'},
                        'unit': {
                            'label': {'en': 'mm/day'},
                            'symbol': {'value': 'mm/day', 'type': 'http://www.opengis.net/def/uom/UCUM/'},
                        },
                    }
                },
                'ranges': {
                    'testfield': {
                        'type': 'NdArray',
                        'dataType': 'float',
                        'axisNames': ['Lat', 'Long'],
                        'shape': [2, 2],
                        'values': [10.0, 12.5, 8.2, 9.1],
                    }
                },
            }
        else:
            # return default (likely binary) representation
            return bytes(112)