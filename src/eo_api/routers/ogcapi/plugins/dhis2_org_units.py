"""DHIS2 Organization Units feature provider for pygeoapi."""

from pygeoapi.provider.base import BaseProvider


class DHIS2OrgUnitsProvider(BaseProvider):
    """DHIS2 Organization Units Provider."""

    def __init__(self, provider_def):
        """Inherit from parent class."""
        super().__init__(provider_def)

    def get_fields(self):
        """Return fields and their datatypes."""
        return {"field1": "string", "field2": "string"}

    def query(
        self,
        offset=0,
        limit=10,
        resulttype="results",
        bbox=None,
        datetime_=None,
        properties=None,
        sortby=None,
        select_properties=None,
        skip_geometry=False,
        **kwargs,
    ):
        """Return feature collection matching the query parameters."""
        if bbox is None:
            bbox = []
        if properties is None:
            properties = []
        if sortby is None:
            sortby = []
        if select_properties is None:
            select_properties = []

        # optionally specify the output filename pygeoapi can use as part
        # of the response (HTTP Content-Disposition header)
        self.filename = "my-cool-filename.dat"

        # open data file (self.data) and process, return
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "371",
                    "geometry": {"type": "Point", "coordinates": [-75, 45]},
                    "properties": {"stn_id": "35", "datetime": "2001-10-30T14:24:55Z", "value": "89.9"},
                }
            ],
        }

    def get_schema(self):
        """Return a JSON schema for the provider."""
        return (
            "application/geo+json",
            {"$ref": "https://geojson.org/schema/Feature.json"},
        )
