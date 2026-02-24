import json
import geopandas as gpd

# constants for org units bbox and country code (hacky hardcoded for now)
# TODO: these should be defined differently or retrieved from DHIS2 connection
GEOJSON_FILE = 'brazil-regions.geojson'
COUNTRY_CODE = 'BRA'
ORG_UNITS_GEOJSON = json.load(open(GEOJSON_FILE))
BBOX = list(map(float, gpd.read_file(GEOJSON_FILE).total_bounds))
