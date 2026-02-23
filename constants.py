
import json

# constants for org units bbox and country code (hacky hardcoded for now)
# TODO: these should be defined differently or retrieved from DHIS2 connection
BBOX = [-13.25, 6.79, -10.23, 10.05]
COUNTRY_CODE = 'SLE'
ORG_UNITS_GEOJSON = json.load(open('sierra-leone-districts.geojson'))
