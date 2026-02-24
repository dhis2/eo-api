"""DHIS2 EO EDR provider for pygeoapi."""

from typing import Any

from pygeoapi.provider.base_edr import BaseEDRProvider


class DHIS2EOProvider(BaseEDRProvider):
    """Minimal EDR provider example."""

    def get_fields(self) -> dict[str, dict[str, Any]]:
        """Return available fields."""
        return {
            "value": {
                "type": "number",
                "title": "Value",
                "x-ogc-unit": "mm/day",
            }
        }

    def position(self, **kwargs: Any) -> dict[str, Any]:
        """Return coverage data for a point position."""
        return {
            "type": "Coverage",
            "domain": {
                "type": "Domain",
                "domainType": "Point",
                "axes": {
                    "Long": {"values": [0.0]},
                    "Lat": {"values": [0.0]},
                },
            },
            "parameters": {"value": {"type": "Parameter"}},
            "ranges": {
                "value": {
                    "type": "NdArray",
                    "dataType": "float",
                    "axisNames": ["Long", "Lat"],
                    "shape": [1, 1],
                    "values": [10.0],
                }
            },
        }
