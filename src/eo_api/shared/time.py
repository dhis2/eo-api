from typing import Any

import numpy as np

def numpy_datetime_to_period_string(datetimes: np.ndarray[Any, Any], period_type: str) -> np.ndarray[Any, Any]:
    """Convert an array of numpy datetimes to truncated period strings."""
    # TODO: this and numpy_period_string should be merged
    s = np.datetime_as_string(datetimes, unit="s")

    # Map periods to string lengths: YYYY-MM-DDTHH (13), YYYY-MM-DD (10), etc.
    lengths = {"hourly": 13, "daily": 10, "monthly": 7, "yearly": 4}
    return s.astype(f"U{lengths[period_type]}")