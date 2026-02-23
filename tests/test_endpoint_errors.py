from eoapi.endpoints.errors import invalid_parameter, not_found


def test_not_found_error_payload() -> None:
    error = not_found("Collection", "missing-id")

    assert error.status_code == 404
    assert error.detail == {
        "code": "NotFound",
        "description": "Collection 'missing-id' not found",
    }


def test_invalid_parameter_error_payload() -> None:
    error = invalid_parameter("bbox must contain 4 comma-separated numbers")

    assert error.status_code == 400
    assert error.detail == {
        "code": "InvalidParameterValue",
        "description": "bbox must contain 4 comma-separated numbers",
    }
