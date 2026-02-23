from fastapi import HTTPException


def not_found(resource: str, identifier: str) -> HTTPException:
    return HTTPException(
        status_code=404,
        detail={
            "code": "NotFound",
            "description": f"{resource} '{identifier}' not found",
        },
    )


def invalid_parameter(description: str) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={
            "code": "InvalidParameterValue",
            "description": description,
        },
    )
