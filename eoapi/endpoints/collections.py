from fastapi import APIRouter

router = APIRouter(tags=["Collections"])


@router.get("/collections")
def get_collections() -> dict:
    return {
        "collections": [],
        "links": [],
    }
