from fastapi import FastAPI
from fastapi.testclient import TestClient

from eoapi.endpoints.root import router as root_router


app = FastAPI()

app.include_router(root_router)


def test_landing_page_links() -> None:
    client = TestClient(app)

    response = client.get("/")

    assert response.status_code == 200
    payload = response.json()
    assert payload["title"] == "DHIS2 EO API"
    rels = {link["rel"] for link in payload["links"]}
    assert {"self", "conformance", "data", "service-doc"}.issubset(rels)
