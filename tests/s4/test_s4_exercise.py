from dotenv import load_dotenv

load_dotenv()

import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from starlette import status  # noqa: E402

from bdi_api.s4.exercise import s4, settings  # noqa: E402

settings.s3_bucket = "bdi-aircraft-klimkin"


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI(title="S4 Tests")
    app.include_router(s4)
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    yield TestClient(app)


class TestS4Exercise:
    def test_s4_download_with_limit(self, client: TestClient) -> None:
        with client as c:
            response = c.post("/api/s4/aircraft/download?file_limit=2")
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == "OK"

    def test_s4_prepare(self, client: TestClient) -> None:
        with client as c:
            response = c.post("/api/s4/aircraft/prepare")
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == "OK"
