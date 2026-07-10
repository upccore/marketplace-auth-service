import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.presentation.api.dependencies import get_settings, get_uow
from src.presentation.api.routes.internal import router as internal_router
from src.presentation.api.routes.public import router as public_router
from src.settings import Settings
from tests.conftest import FakeUnitOfWork

JWT_SECRET = "test-secret-key-that-is-long-enough"

_test_settings = Settings(
    postgres_host="localhost",
    postgres_database_name="fake",
    postgres_password="fake",
    postgres_port=5432,
    postgres_username="fake",
    jwt_secret=JWT_SECRET,
    jwt_algorithm="HS256",
    jwt_expire_hours=24,
    jwt_refresh_expire_days=30,
)


@pytest.fixture
def app(fake_uow: FakeUnitOfWork) -> FastAPI:
    app = FastAPI()
    app.include_router(public_router)
    app.include_router(internal_router)

    app.dependency_overrides[get_uow] = lambda: fake_uow
    app.dependency_overrides[get_settings] = lambda: _test_settings

    return app


@pytest.fixture
async def client(app: FastAPI) -> AsyncClient:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
async def auth_headers(client: AsyncClient) -> dict[str, str]:
    await client.post(
        "/auth/register",
        json={"name": "Auth User", "email": "auth@test.com", "password": "password123"},
    )
    resp = await client.post(
        "/auth/login",
        json={"email": "auth@test.com", "password": "password123"},
    )
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}
