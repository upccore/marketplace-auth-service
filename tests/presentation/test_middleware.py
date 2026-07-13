import uuid

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.presentation.api.middleware import TraceIdMiddleware
from src.tracing import TRACE_ID_HEADER


@pytest.fixture
def middleware_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(TraceIdMiddleware)

    @app.get("/ping")
    async def ping() -> dict:
        return {"ok": True}

    return app


@pytest.fixture
async def middleware_client(middleware_app: FastAPI) -> AsyncClient:
    transport = ASGITransport(app=middleware_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


async def test_generates_trace_id_when_header_missing(
    middleware_client: AsyncClient,
) -> None:
    resp = await middleware_client.get("/ping")

    assert resp.status_code == 200
    trace_id = resp.headers.get(TRACE_ID_HEADER)
    assert trace_id
    uuid.UUID(trace_id)


async def test_reuses_incoming_trace_id(middleware_client: AsyncClient) -> None:
    resp = await middleware_client.get("/ping", headers={TRACE_ID_HEADER: "my-id"})

    assert resp.status_code == 200
    assert resp.headers.get(TRACE_ID_HEADER) == "my-id"
