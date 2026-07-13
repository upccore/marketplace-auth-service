from fastapi import FastAPI

from src.infrastructure.persistence.database import (
    create_engine,
    create_session_factory,
)
from src.presentation.api.dependencies import setup
from src.presentation.api.middleware import TraceIdMiddleware
from src.presentation.api.routes.internal import router as internal_router
from src.presentation.api.routes.public import router as public_router
from src.settings import Settings


def create_app() -> FastAPI:
    settings = Settings()
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    app = FastAPI(title="Auth Service")
    app.add_middleware(TraceIdMiddleware)
    setup(settings, session_factory)
    app.include_router(public_router)
    app.include_router(internal_router)
    return app
