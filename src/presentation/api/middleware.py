from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from src.tracing import TRACE_ID_HEADER, new_trace_id, reset_trace_id, set_trace_id


class TraceIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        trace_id = headers.get(TRACE_ID_HEADER.lower()) or new_trace_id()
        token = set_trace_id(trace_id)

        async def send_wrapper(message: dict) -> None:
            if message["type"] == "http.response.start":
                message["headers"].append(
                    (TRACE_ID_HEADER.lower().encode(), trace_id.encode())
                )
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            reset_trace_id(token)
