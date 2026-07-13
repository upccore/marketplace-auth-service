import contextvars
import uuid

TRACE_ID_HEADER = "X-Trace-Id"
_NO_TRACE = "-"

_trace_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "trace_id", default=_NO_TRACE
)


def get_trace_id() -> str:
    return _trace_id.get()


def get_trace_id_or_none() -> str | None:
    value = _trace_id.get()
    return None if value == _NO_TRACE else value


def set_trace_id(value: str) -> contextvars.Token:
    return _trace_id.set(value)


def reset_trace_id(token: contextvars.Token) -> None:
    _trace_id.reset(token)


def new_trace_id() -> str:
    return str(uuid.uuid4())
