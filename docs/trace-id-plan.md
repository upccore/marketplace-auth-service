# Trace ID сквозь auth/ad/search-service

## Context

Сейчас в auth-service, ad-service и search-service нет никакого correlation id: middleware, contextvars, конфигурации логирования (`logging.getLogger`/`basicConfig`/`dictConfig`) для HTTP-приложений нет вообще (только `bin/outbox.py` и `bin/consumer.py` вызывают голый `logging.basicConfig(level=logging.INFO)`), исходящие HTTP-клиенты (`auth_client.py`, `ad_client.py`) не проставляют заголовков, Kafka-сообщения не несут метаданных кроме `{"event", "payload"}`. Это мешает трассировать один запрос пользователя через всю цепочку auth → ad → Kafka outbox → search.

Цель: end-to-end trace id, который проходит через HTTP-входы, HTTP-клиенты между сервисами и Kafka (outbox → relay → consumer), без ручного прокидывания через сигнатуры use case'ов — только через `contextvars.ContextVar`, читаемый в infrastructure-слое (репозитории, HTTP-клиенты, логирование) и на границах транспорта (middleware, Kafka producer/consumer).

Каждый из трёх сервисов — независимый репозиторий, общего пакета нет, поэтому модули дублируются идентично в каждом (как уже дублируется паттерн `dependencies.py` globals+`setup()`).

**Решение по Kafka-транспорту** (подтверждено пользователем явно): **Kafka message headers**, а не поле в payload. Причины: "тонкое событие" `{"event":..., "payload":{"ad_id":...}}` из CLAUDE.md остаётся неизменным (не меняем бизнес-схему события ради инфраструктурной метаданной), consumer читает `msg.headers` независимо от `value_deserializer`, aiokafka 0.13 (уже используется) поддерживает `headers=` и у producer, и у `ConsumerRecord`. Значения в headers — bytes: producer кодирует `trace_id.encode()`, consumer декодирует обратно в строку при чтении.

## Общий модуль трассировки (одинаковый в каждом сервисе)

Новый файл `src/tracing.py` (рядом с `src/settings.py`, не в `infrastructure/` — как и `settings.py`, это сквозной примитив без привязки к транспорту):

```python
import contextvars
import uuid

TRACE_ID_HEADER = "X-Trace-Id"
_NO_TRACE = "-"

_trace_id: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default=_NO_TRACE)

def get_trace_id() -> str: ...
def get_trace_id_or_none() -> str | None: ...  # None вместо "-"
def set_trace_id(value: str) -> contextvars.Token: ...
def reset_trace_id(token: contextvars.Token) -> None: ...
def new_trace_id() -> str:  # str(uuid.uuid4())
```

Важно: сам объект `ContextVar` — module-level (это единственный способ его завести в Python), но **значение** trace id живёт per-task/per-request в контексте, а не в глобальной переменной — так и выполняется требование 2. Default `"-"` — чтобы лог-строки вне запроса (старт приложения, фон) не падали на `record.trace_id`.

`"-"` — это плейсхолдер только для форматтера логов (`logging_config.py`), он не должен протекать в персистентный слой (outbox). Поэтому рядом с `get_trace_id()` заводится `get_trace_id_or_none() -> str | None`, который возвращает `None` вместо `"-"`, когда contextvar не установлен (`return None if value == _NO_TRACE else value`). Логирование продолжает использовать `get_trace_id()` (нужна строка для форматтера), а persistence-код — `get_trace_id_or_none()`.

Идентичный файл создаётся в `auth-service`, `ad-service`, `search-service`.

## ASGI-middleware (одинаковый паттерн в каждом сервисе)

Новый файл `src/presentation/api/middleware.py`:

```python
class TraceIdMiddleware:
    def __init__(self, app): self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        headers = Headers(scope=scope)
        trace_id = headers.get(TRACE_ID_HEADER.lower()) or new_trace_id()
        token = set_trace_id(trace_id)

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                message["headers"].append((TRACE_ID_HEADER.lower().encode(), trace_id.encode()))
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            reset_trace_id(token)
```

**Решение**: чистый ASGI-middleware (не `starlette.middleware.base.BaseHTTPMiddleware`). `BaseHTTPMiddleware` во многих версиях Starlette выполняет endpoint в отдельной anyio-task через task group, из-за чего contextvar, установленный в middleware, не гарантированно виден внутри route-хендлера. Чистый ASGI-callable этой проблемы не имеет — весь путь выполняется в одном task. Заодно middleware проставляет `X-Trace-Id` в ответ (полезно для ручной проверки через curl).

Регистрация — `app.add_middleware(TraceIdMiddleware)` в каждом `src/fastapi.py`, сразу после `app = FastAPI(...)`. Применяется одинаково к публичным и `/internal/*` роутам (у auth и ad это значит, что входящий вызов от другого сервиса тоже получит trace id из уже проставленного заголовка).

**Тест** (в каждом из трёх сервисов, т.к. middleware дублируется в каждом репозитории отдельно): новый файл `tests/presentation/test_middleware.py`, через `TestClient`/`httpx.AsyncClient` на тестовом `create_app()`. Два обязательных кейса:
1. Запрос **без** `X-Trace-Id` в заголовках: middleware должен сгенерировать новый trace id и вернуть его в ответе (`response.headers["X-Trace-Id"]` присутствует и является валидным непустым значением, например uuid4).
2. Запрос **с явным** `X-Trace-Id: my-id`: в ответе должен вернуться тот же `my-id`, а не сгенерированный — это прямая проверка критерия приёмки (middleware берёт trace id из заголовка, если он есть, а не игнорирует его), не опциональное расширение.

## Логирование (одинаковый паттерн в каждом сервисе)

Новый файл `src/logging_config.py`:

```python
class TraceIdLogFilter(logging.Filter):
    def filter(self, record):
        record.trace_id = get_trace_id()
        return True

def configure_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler()
    handler.addFilter(TraceIdLogFilter())
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s [trace_id=%(trace_id)s] %(name)s: %(message)s"
    ))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
```

Вызывается один раз в начале каждого процесса-entrypoint, **до** создания приложения/engine:
- `bin/api.py` (все три сервиса) — сейчас нигде не настраивает логирование вообще, добавляем вызов.
- `ad-service/bin/outbox.py` — заменяем текущий `logging.basicConfig(level=logging.INFO)` на `configure_logging()`.
- `search-service/bin/consumer.py` — аналогично заменяем `logging.basicConfig(level=logging.INFO)`.

Не трогаем логгеры `uvicorn`/`uvicorn.access`/`uvicorn.error` — у них `propagate=False` и свои handlers, `disable_existing_loggers=False` в дефолтном конфиге uvicorn не сбросит наш root logger. В скоуп входят только логгеры самого приложения (`logging.getLogger(__name__)` в `auth_client.py`, `ad_client.py`, `outbox_relay.py`, `kafka_ads_consumer.py` и любые новые) — они propagate на root и получат `trace_id` через фильтр.

## auth-service — изменения

Нет исходящих HTTP-вызовов и Kafka вообще — только приёмная сторона.

1. `src/tracing.py` — новый файл (см. выше).
2. `src/presentation/api/middleware.py` — новый файл, `TraceIdMiddleware`.
3. `src/fastapi.py` — добавить `app.add_middleware(TraceIdMiddleware)` после создания `FastAPI(title="Auth Service")`. Lifespan по-прежнему не нужен (в этом сервисе `setup()` уже вызывается синхронно в `create_app()` — паттерн не меняется).
4. `src/logging_config.py` — новый файл.
5. `bin/api.py` — вызвать `configure_logging()` перед `create_app()`.

## ad-service — изменения

### HTTP-вход и логирование
1. `src/tracing.py`, `src/presentation/api/middleware.py`, `src/logging_config.py` — новые файлы (идентичны по структуре auth-service).
2. `src/fastapi.py` — `app.add_middleware(TraceIdMiddleware)` после `app = FastAPI(title="Ad Service", lifespan=lifespan)`.
3. `bin/api.py` — `configure_logging()` перед `create_app()`.
4. `bin/outbox.py` — заменить `logging.basicConfig(level=logging.INFO)` на `configure_logging()`.

### Исходящий HTTP-клиент → auth-service
5. `src/infrastructure/http/auth_client.py` — в `AuthServiceUserProfileService.user()` добавить заголовок при вызове:
   ```python
   resp = await self._client.get(url, headers={TRACE_ID_HEADER: get_trace_id()})
   ```
   Импорт `from src.tracing import TRACE_ID_HEADER, get_trace_id`.

### Outbox → Kafka
6. `src/infrastructure/persistence/models.py` — в `OutboxModel` добавить колонку:
   ```python
   trace_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
   ```
7. Новая Alembic-миграция (`make migrate-create name="add_trace_id_to_outbox"`) — `ALTER TABLE outbox ADD COLUMN trace_id VARCHAR(64)` (nullable — существующие/будущие строки без trace id не ломают constraint).
8. `src/application/ports/outbox.py` — в dataclass `OutboxMessage` добавить поле `trace_id: str | None = None`. Сигнатура метода `add(event_type, payload)` в порту **не меняется** — trace id туда не прокидывается вручную.
9. `src/infrastructure/persistence/outbox_repository.py`:
   - `add()`: читает `get_trace_id_or_none()` (не `get_trace_id()` — иначе плейсхолдер `"-"` из logging-контекста утёк бы в БД как значение trace id) и пишет в модель — `OutboxModel(event_type=event_type, payload=payload, trace_id=get_trace_id_or_none())`. Это единственное место, где trace id попадает в БД — вызывающие use cases (`create_ad.py`, `update_ad.py`, `delete_ad.py`) не меняются вообще.
   - `fetch_unpublished()`: добавить `m.trace_id` в конструктор `OutboxMessage(..., trace_id=m.trace_id)`.
10. `src/application/ports/message_broker.py` — сигнатура `send()` меняется на `send(self, payload: dict[str, Any], trace_id: str | None = None) -> None` (порт и реализация должны совпадать дословно — оба с `= None`, а не порт обязательным, а реализация с дефолтом).
11. `src/infrastructure/messaging/kafka_broker.py` — `KafkaMessageBroker.send()`, сигнатура зеркалит порт:
    ```python
    async def send(self, payload: dict[str, Any], trace_id: str | None = None) -> None:
        headers = [(TRACE_ID_HEADER, (trace_id or new_trace_id()).encode())]
        await self._producer.send_and_wait(self._topic, payload, headers=headers)
    ```
    (fallback на `new_trace_id()`, если строка в outbox почему-то оказалась без trace id — не роняем событие).
12. `src/application/services/outbox_relay.py` — `_process_batch()`: `await self._broker.send({"event": message.event_type, "payload": message.payload}, trace_id=message.trace_id)`.

### Тесты, которые потребуют правки (обновить сигнатуры фейков, не логику)
- `tests/conftest.py` — `FakeOutboxRepository`/`FakeMessageBroker` (или как они называются в проекте) должны принять новое поле/параметр, чтобы существующие usecase- и presentation-тесты не падали на несовпадении сигнатур.
- `tests/services/test_outbox_relay.py` — расширить проверку, что `trace_id` из outbox-строки доходит до `broker.send(...)`.

## search-service — изменения

### HTTP-вход и логирование
1. `src/tracing.py`, `src/presentation/api/middleware.py`, `src/logging_config.py` — новые файлы (идентичны).
2. `src/fastapi.py` — `app.add_middleware(TraceIdMiddleware)` после `app = FastAPI(title="Search Service", lifespan=lifespan)`.
3. `bin/api.py` — `configure_logging()` перед `create_app()`.
4. `bin/consumer.py` — заменить `logging.basicConfig(level=logging.INFO)` на `configure_logging()`.

### Исходящий HTTP-клиент → ad-service
5. `src/infrastructure/http/ad_client.py` — в `AdServiceAdSource.get()` добавить заголовок:
   ```python
   resp = await self._client.get(url, headers={TRACE_ID_HEADER: get_trace_id()})
   ```

### Kafka consumer
6. `src/application/services/kafka_ads_consumer.py` — `KafkaAdsConsumer.run()`. Текущий код (уже существует, не создаётся заново):
   ```python
   async for msg in self._consumer:
       try:
           await self._handle(msg.value)
       except Exception:
           logger.exception("failed to handle message %s", msg)
           continue
       await self._consumer.commit()
   ```
   Изменение для trace id — добавить извлечение/установку/сброс contextvar вокруг уже существующего `try/except`, не трогая его логику:
   ```python
   async for msg in self._consumer:
       trace_id = extract_trace_id(msg.headers) or new_trace_id()
       token = set_trace_id(trace_id)
       try:
           await self._handle(msg.value)
       except Exception:
           logger.exception("failed to handle message %s", msg)
           continue
       finally:
           reset_trace_id(token)
       await self._consumer.commit()
   ```
   Из связанного с trace id: (а) новый helper `extract_trace_id(headers: Sequence[tuple[str, bytes]] | None) -> str | None`, ищущий ключ `TRACE_ID_HEADER` и декодирующий значение из bytes; (б) `trace_id = ...` / `token = set_trace_id(...)` перед `try`; (в) `finally: reset_trace_id(token)`.
   Отдельно от trace id, но затрагивает тот же участок кода: сам `try/except Exception: logger.exception(...); continue` — это **существующая** обработка ошибок обработки сообщения (была в коде до задачи трассировки), она не добавляется и не меняется по сути — только оборачивается новым `finally`. Это стоит явно отметить при код-ревью, чтобы не приписывать эту логику изменениям по trace id.
   `finally` гарантирует сброс contextvar перед следующей итерацией цикла (один и тот же task на весь consumer loop — без сброса trace id "утекал" бы в соседние сообщения).
   Благодаря установленному contextvar, `_index_ad`/`_remove_ad` → `AdServiceAdSource.get()` внутри той же обработки автоматически передаст тот же trace id дальше в `GET /internal/ads/{id}` на ad-service — цепочка замыкается.

**Тест**: новый файл `tests/services/test_kafka_ads_consumer.py` (в search-service сейчас нет `tests/services/` вообще — завести директорию с `__init__.py` по аналогии с `ad-service/tests/services/`). Обязательные кейсы:
1. Собрать fake/stub `ConsumerRecord`-подобный объект (или сам `msg`) с `headers=[("X-Trace-Id", b"given-trace-id")]` и `value={"event": "ad.created", "payload": {"ad_id": 1}}`, прогнать через `KafkaAdsConsumer._handle` (или через `run()` с fake async-итератором из одного сообщения) с фейковыми `index_ad`/`remove_ad` usecase-заглушками, и убедиться, что во время обработки `get_trace_id() == "given-trace-id"` (например, fake usecase записывает `get_trace_id()` в атрибут при вызове `execute()`, тест проверяет это значение после `run()`).
2. `extract_trace_id(None) is None` и `extract_trace_id([]) is None` — обязательный кейс, не опциональный: без него нет защиты от регрессии на сообщениях без trace id заголовка (consumer не должен падать/ломать обработку, когда producer или старое сообщение не несёт `X-Trace-Id`).

## End-to-end цепочка после изменений

1. Клиент → `POST /ads` (ad-service) без `X-Trace-Id` → middleware генерирует `T1`, кладёт в contextvar.
2. `CreateAd` usecase → `outbox.add()` → repository сам читает `get_trace_id_or_none()` = `T1`, пишет строку outbox с `trace_id=T1` в той же транзакции.
3. `OutboxRelay` (отдельный процесс) вычитывает строку, `broker.send(payload, trace_id=T1)` → Kafka-сообщение с header `X-Trace-Id: T1`.
4. `KafkaAdsConsumer` (search-service) читает `msg.headers`, `set_trace_id(T1)`.
5. `IndexAd` usecase → `AdServiceAdSource.get(ad_id)` → HTTP GET на `ad-service:/internal/ads/{id}` с заголовком `X-Trace-Id: T1`.
6. ad-service middleware видит входящий `X-Trace-Id: T1`, использует его же (не генерирует новый) — все логи по этому внутреннему запросу тоже помечены `T1`.
7. Все `logger.*` вызовы на всех этапах (1)-(6) содержат `[trace_id=T1]` в строке лога.

## Верификация

1. `make check` (ruff check --fix + ruff format) и `make test` в каждом из трёх сервисов (auth-service, ad-service, search-service) — обновлённые фейки/тесты должны проходить, линтер — без замечаний.
2. Ручная проверка сквозного потока (нужны все 5 процессов, см. корневой `CLAUDE.md`):
   - `curl -i -X POST http://localhost:8002/ads -H "Authorization: Bearer <jwt>" -H "Content-Type: application/json" -d '{...}'` — проверить, что в ответе есть заголовок `X-Trace-Id`, и что этот же id встречается в логах ad-service (`make run`), `make outbox`, `make consumer` (search-service) и в логах search-service `make run` (когда `IndexAd` дёргает `/internal/ads/{id}`).
   - Повторить с явным `-H "X-Trace-Id: my-test-trace"` — убедиться, что id не перегенерируется, а используется указанный, и что он же долетает до Kafka и до search-service.
3. Проверить `GET /search?q=...` после индексации — в логах search-service должен быть тот же trace id, что и в изначальном POST (если оба запроса выполнены с одним explicit `X-Trace-Id`).

## Фактическая проверка (выполнено локально)

План был реализован во всех трёх сервисах и провалидирован:
- `make check` + `make test` — auth-service и search-service чистые; ad-service падает на `make check` только из-за pre-existing долга в `bin/seed.py` (файл не менялся). Все тесты зелёные: 34 (auth) + 55 (ad) + 23 (search) = 112 passed.
- Сквозной e2e-прогон с поднятым docker-инфра (Postgres × 3, Redpanda) и всеми 5 процессами: `X-Trace-Id: my-test-trace-e2e`, отправленный в `POST /ads`, дошёл через outbox → Kafka → search-service consumer → обратный HTTP-вызов к ad-service (обогащение имени через auth-service) → `GET /search` — один и тот же id виден во всех логах и во всех ответах.
