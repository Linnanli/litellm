import logging
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from hmac import compare_digest
from os import environ
from typing import Annotated, Final, TypeAlias
from uuid import uuid4

import anyio
import httpx
from fastapi import APIRouter, Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from prisma.errors import PrismaError, UniqueViolationError
from redis.asyncio import Redis, RedisCluster
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

from litellm.proxy.deps.database import DatabaseDep

from . import inference
from .contracts import (
    BudgetLookup,
    BudgetNew,
    JsonObject,
    KeyDelete,
    KeyNew,
    KeyUpdate,
    MemberAdd,
    MemberIdentity,
    MemberUpdate,
    OrganizationNew,
    OrganizationUpdate,
    PublicError,
    TagLookup,
    TagNew,
    TagUpdate,
    TeamNew,
    TeamUpdate,
    UserNew,
    UserUpdate,
)
from .ledger import Ledger
from .reporting import ReportingQuery, build_reporting_router
from .security import Admission, admission_client, bearer, fingerprint
from .settings import Settings
from .storage import Database
from .subjects import Subjects

_LOG: Final = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(UTC)


def gateway_lifespan(
    config: Settings,
    clock: Callable[[], datetime],
    hook_transport: httpx.AsyncBaseTransport | None,
) -> Callable[[FastAPI], AbstractAsyncContextManager[Mapping[str, object]]]:
    from litellm.proxy import proxy_server

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[Mapping[str, object], None]:
        async with (
            proxy_server.proxy_startup_event(app, register_spend_callbacks=False) as native,
            admission_client(config, hook_transport) as hook,
        ):
            writer: Final = native["database"]
            if writer is None:
                raise RuntimeError("LiteLLM PostgreSQL writer is required")
            coordination: Final = proxy_server.redis_usage_cache
            if coordination is None:
                raise RuntimeError("LiteLLM coordination Redis is required")
            redis: Final[Redis[str | bytes] | RedisCluster[str | bytes]] = coordination.init_async_client()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # native cache omits Redis's response type
            if not isinstance(redis, Redis):
                raise TypeError("Daswork requires standalone coordination Redis")
            db: Final = Database(writer.prisma.writer_db)
            ledger: Final = Ledger(db, redis, clock)
            async with anyio.create_task_group() as tasks:
                await tasks.start(ledger.recovery_worker)
                try:
                    yield {
                        **native,
                        "subjects": Subjects(db, clock),
                        "ledger": ledger,
                        "admission": Admission(hook, config.admission_endpoint),
                    }
                finally:
                    tasks.cancel_scope.cancel()

    return lifespan


def create_app(
    settings: Settings | None = None,
    *,
    clock: Callable[[], datetime] = utc_now,
    hook_transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    config: Final = settings or Settings.load()
    if environ.get("LITELLM_LICENSE"):
        raise ValueError("Daswork gateway must start without a LiteLLM license")
    from litellm.proxy import proxy_server

    app: Final = FastAPI(
        lifespan=gateway_lifespan(config, clock, hook_transport), docs_url=None, redoc_url=None, openapi_url=None
    )

    async def master(request: Request) -> None:
        headers: Final = tuple(request.headers.items())
        key: Final = bearer(headers)
        if not compare_digest(key.encode(), config.master_key.get_secret_value().encode()):
            raise PublicError(403, "master_key_required")

    app.include_router(management(master))
    app.include_router(build_reporting_router(require_master_key=master, get_query=report_reader))

    @app.exception_handler(PublicError)
    async def public_error(_: Request, error: PublicError) -> JSONResponse:
        return JSONResponse({"error": {"message": str(error), "type": error.category}}, status_code=error.status)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_: Request, __: RequestValidationError) -> JSONResponse:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return JSONResponse({"error": {"message": "invalid_request", "type": "invalid_request"}}, status_code=400)

    @app.exception_handler(PrismaError)
    async def database_error(_: Request, error: Exception) -> JSONResponse:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        if isinstance(error, UniqueViolationError):
            return JSONResponse({"error": {"message": "subject_conflict", "type": "subject_conflict"}}, status_code=409)
        _LOG.warning("checkpoint=database category=%s", type(error).__name__)
        return JSONResponse(
            {"error": {"message": "database_unavailable", "type": "database_unavailable"}}, status_code=503
        )

    @app.get("/health/liveliness")
    @app.get("/health/readiness")
    async def health() -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return {"status": "healthy"}

    @app.post("/responses")
    @app.post("/v1/responses")
    async def respond(request: Request, ledger: LedgerDep, admission: AdmissionDep) -> Response:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        key: Final = bearer(tuple(request.headers.items()))
        return await inference.responses(request, ledger, admission, fingerprint(key), str(uuid4()))

    @app.middleware("http")
    async def native_management(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        if not is_native_management(request.method, request.url.path):
            return await call_next(request)
        try:
            await master(request)
        except PublicError as error:
            return await public_error(request, error)
        return await call_next(request)

    app.mount("/", NativeManagement(proxy_server.app))
    return app


async def report_reader(database: DatabaseDep) -> ReportingQuery:
    if database is None:
        raise PublicError(503, "database_unavailable")
    reader: Final = Database(database.prisma.writer_db)

    async def query(statement: str, *values: object) -> tuple[JsonObject, ...]:
        return await reader.rows(statement, values)

    return query


def subjects_dep(request: Request) -> Subjects:
    value: Final[object] = request.state.subjects  # pyright: ignore[reportAny]  # Starlette State is dynamic
    if isinstance(value, Subjects):
        return value
    raise PublicError(503, "subjects_unavailable")


def ledger_dep(request: Request) -> Ledger:
    value: Final[object] = request.state.ledger  # pyright: ignore[reportAny]  # Starlette State is dynamic
    if isinstance(value, Ledger):
        return value
    raise PublicError(503, "ledger_unavailable")


def admission_dep(request: Request) -> Admission:
    value: Final[object] = request.state.admission  # pyright: ignore[reportAny]  # Starlette State is dynamic
    if isinstance(value, Admission):
        return value
    raise PublicError(503, "admission_unavailable")


SubjectsDep: TypeAlias = Annotated[Subjects, Depends(subjects_dep)]
LedgerDep: TypeAlias = Annotated[Ledger, Depends(ledger_dep)]
AdmissionDep: TypeAlias = Annotated[Admission, Depends(admission_dep)]


def management(master: Callable[[Request], Awaitable[None]]) -> APIRouter:  # noqa: C901  # the declarations below register HTTP operations
    router: Final = APIRouter(dependencies=[Depends(master)])

    @router.post("/budget/new")
    async def budget_new(data: BudgetNew, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.budget_new(data)

    @router.post("/budget/info")
    async def budget_info(data: BudgetLookup, subjects: SubjectsDep) -> tuple[JsonObject, ...]:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.budget_info(data)

    @router.post("/organization/new")
    async def organization_new(data: OrganizationNew, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.organization_new(data)

    @router.get("/organization/info")
    async def organization_info(organization_id: str, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.organization_info(organization_id)

    @router.patch("/organization/update")
    async def organization_update(data: OrganizationUpdate, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.organization_update(data)

    @router.post("/tag/new")
    async def tag_new(data: TagNew, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.tag_new(data)

    @router.post("/tag/info")
    async def tag_info(data: TagLookup, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.tag_info(data)

    @router.post("/tag/update")
    async def tag_update(data: TagUpdate, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.tag_update(data)

    @router.post("/user/new")
    async def user_new(data: UserNew, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.user_new(data)

    @router.get("/user/info")
    async def user_info(user_id: str, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.user_info(user_id)

    @router.post("/user/update")
    async def user_update(data: UserUpdate, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.user_update(data)

    @router.post("/team/new")
    async def team_new(data: TeamNew, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.team_new(data)

    @router.get("/team/info")
    async def team_info(team_id: str, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.team_info(team_id)

    @router.post("/team/update")
    async def team_update(data: TeamUpdate, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.team_update(data)

    @router.post("/team/member_add")
    async def member_add(data: MemberAdd, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.member_add(data)

    @router.post("/team/member_update")
    async def member_update(data: MemberUpdate, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.member_update(data)

    @router.post("/team/member_delete")
    async def member_delete(data: MemberIdentity, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.member_delete(data)

    @router.post("/key/generate")
    async def key_generate(data: KeyNew, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.key_new(data)

    @router.post("/key/update")
    async def key_update(data: KeyUpdate, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.key_update(data)

    @router.get("/key/list")
    async def key_list(  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        subjects: SubjectsDep,
        key_alias: str | None = None,
        team_id: str | None = None,
        user_id: str | None = None,
        substring_matching: bool = False,
        return_full_object: bool = True,
        page: Annotated[int, Query(ge=1)] = 1,
        size: Annotated[int, Query(ge=1, le=100)] = 20,
    ) -> JsonObject:
        if substring_matching or not return_full_object:
            raise PublicError(400, "unsupported_key_query")
        return await subjects.key_list(key_alias, team_id, user_id, page, size)

    @router.post("/key/delete")
    async def key_delete(data: KeyDelete, subjects: SubjectsDep) -> JsonObject:  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        return await subjects.key_delete(data)

    return router


_NATIVE_FIXED: Final = frozenset(
    {
        ("GET", "/model/info"),
        ("GET", "/v1/model/info"),
        ("GET", "/health"),
        ("GET", "/utils/model_info"),
        ("GET", "/model/cost_map/source"),
        ("POST", "/reload/model_cost_map"),
        ("GET", "/spend/logs/v2"),
        ("GET", "/credentials"),
        ("POST", "/credentials"),
        ("POST", "/model/new"),
        ("POST", "/model/delete"),
        ("POST", "/model/update"),
    }
)


def is_native_management(method: str, path: str) -> bool:
    segments: Final = path.split("/")
    return (
        (method, path) in _NATIVE_FIXED
        or (method == "PATCH" and len(segments) == 4 and segments[1] == "model" and segments[3] == "update")
        or (method in {"PATCH", "DELETE"} and len(segments) == 3 and segments[1] == "credentials")
        or (method == "GET" and len(segments) == 4 and segments[1:3] == ["credentials", "by_name"])
    )


class NativeManagement:
    def __init__(self, native: ASGIApp) -> None:
        self.native = native

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            request: Final = Request(scope)
            if is_native_management(request.method, request.url.path):
                await self.native(scope, receive, send)
                return
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        await JSONResponse({"detail": "Not Found"}, status_code=404)(scope, receive, send)
