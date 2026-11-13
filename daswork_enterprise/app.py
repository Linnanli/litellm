import logging
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from hmac import compare_digest
from typing import Annotated, Final
from uuid import uuid4

import httpx
from fastapi import APIRouter, Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from psycopg import Error as DatabaseError
from psycopg.errors import ForeignKeyViolation, UniqueViolation
from psycopg_pool import AsyncConnectionPool, PoolTimeout
from pydantic import AwareDatetime, JsonValue
from redis.asyncio import Redis
from starlette.responses import JSONResponse, Response

from . import inference
from .contracts import (
    BudgetLookup, BudgetNew, JsonObject, KeyDelete, KeyNew, KeyUpdate, MemberAdd, MemberIdentity, MemberUpdate,
    OrganizationNew, OrganizationUpdate, PublicError, TagLookup, TagNew, TagUpdate, TeamNew, TeamUpdate, UserNew,
    UserUpdate,
)
from .ledger import Ledger
from .models import ModelDelete, ModelNew, Models
from .security import Admission, bearer, fingerprint
from .settings import Settings
from .storage import Database
from .subjects import Subjects

_LOG: Final = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(UTC)


def create_app(
    settings: Settings | None = None, *, clock: Callable[[], datetime] = utc_now,
    hook_transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    config: Final = settings or Settings.load()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[Mapping[str, object]]:
        store: Final = AsyncConnectionPool(
            conninfo=config.database_url.get_secret_value(), min_size=1, max_size=4, open=False, timeout=2,
            kwargs={"options": "-c timezone=UTC -c statement_timeout=4000 -c lock_timeout=3000"},
        )
        coordination: Final = config.config.general_settings.coordination_redis
        password: Final = coordination.password
        redis: Final = Redis(
            host=coordination.host, port=coordination.port, db=coordination.db,
            password=None if password is None else password.get_secret_value(),
            socket_connect_timeout=1, socket_timeout=2, retry_on_timeout=False,
        )
        async with store, redis, httpx.AsyncClient(
            verify=config.tls(), trust_env=False, transport=hook_transport, timeout=3.0, follow_redirects=False,
        ) as hook:
            db: Final = Database(store)
            await db.migrate()
            models: Final = Models(db)
            for model in config.config.model_list:
                try:
                    await models.add(model)
                except UniqueViolation:
                    pass
            yield {
                "subjects": Subjects(db, clock), "models": models, "ledger": Ledger(db, redis, clock),
                "admission": Admission(hook, config.admission_endpoint),
            }

    app: Final = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    def master(request: Request) -> None:
        headers: Final = tuple(request.headers.items())
        key: Final = bearer(headers)
        if not compare_digest(key.encode(), config.master_key.get_secret_value().encode()):
            raise PublicError(403, "master_key_required")

    app.include_router(management(master))

    @app.exception_handler(PublicError)
    async def public_error(_: Request, error: PublicError) -> JSONResponse:
        return JSONResponse({"error": {"message": str(error), "type": error.category}}, status_code=error.status)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_: Request, __: RequestValidationError) -> JSONResponse:
        return JSONResponse({"error": {"message": "invalid_request", "type": "invalid_request"}}, status_code=400)

    @app.exception_handler(DatabaseError)
    @app.exception_handler(PoolTimeout)
    async def database_error(_: Request, error: Exception) -> JSONResponse:
        if isinstance(error, (UniqueViolation, ForeignKeyViolation)):
            return JSONResponse({"error": {"message": "subject_conflict", "type": "subject_conflict"}}, status_code=409)
        _LOG.warning("checkpoint=database category=%s", type(error).__name__)
        return JSONResponse({"error": {"message": "database_unavailable", "type": "database_unavailable"}}, status_code=503)

    @app.get("/health/liveliness")
    @app.get("/health/readiness")
    async def health() -> JsonObject:
        return {"status": "healthy"}

    @app.post("/responses")
    @app.post("/v1/responses")
    async def respond(request: Request, ledger: LedgerDep, admission: AdmissionDep) -> Response:
        key: Final = bearer(tuple(request.headers.items()))
        return await inference.responses(request, ledger, admission, fingerprint(key), str(uuid4()))

    return app


def subjects_dep(request: Request) -> Subjects:
    value: Final = request.state.subjects
    if isinstance(value, Subjects):
        return value
    raise PublicError(503, "subjects_unavailable")


def model_dep(request: Request) -> Models:
    value: Final = request.state.models
    if isinstance(value, Models):
        return value
    raise PublicError(503, "models_unavailable")


def ledger_dep(request: Request) -> Ledger:
    value: Final = request.state.ledger
    if isinstance(value, Ledger):
        return value
    raise PublicError(503, "ledger_unavailable")


def admission_dep(request: Request) -> Admission:
    value: Final = request.state.admission
    if isinstance(value, Admission):
        return value
    raise PublicError(503, "admission_unavailable")


SubjectsDep = Annotated[Subjects, Depends(subjects_dep)]
ModelsDep = Annotated[Models, Depends(model_dep)]
LedgerDep = Annotated[Ledger, Depends(ledger_dep)]
AdmissionDep = Annotated[Admission, Depends(admission_dep)]


def management(master: Callable[[Request], None]) -> APIRouter:
    router: Final = APIRouter(dependencies=[Depends(master)])

    @router.post("/budget/new")
    async def budget_new(data: BudgetNew, subjects: SubjectsDep) -> JsonObject:
        return await subjects.budget_new(data)

    @router.post("/budget/info")
    async def budget_info(data: BudgetLookup, subjects: SubjectsDep) -> tuple[JsonObject, ...]:
        return await subjects.budget_info(data)

    @router.post("/organization/new")
    async def organization_new(data: OrganizationNew, subjects: SubjectsDep) -> JsonObject:
        return await subjects.organization_new(data)

    @router.get("/organization/info")
    async def organization_info(organization_id: str, subjects: SubjectsDep) -> JsonObject:
        return await subjects.organization_info(organization_id)

    @router.patch("/organization/update")
    async def organization_update(data: OrganizationUpdate, subjects: SubjectsDep) -> JsonObject:
        return await subjects.organization_update(data)

    @router.post("/tag/new")
    async def tag_new(data: TagNew, subjects: SubjectsDep) -> JsonObject:
        return await subjects.tag_new(data)

    @router.post("/tag/info")
    async def tag_info(data: TagLookup, subjects: SubjectsDep) -> JsonObject:
        return await subjects.tag_info(data)

    @router.post("/tag/update")
    async def tag_update(data: TagUpdate, subjects: SubjectsDep) -> JsonObject:
        return await subjects.tag_update(data)

    @router.post("/user/new")
    async def user_new(data: UserNew, subjects: SubjectsDep) -> JsonObject:
        return await subjects.user_new(data)

    @router.get("/user/info")
    async def user_info(user_id: str, subjects: SubjectsDep) -> JsonObject:
        return await subjects.user_info(user_id)

    @router.post("/user/update")
    async def user_update(data: UserUpdate, subjects: SubjectsDep) -> JsonObject:
        return await subjects.user_update(data)

    @router.post("/team/new")
    async def team_new(data: TeamNew, subjects: SubjectsDep) -> JsonObject:
        return await subjects.team_new(data)

    @router.get("/team/info")
    async def team_info(team_id: str, subjects: SubjectsDep) -> JsonObject:
        return await subjects.team_info(team_id)

    @router.post("/team/update")
    async def team_update(data: TeamUpdate, subjects: SubjectsDep) -> JsonObject:
        return await subjects.team_update(data)

    @router.post("/team/member_add")
    async def member_add(data: MemberAdd, subjects: SubjectsDep) -> JsonObject:
        return await subjects.member_add(data)

    @router.post("/team/member_update")
    async def member_update(data: MemberUpdate, subjects: SubjectsDep) -> JsonObject:
        return await subjects.member_update(data)

    @router.post("/team/member_delete")
    async def member_delete(data: MemberIdentity, subjects: SubjectsDep) -> JsonObject:
        return await subjects.member_delete(data)

    @router.post("/key/generate")
    async def key_generate(data: KeyNew, subjects: SubjectsDep) -> JsonObject:
        return await subjects.key_new(data)

    @router.post("/key/update")
    async def key_update(data: KeyUpdate, subjects: SubjectsDep) -> JsonObject:
        return await subjects.key_update(data)

    @router.get("/key/list")
    async def key_list(
        subjects: SubjectsDep, key_alias: str | None = None, team_id: str | None = None, user_id: str | None = None,
        substring_matching: bool = False, return_full_object: bool = True,
        page: Annotated[int, Query(ge=1)] = 1, size: Annotated[int, Query(ge=1, le=100)] = 20,
    ) -> JsonObject:
        if substring_matching or not return_full_object:
            raise PublicError(400, "unsupported_key_query")
        return await subjects.key_list(key_alias, team_id, user_id, page, size)

    @router.post("/key/delete")
    async def key_delete(data: KeyDelete, subjects: SubjectsDep) -> JsonObject:
        return await subjects.key_delete(data)

    @router.get("/model/info")
    async def model_info(models: ModelsDep) -> JsonObject:
        return await models.list()

    @router.post("/model/new")
    async def model_new(data: ModelNew, models: ModelsDep) -> JsonObject:
        return await models.add(data)

    @router.post("/model/delete")
    async def model_delete(data: ModelDelete, models: ModelsDep) -> JsonObject:
        return await models.remove(data.id)

    @router.get("/spend/logs/v2")
    async def logs(
        ledger: LedgerDep, start_date: AwareDatetime, end_date: AwareDatetime,
        page: Annotated[int, Query(ge=1)] = 1, page_size: Annotated[int, Query(ge=1, le=100)] = 20,
        sort_by: str = "startTime", sort_order: str = "desc",
    ) -> dict[str, object]:
        if sort_by != "startTime" or sort_order != "desc" or start_date >= end_date:
            raise PublicError(400, "invalid_spend_query")
        return await ledger.logs(start_date, end_date, page, page_size)

    return router
