import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Final

from psycopg import Error as DatabaseError
from psycopg_pool import PoolTimeout
from pydantic import AwareDatetime, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from .contracts import Contract, Money, PublicError, Rates, SessionMetadata, month_start
from .models import Deployment
from .storage import Database

_LOG: Final = logging.getLogger(__name__)
_PENDING: Final = "daswork:session:uncommitted-fees:v1"
_UNAVAILABLE: Final = "budget_enforcement_unavailable"
_FIND: Final = """
WITH binding AS (
 SELECT k.token,k.expires,k.blocked,k.allowed_routes,k.organization_id,k.user_id,k.team_id,k.metadata,
        o.budget_id,t.blocked AS team_blocked,t.models,u.max_budget,m.daswork_allowed_models
 FROM "LiteLLM_VerificationToken" k
 JOIN "LiteLLM_OrganizationTable" o ON o.organization_id=k.organization_id
 JOIN "LiteLLM_UserTable" u ON u.user_id=k.user_id AND u.organization_id=k.organization_id
 JOIN "LiteLLM_TeamTable" t ON t.team_id=k.team_id AND t.organization_id=k.organization_id
 LEFT JOIN "LiteLLM_TeamMembership" m ON m.team_id=k.team_id AND m.user_id=k.user_id
 WHERE k.token=%s
), limits AS (
 SELECT 'organization' AS kind,a.organization_id AS identity,b.max_budget
 FROM binding a JOIN "LiteLLM_BudgetTable" b ON b.budget_id=a.budget_id
 UNION ALL SELECT 'user',user_id,max_budget FROM binding
 UNION ALL SELECT 'tag',t.tag_name,b.max_budget FROM binding a
 JOIN "LiteLLM_TagTable" t ON (a.metadata->'tags') ? t.tag_name AND t.daswork_organization_id=a.organization_id
 JOIN "LiteLLM_BudgetTable" b USING (budget_id)
)
SELECT a.token,a.expires::text,a.blocked,a.allowed_routes,a.organization_id,a.user_id,a.team_id,a.metadata,
       a.team_blocked,a.models,a.daswork_allowed_models,
       (SELECT jsonb_agg(jsonb_build_object('kind',l.kind,'identity',l.identity,
          'maximum',l.max_budget::text,'spend',COALESCE(w.spend,0)::text))
        FROM limits l LEFT JOIN "LiteLLM_BudgetWindowSpend" w ON w.entity_type=l.kind AND w.entity_id=l.identity
        AND w.window_duration='1mo' AND w.window_start=%s) AS limits,
       (SELECT jsonb_build_object('model_id',model_id,'model_name',model_name,'params',litellm_params,'price',model_info)
        FROM "LiteLLM_ProxyModelTable" WHERE model_name=%s AND NOT blocked) AS deployment
FROM binding a
"""
_SETTLE: Final = """
WITH new_fee AS (
 INSERT INTO "LiteLLM_SpendLogs"(request_id,litellm_call_id,call_type,api_key,"startTime","endTime",spend,prompt_tokens,
 completion_tokens,total_tokens,organization_id,"user",team_id,request_tags,model,model_id,model_group)
 VALUES (%(id)s,%(id)s,'aresponses',%(fingerprint)s,%(started)s,%(ended)s,%(amount)s,%(prompt)s,%(completion)s,
 %(prompt)s + %(completion)s,%(org)s,%(user)s,%(team)s,to_jsonb(%(tags)s::text[]),%(model)s,%(model_id)s,%(model)s)
 ON CONFLICT DO NOTHING RETURNING request_id
), entities AS (
 SELECT 'organization' AS kind,%(org)s::text AS identity UNION ALL SELECT 'user',%(user)s
 UNION ALL SELECT 'tag',unnest(%(tags)s::text[])
)
INSERT INTO "LiteLLM_BudgetWindowSpend" AS balance(entity_type,entity_id,window_duration,window_start,spend,updated_at)
SELECT kind,identity,'1mo',%(month)s,%(amount)s,now() FROM entities CROSS JOIN new_fee ORDER BY kind,identity
ON CONFLICT(entity_type,entity_id,window_duration) DO UPDATE SET
 window_start=EXCLUDED.window_start,spend=CASE WHEN balance.window_start=EXCLUDED.window_start
 THEN balance.spend + EXCLUDED.spend ELSE EXCLUDED.spend END,updated_at=now()
WHERE balance.window_start <= EXCLUDED.window_start
RETURNING entity_id
"""


class Limit(Contract):
    kind: str
    identity: str
    maximum: Money | None
    spend: Decimal

    @property
    def scope(self) -> tuple[str, str]:
        return (self.kind, self.identity)


class Binding(Contract):
    token: str
    expires: AwareDatetime
    blocked: bool
    allowed_routes: tuple[str, ...]
    organization_id: str
    user_id: str
    team_id: str
    metadata: SessionMetadata
    team_blocked: bool
    models: tuple[str, ...]
    daswork_allowed_models: tuple[str, ...] | None
    limits: tuple[Limit, ...]
    deployment: Deployment | None


@dataclass(frozen=True, slots=True)
class Reservation:
    binding: Binding
    deployment: Deployment
    rates: Rates
    started: datetime
    estimate: Decimal
    call_id: str

    @property
    def scopes(self) -> frozenset[tuple[str, str]]:
        return frozenset(limit.scope for limit in self.binding.limits)


class Fee(Contract):
    call_id: str
    token: str
    organization_id: str
    user_id: str
    team_id: str
    tags: tuple[str, ...]
    started: AwareDatetime
    ended: AwareDatetime
    amount: Decimal
    prompt: int
    completion: int
    model: str
    model_id: str


class Ledger:
    def __init__(self, db: Database, redis: Redis, clock: Callable[[], datetime]) -> None:
        self.db = db
        self.redis = redis
        self.clock = clock
        self.gate = asyncio.Lock()
        self.active: Final = dict[str, Reservation]()
        self.undelivered: Final = dict[str, Fee]()

    async def lookup(self, presented: str, model: str, path: str, now: datetime) -> Binding:
        try:
            row: Final = await self.db.one(_FIND, (presented, month_start(now), model))
        except PublicError as exc:
            raise PublicError(401, "token_not_found_in_db") from exc
        binding: Final = Binding.model_validate(row)
        if binding.expires <= now:
            raise PublicError(401, "token_expired")
        if binding.blocked or binding.team_blocked or path not in binding.allowed_routes:
            raise PublicError(403, "session_blocked")
        if binding.daswork_allowed_models is None or model not in binding.models:
            raise PublicError(403, "model_not_allowed")
        if binding.daswork_allowed_models and model not in binding.daswork_allowed_models:
            raise PublicError(403, "model_not_allowed")
        if binding.deployment is None:
            raise PublicError(404, "model_not_found")
        if binding.deployment.price.organization_id not in (None, binding.organization_id):
            raise PublicError(403, "model_not_allowed")
        if len(binding.limits) != 2 + len(binding.metadata.tags):
            raise PublicError(503, _UNAVAILABLE)
        return binding

    async def reserve(
        self, presented: str, model: str, path: str, call_id: str, prompt: int, completion: int | None,
    ) -> Reservation:
        async with self.gate:
            try:
                await self.recover()
                now: Final = self.clock()
                binding: Final = await self.lookup(presented, model, path, now)
            except (DatabaseError, PoolTimeout, RedisError, ValidationError) as exc:
                _LOG.warning("checkpoint=admission call=%s category=%s", call_id, type(exc).__name__)
                raise PublicError(503, _UNAVAILABLE) from exc
            deployment: Final = binding.deployment
            if deployment is None:
                raise PublicError(404, "model_not_found")
            rates: Final = deployment.price.at(now)
            estimate: Final = rates.cost(prompt, deployment.price.max_output_tokens if completion is None else completion)
            reservation: Final = Reservation(binding, deployment, rates, now, estimate, call_id)
            if any(self.exceeds(limit, estimate) for limit in binding.limits):
                raise PublicError(422, "budget_exceeded")
            self.active[call_id] = reservation
            return reservation

    def exceeds(self, limit: Limit, estimate: Decimal) -> bool:
        if estimate == 0 or limit.maximum is None:
            return False
        reserved: Final = sum(entry.estimate for entry in self.active.values() if limit.scope in entry.scopes)
        return limit.spend + reserved + estimate > limit.maximum

    async def commit(self, fee: Fee) -> None:
        params: Final[Mapping[str, object]] = {
            "id": fee.call_id, "fingerprint": fee.token, "started": fee.started, "ended": fee.ended,
            "amount": fee.amount, "prompt": fee.prompt, "completion": fee.completion, "org": fee.organization_id,
            "user": fee.user_id, "team": fee.team_id, "tags": list(fee.tags), "model": fee.model,
            "model_id": fee.model_id, "month": month_start(fee.ended),
        }
        await self.db.rows(_SETTLE, params)

    async def recover(self) -> None:
        pending: Final = await self.redis.hgetall(_PENDING)
        stored: Final = tuple(Fee.model_validate_json(value) for value in pending.values())
        for fee in (*stored, *tuple(self.undelivered.values())):
            await self.commit(fee)
            await self.redis.hdel(_PENDING, fee.call_id)
            self.undelivered.pop(fee.call_id, None)

    async def finish(self, entry: Reservation, usage: tuple[int, int, int] | None) -> None:
        async with self.gate:
            if usage is None:
                self.active.pop(entry.call_id, None)
                return
            prompt, completion, cached = usage
            binding: Final = entry.binding
            fee: Final = Fee(
                call_id=entry.call_id, token=binding.token, organization_id=binding.organization_id,
                user_id=binding.user_id, team_id=binding.team_id, tags=binding.metadata.tags, started=entry.started,
                ended=self.clock(), amount=entry.rates.cost(prompt, completion, cached), prompt=prompt,
                completion=completion, model=entry.deployment.model_name, model_id=entry.deployment.model_id,
            )
            try:
                await self.commit(fee)
            except (DatabaseError, PoolTimeout) as exc:
                _LOG.warning("checkpoint=settlement call=%s category=%s", fee.call_id, type(exc).__name__)
                self.undelivered[fee.call_id] = fee
                await self.save_pending(fee)
            finally:
                self.active.pop(entry.call_id, None)

    async def save_pending(self, fee: Fee) -> None:
        try:
            async with self.redis.pipeline(transaction=False) as pipeline:
                await pipeline.hset(_PENDING, fee.call_id, fee.model_dump_json()).execute_command("WAITAOF", 1, 0, 1000).execute()
        except RedisError as exc:
            _LOG.error("checkpoint=fee_recovery call=%s category=%s", fee.call_id, type(exc).__name__)

    async def logs(self, start: datetime, end: datetime, page: int, size: int) -> dict[str, object]:
        record: Final = await self.db.one(
            'WITH matching AS (SELECT * FROM "LiteLLM_SpendLogs" WHERE "startTime" >= %s AND "startTime" < %s),'
            ' paged AS (SELECT litellm_call_id,organization_id,"user",team_id,request_tags,spend,'
            ' prompt_tokens,completion_tokens,"startTime","endTime",model FROM matching'
            ' ORDER BY "startTime" DESC, request_id DESC LIMIT %s OFFSET %s)'
            " SELECT COALESCE((SELECT jsonb_agg(to_jsonb(paged)) FROM paged),'[]') AS data,"
            " (SELECT count(*) FROM matching) AS total",
            (start, end, size, (page - 1) * size),
        )
        total: Final = record["total"]
        if not isinstance(total, int):
            raise PublicError(503, "invalid_stored_record")
        return {**record, "total_pages": (total + size - 1) // size, "page": page, "page_size": size, "total_is_capped": False}
