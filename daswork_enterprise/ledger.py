import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Final

from prisma.errors import PrismaError
from pydantic import AwareDatetime, TypeAdapter, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from .contracts import Contract, Money, PublicError, SessionMetadata, month_start
from .models import Deployment, resolve_deployment
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
 WHERE k.token=$1 AND k.metadata->>'dw_contract'='LITELLM_SESSION_V1'
), limits AS (
 SELECT 'organization' AS kind,a.organization_id AS identity,b.max_budget
 FROM binding a JOIN "LiteLLM_BudgetTable" b ON b.budget_id=a.budget_id
 UNION ALL SELECT 'user',user_id,max_budget FROM binding
 UNION ALL SELECT 'tag',t.tag_name,b.max_budget FROM binding a
 JOIN "LiteLLM_TagTable" t ON (a.metadata->'tags') ? t.tag_name AND t.daswork_organization_id=a.organization_id
 JOIN "LiteLLM_BudgetTable" b ON b.budget_id=t.budget_id
)
SELECT a.token,to_jsonb(a.expires AT TIME ZONE 'UTC') AS expires,a.blocked,a.allowed_routes,a.organization_id,a.user_id,a.team_id,a.metadata,
       a.team_blocked,a.models,a.daswork_allowed_models,
       (SELECT jsonb_agg(jsonb_build_object('kind',l.kind,'identity',l.identity,
          'maximum',l.max_budget::text,'spend',COALESCE(w.spend,0)::text))
        FROM limits l LEFT JOIN "LiteLLM_BudgetWindowSpend" w ON w.entity_type=l.kind AND w.entity_id=l.identity
        AND w.window_duration='1mo' AND w.window_start=($2::text::timestamptz AT TIME ZONE 'UTC')) AS limits
FROM binding a
"""
_SETTLE: Final = """
WITH new_fee AS (
 INSERT INTO "LiteLLM_SpendLogs"(request_id,litellm_call_id,call_type,api_key,"startTime","endTime",spend,prompt_tokens,
 completion_tokens,total_tokens,organization_id,"user",team_id,request_tags,model,model_id,model_group,
 custom_llm_provider,request_duration_ms,status,metadata)
 VALUES ($1,$1,'aresponses',$2,($3::text::timestamptz AT TIME ZONE 'UTC'),($4::text::timestamptz AT TIME ZONE 'UTC'),$5::text::float8,$6::int,$7::int,
 $6::int + $7::int,$8,$9,$10,to_jsonb($11::text[]),$12,$13,$12,$15,$16,$17,
 jsonb_build_object('status',$17::text,'usage_object',jsonb_build_object(
 'prompt_tokens',$6::int,'completion_tokens',$7::int,'total_tokens',$6::int + $7::int,
 'prompt_tokens_details',jsonb_build_object('cached_tokens',$18::int))))
 ON CONFLICT DO NOTHING RETURNING request_id
), entities AS (
 SELECT 'organization' AS kind,$8::text AS identity UNION ALL SELECT 'user',$9
 UNION ALL SELECT 'tag',unnest($11::text[])
)
INSERT INTO "LiteLLM_BudgetWindowSpend" AS balance(entity_type,entity_id,window_duration,window_start,spend,updated_at)
SELECT kind,identity,'1mo',($14::text::timestamptz AT TIME ZONE 'UTC'),$5::text::float8,now() FROM entities CROSS JOIN new_fee ORDER BY kind,identity
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


@dataclass(frozen=True, slots=True)
class Reservation:
    binding: Binding
    deployment: Deployment
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
    provider: str
    duration_ms: int
    status: str
    cached: int


_PENDING_VALUES: Final = TypeAdapter(dict[str, bytes])
_AOF_ACK: Final = TypeAdapter(tuple[object, tuple[int, int]])


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
            row: Final = await self.db.one(_FIND, (presented, month_start(now).isoformat()))
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
        if len(binding.limits) != 2 + len(binding.metadata.tags):
            raise PublicError(503, _UNAVAILABLE)
        return binding

    async def reserve(
        self,
        presented: str,
        model: str,
        path: str,
        call_id: str,
        request: str,
        completion: int | None,
    ) -> Reservation:
        async with self.gate:
            try:
                now: Final = self.clock()
                binding: Final = await self.lookup(presented, model, path, now)
            except (PrismaError, RedisError, ValidationError, TimeoutError) as exc:
                _LOG.warning("checkpoint=admission call=%s category=%s", call_id, type(exc).__name__)
                raise PublicError(503, _UNAVAILABLE) from exc
            deployment: Final = resolve_deployment(binding.organization_id, binding.team_id, model)
            estimate: Final = deployment.estimate(request, completion, now)
            reservation: Final = Reservation(binding, deployment, now, estimate, call_id)
            if any(self.exceeds(limit, estimate, now) for limit in binding.limits):
                raise PublicError(422, "budget_exceeded")
            try:
                if await self.recover():
                    binding_after: Final = await self.lookup(presented, model, path, now)
                    if any(self.exceeds(limit, estimate, now) for limit in binding_after.limits):
                        raise PublicError(422, "budget_exceeded")
                    reservation = Reservation(binding_after, deployment, now, estimate, call_id)
            except (PrismaError, RedisError, ValidationError, TimeoutError) as exc:
                _LOG.warning("checkpoint=recovery_admission call=%s category=%s", call_id, type(exc).__name__)
                raise PublicError(503, _UNAVAILABLE) from exc
            if any(self.exceeds(limit, estimate, now) for limit in reservation.binding.limits):
                raise PublicError(422, "budget_exceeded")
            self.active[call_id] = reservation
            return reservation

    def exceeds(self, limit: Limit, estimate: Decimal, now: datetime) -> bool:
        if estimate == 0 or limit.maximum is None:
            return False
        reserved: Final = sum(entry.estimate for entry in self.active.values() if limit.scope in entry.scopes)
        confirmed: Final = sum(
            fee.amount
            for fee in self.undelivered.values()
            if month_start(fee.ended) == month_start(now)
            and (
                limit.scope in {("organization", fee.organization_id), ("user", fee.user_id)}
                or (limit.kind == "tag" and limit.identity in fee.tags)
            )
        )
        return limit.spend + reserved + confirmed + estimate > limit.maximum

    async def commit(self, fee: Fee) -> None:
        await self.db.rows(
            _SETTLE,
            (
                fee.call_id,
                fee.token,
                fee.started.isoformat(),
                fee.ended.isoformat(),
                str(fee.amount),
                fee.prompt,
                fee.completion,
                fee.organization_id,
                fee.user_id,
                fee.team_id,
                list(fee.tags),
                fee.model,
                fee.model_id,
                month_start(fee.ended).isoformat(),
                fee.provider,
                fee.duration_ms,
                fee.status,
                fee.cached,
            ),
        )

    async def recover(self) -> bool:
        payload: Final = await self.redis.execute_command("HGETALL", _PENDING)  # pyright: ignore[reportAny]  # validated below
        pending: Final = _PENDING_VALUES.validate_python(payload)
        fees: Final = {identity: Fee.model_validate_json(value) for identity, value in pending.items()}
        fees.update(self.undelivered)
        for fee in fees.values():
            await self.commit(fee)
            self.undelivered.pop(fee.call_id, None)
            await self.redis.execute_command("HDEL", _PENDING, fee.call_id)
        return bool(fees)

    async def finish(self, entry: Reservation, usage: tuple[int, int, int] | None, status: str = "success") -> None:
        async with self.gate:
            if usage is None:
                self.active.pop(entry.call_id, None)
                return
            prompt, completion, cached = usage
            binding: Final = entry.binding
            ended: Final = self.clock()
            fee: Final = Fee(
                call_id=entry.call_id,
                token=binding.token,
                organization_id=binding.organization_id,
                user_id=binding.user_id,
                team_id=binding.team_id,
                tags=binding.metadata.tags,
                started=entry.started,
                ended=ended,
                amount=entry.deployment.cost(prompt, completion, entry.started, cached),
                prompt=prompt,
                completion=completion,
                model=entry.deployment.model_name,
                model_id=entry.deployment.model_id,
                provider=entry.deployment.provider,
                duration_ms=max(0, int((ended - entry.started).total_seconds() * 1000)),
                status=status,
                cached=cached,
            )
            self.undelivered[fee.call_id] = fee
            self.active.pop(entry.call_id, None)
            durable: Final = await self.save_pending(fee)
            try:
                await self.commit(fee)
            except (PrismaError, TimeoutError) as exc:
                _LOG.warning(
                    "checkpoint=settlement call=%s category=%s durable=%s", fee.call_id, type(exc).__name__, durable
                )
                if not durable:
                    raise PublicError(503, _UNAVAILABLE) from exc
                return
            self.undelivered.pop(fee.call_id, None)
            if durable:
                try:
                    await self.redis.execute_command("HDEL", _PENDING, fee.call_id)
                except RedisError as exc:
                    _LOG.warning("checkpoint=journal_cleanup call=%s category=%s", fee.call_id, type(exc).__name__)

    async def save_pending(self, fee: Fee) -> bool:
        try:
            async with self.redis.pipeline(transaction=False) as pipeline:
                pipeline.execute_command("HSET", _PENDING, fee.call_id, fee.model_dump_json())
                pipeline.execute_command("WAITAOF", 1, 0, 1000)
                _, (local, _) = _AOF_ACK.validate_python(await pipeline.execute())  # pyright: ignore[reportUnknownMemberType]  # Redis stub lacks its response type
                return local == 1
        except (RedisError, ValidationError) as exc:
            _LOG.error("checkpoint=fee_recovery call=%s category=%s", fee.call_id, type(exc).__name__)
            return False
