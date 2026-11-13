import os
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from re import sub
from typing import Final, Literal
from uuid import uuid4

import anyio
import pytest
from anyio.lowlevel import checkpoint_if_cancelled
from prisma.errors import PrismaError
from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from pydantic import TypeAdapter
from redis.asyncio import Redis

from daswork_enterprise.batching import Batcher
from daswork_enterprise.contracts import JsonObject, PublicError, month_start
from daswork_enterprise.ledger import Fee, Ledger
from daswork_enterprise.models import Deployment
from daswork_enterprise.storage import Database
from litellm import Router
from litellm.types.utils import ModelInfo

_NOW: Final = datetime(2026, 11, 25, tzinfo=UTC)
_MODEL: Final = "synthetic-model"
_PRICE: Final = ModelInfo(
    key=_MODEL,
    max_tokens=16,
    max_input_tokens=8,
    max_output_tokens=8,
    input_cost_per_token=1,
    output_cost_per_token=1,
    litellm_provider="openai",
    mode="responses",
    supported_openai_params=None,
)
_REQUESTS: Final = TypeAdapter(tuple[dict[str, str], ...])
_FEES: Final = TypeAdapter(tuple[Fee, ...])
_ROWS: Final = TypeAdapter(tuple[dict[str, str | int | float], ...])
_SETTLEMENT_TABLES: Final = """
CREATE TEMP TABLE "LiteLLM_SpendLogs" (
 request_id text PRIMARY KEY, litellm_call_id text, call_type text, api_key text, "startTime" timestamp,
 "endTime" timestamp, spend double precision, prompt_tokens integer, completion_tokens integer,
 total_tokens integer, organization_id text, "user" text, team_id text, request_tags jsonb, model text,
 model_id text, model_group text, custom_llm_provider text, request_duration_ms integer, status text, metadata jsonb
);
CREATE TEMP TABLE "LiteLLM_BudgetWindowSpend" (
 entity_type text, entity_id text, window_duration text, window_start timestamp, spend double precision,
 updated_at timestamp, PRIMARY KEY(entity_type, entity_id, window_duration)
);
"""


class Gate:
    def __init__(self) -> None:
        self.entered: Final = anyio.Event()
        self.resume: Final = anyio.Event()

    async def wait(self) -> None:
        self.entered.set()
        await self.resume.wait()


class SettlementDatabase(Database):
    def __init__(self, connection: AsyncConnection[DictRow]) -> None:
        self.connection = connection
        self.writes = 0

    async def rows(self, statement: str, parameters: Sequence[object] = ()) -> tuple[JsonObject, ...]:
        self.writes += 1
        query: Final = sub(r"\$(\d+)", r"%(parameter_\1)s", statement)
        bindings: Final = {f"parameter_{index}": value for index, value in enumerate(parameters, start=1)}
        cursor: Final = await self.connection.execute(query, bindings)  # pyright: ignore[reportArgumentType]  # Only synthetic fee JSON is passed here
        return _ROWS.validate_python(await cursor.fetchall())


class BudgetDatabase(Database):
    def __init__(self, maximum: str, gate: Gate | None) -> None:
        self.maximum = maximum
        self.gate = gate
        self.balance = Decimal(0)
        self.reads = 0

    def snapshot(self, token: str, window: str) -> JsonObject:
        spend: Final = str(self.balance)
        return {
            "token": token,
            "window_start": window,
            "expires": (_NOW + timedelta(days=1)).isoformat(),
            "blocked": False,
            "allowed_routes": ["/v1/responses"],
            "organization_id": "org",
            "user_id": token,
            "team_id": f"team-{token}",
            "metadata": {
                "dw_contract": "LITELLM_SESSION_V1",
                "dw_session_ref": token,
                "dw_organization_version": 1,
                "dw_policy_epoch": 1,
                "tags": ["parent", f"leaf-{token}"],
            },
            "team_blocked": False,
            "models": [_MODEL],
            "daswork_allowed_models": [_MODEL],
            "limits": [
                {"kind": "organization", "identity": "org", "maximum": None, "spend": "0"},
                {"kind": "user", "identity": token, "maximum": None, "spend": "0"},
                {"kind": "tag", "identity": "parent", "maximum": self.maximum, "spend": spend},
                {"kind": "tag", "identity": f"leaf-{token}", "maximum": None, "spend": "0"},
            ],
            "settled": [],
        }

    async def rows(self, statement: str, parameters: Sequence[object] = ()) -> tuple[JsonObject, ...]:
        payload: Final = parameters[0]
        assert isinstance(payload, str)
        if len(parameters) == 2:
            self.reads += 1
            requests: Final = _REQUESTS.validate_json(payload)
            snapshots: Final = tuple(self.snapshot(item["token"], item["window_start"]) for item in requests)
            if self.gate is not None and any(item["token"] == "sibling" for item in requests):
                await self.gate.wait()
            return snapshots
        self.balance += sum((fee.amount for fee in _FEES.validate_json(payload, extra="ignore")), Decimal(0))
        return ()


class CoordinatedLedger(Ledger):
    gate: Gate | None = None
    redis_ok: bool = True

    async def check_coordination(self) -> bool:
        if self.gate is not None:
            await self.gate.wait()
        return self.redis_ok


class InvalidBindingDatabase(BudgetDatabase):
    def snapshot(self, token: str, window: str) -> JsonObject:
        row: Final = super().snapshot(token, window)
        return {**row, "metadata": {}} if token == "invalid" else row


class UnavailableWriter(BudgetDatabase):
    def __init__(self, gate: Gate) -> None:
        super().__init__("2", None)
        self.write_gate = gate
        self.writes: Final = list[tuple[str, ...]]()

    async def rows(self, statement: str, parameters: Sequence[object] = ()) -> tuple[JsonObject, ...]:
        if len(parameters) == 2:
            return await super().rows(statement, parameters)
        payload: Final = parameters[0]
        assert isinstance(payload, str)
        self.writes.append(tuple(fee.call_id for fee in _FEES.validate_json(payload, extra="ignore")))
        await self.write_gate.wait()
        raise PrismaError("synthetic settlement failure")


class SelectiveJournal(CoordinatedLedger):
    saved: Final = list[Fee]()

    async def save_pending(self, fee: Fee) -> bool:
        self.saved.append(fee)
        return fee.call_id == "durable"


class FixedDeployment(Deployment):
    def estimate(self, request: str, completion: int | None, instant: datetime) -> Decimal:
        return Decimal(1)

    def cost(self, prompt: int, completion: int, instant: datetime, cached: int = 0) -> Decimal:
        return Decimal(prompt + completion)


def fixed_deployment(organization_id: str, team_id: str, model: str) -> Deployment:
    return FixedDeployment(Router(model_list=[]), "deployment", model, "synthetic", "openai", _PRICE)


@pytest.mark.asyncio
async def test_batch_seals_before_query_and_survives_leader_cancellation() -> None:
    first: Final = Gate()
    entered: Final = anyio.Event()
    late_entered: Final = anyio.Event()
    last_done: Final = anyio.Event()
    results: Final = dict[str, tuple[str, ...]]()
    leader: Final = anyio.CancelScope()

    async def query(items: tuple[str, ...]) -> tuple[str, ...]:
        if items == ("leader", "follower"):
            await first.wait()
        return items

    batcher: Final = Batcher(query, 1, 2)

    async def submit_leader() -> None:
        with leader:
            entered.set()
            await batcher.submit("leader")
            await checkpoint_if_cancelled()

    async def submit(name: str) -> None:
        if name == "late":
            late_entered.set()
        results[name] = await batcher.submit(name)
        if name == "last":
            last_done.set()

    with anyio.fail_after(2):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(submit_leader)
            await entered.wait()
            tasks.start_soon(submit, "follower")
            await first.entered.wait()
            leader.cancel()
            tasks.start_soon(submit, "late")
            await late_entered.wait()
            tasks.start_soon(submit, "last")
            await last_done.wait()
            first.resume.set()

    assert leader.cancelled_caught
    assert results == {
        "follower": ("leader", "follower"),
        "late": ("late", "last"),
        "last": ("late", "last"),
    }


@pytest.mark.asyncio
async def test_competing_keys_share_one_read_and_hold_shared_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("daswork_enterprise.ledger.resolve_deployment", fixed_deployment)
    monkeypatch.setattr("daswork_enterprise.ledger._BATCH_SIZE", 2)
    database: Final = BudgetDatabase("1", None)
    admitted: Final = list[str]()
    denied: Final = dict[str, tuple[int, str]]()
    async with Redis(decode_responses=False) as redis:
        ledger: Final = CoordinatedLedger(database, redis, lambda: _NOW)  # pyright: ignore[reportArgumentType]  # the Redis stub is invariant
        ledger.ready = True

        async def admit(token: str) -> None:
            try:
                await ledger.reserve(token, _MODEL, "/v1/responses", token, "hello", 1)
            except PublicError as error:
                denied[token] = (error.status, error.category)
            else:
                admitted.append(token)

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(admit, "first")
            tasks.start_soon(admit, "sibling")
        assert database.reads == 1
        assert {*admitted, *denied} == {"first", "sibling"}
        assert len(admitted) == len(denied) == 1
        rejected: Final = next(iter(denied))
        assert denied[rejected] == (422, "budget_exceeded")
        await ledger.finish(ledger.active[admitted[0]], None)
        retry: Final = await ledger.reserve(rejected, _MODEL, "/v1/responses", "retry", "hello", 1)
        await ledger.finish(retry, None)
        assert not ledger.active
        assert not ledger.reserved


@pytest.mark.asyncio
async def test_bad_binding_does_not_poison_other_keys_in_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("daswork_enterprise.ledger.resolve_deployment", fixed_deployment)
    monkeypatch.setattr("daswork_enterprise.ledger._BATCH_SIZE", 2)
    monkeypatch.setattr("daswork_enterprise.ledger._BATCH_DELAY", 1)
    database: Final = InvalidBindingDatabase("2", None)
    async with Redis(decode_responses=False) as redis:
        ledger: Final = CoordinatedLedger(database, redis, lambda: _NOW)  # pyright: ignore[reportArgumentType]  # the Redis stub is invariant
        ledger.ready = True

        async def valid() -> None:
            entry: Final = await ledger.reserve("valid", _MODEL, "/v1/responses", "valid", "hello", 1)
            await ledger.finish(entry, None)

        async def invalid() -> None:
            with pytest.raises(PublicError, match="budget_enforcement_unavailable") as rejection:
                await ledger.reserve("invalid", _MODEL, "/v1/responses", "invalid", "hello", 1)
            assert rejection.value.status == 503

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(valid)
            tasks.start_soon(invalid)
        assert database.reads == 1


@pytest.mark.asyncio
async def test_batched_failure_keeps_each_fee_through_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("daswork_enterprise.ledger.resolve_deployment", fixed_deployment)
    monkeypatch.setattr("daswork_enterprise.ledger._BATCH_SIZE", 2)
    monkeypatch.setattr("daswork_enterprise.ledger._BATCH_DELAY", 1)
    write: Final = Gate()
    database: Final = UnavailableWriter(write)
    cancelled: Final = anyio.CancelScope()
    entered: Final = anyio.Event()
    async with Redis(decode_responses=False) as redis:
        ledger: Final = SelectiveJournal(database, redis, lambda: _NOW)  # pyright: ignore[reportArgumentType]  # the Redis stub is invariant
        ledger.ready = True
        first: Final = await ledger.reserve("first", _MODEL, "/v1/responses", "durable", "hello", 1)
        second: Final = await ledger.reserve("sibling", _MODEL, "/v1/responses", "volatile", "hello", 1)

        async def finish_first() -> None:
            with cancelled:
                entered.set()
                await ledger.finish(first, (1, 1, 0))
                await checkpoint_if_cancelled()

        async def finish_second() -> None:
            with pytest.raises(PublicError, match="budget_enforcement_unavailable") as rejection:
                await ledger.finish(second, (2, 1, 0))
            assert rejection.value.status == 503

        with anyio.fail_after(2):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(finish_first)
                await entered.wait()
                tasks.start_soon(finish_second)
                await write.entered.wait()
                assert set(ledger.undelivered) == {"durable", "volatile"}
                assert not ledger.reserved
                cancelled.cancel()
                write.resume.set()

        assert cancelled.cancelled_caught
        assert database.writes == [("durable", "volatile")]
        assert {fee.call_id: fee.amount for fee in ledger.saved} == {"durable": Decimal(2), "volatile": Decimal(3)}
        assert set(ledger.undelivered) == ledger.retryable == {"durable", "volatile"}
        assert ledger.volatile == {"volatile"}


def settlement_fee(call_id: str, user: str, amount: int, instant: datetime) -> Fee:
    return Fee(
        call_id=call_id,
        token=f"key-{user}",
        organization_id="org",
        user_id=user,
        team_id="team",
        tags=("parent", f"leaf-{user}"),
        started=instant - timedelta(seconds=1),
        ended=instant,
        amount=Decimal(amount),
        prompt=amount,
        completion=0,
        model=_MODEL,
        model_id="deployment",
        provider="openai",
        duration_ms=1000,
        status="success",
        cached=0,
    )


@pytest.mark.asyncio
async def test_replay_batches_persisted_duplicate_and_mixed_months(monkeypatch: pytest.MonkeyPatch) -> None:
    dsn: Final = os.environ.get("DASWORK_TEST_DATABASE_URL")
    redis_url: Final = os.environ.get("DASWORK_TEST_REDIS_URL")
    if not dsn or not redis_url:
        pytest.skip("PostgreSQL and Redis are required for settlement replay")

    pending: Final = f"daswork:test:{uuid4()}"
    monkeypatch.setattr("daswork_enterprise.ledger._PENDING", pending)
    current: Final = month_start(_NOW)
    prior: Final = month_start(current - timedelta(seconds=1))
    persisted: Final = settlement_fee("current-already-persisted", "first", 2, current)
    old_first: Final = settlement_fee("old-first", "first", 9, prior)
    old_third: Final = settlement_fee("old-third", "third", 7, prior)
    current_first: Final = settlement_fee("current-first", "first", 3, current)
    current_second: Final = settlement_fee("current-second", "second", 5, current)
    entries: Final = (persisted, old_first, current_first, old_third, current_second)

    async with (
        await AsyncConnection[DictRow].connect(dsn, autocommit=True, row_factory=dict_row) as connection,
        Redis.from_url(redis_url, decode_responses=False) as redis,
    ):
        await connection.execute(_SETTLEMENT_TABLES)
        database: Final = SettlementDatabase(connection)
        ledger: Final = Ledger(database, redis, lambda: _NOW)  # pyright: ignore[reportArgumentType]  # the Redis stub is invariant
        await ledger.commit((persisted, persisted))
        try:
            await redis.hset(  # pyright: ignore[reportGeneralTypeIssues, reportUnknownMemberType]  # Redis stub shares async and sync commands
                pending, mapping={fee.call_id: fee.model_dump_json() for fee in entries}
            )
            await ledger.recover()
            await ledger.recover()
            remaining: Final[object] = await redis.exists(pending)  # pyright: ignore[reportGeneralTypeIssues]  # Redis stub shares async and sync commands
        finally:
            await redis.delete(pending)  # pyright: ignore[reportGeneralTypeIssues]  # Redis stub shares async and sync commands

        cursor: Final = await connection.execute('SELECT request_id,spend FROM "LiteLLM_SpendLogs"')
        spend: Final = {row["request_id"]: row["spend"] for row in _ROWS.validate_python(await cursor.fetchall())}
        window_query: Final = """
        SELECT entity_id,window_start::text AS month,spend FROM "LiteLLM_BudgetWindowSpend"
        WHERE entity_type='tag' OR entity_type='organization'
        """
        window_cursor: Final = await connection.execute(window_query)
        balances: Final = {
            row["entity_id"]: (row["month"], row["spend"])
            for row in _ROWS.validate_python(await window_cursor.fetchall())
        }
        assert remaining == 0
        assert database.writes == 2
        assert spend == {fee.call_id: fee.amount for fee in entries}
        current_stamp: Final = str(current.astimezone(UTC).replace(tzinfo=None))
        prior_stamp: Final = str(prior.astimezone(UTC).replace(tzinfo=None))
        assert balances == {
            "org": (current_stamp, 10),
            "parent": (current_stamp, 10),
            "leaf-first": (current_stamp, 5),
            "leaf-second": (current_stamp, 5),
            "leaf-third": (prior_stamp, 7),
        }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("wait_at", "redis_ok", "maximum", "status", "message", "reads"),
    (
        ("ping", True, "2", 422, "budget_exceeded", 2),
        ("sql", True, "2", 422, "budget_exceeded", 3),
        ("ping", False, "2", 422, "budget_exceeded", 2),
        ("ping", False, "3", 503, "budget_enforcement_unavailable", 2),
    ),
)
async def test_sibling_admission_after_concurrent_settlement(
    monkeypatch: pytest.MonkeyPatch,
    wait_at: Literal["ping", "sql"],
    redis_ok: bool,
    maximum: str,
    status: int,
    message: str,
    reads: int,
) -> None:
    monkeypatch.setattr("daswork_enterprise.ledger.resolve_deployment", fixed_deployment)
    gate: Final = Gate()
    database: Final = BudgetDatabase(maximum, gate if wait_at == "sql" else None)
    async with Redis(decode_responses=False) as redis:
        ledger: Final = CoordinatedLedger(database, redis, lambda: _NOW)  # pyright: ignore[reportArgumentType]  # the Redis stub is invariant; coordination is injected in this test
        ledger.ready = True
        first: Final = await ledger.reserve("first", _MODEL, "/v1/responses", "first-call", "hello", 1)
        ledger.gate = gate if wait_at == "ping" else None
        ledger.redis_ok = redis_ok

        async def admit_sibling() -> None:
            with pytest.raises(PublicError, match=message) as rejection:
                await ledger.reserve("sibling", _MODEL, "/v1/responses", "sibling-call", "hello", 1)
            assert rejection.value.status == status

        with anyio.fail_after(2):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(admit_sibling)
                await gate.entered.wait()
                await ledger.finish(first, (1, 1, 0))
                gate.resume.set()

        assert database.balance == Decimal(2)
        assert database.reads == reads
        assert not ledger.active
