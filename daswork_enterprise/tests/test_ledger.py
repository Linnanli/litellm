from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Final, Literal

import anyio
import pytest
from redis.asyncio import Redis

from daswork_enterprise.contracts import JsonObject, PublicError
from daswork_enterprise.ledger import Ledger
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


class Gate:
    def __init__(self) -> None:
        self.entered: Final = anyio.Event()
        self.resume: Final = anyio.Event()

    async def wait(self) -> None:
        self.entered.set()
        await self.resume.wait()


class BudgetDatabase(Database):
    def __init__(self, maximum: str, gate: Gate | None) -> None:
        self.maximum = maximum
        self.gate = gate
        self.balance = Decimal(0)
        self.reads = 0

    async def one(self, statement: str, parameters: Sequence[object] = ()) -> JsonObject:
        self.reads += 1
        token: Final = parameters[0]
        assert isinstance(token, str)
        spend: Final = str(self.balance)
        if self.gate is not None and token == "sibling":
            await self.gate.wait()
        return {
            "token": token,
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
        amount: Final = parameters[4]
        assert isinstance(amount, str)
        self.balance += Decimal(amount)
        return ()


class CoordinatedLedger(Ledger):
    gate: Gate | None = None
    redis_ok: bool = True

    async def check_coordination(self) -> bool:
        if self.gate is not None:
            await self.gate.wait()
        return self.redis_ok


class FixedDeployment(Deployment):
    def estimate(self, request: str, completion: int | None, instant: datetime) -> Decimal:
        return Decimal(1)

    def cost(self, prompt: int, completion: int, instant: datetime, cached: int = 0) -> Decimal:
        return Decimal(prompt + completion)


def fixed_deployment(organization_id: str, team_id: str, model: str) -> Deployment:
    return FixedDeployment(Router(model_list=[]), "deployment", model, "synthetic", "openai", _PRICE)


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
