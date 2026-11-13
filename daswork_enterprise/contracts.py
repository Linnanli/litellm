from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Annotated, Final, Literal
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, StringConstraints, model_validator

RESPONSES_PATHS: Final = frozenset({"/responses", "/v1/responses"})
SESSION_CONTRACT: Final = "LITELLM_SESSION_V1"
ADMISSION_CONTRACT: Final = "LITELLM_SESSION_ADMISSION_V1"
SHANGHAI: Final = ZoneInfo("Asia/Shanghai")
Money = Annotated[Decimal, Field(ge=0, max_digits=30, decimal_places=18, allow_inf_nan=False)]
Identifier = Annotated[str, StringConstraints(min_length=1, max_length=256)]
Fingerprint = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
ClockRange = Annotated[str, StringConstraints(pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d-(?:[01]\d|2[0-3]):[0-5]\d$")]
CalendarDate = Annotated[str, StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}$")]
JsonObject = Mapping[str, JsonValue]


class Contract(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class PublicError(Exception):
    def __init__(self, status: int, message: str, category: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.category = category or message


class BudgetNew(Contract):
    budget_id: Identifier
    max_budget: Money | None
    budget_duration: Literal["1mo"]
    budget_reset_at: AwareDatetime


class BudgetLookup(Contract):
    budgets: Annotated[tuple[Identifier, ...], Field(min_length=1, max_length=100)]


class OrganizationNew(Contract):
    organization_id: Identifier
    organization_alias: Identifier
    budget_id: Identifier


class OrganizationUpdate(Contract):
    organization_id: Identifier
    max_budget: Money


class TagNew(Contract):
    name: Identifier
    budget_id: Identifier


class TagLookup(Contract):
    names: Annotated[tuple[Identifier, ...], Field(min_length=1, max_length=100)]


class TagUpdate(Contract):
    name: Identifier
    max_budget: Money | None


class UserNew(Contract):
    user_id: Identifier
    max_budget: Money | None
    budget_duration: Literal["1mo"]
    user_role: Literal["internal_user"]
    auto_create_key: Literal[False] = False
    send_invite_email: Literal[False] = False


class UserUpdate(Contract):
    user_id: Identifier
    max_budget: Money | None


class TeamNew(Contract):
    team_id: Identifier
    team_alias: Identifier
    organization_id: Identifier
    models: tuple[Identifier, ...] = ()
    blocked: bool = False


class TeamUpdate(Contract):
    team_id: Identifier
    blocked: bool | None = None
    models: tuple[Identifier, ...] | None = None


class Member(Contract):
    user_id: Identifier
    role: Literal["user"]


class MemberAdd(Contract):
    team_id: Identifier
    member: Member
    allowed_models: tuple[Identifier, ...] = ()


class MemberIdentity(Contract):
    team_id: Identifier
    user_id: Identifier


class MemberUpdate(MemberIdentity):
    allowed_models: tuple[Identifier, ...] = ()


class SessionMetadata(Contract):
    dw_contract: Literal["LITELLM_SESSION_V1"]
    dw_session_ref: Identifier
    dw_organization_version: int
    dw_policy_epoch: int
    tags: Annotated[tuple[Identifier, ...], Field(min_length=1, max_length=128)]


class KeyPolicy(Contract):
    team_id: Identifier
    organization_id: Identifier
    models: tuple[Literal["all-team-models"]]
    allowed_routes: tuple[str, str]
    blocked: bool
    tags: tuple[Identifier, ...]
    metadata: SessionMetadata

    @model_validator(mode="after")
    def check_policy(self) -> "KeyPolicy":
        if (
            self.tags != self.metadata.tags
            or len(set(self.tags)) != len(self.tags)
            or frozenset(self.allowed_routes) != RESPONSES_PATHS
            or self.models != ("all-team-models",)
        ):
            raise ValueError("invalid session policy")
        return self


class KeyNew(KeyPolicy):
    key_alias: Annotated[str, StringConstraints(pattern=r"^dw-session-[A-Za-z0-9_-]{1,160}$")]
    user_id: Identifier
    duration: Annotated[str, StringConstraints(pattern=r"^[1-9]\d{0,8}s$")] = "86400s"
    auto_rotate: Literal[False] = False
    send_invite_email: Literal[False] = False


class KeyUpdate(KeyPolicy):
    key: Fingerprint


class KeyDelete(Contract):
    keys: Annotated[tuple[Fingerprint, ...], Field(min_length=1, max_length=100)]


class AdmissionData(Contract):
    contract: Literal["LITELLM_SESSION_ADMISSION_V1"]
    requestId: str
    checkedAt: AwareDatetime


class AdmissionReply(Contract):
    code: Literal[200]
    msg: str
    data: AdmissionData


class Rates(Contract):
    input_cost_per_token: Money
    output_cost_per_token: Money
    cache_read_input_token_cost: Money = Decimal(0)

    def cost(self, prompt: int, completion: int, cached: int = 0) -> Decimal:
        return (
            (prompt - cached) * self.input_cost_per_token
            + completion * self.output_cost_per_token
            + cached * self.cache_read_input_token_cost
        )


class WeekdayWindow(Contract):
    hours_utc: ClockRange
    weekdays: Annotated[tuple[Annotated[int, Field(ge=1, le=7)], ...], Field(min_length=1, max_length=7)]


def in_hours(window: str, when: datetime) -> bool:
    start, end = window.split("-")
    minute: Final = when.astimezone(UTC).strftime("%H:%M")
    return start <= minute < end if start < end else minute >= start or minute < end


class OffPeakRates(Rates):
    hours_utc: tuple[ClockRange, ...] = ()
    windows: tuple[WeekdayWindow, ...] = ()
    weekday_timezone: Literal["UTC", "Asia/Shanghai"] | None = None
    off_peak_dates: Annotated[tuple[CalendarDate, ...], Field(min_length=1, max_length=366)] | None = None

    @model_validator(mode="after")
    def valid_dates(self) -> "OffPeakRates":
        if self.off_peak_dates is not None:
            if self.weekday_timezone is None or len(set(self.off_peak_dates)) != len(self.off_peak_dates):
                raise ValueError("off_peak_dates requires unique dates and a weekday_timezone")
            tuple(map(date.fromisoformat, self.off_peak_dates))
        return self

    def active(self, when: datetime) -> bool:
        local: Final = when.astimezone(ZoneInfo(self.weekday_timezone or "UTC"))
        return (
            local.date().isoformat() in (self.off_peak_dates or ())
            or any(in_hours(window, when) for window in self.hours_utc)
            or any(local.isoweekday() in window.weekdays and in_hours(window.hours_utc, when) for window in self.windows)
        )


class Price(Rates):
    model_config = ConfigDict(frozen=True, extra="ignore")
    off_peak_pricing: OffPeakRates | None = None

    def at(self, when: datetime) -> Rates:
        return self.off_peak_pricing if self.off_peak_pricing and self.off_peak_pricing.active(when) else self


def month_start(when: datetime) -> datetime:
    return when.astimezone(SHANGHAI).replace(day=1, hour=0, minute=0, second=0, microsecond=0).astimezone(UTC)


def next_month(when: datetime) -> datetime:
    first: Final = month_start(when).astimezone(SHANGHAI)
    return first.replace(year=first.year + first.month // 12, month=first.month % 12 + 1).astimezone(UTC)
