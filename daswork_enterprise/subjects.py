from collections.abc import Callable
from datetime import UTC, datetime
from secrets import token_urlsafe
from typing import Final

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
    next_month,
)
from .security import fingerprint
from .storage import Database

_BUDGET: Final = """
jsonb_build_object('budget_id', b.budget_id, 'max_budget', b.max_budget,
'budget_duration', b.budget_duration, 'budget_reset_at', b.budget_reset_at AT TIME ZONE 'UTC')
"""
_KEY: Final = """
jsonb_build_object('token', k.token, 'key_alias', k.key_alias, 'user_id', k.user_id, 'team_id', k.team_id,
'org_id', k.organization_id, 'expires', k.expires AT TIME ZONE 'UTC', 'models', k.models, 'blocked', k.blocked,
'allowed_routes', k.allowed_routes, 'metadata', k.metadata, 'project_id', k.project_id, 'max_budget', k.max_budget,
'budget_duration', k.budget_duration, 'budget_id', k.budget_id, 'auto_rotate', k.auto_rotate)
"""
_BINDING: Final = """
WITH requested AS (SELECT $1::text AS organization_id, $2::text AS team_id, $3::text AS user_id,
                         $4::text[] AS tags),
valid AS (
 SELECT r.* FROM requested r
 JOIN "LiteLLM_TeamTable" t ON t.team_id=r.team_id AND t.organization_id=r.organization_id
 JOIN "LiteLLM_TeamMembership" m ON m.team_id=r.team_id AND m.user_id=r.user_id
 JOIN "LiteLLM_UserTable" u ON u.user_id=r.user_id AND u.organization_id=r.organization_id
 WHERE cardinality(r.tags) = (SELECT count(*) FROM "LiteLLM_TagTable" a WHERE a.tag_name=ANY(r.tags)
   AND (a.daswork_organization_id IS NULL OR a.daswork_organization_id=r.organization_id))
),
claim_tags AS (
 UPDATE "LiteLLM_TagTable" a SET daswork_organization_id=v.organization_id FROM valid v
 WHERE a.tag_name=ANY(v.tags) AND a.daswork_organization_id IS NULL
)
"""


_UPDATE_BINDING: Final = _BINDING.replace(
    "requested AS (SELECT $1::text AS organization_id, $2::text AS team_id, $3::text AS user_id",
    "requested AS (SELECT $1::text AS organization_id, $2::text AS team_id,"
    ' (SELECT user_id FROM "LiteLLM_VerificationToken" WHERE token=$3 AND organization_id=$1'
    " AND metadata->>'dw_session_ref'=$10) AS user_id",
)


def _iso(when: datetime) -> str:
    return when.astimezone(UTC).isoformat()


class Subjects:
    def __init__(self, db: Database, clock: Callable[[], datetime]) -> None:
        self.db = db
        self.clock = clock

    @staticmethod
    def _nested(record: JsonObject, field: str) -> JsonObject:
        value: Final = record.get(field)
        if not isinstance(value, dict):
            raise PublicError(503, "invalid_stored_record")
        return value

    async def budget_new(self, data: BudgetNew) -> JsonObject:
        return await self.db.one(
            'INSERT INTO "LiteLLM_BudgetTable" AS b (budget_id,max_budget,budget_duration,budget_reset_at,created_by,updated_by)'
            f" VALUES ($1,$2::text::float8,$3,$4::text::timestamptz,'daswork','daswork') RETURNING {_BUDGET} AS budget",
            (
                data.budget_id,
                None if data.max_budget is None else str(data.max_budget),
                data.budget_duration,
                _iso(data.budget_reset_at),
            ),
        )

    async def budget_info(self, data: BudgetLookup) -> tuple[JsonObject, ...]:
        records: Final = await self.db.rows(
            f'SELECT {_BUDGET} AS budget FROM "LiteLLM_BudgetTable" b WHERE budget_id=ANY($1)',
            (list(data.budgets),),
        )
        return tuple(self._nested(record, "budget") for record in records)

    async def organization_new(self, data: OrganizationNew) -> JsonObject:
        return await self.db.one(
            'INSERT INTO "LiteLLM_OrganizationTable" (organization_id,organization_alias,budget_id,models,created_by,updated_by)'
            " VALUES ($1,$2,$3,'{}','daswork','daswork') RETURNING organization_id,organization_alias,budget_id",
            (data.organization_id, data.organization_alias, data.budget_id),
        )

    async def organization_info(self, identity: str) -> JsonObject:
        return await self.db.one(
            "SELECT o.organization_id,o.organization_alias,o.budget_id,COALESCE(w.spend,0)::float8 AS spend,"
            f'{_BUDGET} AS litellm_budget_table FROM "LiteLLM_OrganizationTable" o'
            ' JOIN "LiteLLM_BudgetTable" b USING (budget_id)'
            " LEFT JOIN \"LiteLLM_BudgetWindowSpend\" w ON w.entity_type='organization'"
            " AND w.entity_id=o.organization_id AND w.window_duration='1mo' AND w.window_start=($1::text::timestamptz AT TIME ZONE 'UTC')"
            " WHERE o.organization_id=$2",
            (_iso(self._window()), identity),
        )

    async def organization_update(self, data: OrganizationUpdate) -> JsonObject:
        return await self.db.one(
            'UPDATE "LiteLLM_BudgetTable" b SET max_budget=$1::text::float8 FROM "LiteLLM_OrganizationTable" o'
            " WHERE o.budget_id=b.budget_id AND o.organization_id=$2 RETURNING o.organization_id,b.budget_id,"
            " b.max_budget::float8 AS max_budget",
            (str(data.max_budget), data.organization_id),
        )

    async def tag_new(self, data: TagNew) -> JsonObject:
        return await self.db.one(
            "INSERT INTO \"LiteLLM_TagTable\" (tag_name,budget_id,models) VALUES ($1,$2,'{}')"
            " RETURNING tag_name AS name,budget_id",
            (data.name, data.budget_id),
        )

    async def tag_info(self, data: TagLookup) -> JsonObject:
        record: Final = await self.db.one(
            f"SELECT COALESCE(jsonb_object_agg(t.tag_name,jsonb_build_object('name',t.tag_name,"
            f"'litellm_budget_table',{_BUDGET})), '{{}}') AS tags"
            ' FROM "LiteLLM_TagTable" t JOIN "LiteLLM_BudgetTable" b USING (budget_id) WHERE t.tag_name=ANY($1)',
            (list(data.names),),
        )
        return self._nested(record, "tags")

    async def tag_update(self, data: TagUpdate) -> JsonObject:
        return await self.db.one(
            'UPDATE "LiteLLM_BudgetTable" b SET max_budget=$1::text::float8 FROM "LiteLLM_TagTable" t'
            " WHERE t.budget_id=b.budget_id AND t.tag_name=$2 RETURNING t.tag_name AS name,b.budget_id,"
            " b.max_budget::float8 AS max_budget",
            (None if data.max_budget is None else str(data.max_budget), data.name),
        )

    async def user_new(self, data: UserNew) -> JsonObject:
        return await self.db.one(
            'INSERT INTO "LiteLLM_UserTable" (user_id,max_budget,budget_duration,budget_reset_at,user_role,models)'
            " VALUES ($1,$2::text::float8,$3,$4::text::timestamptz,$5,'{}') RETURNING user_id",
            (
                data.user_id,
                None if data.max_budget is None else str(data.max_budget),
                data.budget_duration,
                _iso(next_month(self.clock())),
                data.user_role,
            ),
        )

    async def user_info(self, identity: str) -> JsonObject:
        return await self.db.one(
            "SELECT jsonb_build_object('user_id', u.user_id, 'spend', COALESCE(w.spend,0),"
            " 'max_budget', u.max_budget, 'budget_duration', u.budget_duration, 'budget_reset_at', u.budget_reset_at AT TIME ZONE 'UTC',"
            " 'teams', u.teams) AS user_info"
            ' FROM "LiteLLM_UserTable" u LEFT JOIN "LiteLLM_BudgetWindowSpend" w ON w.entity_type=\'user\''
            " AND w.entity_id=u.user_id AND w.window_duration='1mo' AND w.window_start=($1::text::timestamptz AT TIME ZONE 'UTC') WHERE u.user_id=$2",
            (_iso(self._window()), identity),
        )

    def _window(self) -> datetime:
        from .contracts import month_start

        return month_start(self.clock())

    async def user_update(self, data: UserUpdate) -> JsonObject:
        return await self.db.one(
            'UPDATE "LiteLLM_UserTable" SET max_budget=$1::text::float8 WHERE user_id=$2 RETURNING user_id,max_budget::float8 AS max_budget',
            (None if data.max_budget is None else str(data.max_budget), data.user_id),
        )

    async def team_new(self, data: TeamNew) -> JsonObject:
        return await self.db.one(
            'INSERT INTO "LiteLLM_TeamTable" (team_id,team_alias,organization_id,models,blocked,members,admins)'
            " VALUES ($1,$2,$3,$4,$5,'{}','{}') RETURNING team_id,team_alias,organization_id,models,blocked",
            (data.team_id, data.team_alias, data.organization_id, list(data.models), data.blocked),
        )

    async def team_info(self, identity: str) -> JsonObject:
        return await self.db.one(
            "SELECT jsonb_build_object('team_id',t.team_id,'team_alias',t.team_alias,'organization_id',t.organization_id,"
            " 'models',t.models,'blocked',t.blocked,'max_budget',t.max_budget,'model_aliases',null,"
            " 'access_group_ids',t.access_group_ids,'members_with_roles',COALESCE((SELECT jsonb_agg("
            " jsonb_build_object('user_id',m.user_id,'role','user'))"
            " FROM \"LiteLLM_TeamMembership\" m WHERE m.team_id=t.team_id),'[]')) AS team_info"
            ' FROM "LiteLLM_TeamTable" t WHERE t.team_id=$1',
            (identity,),
        )

    async def team_update(self, data: TeamUpdate) -> JsonObject:
        return await self.db.one(
            'UPDATE "LiteLLM_TeamTable" SET models=COALESCE($1,models),blocked=COALESCE($2,blocked)'
            " WHERE team_id=$3 RETURNING team_id,models,blocked",
            (None if data.models is None else list(data.models), data.blocked, data.team_id),
        )

    async def member_add(self, data: MemberAdd) -> JsonObject:
        return await self.db.one(
            'WITH claimed AS (UPDATE "LiteLLM_UserTable" u SET organization_id=t.organization_id,'
            " teams=CASE WHEN t.team_id=ANY(u.teams) THEN u.teams ELSE array_append(u.teams,t.team_id) END"
            ' FROM "LiteLLM_TeamTable" t WHERE t.team_id=$1 AND u.user_id=$2'
            " AND (u.organization_id IS NULL OR u.organization_id=t.organization_id) RETURNING u.user_id,t.team_id)"
            ' INSERT INTO "LiteLLM_TeamMembership" (user_id,team_id,daswork_allowed_models)'
            " SELECT user_id,team_id,$3 FROM claimed RETURNING user_id,team_id,daswork_allowed_models AS allowed_models",
            (data.team_id, data.member.user_id, list(data.allowed_models)),
        )

    async def member_update(self, data: MemberUpdate) -> JsonObject:
        return await self.db.one(
            'UPDATE "LiteLLM_TeamMembership" SET daswork_allowed_models=$1 WHERE team_id=$2 AND user_id=$3'
            " RETURNING team_id,user_id,daswork_allowed_models AS allowed_models",
            (list(data.allowed_models), data.team_id, data.user_id),
        )

    async def member_delete(self, data: MemberIdentity) -> JsonObject:
        return await self.db.one(
            'WITH removed AS (DELETE FROM "LiteLLM_TeamMembership" m WHERE m.team_id=$1 AND m.user_id=$2'
            ' AND NOT EXISTS (SELECT 1 FROM "LiteLLM_VerificationToken" k WHERE k.team_id=m.team_id AND k.user_id=m.user_id)'
            ' RETURNING m.team_id,m.user_id) UPDATE "LiteLLM_UserTable" u'
            " SET teams=array_remove(u.teams,r.team_id) FROM removed r WHERE u.user_id=r.user_id"
            " RETURNING u.user_id,r.team_id",
            (data.team_id, data.user_id),
        )

    async def key_new(self, data: KeyNew) -> JsonObject:
        if data.key_alias != f"dw-session-{data.metadata.dw_session_ref}":
            raise PublicError(400, "invalid_key_alias")
        secret: Final = f"sk-{token_urlsafe(32)}"
        written: Final = await self.db.one(
            _BINDING
            + 'INSERT INTO "LiteLLM_VerificationToken" (token,key_alias,user_id,team_id,organization_id,expires,models,'
            " blocked,allowed_routes,metadata) SELECT $5,$6,user_id,team_id,organization_id,"
            " $7::text::timestamptz + make_interval(secs => $8::int),$9::text[],$10,$11::text[],$12::text::jsonb FROM valid RETURNING key_alias",
            (
                data.organization_id,
                data.team_id,
                data.user_id,
                list(data.tags),
                fingerprint(secret),
                data.key_alias,
                _iso(self.clock()),
                int(data.duration[:-1]),
                list(data.models),
                data.blocked,
                list(data.allowed_routes),
                data.metadata.model_dump_json(),
            ),
        )
        return {**written, "key": secret}

    async def key_update(self, data: KeyUpdate) -> JsonObject:
        return await self.db.one(
            _UPDATE_BINDING
            + 'UPDATE "LiteLLM_VerificationToken" k SET team_id=v.team_id,models=$5,blocked=$6,allowed_routes=$7,'
            " metadata=$8::text::jsonb FROM valid v WHERE k.token=$9 RETURNING " + _KEY + " AS key",
            (
                data.organization_id,
                data.team_id,
                data.key,
                list(data.tags),
                list(data.models),
                data.blocked,
                list(data.allowed_routes),
                data.metadata.model_dump_json(),
                data.key,
                data.metadata.dw_session_ref,
            ),
        )

    async def key_list(self, alias: str | None, team: str | None, user: str | None, page: int, size: int) -> JsonObject:
        return await self.db.one(
            'WITH matching AS (SELECT k.* FROM "LiteLLM_VerificationToken" k WHERE'
            " ($1::text IS NULL OR k.key_alias=$2) AND ($3::text IS NULL OR k.team_id=$4)"
            " AND ($5::text IS NULL OR k.user_id=$6)),"
            f" paged AS (SELECT {_KEY} AS entry FROM matching k ORDER BY k.key_alias LIMIT $7 OFFSET $8)"
            " SELECT COALESCE((SELECT jsonb_agg(entry) FROM paged),'[]') AS keys,"
            " (SELECT count(*) FROM matching) AS total_count,$9::int AS current_page",
            (alias, alias, team, team, user, user, size, (page - 1) * size, page),
        )

    async def key_delete(self, data: KeyDelete) -> JsonObject:
        await self.db.one(
            'WITH removed AS (DELETE FROM "LiteLLM_VerificationToken" WHERE token=ANY($1) RETURNING token)'
            " SELECT count(*) FROM removed",
            (list(data.keys),),
        )
        return {"deleted_keys": list(data.keys)}
