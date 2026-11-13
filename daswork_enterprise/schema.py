from importlib.resources import files
from os import environ
from pathlib import Path
from typing import Final

from psycopg import Connection
from psycopg.sql import SQL

_EXPECTED: Final = """
WITH expected(name, fields) AS (VALUES
    ('LiteLLM_BudgetTable', ARRAY['budget_id','max_budget','budget_reset_at','allowed_models']),
    ('LiteLLM_OrganizationTable', ARRAY['organization_id','budget_id']),
    ('LiteLLM_UserTable', ARRAY['user_id','organization_id','max_budget']),
    ('LiteLLM_TeamTable', ARRAY['team_id','organization_id','blocked','models']),
    ('LiteLLM_TagTable', ARRAY['tag_name','budget_id','daswork_organization_id']),
    ('LiteLLM_TeamMembership', ARRAY['user_id','team_id','budget_id','daswork_allowed_models']),
    ('LiteLLM_VerificationToken', ARRAY['token','key_alias','organization_id','metadata']),
    ('LiteLLM_ProxyModelTable', ARRAY['model_id','litellm_params','model_info']),
    ('LiteLLM_CredentialsTable', ARRAY['credential_name','credential_values']),
    ('LiteLLM_SpendLogs', ARRAY['request_id','litellm_call_id','startTime','endTime','request_tags','organization_id']),
    ('LiteLLM_BudgetWindowSpend', ARRAY['entity_type','entity_id','window_duration','window_start','spend'])
)
SELECT name, field FROM expected CROSS JOIN LATERAL unnest(fields) AS field
WHERE NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema=current_schema() AND table_name=name AND column_name=field
)
UNION ALL SELECT 'LiteLLM_VerificationToken','daswork_session_alias'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname=current_schema() AND indexname='daswork_session_alias'
    AND indexdef LIKE 'CREATE UNIQUE INDEX%'
)
"""


def check_schema(connection: Connection[tuple[object, ...]]) -> None:
    missing: Final = connection.execute(_EXPECTED).fetchall()
    if missing:
        fields: Final = ", ".join(f"{table}.{column}" for table, column in missing)
        raise ValueError(f"Daswork schema is incomplete ({fields})")


def run_schema(initialize: bool) -> None:
    with Connection.connect(environ["DATABASE_URL"], connect_timeout=5) as connection:
        if not initialize:
            connection.execute("SET TRANSACTION READ ONLY")
            check_schema(connection)
            return
        connection.execute("SELECT pg_advisory_xact_lock(4187726262002637646)")
        if connection.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() LIMIT 1"
        ).fetchone():
            raise ValueError("schema initialization requires an empty database")
        ddl: Final = Path("/app/schema.generated.sql").read_text(encoding="utf-8")
        extension: Final = files("daswork_enterprise").joinpath("schema.sql").read_text(encoding="utf-8")
        connection.execute(SQL(ddl), prepare=False)
        connection.execute(SQL(extension), prepare=False)
        check_schema(connection)
