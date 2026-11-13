import csv
import io
import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Final, TypeAlias

import pytest
from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient
from pydantic import TypeAdapter

from daswork_enterprise.reporting import ReportingQuery, build_reporting_router

_ORG: Final = "/organization/daily/activity/aggregated"
_TAG: Final = "/tag/daily/activity"
_EXPORT: Final = "/organization/daily/activity/export"
_AUTH: Final = {"Authorization": "Bearer synthetic-master"}
_DATES: Final = {"start_date": "2026-11-13", "end_date": "2026-11-14"}

_JsonObject: TypeAlias = Mapping[str, object]
_OBJECT: Final = TypeAdapter(Mapping[str, object])
_OBJECTS: Final = TypeAdapter(tuple[Mapping[str, object], ...])


async def _master_key(authorization: Annotated[str | None, Header()] = None) -> None:
    if authorization is None:
        raise HTTPException(status_code=401)
    if authorization != _AUTH["Authorization"]:
        raise HTTPException(status_code=403)


def _client(get_query: Callable[[], ReportingQuery]) -> TestClient:
    app: Final = FastAPI()
    app.include_router(build_reporting_router(require_master_key=_master_key, get_query=get_query))
    return TestClient(app)


def _unexpected_query() -> ReportingQuery:
    pytest.fail("A denied request acquired the report reader")


@pytest.mark.parametrize(
    ("path", "selector"),
    ((_ORG, {"organization_ids": "org"}), (_TAG, {"tags": "parent"}), (_EXPORT, {"organization_ids": "org"})),
)
def test_report_requires_master_key(path: str, selector: Mapping[str, str]) -> None:
    with _client(_unexpected_query) as client:
        assert client.get(path, params={**_DATES, **selector}).status_code == 401
        assert (
            client.get(
                path, params={**_DATES, **selector}, headers={"Authorization": "Bearer synthetic-session"}
            ).status_code
            == 403
        )


async def _no_query(sql: str, *params: object) -> Sequence[_JsonObject] | None:
    pytest.fail(f"Invalid input ran query with {len(sql)} characters and {len(params)} parameters")


def _get_no_query() -> ReportingQuery:
    return _no_query


@pytest.mark.parametrize(
    ("path", "params", "status"),
    (
        (_ORG, {"organization_ids": "org", "start_date": "2026-11-15"}, 400),
        (_ORG, {"organization_ids": "org", "api_key_limit": "2"}, 422),
        (_TAG, {"tags": "parent", "page": "0"}, 422),
        (_TAG, {"tags": "parent", "end_date": "20261114"}, 400),
        (_EXPORT, {"organization_ids": "org", "export_type": "daily_with_keys"}, 422),
        (_EXPORT, {"organization_ids": "org", "format": "json"}, 422),
    ),
)
def test_rejects_invalid_input(path: str, params: Mapping[str, str], status: int) -> None:
    with _client(_get_no_query) as client:
        assert client.get(path, params={**_DATES, **params}, headers=_AUTH).status_code == status


_INSERT: Final = """
    INSERT INTO "LiteLLM_SpendLogs" (
        request_id, "startTime", api_key, organization_id, model, model_group, custom_llm_provider,
        spend, prompt_tokens, completion_tokens, total_tokens, request_duration_ms, status, metadata, request_tags
    ) VALUES (
        %(id)s, %(start)s, %(key)s, %(org)s, %(model)s, %(group)s, %(provider)s,
        %(spend)s, %(prompt)s, 3, %(prompt)s + 3, %(duration)s, %(status)s, %(metadata)s, %(tags)s
    )
"""
_TABLES: Final = """
    CREATE TEMP TABLE "LiteLLM_SpendLogs" (
        request_id TEXT PRIMARY KEY, "startTime" TIMESTAMP NOT NULL, api_key TEXT NOT NULL, organization_id TEXT,
        model TEXT NOT NULL, model_group TEXT, custom_llm_provider TEXT, spend DOUBLE PRECISION NOT NULL,
        prompt_tokens INTEGER NOT NULL, completion_tokens INTEGER NOT NULL, total_tokens INTEGER NOT NULL,
        request_duration_ms INTEGER, status TEXT, metadata JSONB, request_tags JSONB
    );
    CREATE INDEX ON "LiteLLM_SpendLogs" ("startTime", request_id);
    CREATE TEMP TABLE "LiteLLM_OrganizationTable" (organization_id TEXT PRIMARY KEY, organization_alias TEXT NOT NULL);
    INSERT INTO "LiteLLM_OrganizationTable" VALUES ('org', E'测试, "引用"\\n下一行');
"""
_START_FIRST: Final = datetime.fromisoformat("2026-11-13T15:25:00")
_START_SECOND: Final = datetime.fromisoformat("2026-11-13T16:25:00")
_START_THIRD: Final = datetime.fromisoformat("2026-11-14T00:25:00")


def _numbered_query(sql: str, params: Sequence[object]) -> tuple[str, Mapping[str, object]]:
    from re import sub

    return (
        sub(r"\$(\d+)", r"%(parameter_\1)s", sql),
        {f"parameter_{index}": value for index, value in enumerate(params, start=1)},
    )


@pytest.fixture
def reports() -> Iterator[TestClient]:
    dsn: Final = os.environ.get("DASWORK_REPORTING_TEST_DSN")
    if not dsn:
        pytest.skip("DASWORK_REPORTING_TEST_DSN is required for PostgreSQL reporting tests")

    from psycopg import Connection, sql
    from psycopg.rows import DictRow, dict_row
    from psycopg.types.json import Jsonb

    ordinary: Final = {
        "start": _START_FIRST,
        "key": "key-old",
        "org": "org",
        "model": 'native,"model"',
        "group": "public",
        "provider": "unknown",
        "spend": Decimal("0.026"),
        "prompt": 20,
        "duration": 23,
        "status": "success",
        "metadata": Jsonb({"status": "success", "usage_object": {"prompt_tokens_details": {"cached_tokens": 9}}}),
        "tags": Jsonb(["parent", "old", "leaf"]),
    }
    second: Final = {
        **ordinary,
        "id": "second",
        "start": _START_SECOND,
        "model": "=公式",
        "group": None,
        "provider": None,
        "key": "key-new",
        "status": "failure",
        "metadata": Jsonb({"status": "failure", "usage_object": {"cache_creation_input_tokens": 7}}),
        "tags": Jsonb(["parent", "new", "leaf"]),
    }
    third: Final = {
        **ordinary,
        "id": "third",
        "start": _START_THIRD,
        "prompt": 1_500_000_000,
        "tags": Jsonb(["parent", "new", "new"]),
    }
    bulk: Final = """
        INSERT INTO "LiteLLM_SpendLogs" (
            request_id, "startTime", api_key, organization_id, model, spend, prompt_tokens, completion_tokens,
            total_tokens, status, request_tags
        )
        SELECT 'bulk-' || n, timestamp '2026-11-14 23:59:59', 'key-bulk', 'bulk', 'bulk',
            0, 1, 0, 1, NULL, '["bulk-tag"]'::jsonb
        FROM generate_series(1, 52) AS n
    """
    with Connection[DictRow].connect(dsn, autocommit=True, row_factory=dict_row) as connection:
        connection.execute(sql.SQL(_TABLES))
        connection.execute(sql.SQL("SET TIME ZONE 'Asia/Shanghai'"))
        with connection.cursor() as cursor:
            cursor.executemany(
                sql.SQL(_INSERT),
                (
                    {**ordinary, "id": "first"},
                    second,
                    third,
                    {**third, "id": "large", "spend": Decimal("0"), "tags": Jsonb(["large-tag"])},
                    {**ordinary, "id": "diagnostic", "org": "diagnostic", "tags": Jsonb(["diagnostic-tag"])},
                    {**ordinary, "id": "other", "org": "org-sibling", "tags": Jsonb(["parent-sibling"])},
                    {**ordinary, "id": "outside", "start": datetime.fromisoformat("2026-11-15T00:00:00")},
                ),
            )
        connection.execute(sql.SQL(bulk))
        connection.execute(sql.SQL(_BULK_EXPORT_ROWS))

        async def query(statement: str, *params: object) -> Sequence[_JsonObject]:
            rewritten, parameters = _numbered_query(statement, params)
            cursor: Final = connection.execute(
                sql.SQL(rewritten),  # pyright: ignore[reportArgumentType]  # The statement only comes from static report SQL.
                parameters,
            )
            return _OBJECTS.validate_python(cursor.fetchall())

        def get_query() -> ReportingQuery:
            return query

        with _client(get_query) as client:
            yield client


def _read_report(
    client: TestClient,
    path: str,
    **params: str,  # kwargs-ok: optional HTTP query strings are forwarded unchanged
) -> tuple[_JsonObject, tuple[_JsonObject, ...]]:
    response: Final = client.get(path, headers=_AUTH, params={**_DATES, **params})
    assert response.status_code == 200, response.text
    payload: Final = _OBJECT.validate_json(response.content)
    return _OBJECT.validate_python(payload["metadata"]), _OBJECTS.validate_python(payload["results"])


def test_organization_uses_recorded_ownership_and_utc_dates(reports: TestClient) -> None:
    metadata, days = _read_report(
        reports, _ORG, organization_ids="org", exclude_organization_ids="diagnostic", api_key_limit="1"
    )
    assert (metadata["page"], metadata["total_pages"], metadata["has_more"], metadata["api_key_limit"]) == (
        1,
        1,
        False,
        1,
    )
    assert metadata["total_api_requests"] == 4
    assert metadata["total_prompt_tokens"] == 3_000_000_040
    assert isinstance(metadata["total_prompt_tokens"], int)
    assert metadata["total_spend"] == pytest.approx(0.078)
    assert metadata["total_response_time_ms"] == 69
    assert metadata["total_timed_requests"] == 3
    assert [day["date"] for day in days] == ["2026-11-14", "2026-11-13"]

    breakdown: Final = _OBJECT.validate_python(days[1]["breakdown"])
    day: Final = _OBJECT.validate_python(days[1]["metrics"])
    assert day["spend"] == pytest.approx(0.052)
    assert day["api_requests"] == 2
    assert day["successful_requests"] == day["failed_requests"] == 1
    assert day["prompt_tokens"] == 40
    assert day["completion_tokens"] == 6
    assert day["total_tokens"] == 46
    assert day["cache_read_input_tokens"] == 9
    assert day["cache_creation_input_tokens"] == 7
    assert tuple(_OBJECT.validate_python(breakdown["entities"])) == ("org",)
    assert tuple(_OBJECT.validate_python(breakdown["model_groups"])) == ("public",)
    assert tuple(_OBJECT.validate_python(breakdown["providers"])) == ("unknown",)
    assert tuple(_OBJECT.validate_python(breakdown["api_keys"])) == ("key-old",)
    assert set(_OBJECT.validate_python(breakdown["models"])) == {'native,"model"', "=公式"}


def test_tag_pages_by_requests_and_preserves_old_department(reports: TestClient) -> None:
    old, old_days = _read_report(reports, _TAG, tags="old")
    new, new_days = _read_report(reports, _TAG, tags="new")
    parent, parent_days = _read_report(reports, _TAG, tags="parent")
    assert (old["total_api_requests"], new["total_api_requests"], parent["total_api_requests"]) == (1, 2, 3)
    assert old["total_spend"] == pytest.approx(0.026)
    assert new["total_spend"] == pytest.approx(0.052)
    assert parent["total_spend"] == pytest.approx(0.078)
    for tag, days in (("old", old_days), ("new", new_days), ("parent", parent_days)):
        for day in days:
            breakdown = _OBJECT.validate_python(day["breakdown"])
            assert tuple(_OBJECT.validate_python(breakdown["entities"])) == (tag,)

    first, first_days = _read_report(reports, _TAG, tags="bulk-tag", page="1", page_size="50")
    second, second_days = _read_report(reports, _TAG, tags="bulk-tag", page="2", page_size="50")
    beyond, beyond_days = _read_report(reports, _TAG, tags="bulk-tag", page="3", page_size="50")
    empty, empty_days = _read_report(reports, _TAG, tags="unmatched", page="1", page_size="50")
    assert (first["page"], second["page"], beyond["page"], empty["page"]) == (1, 2, 3, 1)
    assert (first["total_pages"], second["total_pages"], beyond["total_pages"], empty["total_pages"]) == (2, 2, 2, 0)
    assert (first["has_more"], second["has_more"], beyond["has_more"], empty["has_more"]) == (True, False, False, False)
    assert (
        first["total_api_requests"],
        second["total_api_requests"],
        beyond["total_api_requests"],
        empty["total_api_requests"],
    ) == (50, 2, 0, 0)
    assert first_days[0]["date"] == second_days[0]["date"] == "2026-11-14"
    assert _OBJECT.validate_python(first_days[0]["metrics"])["successful_requests"] == 50
    assert _OBJECT.validate_python(second_days[0]["metrics"])["successful_requests"] == 2
    assert beyond_days == empty_days == ()


_HEADER: Final = (
    "date,entity_id,entity_alias,api_key,key_alias,user_id,user_email,model,spend,flat_cost,prompt_tokens,"
    "completion_tokens,api_requests,successful_requests,failed_requests,cache_read_input_tokens,"
    "cache_creation_input_tokens\r\n"
)


def test_export_formats_grouping_and_spreadsheet_safety(reports: TestClient) -> None:
    daily: Final = reports.get(_EXPORT, headers=_AUTH, params={**_DATES, "organization_ids": "org", "format": "csv"})
    models: Final = reports.get(
        _EXPORT,
        headers=_AUTH,
        params={**_DATES, "organization_ids": "org", "export_type": "daily_with_models", "format": "csv"},
    )
    empty: Final = reports.get(_EXPORT, headers=_AUTH, params={**_DATES, "organization_ids": "absent"})
    assert daily.status_code == models.status_code == empty.status_code == 200
    assert daily.headers["content-type"] == "text/csv; charset=utf-8"
    assert daily.content.startswith(_HEADER.encode())
    assert models.content.count(b"\r\n") == 4
    assert b'""' in models.content and '引用""\n下一行'.encode() in models.content
    assert empty.content == _HEADER.encode()
    daily_rows: Final = tuple(csv.DictReader(io.StringIO(daily.text, newline="")))
    model_rows: Final = tuple(csv.DictReader(io.StringIO(models.text, newline="")))
    assert [row["date"] for row in daily_rows] == ["2026-11-13", "2026-11-14"]
    assert tuple(row["model"] for row in daily_rows) == ("", "")
    assert {row["model"] for row in model_rows} == {'native,"model"', "'=公式"}
    for rows in (daily_rows, model_rows):
        assert all(row["entity_id"] == "org" and row["entity_alias"] == '测试, "引用"\n下一行' for row in rows)
        assert all(
            (row["api_key"], row["key_alias"], row["user_id"], row["user_email"]) == ("", "", "", "") for row in rows
        )
        assert sum(Decimal(row["spend"]) for row in rows) == Decimal("0.078")
        assert sum(int(row["api_requests"]) for row in rows) == 4


_BULK_EXPORT_ROWS: Final = """
    INSERT INTO "LiteLLM_SpendLogs" (
        request_id, "startTime", api_key, organization_id, model, spend, prompt_tokens, completion_tokens, total_tokens
    )
    SELECT org || '-' || starts_at::date || '-' || n, starts_at, 'bulk-key', org, org || '-' || n || tail, 0.026, 1, 0, 1
    FROM (
        VALUES
            ('row-limit', 10000, timestamp '2026-11-13 15:25:00', ''),
            ('row-limit', 1, timestamp '2026-11-14 00:25:00', ''),
            ('byte-limit', 1000, timestamp '2026-11-13 15:25:00', repeat('测', 1500))
    ) AS fixture(org, num_rows, starts_at, tail)
    CROSS JOIN LATERAL generate_series(1, num_rows) AS n
"""


def test_export_preserves_payload_for_java_limits(reports: TestClient) -> None:
    for org, end_date, expected_rows, exceeds_bytes in (
        ("row-limit", "2026-11-13", 10_000, False),
        ("row-limit", "2026-11-14", 10_001, False),
        ("byte-limit", "2026-11-14", 1_000, True),
    ):
        response = reports.get(
            _EXPORT,
            headers=_AUTH,
            params={
                **_DATES,
                "organization_ids": org,
                "end_date": end_date,
                "export_type": "daily_with_models",
            },
        )
        assert response.status_code == 200, response.text
        assert (len(response.content) > 4 * 1024 * 1024) is exceeds_bytes
        rows = tuple(csv.DictReader(io.StringIO(response.text, newline="")))
        assert len(rows) == expected_rows
        assert all(row["entity_id"] == org for row in rows)
