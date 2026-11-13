from typing import Final

from litellm.constants import BILLED_RETRY_ATTEMPT_METADATA_KEY, INTERNAL_CALL_ORIGIN_METADATA_KEY

_WINDOW: Final = """
    "startTime" >= $1::text::date AND "startTime" < $2::text::date + INTERVAL '1 day'
"""
_ORGANIZATION: Final = f"{_WINDOW} AND organization_id = $3::text AND organization_id IS DISTINCT FROM $4::text"
_TAG: Final = f"{_WINDOW} AND request_tags @> jsonb_build_array($3::text)"


def _cache_count(explicit: str, *details: str) -> str:
    paths: Final = (
        f"usage -> '{explicit}'",
        *(f"usage #> '{{prompt_tokens_details,{detail}}}'" for detail in details),
    )
    counts: Final = tuple(
        f"CASE WHEN jsonb_typeof({path}) = 'number' THEN NULLIF(({path})::numeric, 0) END" for path in paths
    )
    return f"TRUNC(COALESCE({', '.join(counts)}, 0))::bigint"


_CACHE_READ: Final = _cache_count("cache_read_input_tokens", "cached_tokens")
_CACHE_CREATION: Final = _cache_count("cache_creation_input_tokens", "cache_write_tokens", "cache_creation_tokens")


def _report_source(scope: str, page: str = "") -> str:
    return f"""
        matched AS (
            SELECT request_id, "startTime", api_key, NULLIF(model, '') AS model,
                NULLIF(model_group, '') AS model_group, NULLIF(custom_llm_provider, '') AS provider,
                spend, prompt_tokens, completion_tokens, total_tokens, request_duration_ms,
                metadata -> 'usage_object' AS usage,
                COALESCE(metadata ->> 'status', status) IS NOT DISTINCT FROM 'failure' AS failed,
                COALESCE(metadata ->> '{INTERNAL_CALL_ORIGIN_METADATA_KEY}', '') NOT IN ('', 'false') AS internal,
                COALESCE(metadata -> '{BILLED_RETRY_ATTEMPT_METADATA_KEY}' = 'true'::jsonb, FALSE) AS retry
            FROM "LiteLLM_SpendLogs"
            WHERE {scope}
        ),
        usage_rows AS (
            SELECT "startTime"::date AS day, api_key, model, model_group, provider,
                spend, prompt_tokens, completion_tokens, total_tokens, request_duration_ms,
                failed, NOT (internal OR retry) AS caller_request, NOT internal AND NOT failed
                    AND request_duration_ms >= 0 AS timed_request,
                {_CACHE_READ} AS cache_read_input_tokens,
                {_CACHE_CREATION} AS cache_creation_input_tokens
            FROM (SELECT * FROM matched {page}) AS selected
        )
    """


_METRICS: Final = """
    COALESCE(SUM(spend), 0)::double precision AS spend,
    COALESCE(SUM(prompt_tokens), 0)::bigint AS prompt_tokens,
    COALESCE(SUM(completion_tokens), 0)::bigint AS completion_tokens,
    COALESCE(SUM(cache_read_input_tokens), 0)::bigint AS cache_read_input_tokens,
    COALESCE(SUM(cache_creation_input_tokens), 0)::bigint AS cache_creation_input_tokens,
    COUNT(*) FILTER (WHERE caller_request) AS api_requests,
    COUNT(*) FILTER (WHERE caller_request AND NOT failed) AS successful_requests,
    COUNT(*) FILTER (WHERE caller_request AND failed) AS failed_requests
"""

_PAGED_ACTIVITY: Final = """
    ORDER BY "startTime" DESC, request_id DESC
    LIMIT $4::bigint OFFSET $5::bigint
"""


def _activity_sql(source: str, key_limit: str, row_count: str) -> str:
    return f"""
        WITH {source},
        top_keys AS (
            SELECT api_key
            FROM usage_rows
            GROUP BY api_key
            ORDER BY SUM(spend) DESC, api_key
            LIMIT {key_limit}::bigint
        ),
        counts AS (
            SELECT {row_count} AS row_count, COUNT(DISTINCT api_key) AS total_api_keys
            FROM usage_rows
        ),
        rollups AS (
            SELECT day AS date,
                CASE WHEN GROUPING(model) = 0 THEN 'models'
                     WHEN GROUPING(model_group) = 0 THEN 'model_groups'
                     WHEN GROUPING(provider) = 0 THEN 'providers' END AS dimension,
                COALESCE(model, model_group, provider) AS label,
                GROUPING(top_keys.api_key) = 0 AS by_key, top_keys.api_key,
                {_METRICS},
                COALESCE(SUM(total_tokens), 0)::bigint AS total_tokens,
                COALESCE(SUM(request_duration_ms) FILTER (WHERE timed_request), 0)::bigint AS total_response_time_ms,
                COUNT(*) FILTER (WHERE timed_request) AS timed_requests
            FROM usage_rows
            LEFT JOIN top_keys USING (api_key)
            GROUP BY GROUPING SETS (
                (), (day), (day, model), (day, model_group), (day, provider),
                (day, top_keys.api_key), (day, model, top_keys.api_key),
                (day, model_group, top_keys.api_key), (day, provider, top_keys.api_key)
            )
            HAVING GROUPING(top_keys.api_key) <> 0 OR top_keys.api_key IS NOT NULL
        )
        SELECT rollups.*, counts.row_count, counts.total_api_keys
        FROM rollups CROSS JOIN counts
        ORDER BY date DESC NULLS FIRST, dimension NULLS FIRST, label NULLS FIRST, by_key, api_key
    """


ORGANIZATION_ACTIVITY: Final = _activity_sql(_report_source(_ORGANIZATION), "$5", "COUNT(*)")
TAG_ACTIVITY: Final = _activity_sql(_report_source(_TAG, _PAGED_ACTIVITY), "$4", "(SELECT COUNT(*) FROM matched)")

ORGANIZATION_EXPORT: Final = f"""
    WITH {_report_source(_ORGANIZATION)}
    SELECT TO_CHAR(day, 'YYYY-MM-DD') AS date, $3::text AS entity_id,
        (SELECT organization_alias FROM "LiteLLM_OrganizationTable" WHERE organization_id = $3::text)
            AS entity_alias,
        NULL::text AS api_key, NULL::text AS key_alias, NULL::text AS user_id, NULL::text AS user_email,
        CASE WHEN $5::boolean THEN model END AS model, 0.0::double precision AS flat_cost,
        {_METRICS}
    FROM usage_rows
    GROUP BY day, CASE WHEN $5::boolean THEN model END
    ORDER BY day, CASE WHEN $5::boolean THEN model END
"""
