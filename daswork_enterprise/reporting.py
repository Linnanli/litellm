from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from datetime import date
from itertools import groupby
from math import isfinite
from typing import Annotated, Final, Literal, Protocol, TypeAlias, TypeVar

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from prisma.errors import PrismaError
from pydantic import ConfigDict, FiniteFloat, TypeAdapter, ValidationError

from litellm._logging import verbose_proxy_logger
from litellm.proxy.management_endpoints.common_daily_activity import (
    InvalidDateRange,
    parse_canonical_date_range,
    raise_public,
)
from litellm.proxy.management_endpoints.daily_activity_routes import (
    _content_disposition,  # pyright: ignore[reportPrivateUsage]  # Reuse the native filename convention.
    _stream_export_rows,  # pyright: ignore[reportPrivateUsage]  # Reuse the native CSV format and formula protection.
)
from litellm.proxy.management_endpoints.team_endpoints import aggregated_date_range_error
from litellm.repositories.daily_activity_buckets import api_key_bucket
from litellm.types.proxy.management_endpoints.common_daily_activity import (
    BreakdownMetrics,
    DailySpendData,
    DailySpendMetadata,
    KeyMetricWithMetadata,
    MetricWithMetadata,
    SpendAnalyticsPaginatedResponse,
    SpendMetrics,
)
from litellm.types.repositories.daily_activity import ExportRow, ExportType

from .reporting_sql import ORGANIZATION_ACTIVITY, ORGANIZATION_EXPORT, TAG_ACTIVITY

Dimension: TypeAlias = Literal["models", "model_groups", "providers"]
ExportGroup: TypeAlias = Literal["daily", "daily_with_models"]


class ReportingQuery(Protocol):
    async def __call__(self, query: str, /, *values: object) -> Sequence[Mapping[str, object]] | None: ...


class _Rollup(SpendMetrics):
    model_config = ConfigDict(frozen=True)
    spend: FiniteFloat = 0.0
    date: date | None
    dimension: Dimension | None
    label: str | None
    by_key: bool
    api_key: str | None
    row_count: int
    total_api_keys: int


_ROLLUPS: Final = TypeAdapter(tuple[_Rollup, ...])
_EXPORT_ROWS: Final = TypeAdapter(tuple[ExportRow, ...])
_T: Final = TypeVar("_T")


async def _read(
    query: ReportingQuery, operation: str, adapter: TypeAdapter[tuple[_T, ...]], sql: str, *params: object
) -> tuple[_T, ...]:
    try:
        raw: Final = await query(sql, *params)
    except (PrismaError, ConnectionError, TimeoutError) as exc:
        verbose_proxy_logger.error("Reporting read failed operation=%s category=%s", operation, type(exc).__name__)
        raise HTTPException(status_code=503, detail="Reporting database unavailable") from None
    try:
        return adapter.validate_python(raw or ())
    except ValidationError as exc:
        verbose_proxy_logger.error("Reporting result invalid operation=%s errors=%s", operation, exc.error_count())
        raise HTTPException(status_code=500, detail="Reporting result invalid") from None


def _required_rollup(rows: Sequence[_Rollup]) -> _Rollup:
    if rows:
        return rows[0]
    raise HTTPException(status_code=500, detail="Reporting result invalid")


def _validate_organization_dates(start_date: str, end_date: str) -> None:
    range_error: Final = aggregated_date_range_error(start_date, end_date)
    if range_error:
        raise_public(InvalidDateRange(reason=range_error))


def _metrics(row: _Rollup) -> SpendMetrics:
    return SpendMetrics(
        spend=row.spend,
        prompt_tokens=row.prompt_tokens,
        completion_tokens=row.completion_tokens,
        total_tokens=row.total_tokens,
        cache_read_input_tokens=row.cache_read_input_tokens,
        cache_creation_input_tokens=row.cache_creation_input_tokens,
        api_requests=row.api_requests,
        successful_requests=row.successful_requests,
        failed_requests=row.failed_requests,
        total_response_time_ms=row.total_response_time_ms,
        timed_requests=row.timed_requests,
    )


def _key_metrics(rows: Sequence[_Rollup]) -> dict[str, KeyMetricWithMetadata]:  # mutable-ok: native response field
    return {api_key_bucket(row.api_key): KeyMetricWithMetadata(metrics=_metrics(row)) for row in rows if row.by_key}


def _entity_metric(rows: Sequence[_Rollup]) -> MetricWithMetadata:
    return MetricWithMetadata(metrics=_metrics(_required_rollup(rows)), api_key_breakdown=_key_metrics(rows))


def _dimension_metrics(
    rows: Sequence[_Rollup], dimension: Dimension
) -> dict[str, MetricWithMetadata]:  # mutable-ok: native response field
    matching: Final = (row for row in rows if row.dimension == dimension)
    return {
        label: _entity_metric(tuple(bucket))
        for label, bucket in groupby(matching, key=lambda row: row.label)
        if label is not None
    }


def _day(date_value: date, entity_id: str, rows: Sequence[_Rollup]) -> DailySpendData:
    entity_rows: Final = tuple(row for row in rows if row.dimension is None)
    entity: Final = _entity_metric(entity_rows)
    return DailySpendData(
        date=date_value,
        metrics=entity.metrics,
        breakdown=BreakdownMetrics(
            entities={entity_id: entity},
            models=_dimension_metrics(rows, "models"),
            model_groups=_dimension_metrics(rows, "model_groups"),
            providers=_dimension_metrics(rows, "providers"),
            api_keys=entity.api_key_breakdown,
        ),
    )


def _activity(
    rows: Sequence[_Rollup], entity_id: str, *, page: int, page_size: int, api_key_limit: int | None = None
) -> SpendAnalyticsPaginatedResponse:
    totals: Final = _required_rollup(rows)
    total_pages: Final = 1 if api_key_limit is not None else (totals.row_count + page_size - 1) // page_size
    return SpendAnalyticsPaginatedResponse(
        results=[
            _day(day, entity_id, tuple(day_rows))
            for day, day_rows in groupby(rows, key=lambda row: row.date)
            if day is not None
        ],
        metadata=DailySpendMetadata(
            total_spend=totals.spend,
            total_prompt_tokens=totals.prompt_tokens,
            total_completion_tokens=totals.completion_tokens,
            total_tokens=totals.total_tokens,
            total_cache_read_input_tokens=totals.cache_read_input_tokens,
            total_cache_creation_input_tokens=totals.cache_creation_input_tokens,
            total_api_requests=totals.api_requests,
            total_successful_requests=totals.successful_requests,
            total_failed_requests=totals.failed_requests,
            total_response_time_ms=totals.total_response_time_ms,
            total_timed_requests=totals.timed_requests,
            page=page,
            total_pages=total_pages,
            has_more=page < total_pages,
            api_key_limit=api_key_limit,
            total_api_keys=totals.total_api_keys,
        ),
    )


def build_reporting_router(
    *,
    require_master_key: Callable[..., Awaitable[None]],
    get_query: Callable[..., ReportingQuery | Awaitable[ReportingQuery]],
) -> APIRouter:
    router: Final = APIRouter(dependencies=[Depends(require_master_key)])

    @router.get("/organization/daily/activity/aggregated")
    async def organization_activity(  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        query: Annotated[ReportingQuery, Depends(get_query)],
        organization_ids: Annotated[str, Query(min_length=1)],
        start_date: str,
        end_date: str,
        api_key_limit: Annotated[int, Query(ge=1, le=1)] = 1,
        exclude_organization_ids: str | None = None,
    ) -> SpendAnalyticsPaginatedResponse:
        _validate_organization_dates(start_date, end_date)
        rows: Final = await _read(
            query,
            "organization_activity",
            _ROLLUPS,
            ORGANIZATION_ACTIVITY,
            start_date,
            end_date,
            organization_ids,
            exclude_organization_ids,
            api_key_limit,
        )
        return _activity(rows, organization_ids, page=1, page_size=1, api_key_limit=api_key_limit)

    @router.get("/tag/daily/activity")
    async def tag_activity(  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        query: Annotated[ReportingQuery, Depends(get_query)],
        tags: Annotated[str, Query(min_length=1)],
        start_date: str,
        end_date: str,
        page: Annotated[int, Query(ge=1, le=2_147_483_647)] = 1,
        page_size: Annotated[int, Query(ge=1, le=1000)] = 50,
    ) -> SpendAnalyticsPaginatedResponse:
        window: Final = parse_canonical_date_range(start_date, end_date)
        if isinstance(window, InvalidDateRange):
            raise_public(window)
        rows: Final = await _read(
            query,
            "tag_activity",
            _ROLLUPS,
            TAG_ACTIVITY,
            start_date,
            end_date,
            tags,
            page_size,
            (page - 1) * page_size,
        )
        return _activity(rows, tags, page=page, page_size=page_size)

    @router.get("/organization/daily/activity/export", response_class=StreamingResponse)
    async def organization_export(  # pyright: ignore[reportUnusedFunction]  # registered by the FastAPI decorator
        query: Annotated[ReportingQuery, Depends(get_query)],
        organization_ids: Annotated[str, Query(min_length=1)],
        start_date: str,
        end_date: str,
        export_type: Annotated[ExportGroup, Query()] = "daily",
        file_format: Annotated[Literal["csv"], Query(alias="format")] = "csv",
    ) -> StreamingResponse:
        _validate_organization_dates(start_date, end_date)
        rows: Final = await _read(
            query,
            "organization_export",
            _EXPORT_ROWS,
            ORGANIZATION_EXPORT,
            start_date,
            end_date,
            organization_ids,
            None,
            export_type == "daily_with_models",
        )

        if any(not isfinite(row.spend) for row in rows):
            verbose_proxy_logger.error(
                "Reporting result invalid operation=organization_export category=nonfinite_spend"
            )
            raise HTTPException(status_code=500, detail="Reporting result invalid")

        async def stream_rows() -> AsyncIterator[ExportRow]:
            for row in rows:
                yield row

        return StreamingResponse(
            _stream_export_rows(None, stream_rows(), file_format),
            media_type="text/csv",
            headers={
                "Cache-Control": "no-store",
                "Content-Disposition": _content_disposition(
                    "organization",
                    date.fromisoformat(start_date),
                    date.fromisoformat(end_date),
                    ExportType(export_type),
                    file_format,
                ),
            },
        )

    return router
