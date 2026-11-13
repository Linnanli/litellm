from collections.abc import AsyncIterator, Awaitable
from typing import Annotated, Final

import anyio
from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, ValidationError
from starlette.responses import JSONResponse, Response, StreamingResponse

import litellm
from litellm.router_api import RoutingContext

from .contracts import JsonObject, PublicError
from .ledger import Ledger, Reservation
from .security import Admission, reject_attribution

_JSON: Final = TypeAdapter(dict[str, JsonValue])
_TERMINAL: Final = frozenset({"response.completed", "response.incomplete", "response.failed"})
_ALLOWED: Final = frozenset(
    {
        "include",
        "instructions",
        "max_output_tokens",
        "metadata",
        "parallel_tool_calls",
        "previous_response_id",
        "reasoning",
        "store",
        "stream",
        "temperature",
        "text",
        "tool_choice",
        "tools",
        "top_p",
        "truncation",
        "service_tier",
        "safety_identifier",
        "prompt_cache_key",
        "prompt_cache_retention",
        "max_tool_calls",
    }
)


class ResponsesRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    model: str
    input: JsonValue
    stream: bool = False
    background: bool = False
    max_output_tokens: Annotated[int, Field(gt=0, le=1_000_000)] | None = None


class CachedUsage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    cached_tokens: Annotated[int, Field(ge=0)] = 0


class Usage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    input_tokens: Annotated[int, Field(ge=0, le=1_000_000_000)]
    output_tokens: Annotated[int, Field(ge=0, le=1_000_000_000)]
    input_tokens_details: CachedUsage | None = None

    def counts(self) -> tuple[int, int, int]:
        cached: Final = self.input_tokens_details.cached_tokens if self.input_tokens_details else 0
        return self.input_tokens, self.output_tokens, min(cached, self.input_tokens)


def output_usage(event: JsonObject) -> tuple[int, int, int] | None:
    usage: Final = event.get("usage")
    return None if usage is None else Usage.model_validate(usage).counts()


def estimate_input(body: JsonObject) -> str:
    material: Final = {field: body[field] for field in ("input", "instructions", "tools") if field in body}
    return _JSON.dump_json(material).decode()


async def settled(
    ledger: Ledger, entry: Reservation, usage: tuple[int, int, int] | None, status: str = "success"
) -> None:
    with anyio.CancelScope(shield=True):
        await ledger.finish(entry, usage, status)


async def events(ledger: Ledger, entry: Reservation, stream: AsyncIterator[object]) -> AsyncIterator[str]:
    try:
        async for event in stream:
            if not isinstance(event, BaseModel):
                raise PublicError(502, "invalid_provider_event")
            wire = event.model_dump_json(exclude_none=True)
            data = _JSON.validate_json(wire)
            if (kind := data.get("type")) in _TERMINAL:
                response = data.get("response")
                if isinstance(response, dict):
                    await settled(
                        ledger, entry, output_usage(response), "failure" if kind == "response.failed" else "success"
                    )
            yield f"data: {wire}\n\n"
        yield "data: [DONE]\n\n"
    finally:
        with anyio.CancelScope(shield=True):
            try:
                await close_stream(stream)
            finally:
                await ledger.finish(entry, None)


async def close_stream(stream: AsyncIterator[object]) -> None:
    closer: Final[object] = getattr(stream, "aclose", None)  # pyright: ignore[reportAny]  # iterator variants have optional close
    if callable(closer):
        result: Final = closer()
        if isinstance(result, Awaitable):
            await result


async def responses(request: Request, ledger: Ledger, admission: Admission, presented: str, call_id: str) -> Response:
    try:
        body: Final = _JSON.validate_json(await request.body())
        data: Final = ResponsesRequest.model_validate(body)
    except ValidationError as exc:
        raise PublicError(400, "invalid_responses_request") from exc
    reject_attribution(body, tuple(request.headers.items()))
    if data.background:
        raise PublicError(400, "background_not_allowed")
    if "litellm_metadata" in body or any(name not in _ALLOWED | {"model", "input", "background"} for name in body):
        raise PublicError(400, "unsupported_responses_parameter")
    entry: Final = await ledger.reserve(
        presented,
        data.model,
        request.url.path,
        call_id,
        estimate_input(body),
        data.max_output_tokens,
    )
    try:
        options: Final = {name: body[name] for name in _ALLOWED if name in body}
        await admission.check(presented, call_id)
        result: Final = await RoutingContext(entry.deployment.router).aresponses(
            input=data.input,
            model=entry.deployment.model_id,
            disable_fallbacks=True,
            litellm_metadata={"user_api_key_team_id": entry.binding.team_id},
            max_retries=0,
            _skip_mcp_handler=True,
            num_retries=0,
            timeout=120,
            litellm_call_id=call_id,
            **options,
        )
        headers: Final = {"x-litellm-call-id": call_id}
        if isinstance(result, BaseModel):
            serialized: Final = _JSON.validate_json(result.model_dump_json(exclude_none=True))
            await settled(
                ledger,
                entry,
                output_usage(serialized),
                "failure" if serialized.get("status") == "failed" else "success",
            )
            return JSONResponse(serialized, headers=headers)
        if not isinstance(result, AsyncIterator):
            raise PublicError(502, "invalid_provider_response")
        return StreamingResponse(events(ledger, entry, result), media_type="text/event-stream", headers=headers)
    except (litellm.APIError, litellm.Timeout, litellm.APIConnectionError) as exc:
        await settled(ledger, entry, None)
        raise PublicError(502, "provider_request_failed") from exc
    except BaseException:
        await settled(ledger, entry, None)
        raise
