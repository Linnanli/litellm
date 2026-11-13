import logging
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Mapping
from typing import Annotated, Final

import anyio
import httpx
from fastapi import Request
from openai import APIConnectionError, APITimeoutError, OpenAIError
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, ValidationError
from starlette.responses import JSONResponse, Response

import litellm
from litellm.exceptions import MidStreamFallbackError
from litellm.litellm_core_utils.litellm_logging import Logging
from litellm.proxy._types import ProxyException
from litellm.proxy.common_request_processing import create_response
from litellm.proxy.common_utils.openai_error_payload import (
    LITELLM_CALL_ID_HEADER,
    error_status_code,
    openai_error_type_for_status,
)
from litellm.proxy.common_utils.responses_stream_errors import ResponsesStreamErrorState
from litellm.responses.streaming_iterator import BaseResponsesAPIStreamingIterator
from litellm.router_api import RoutingContext

from .contracts import JsonObject, PublicError
from .ledger import Ledger, Reservation
from .security import Admission, reject_attribution

_JSON: Final = TypeAdapter(dict[str, JsonValue])
_ERROR_PAYLOAD: Final = TypeAdapter(Mapping[str, Mapping[str, str]])
_LOG: Final = logging.getLogger(__name__)
_CLIENT_METADATA: Final[TypeAdapter[Mapping[str, str] | None]] = TypeAdapter(Mapping[str, str] | None)
_TERMINAL: Final = frozenset({"response.completed", "response.incomplete", "response.failed"})
_FAILED_RESPONSE: Final = "provider_response_failed"
_PROVIDER_OPTIONS: Final = frozenset(
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


class ProviderEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    type: str
    response: JsonObject | None = None

    def settlement(self) -> tuple[tuple[int, int, int] | None, str | None]:
        if self.response is None:
            raise PublicError(502, "invalid_provider_event")
        return output_usage(self.response), _FAILED_RESPONSE if self.type == "response.failed" else None


def output_usage(event: JsonObject) -> tuple[int, int, int] | None:
    usage: Final = event.get("usage")
    return None if usage is None else Usage.model_validate(usage).counts()


def interrupted_usage(stream: AsyncIterator[object]) -> tuple[int, int, int] | None:
    if not isinstance(stream, BaseResponsesAPIStreamingIterator):
        return None
    logging_obj: Final[object] = getattr(stream, "logging_obj", None)  # pyright: ignore[reportAny]  # router wrappers may lack a Logging instance
    if not isinstance(logging_obj, Logging):
        return None
    usage: Final[object] = logging_obj.model_call_details.get("combined_usage_object")
    if not isinstance(usage, litellm.Usage):
        return None
    details: Final = usage.prompt_tokens_details
    cached: Final = details.cached_tokens or 0 if details is not None else 0
    return usage.prompt_tokens, usage.completion_tokens, min(usage.prompt_tokens, cached)


def estimate_input(prompt: JsonValue, options: JsonObject) -> str:
    context: Final = {field: options[field] for field in ("instructions", "tools") if field in options}
    return _JSON.dump_json({"input": prompt, **context}).decode()


async def settled(
    ledger: Ledger, entry: Reservation, usage: tuple[int, int, int] | None, *, error_code: str | None = None
) -> None:
    with anyio.CancelScope(shield=True):
        await ledger.finish(entry, usage, error_code=error_code)


def provider_status(error: Exception) -> int:
    if isinstance(error, MidStreamFallbackError) and error.original_exception is not None:
        return provider_status(error.original_exception)
    if isinstance(error, PublicError):
        return error.status
    if isinstance(error, APIConnectionError) and not isinstance(error, APITimeoutError):
        return 502
    default: Final = 502 if isinstance(error, (OpenAIError, httpx.HTTPError, ValidationError)) else 500
    return error_status_code(error, default)


class ProviderError(PublicError):
    def __init__(self, error: Exception, call_id: str) -> None:
        upstream: Final = provider_status(error)
        status: Final = 502 if upstream in (401, 403) else upstream
        category: Final = (
            "provider_authentication_failed" if upstream in (401, 403) else openai_error_type_for_status(status)
        )
        super().__init__(status, "provider_request_failed", category)
        self.call_id = call_id
        self.error_code = str(upstream)
        _LOG.warning("checkpoint=provider call=%s status=%s category=%s", call_id, upstream, type(error).__name__)

    def frame(self) -> str:
        payload: Final = {"error": {"message": str(self), "type": self.category, "code": str(self.status)}}
        return f"data: {_ERROR_PAYLOAD.dump_json(payload).decode()}\n\n"


async def events(ledger: Ledger, entry: Reservation, stream: AsyncIterator[object]) -> AsyncGenerator[str, None]:
    state: Final = ResponsesStreamErrorState()
    started = False  # rebind-ok: once a frame is sent, subsequent errors must be sent in-band
    try:
        while True:
            try:
                event = await anext(stream, None)
                if not isinstance(event, BaseModel):
                    raise PublicError(502, "invalid_provider_event")
                wire = event.model_dump_json(exclude_none=True)
                data = ProviderEvent.model_validate_json(wire)
                settlement = data.settlement() if data.type in _TERMINAL else None
                state.observe_chunk(event)
            except Exception as exc:  # noqa: BLE001  # the upstream iterator must close an already started response in-band
                error = ProviderError(exc, entry.call_id)
                await settled(ledger, entry, interrupted_usage(stream), error_code=error.error_code)
                if not started:
                    yield error.frame()
                    return
                if frame := state.format_failure(ProxyException(str(error), error.category, None, error.status)):
                    yield frame
                break
            if settlement is not None:
                usage, error_code = settlement
                await settled(ledger, entry, usage, error_code=error_code)
            started = True
            frame = f"data: {wire}\n\n"
            state.mark_emitted(frame)
            yield frame
            if settlement is not None:
                break
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


def parse_responses_request(raw: bytes, headers: tuple[tuple[str, str], ...]) -> tuple[ResponsesRequest, JsonObject]:
    try:
        body: Final = _JSON.validate_json(raw)
        data: Final = ResponsesRequest.model_validate(body)
        reject_attribution(body, headers)
        _CLIENT_METADATA.validate_python(body.get("client_metadata"), strict=True)
    except ValidationError as exc:
        raise PublicError(400, "invalid_responses_request") from exc
    if data.background:
        raise PublicError(400, "background_not_allowed")
    if any(name not in _PROVIDER_OPTIONS | {"model", "input", "background", "client_metadata"} for name in body):
        raise PublicError(400, "unsupported_responses_parameter")
    return data, {name: body[name] for name in _PROVIDER_OPTIONS if name in body}


async def responses(request: Request, ledger: Ledger, admission: Admission, presented: str, call_id: str) -> Response:
    data, options = parse_responses_request(await request.body(), tuple(request.headers.items()))
    entry: Final = await ledger.reserve(
        presented,
        data.model,
        request.url.path,
        call_id,
        estimate_input(data.input, options),
        data.max_output_tokens,
    )
    try:
        await admission.check(presented, call_id)
        return await dispatch(request, ledger, entry, data, options)
    except BaseException:
        await settled(ledger, entry, None)
        raise


async def dispatch(
    request: Request, ledger: Ledger, entry: Reservation, data: ResponsesRequest, options: JsonObject
) -> Response:
    call_id: Final = entry.call_id
    try:
        result: Final[object] = await RoutingContext(entry.deployment.router).aresponses(
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
        serialized: Final = None if isinstance(result, AsyncIterator) else serialize_response(result)
        usage: Final = output_usage(serialized) if serialized is not None else None
    except Exception as exc:
        error: Final = ProviderError(exc, call_id)
        await settled(ledger, entry, None, error_code=error.error_code)
        raise error from exc
    headers: Final = {LITELLM_CALL_ID_HEADER: call_id}
    if isinstance(result, AsyncIterator):
        return await create_response(
            events(ledger, entry, result), media_type="text/event-stream", headers=headers, request=request
        )
    assert serialized is not None
    await settled(ledger, entry, usage, error_code=_FAILED_RESPONSE if serialized.get("status") == "failed" else None)
    return JSONResponse(serialized, headers=headers)


def serialize_response(result: object) -> JsonObject:
    if not isinstance(result, BaseModel):
        raise PublicError(502, "invalid_provider_response")
    return _JSON.validate_json(result.model_dump_json(exclude_none=True))
