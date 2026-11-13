import hashlib
import logging
from collections.abc import Sequence
from datetime import UTC
from secrets import token_urlsafe
from typing import Final
from uuid import uuid4

import httpx
from pydantic import ValidationError

from .contracts import ADMISSION_CONTRACT, AdmissionReply, JsonObject, PublicError
from .settings import Settings

_ATTRIBUTION: Final = frozenset(
    {
        "tag",
        "tags",
        "requesttags",
        "user",
        "userid",
        "enduser",
        "enduserid",
        "team",
        "teamid",
        "organization",
        "organizationid",
        "org",
        "orgid",
        "project",
        "projectid",
        "dwcontract",
        "dwsessionref",
        "dworganizationversion",
        "dwpolicyepoch",
        "litellmcallid",
        "requestid",
    }
)
_ALT_AUTH: Final = frozenset(
    {"api-key", "x-api-key", "x-litellm-api-key", "proxy-authorization", "x-authorization", "x-openai-api-key"}
)


def fingerprint(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def bearer(headers: Sequence[tuple[str, str]]) -> str:
    values: Final = tuple(value for name, value in headers if name == "authorization")
    if len(values) != 1 or any(name in _ALT_AUTH for name, _ in headers):
        raise PublicError(401, "invalid_authorization")
    scheme, _, value = values[0].partition(" ")
    if scheme.casefold() != "bearer" or not value or any(char.isspace() or char == "," for char in value):
        raise PublicError(401, "invalid_authorization")
    return value


def attribution_name(name: str) -> bool:
    normalized: Final = "".join(char for char in name.casefold() if char.isalnum())
    return normalized.removeprefix("x").removeprefix("litellm").removeprefix("openai") in _ATTRIBUTION


def reject_attribution(body: JsonObject, headers: Sequence[tuple[str, str]]) -> None:
    metadata: Final = (body.get("metadata"), body.get("litellm_metadata"))
    if (
        any(map(attribution_name, body))
        or any(attribution_name(name) for name, _ in headers if name.startswith(("x-", "openai-")))
        or any(any(map(attribution_name, entry)) for entry in metadata if isinstance(entry, dict))
    ):
        raise PublicError(400, "NATIVE_SESSION_ATTRIBUTION_INVALID", "daswork_policy_rejected")


_LOG: Final = logging.getLogger(__name__)


class Admission:
    def __init__(self, client: httpx.AsyncClient, endpoint: str) -> None:
        self.client = client
        self.endpoint = endpoint

    async def request(self, presented: str, call_id: str) -> httpx.Response:
        return await self.client.post(
            self.endpoint,
            json={
                "contract": ADMISSION_CONTRACT,
                "presentedKeyFingerprint": presented,
                "requestId": call_id,
            },
        )

    async def check(self, presented: str, call_id: str) -> None:
        try:
            reply: Final = await self.request(presented, call_id)
            reply.raise_for_status()
            payload: Final = AdmissionReply.model_validate_json(reply.content)
            if payload.data.requestId != call_id or payload.data.checkedAt.utcoffset() != UTC.utcoffset(None):
                raise ValueError("invalid hook acknowledgement")
        except (httpx.HTTPError, ValidationError, ValueError) as exc:
            _LOG.warning("checkpoint=session_admission call=%s category=%s", call_id, type(exc).__name__)
            raise PublicError(400, "NATIVE_SESSION_CHECK_UNAVAILABLE", "daswork_policy_rejected") from exc


def admission_client(settings: Settings, transport: httpx.AsyncBaseTransport | None = None) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        verify=settings.tls(),
        trust_env=False,
        transport=transport,
        timeout=3.0,
        follow_redirects=False,
    )


async def require_hook_denied(admission: Admission, presented: str) -> None:
    try:
        await admission.check(presented, str(uuid4()))
    except PublicError as error:
        if error.status == 400 and str(error) == "NATIVE_SESSION_CHECK_UNAVAILABLE":
            return
        raise ValueError("unexpected admission rejection") from error
    raise ValueError("unexpected admission success")


async def smoke_hook() -> None:
    settings: Final = Settings.load()
    async with admission_client(settings) as client:
        production: Final = Admission(client, settings.admission_endpoint)
        await require_hook_denied(production, fingerprint(token_urlsafe(32)))
        try:
            reply: Final = await production.request(fingerprint(token_urlsafe(32)), str(uuid4()))
        except httpx.HTTPError as exc:
            raise ValueError("Java admission probe was unavailable") from exc
        if reply.status_code != 403 or b"SESSION_KEY_NOT_ACTIVE" not in reply.content:
            raise ValueError("Java admission did not report an inactive session")
        offline: Final = Admission(client, "https://127.0.0.1:1/enterprise/internal/ai/session-admission")
        await require_hook_denied(offline, fingerprint(token_urlsafe(32)))
