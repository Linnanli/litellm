import hashlib
from collections.abc import Sequence
from datetime import UTC
from typing import Final

import httpx
from pydantic import ValidationError

from .contracts import ADMISSION_CONTRACT, AdmissionReply, JsonObject, PublicError

_ATTRIBUTION: Final = frozenset(
    {"tag", "tags", "requesttags", "user", "userid", "enduser", "enduserid", "team", "teamid", "organization",
     "organizationid", "org", "orgid", "project", "projectid", "dwcontract", "dwsessionref", "dworganizationversion",
     "dwpolicyepoch", "litellmcallid", "requestid"}
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


class Admission:
    def __init__(self, client: httpx.AsyncClient, endpoint: str) -> None:
        self.client = client
        self.endpoint = endpoint

    async def check(self, presented: str, call_id: str) -> None:
        try:
            reply: Final = await self.client.post(
                self.endpoint, json={
                    "contract": ADMISSION_CONTRACT, "presentedKeyFingerprint": presented, "requestId": call_id,
                }
            )
            reply.raise_for_status()
            payload: Final = AdmissionReply.model_validate_json(reply.content)
        except (httpx.HTTPError, ValidationError) as exc:
            raise PublicError(400, "NATIVE_SESSION_CHECK_UNAVAILABLE", "daswork_policy_rejected") from exc
        if payload.data.requestId != call_id or payload.data.checkedAt.utcoffset() != UTC.utcoffset(None):
            raise PublicError(400, "NATIVE_SESSION_CHECK_UNAVAILABLE", "daswork_policy_rejected")
