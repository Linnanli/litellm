from os import environ
from typing import Final
from urllib.parse import urlsplit
from uuid import uuid4

from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, SecretStr, model_validator

from .contracts import Contract, Identifier, JsonObject, Price, PublicError
from .storage import Database


class ProviderConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    model: Identifier
    api_base: str
    api_key: SecretStr | None = None

    @model_validator(mode="after")
    def supported_provider(self) -> "ProviderConfig":
        parsed: Final = urlsplit(self.api_base)
        if (
            not self.model.startswith(("openai/", "openai_like/"))
            or parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("a Responses-compatible provider and base URL are required")
        return self

    @property
    def model_name(self) -> str:
        return self.model.partition("/")[2]

    def credential(self) -> str | None:
        if self.api_key is None:
            return None
        configured: Final = self.api_key.get_secret_value()
        if configured.startswith("os.environ/"):
            resolved: Final = environ.get(configured.removeprefix("os.environ/"))
            if resolved is None:
                raise PublicError(503, "provider_credential_unavailable")
            return resolved
        return configured


class ModelInfo(Price):
    id: Identifier | None = None
    organization_id: Identifier | None = None
    max_output_tokens: int = 4096


class ModelNew(Contract):
    model_name: Identifier
    litellm_params: ProviderConfig
    model_info: ModelInfo


class ModelDelete(Contract):
    id: Identifier


class Deployment(Contract):
    model_id: str
    model_name: str
    params: ProviderConfig
    price: ModelInfo


class Models:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def add(self, model: ModelNew) -> JsonObject:
        model_id: Final = model.model_info.id or str(uuid4())
        credential: Final = model.litellm_params.api_key
        params: Final = {
            "model": model.litellm_params.model,
            "api_base": model.litellm_params.api_base,
            "api_key": None if credential is None else credential.get_secret_value(),
        }
        info: Final = model.model_info.model_copy(update={"id": model_id}).model_dump(mode="json")
        return await self.db.one(
            'INSERT INTO "LiteLLM_ProxyModelTable" (model_id,model_name,litellm_params,model_info,created_by,updated_by)'
            " VALUES (%s,%s,%s,%s,'daswork','daswork') RETURNING model_id,model_name,model_info",
            (model_id, model.model_name, Jsonb(params), Jsonb(info)),
        )

    async def remove(self, model_id: str) -> JsonObject:
        return await self.db.one(
            'DELETE FROM "LiteLLM_ProxyModelTable" WHERE model_id=%s RETURNING model_id,model_name',
            (model_id,),
        )

    async def list(self) -> JsonObject:
        result: Final = await self.db.one(
            "SELECT COALESCE(jsonb_agg(jsonb_build_object('model_name',model_name,'model_info',model_info,"
            " 'litellm_params',litellm_params - 'api_key') ORDER BY model_name),'[]') AS data"
            ' FROM "LiteLLM_ProxyModelTable" WHERE NOT blocked'
        )
        return result
