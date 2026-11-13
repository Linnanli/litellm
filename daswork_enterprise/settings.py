from os import environ
from pathlib import Path
from ssl import SSLContext, create_default_context
from typing import Final, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, SecretStr
from yaml import safe_load

from .contracts import ADMISSION_CONTRACT, Contract


class Coordination(Contract):
    host: str
    port: int = 6379
    db: int = 0
    password: SecretStr | None = None


class General(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    coordination_redis: Coordination
    apply_user_budget_to_team_keys: Literal[True] = True
    fail_closed_budget_enforcement: Literal[True] = True
    disable_budget_reservation: Literal[False] = False
    disable_reset_budget: Literal[False] = False


class LiteLLMSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    timezone: Literal["Asia/Shanghai"] = "Asia/Shanghai"
    budget_reset_time: Literal["00:00"] = "00:00"


class Config(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    general_settings: General
    litellm_settings: LiteLLMSettings = LiteLLMSettings()


class Settings(Contract):
    config: Config
    database_url: SecretStr
    master_key: SecretStr
    admission_endpoint: str
    admission_ca: Path

    @staticmethod
    def load() -> "Settings":
        config_file: Final = Path(environ["CONFIG_FILE_PATH"])
        ca_file: Final = Path(environ["DASWORK_NATIVE_CONTROL_CA_FILE"])
        origin: Final = urlsplit(environ["DASWORK_NATIVE_CONTROL_ORIGIN"])
        if (
            origin.scheme != "https"
            or not origin.hostname
            or origin.username
            or origin.password
            or origin.path not in {"", "/"}
            or origin.query
            or origin.fragment
        ):
            raise ValueError("DASWORK_NATIVE_CONTROL_ORIGIN must be an HTTPS origin")
        if not all(path.is_absolute() and path.is_file() for path in (config_file, ca_file)):
            raise ValueError("configuration and CA must be absolute file paths")
        if environ.get("DASWORK_NATIVE_REQUEST_ADMISSION_PROFILE") != ADMISSION_CONTRACT:
            raise ValueError("the session admission profile is required")
        if environ.get("STORE_MODEL_IN_DB", "").casefold() != "true":
            raise ValueError("STORE_MODEL_IN_DB must be enabled")
        if environ.get("LITELLM_LOCAL_MODEL_COST_MAP", "").casefold() != "true":
            raise ValueError("LITELLM_LOCAL_MODEL_COST_MAP must be enabled")
        master: Final = environ["LITELLM_MASTER_KEY"]
        if not master or not environ.get("LITELLM_SALT_KEY"):
            raise ValueError("master and salt keys are required")
        return Settings(
            config=Config.model_validate(safe_load(config_file.read_bytes())),
            database_url=SecretStr(environ["DATABASE_URL"]),
            master_key=SecretStr(master),
            admission_endpoint=f"https://{origin.netloc}/enterprise/internal/ai/session-admission",
            admission_ca=ca_file,
        )

    def tls(self) -> SSLContext:
        return create_default_context(cafile=str(self.admission_ca))
