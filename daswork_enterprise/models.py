from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Final

from litellm import Router
from litellm.litellm_core_utils.get_llm_provider_logic import get_llm_provider
from litellm.litellm_core_utils.llm_cost_calc.utils import generic_cost_per_token
from litellm.types.utils import ModelInfo, PromptTokensDetailsWrapper, Usage
from litellm.utils import token_counter

from .contracts import PublicError


@dataclass(frozen=True, slots=True)
class Deployment:
    router: Router
    model_id: str
    model_name: str
    provider_model: str
    provider: str
    pricing: ModelInfo

    def cost(self, prompt: int, completion: int, instant: datetime, cached: int = 0) -> Decimal:
        usage: Final = Usage(
            prompt_tokens=prompt,
            completion_tokens=completion,
            total_tokens=prompt + completion,
            prompt_tokens_details=PromptTokensDetailsWrapper(cached_tokens=cached),
        )
        inputs, outputs = generic_cost_per_token(
            self.provider_model,
            usage,
            self.provider,
            model_info=self.pricing,
            current_time=instant,
        )
        return Decimal(format(inputs + outputs, ".15g"))

    def estimate(self, request: str, completion: int | None, instant: datetime) -> Decimal:
        prompt: Final = token_counter(model=self.provider_model, text=request)
        output: Final = completion if completion is not None else self.pricing.get("max_output_tokens") or 4096
        return self.cost(prompt, output, instant)


def resolve_deployment(organization_id: str, team_id: str, model: str) -> Deployment:
    from litellm.proxy import proxy_server

    router: Final = proxy_server.llm_router
    if router is None:
        raise PublicError(503, "model_router_unavailable")
    registry: Final = router.registry()
    candidates: Final = registry.deployments(model, source="all", team_id=team_id)
    for candidate in candidates:
        identity = candidate.get("model_info", {}).get("id")
        if not isinstance(identity, str):
            continue
        deployment = registry.deployment(identity)
        if deployment is None or deployment.blocked or deployment.model_info.blocked:
            continue
        info = deployment.model_info
        owner = info.get("organization_id")
        if owner is not None and owner != organization_id or info.team_id not in {None, team_id}:
            continue
        provider_model, provider, _, _ = get_llm_provider(
            deployment.litellm_params.model, litellm_params=deployment.litellm_params
        )
        price = registry.deployment_model_info(identity, deployment.litellm_params.model)
        if price is None:
            raise PublicError(503, "model_pricing_unavailable")
        return Deployment(router, identity, model, provider_model, provider, price)
    if candidates or registry.deployments(model, source="named"):
        raise PublicError(403, "model_not_allowed")
    raise PublicError(404, "model_not_found")
