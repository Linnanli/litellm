from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from os import environ
from subprocess import run
from sys import executable
from textwrap import dedent
from typing import Final

import pytest
from pydantic import JsonValue, TypeAdapter, ValidationError

from daswork_enterprise.contracts import PublicError, month_start, next_month
from daswork_enterprise.inference import estimate_input, parse_responses_request
from daswork_enterprise.models import Deployment
from litellm import Router
from litellm.types.router import ModelInfo
from litellm.types.utils import ModelInfo as PricingInfo

RATES: Final = {
    "input_cost_per_token": 0.000002,
    "output_cost_per_token": 0.000008,
    "cache_read_input_token_cost": 0.00000004,
}
OFF_PEAK: Final = {
    "input_cost_per_token": 0.000001,
    "output_cost_per_token": 0.000004,
    "cache_read_input_token_cost": 0.00000002,
    "hours_utc": ("10:00-01:00", "04:00-06:00"),
    "windows": ({"hours_utc": "00:00-00:00", "weekdays": (6, 7)},),
    "weekday_timezone": "Asia/Shanghai",
    "off_peak_dates": ("2026-10-01",),
}
_REQUEST_JSON: Final = TypeAdapter(Mapping[str, JsonValue])
_RESPONSES_BODY: Final = {"model": "synthetic-model", "input": "hello", "stream": True}


@pytest.mark.parametrize(
    ("time", "amount"),
    (
        ("01T09", "0.001204"),
        ("01T14", "0.001204"),
        ("02T09", "0.002408"),
        ("21T09", "0.002408"),
        ("21T12", "0.001204"),
        ("21T14", "0.002408"),
        ("21T18", "0.001204"),
        ("24T10", "0.001204"),
    ),
)
def test_all_day_rates_and_existing_windows(time: str, amount: str) -> None:
    info: Final = ModelInfo.model_validate({**RATES, "off_peak_pricing": OFF_PEAK})
    price: Final = PricingInfo(
        key="openai/gpt-5.2",
        max_tokens=16384,
        max_input_tokens=16384,
        max_output_tokens=4096,
        mode="responses",
        litellm_provider="openai",
        supported_openai_params=None,
        input_cost_per_token=RATES["input_cost_per_token"],
        output_cost_per_token=RATES["output_cost_per_token"],
        cache_read_input_token_cost=RATES["cache_read_input_token_cost"],
        off_peak_pricing=info.off_peak_pricing,
    )
    deployment: Final = Deployment(Router(model_list=[]), "test", "test", "gpt-5.2", "openai", price)
    instant: Final = datetime.fromisoformat(f"2026-10-{time}:00:00+08:00")
    assert deployment.cost(1000, 100, instant, 200) == Decimal(amount)


@pytest.mark.parametrize(
    "override",
    (
        {"off_peak_dates": ("2026-02-30",)},
        {"off_peak_dates": ("2026-10-01", "2026-10-01")},
        {"weekday_timezone": None},
    ),
)
def test_invalid_off_peak_dates(override: object) -> None:
    assert isinstance(override, dict)
    with pytest.raises(ValidationError):
        ModelInfo.model_validate({**RATES, "off_peak_pricing": {**OFF_PEAK, **override}})  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]  # input is intentionally unvalidated


def test_shanghai_budget_boundary() -> None:
    before: Final = datetime.fromisoformat("2026-11-30T15:59:59+00:00")
    after: Final = datetime.fromisoformat("2026-11-30T16:00:00+00:00")
    assert next_month(before) == month_start(after) == after
    assert month_start(before) < month_start(after) < next_month(after)


def test_client_metadata_does_not_enter_provider_options_or_budget_input() -> None:
    baseline, baseline_options = parse_responses_request(_REQUEST_JSON.dump_json(_RESPONSES_BODY), ())
    request, options = parse_responses_request(
        _REQUEST_JSON.dump_json(
            {**_RESPONSES_BODY, "client_metadata": {"x-codex-turn-metadata": '{"turn":"synthetic"}', "trace": "abc"}}
        ),
        (),
    )
    assert request == baseline
    assert options == baseline_options == {"stream": True}
    assert estimate_input(request.input, options) == estimate_input(baseline.input, baseline_options)


@pytest.mark.parametrize(
    ("fields", "message"),
    (
        ({"client_metadata": {"x-litellm-team-id": "foreign-team"}}, "NATIVE_SESSION_ATTRIBUTION_INVALID"),
        ({"client_metadata": {"tags": ["foreign-tag"]}}, "NATIVE_SESSION_ATTRIBUTION_INVALID"),
        ({"client_metadata": {"trace": 123}}, "invalid_responses_request"),
        ({"client_metadata": "synthetic"}, "invalid_responses_request"),
        ({"future_option": True}, "unsupported_responses_parameter"),
    ),
)
def test_responses_rejects_unsafe_metadata_and_unknown_options(fields: Mapping[str, JsonValue], message: str) -> None:
    with pytest.raises(PublicError, match=message) as rejection:
        parse_responses_request(_REQUEST_JSON.dump_json({**_RESPONSES_BODY, **fields}), ())
    assert rejection.value.status == 400


def test_empty_license_skips_remote_validation_at_startup() -> None:
    result: Final = run(
        (
            executable,
            "-c",
            dedent("""
                from sys import addaudithook

                destinations = []

                def deny_network(event, args):
                    if event == "socket.getaddrinfo":
                        destinations.append(args[0])
                    if event in {"socket.getaddrinfo", "socket.connect"}:
                        raise OSError("Network disabled in the startup test")

                addaudithook(deny_network)
                from daswork_enterprise.app import create_app
                from daswork_enterprise.settings import Settings

                create_app(Settings.model_validate({
                    "config": {"general_settings": {"coordination_redis": {"host": "localhost"}}},
                    "database_url": "postgresql://localhost/synthetic",
                    "master_key": "synthetic-master",
                    "admission_endpoint": "https://localhost/admission",
                    "admission_ca": "/synthetic-ca.pem",
                }))
                assert "license.litellm.ai" not in destinations, destinations
                """),
        ),
        env={
            **environ,
            "LITELLM_LICENSE": "",
            "LITELLM_LOCAL_MODEL_COST_MAP": "true",
            "NO_PROXY": "*",
            "no_proxy": "*",
        },
        capture_output=True,
        check=False,
        text=True,
        timeout=150,
    )
    assert result.returncode == 0, result.stderr
