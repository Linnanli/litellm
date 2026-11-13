from datetime import datetime
from decimal import Decimal
from typing import Final

import pytest
from pydantic import ValidationError

from daswork_enterprise.contracts import month_start, next_month
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
    price: Final = PricingInfo(**RATES, off_peak_pricing=info.off_peak_pricing)
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
