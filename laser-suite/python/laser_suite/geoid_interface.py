from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol


class InterpolationMethod(str, Enum):
    BILINEAR = "bilinear"
    BIQUADRATIC = "biquadratic"


@dataclass(slots=True)
class GGXFMetadata:
    source: str
    epoch_reference: float
    interpolation_method: InterpolationMethod


class GGXFProvider(Protocol):
    def metadata(self) -> GGXFMetadata: ...

    def geoid_height(self, lat_deg: float, lon_deg: float, epoch_year: float) -> float: ...

    def uncertainty_components(self, lat_deg: float, lon_deg: float) -> tuple[float, float]: ...


def gregorian_to_mjd(dt: datetime) -> int:
    year = dt.year
    month = dt.month
    day = dt.day
    a = (14 - month) // 12
    y = year + 4800 - a
    m = month + 12 * a - 3
    jdn = day + (153 * m + 2) // 5 + 365 * y + (y // 4) - (y // 100) + (y // 400) - 32045
    return jdn - 2400001


def combine_uncertainty(static_sigma: float, velocity_sigma: float, delta_t_years: float) -> float:
    return (static_sigma * static_sigma + (delta_t_years * velocity_sigma) ** 2) ** 0.5
