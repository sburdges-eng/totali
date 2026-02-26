from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .io_csv import CanonicalBundle
from .schemas import AdjacencyPair


@dataclass(slots=True)
class RPPRow:
    pair_id: str
    station_i: str
    station_j: str
    distance_m: float
    rpp_actual_m: float
    rpp_allowable_m: float
    margin_m: float
    compliant: bool


def _pair_covariance_blocks(cov_full: np.ndarray, station_index_i: int, station_index_j: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    i = 2 * station_index_i
    j = 2 * station_index_j
    sii = cov_full[i : i + 2, i : i + 2]
    sjj = cov_full[j : j + 2, j : j + 2]
    sij = cov_full[i : i + 2, j : j + 2]
    sji = cov_full[j : j + 2, i : i + 2]
    return sii, sjj, sij, sji


def compute_rpp_rows(
    *,
    bundle: CanonicalBundle,
    adjusted_xy: dict[str, tuple[float, float]],
    covariance_xy_full: np.ndarray,
    k95: float,
    allowable_base_m: float,
    allowable_ppm: float,
) -> list[RPPRow]:
    station_to_index = {station.station_id: idx for idx, station in enumerate(bundle.stations)}

    rows: list[RPPRow] = []
    for idx, pair in enumerate(bundle.adjacency, start=1):
        row = _compute_row(
            pair_id=f"PAIR-{idx:04d}",
            pair=pair,
            station_to_index=station_to_index,
            adjusted_xy=adjusted_xy,
            covariance_xy_full=covariance_xy_full,
            k95=k95,
            allowable_base_m=allowable_base_m,
            allowable_ppm=allowable_ppm,
        )
        rows.append(row)

    rows.sort(key=lambda item: (item.station_i, item.station_j, item.pair_id))
    return rows


def _compute_row(
    *,
    pair_id: str,
    pair: AdjacencyPair,
    station_to_index: dict[str, int],
    adjusted_xy: dict[str, tuple[float, float]],
    covariance_xy_full: np.ndarray,
    k95: float,
    allowable_base_m: float,
    allowable_ppm: float,
) -> RPPRow:
    i = station_to_index[pair.station_i]
    j = station_to_index[pair.station_j]

    xi, yi = adjusted_xy[pair.station_i]
    xj, yj = adjusted_xy[pair.station_j]
    distance = float(np.hypot(xj - xi, yj - yi))

    sii, sjj, sij, sji = _pair_covariance_blocks(covariance_xy_full, i, j)
    s_delta = sii + sjj - sij - sji
    s_delta = 0.5 * (s_delta + s_delta.T)

    eigvals = np.linalg.eigvalsh(s_delta)
    lambda_max = float(np.max(eigvals))
    if lambda_max < 0 and lambda_max > -1e-12:
        lambda_max = 0.0
    if lambda_max < 0:
        raise ValueError(f"Negative propagated covariance eigenvalue for pair {pair.station_i}-{pair.station_j}")

    rpp_actual = float(k95 * np.sqrt(lambda_max))
    rpp_allowable = float(allowable_base_m + (allowable_ppm * 1e-6 * distance))
    margin = float(rpp_allowable - rpp_actual)

    return RPPRow(
        pair_id=pair_id,
        station_i=pair.station_i,
        station_j=pair.station_j,
        distance_m=distance,
        rpp_actual_m=rpp_actual,
        rpp_allowable_m=rpp_allowable,
        margin_m=margin,
        compliant=rpp_actual <= rpp_allowable,
    )
