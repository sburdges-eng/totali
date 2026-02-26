from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from .io_csv import CanonicalBundle


class AdjustmentError(RuntimeError):
    pass


@dataclass(slots=True)
class AdjustmentResult:
    adjusted_xy: dict[str, tuple[float, float]]
    covariance_xy_full: np.ndarray
    solver_path: str
    condition_number: float
    iterations: int
    converged: bool
    posterior_variance_factor: float
    residual_norm: float


def _build_weight_lookup(bundle: CanonicalBundle) -> dict[str, tuple[float, float]]:
    lookup: dict[str, tuple[float, float]] = {}
    for rule in bundle.weights:
        lookup[rule.obs_type] = (rule.std_dev, rule.ppm)
    return lookup


def _effective_sigma(obs_type: str, value: float, lookup: dict[str, tuple[float, float]], sigma_override: float | None) -> float:
    if sigma_override is not None:
        if sigma_override <= 0:
            raise AdjustmentError(f"sigma_override must be > 0 for observation type {obs_type}")
        return sigma_override
    if obs_type not in lookup:
        raise AdjustmentError(f"No weight rule for observation type: {obs_type}")
    std_dev, ppm = lookup[obs_type]
    sigma = std_dev + (abs(value) * ppm * 1e-6)
    if sigma <= 0:
        raise AdjustmentError(f"Computed sigma <= 0 for observation type {obs_type}")
    return sigma


def _station_index_maps(bundle: CanonicalBundle) -> tuple[dict[str, int], dict[str, int], np.ndarray]:
    station_ids = [station.station_id for station in bundle.stations]
    station_to_global = {sid: idx for idx, sid in enumerate(station_ids)}

    free_ids = [station.station_id for station in bundle.stations if station.status == "free"]
    fixed_ids = [station.station_id for station in bundle.stations if station.status != "free"]

    if not fixed_ids:
        raise AdjustmentError("At least one fixed station is required")

    free_to_unknown: dict[str, int] = {}
    for idx, sid in enumerate(free_ids):
        free_to_unknown[sid] = idx * 2

    x0 = np.zeros(len(free_ids) * 2, dtype=float)
    for station in bundle.stations:
        if station.station_id in free_to_unknown:
            base = free_to_unknown[station.station_id]
            x0[base] = station.x
            x0[base + 1] = station.y

    return station_to_global, free_to_unknown, x0


def _xy_from_state(
    bundle: CanonicalBundle,
    state_vector: np.ndarray,
    free_to_unknown: dict[str, int],
) -> dict[str, tuple[float, float]]:
    xy: dict[str, tuple[float, float]] = {}
    for station in bundle.stations:
        if station.station_id in free_to_unknown:
            base = free_to_unknown[station.station_id]
            xy[station.station_id] = (float(state_vector[base]), float(state_vector[base + 1]))
        else:
            xy[station.station_id] = (station.x, station.y)
    return xy


def _observation_row(
    obs_type: str,
    from_xy: tuple[float, float],
    to_xy: tuple[float, float],
    observed: float,
) -> tuple[np.ndarray, float]:
    x1, y1 = from_xy
    x2, y2 = to_xy

    if obs_type == "distance":
        dx = x2 - x1
        dy = y2 - y1
        dist = float(np.hypot(dx, dy))
        if dist <= 0:
            raise AdjustmentError("Zero-length baseline for distance observation")
        partial = np.array([-(dx / dist), -(dy / dist), (dx / dist), (dy / dist)], dtype=float)
        l_i = observed - dist
        return partial, l_i

    if obs_type == "dx":
        computed = x2 - x1
        partial = np.array([-1.0, 0.0, 1.0, 0.0], dtype=float)
        return partial, observed - computed

    if obs_type == "dy":
        computed = y2 - y1
        partial = np.array([0.0, -1.0, 0.0, 1.0], dtype=float)
        return partial, observed - computed

    raise AdjustmentError(f"Unsupported observation type: {obs_type}")


def _solve_normal_equations(
    A: np.ndarray,
    P: np.ndarray,
    l: np.ndarray,
    condition_number_limit: float,
    svd_rcond: float,
) -> tuple[np.ndarray, np.ndarray, str, float]:
    N = A.T @ P @ A
    u = A.T @ P @ l

    cond = float(np.linalg.cond(N))
    if not np.isfinite(cond):
        cond = float("inf")

    if cond <= condition_number_limit:
        try:
            L = np.linalg.cholesky(N)
            y = np.linalg.solve(L, u)
            dx = np.linalg.solve(L.T, y)
            invN = np.linalg.solve(L.T, np.linalg.solve(L, np.eye(N.shape[0], dtype=float)))
            return dx, invN, "cholesky", cond
        except np.linalg.LinAlgError:
            pass

    invN = np.linalg.pinv(N, rcond=svd_rcond)
    dx = invN @ u
    return dx, invN, "svd", cond


def run_adjustment(bundle: CanonicalBundle, config: dict[str, Any]) -> AdjustmentResult:
    station_to_global, free_to_unknown, x = _station_index_maps(bundle)
    weight_lookup = _build_weight_lookup(bundle)

    max_iter = int(config["laser"]["adjustment"]["max_iterations"])
    tol = float(config["laser"]["adjustment"]["convergence_tol"])
    cond_limit = float(config["laser"]["adjustment"]["condition_number_limit"])
    svd_rcond = float(config["laser"]["adjustment"]["svd_rcond"])

    if x.size == 0:
        raise AdjustmentError("No free stations to adjust")

    last_invN: np.ndarray | None = None
    last_A: np.ndarray | None = None
    last_P: np.ndarray | None = None
    last_l: np.ndarray | None = None
    solver_path = "cholesky"
    condition_number = 0.0
    converged = False

    for iteration in range(1, max_iter + 1):
        xy = _xy_from_state(bundle, x, free_to_unknown)

        rows = []
        misclosures = []
        sigmas = []

        for obs in bundle.observations:
            from_xy = xy[obs.from_stn]
            to_xy = xy[obs.to_stn]
            partial4, l_i = _observation_row(obs.obs_type, from_xy, to_xy, obs.value)

            row = np.zeros(x.size, dtype=float)
            if obs.from_stn in free_to_unknown:
                base = free_to_unknown[obs.from_stn]
                row[base] = partial4[0]
                row[base + 1] = partial4[1]
            if obs.to_stn in free_to_unknown:
                base = free_to_unknown[obs.to_stn]
                row[base] = partial4[2]
                row[base + 1] = partial4[3]

            sigma = _effective_sigma(obs.obs_type, obs.value, weight_lookup, obs.sigma_override)
            rows.append(row)
            misclosures.append(l_i)
            sigmas.append(sigma)

        A = np.vstack(rows)
        l_vec = np.array(misclosures, dtype=float)
        W = 1.0 / (np.array(sigmas, dtype=float) ** 2)
        P = np.diag(W)

        dx, invN, solver_path, condition_number = _solve_normal_equations(A, P, l_vec, cond_limit, svd_rcond)

        x = x + dx

        last_invN = invN
        last_A = A
        last_P = P
        last_l = l_vec

        if float(np.max(np.abs(dx))) < tol:
            converged = True
            break

    if not converged:
        raise AdjustmentError("Adjustment did not converge")

    assert last_invN is not None and last_A is not None and last_P is not None and last_l is not None

    v = last_l - (last_A @ dx)
    dof = last_A.shape[0] - last_A.shape[1]
    if dof <= 0:
        raise AdjustmentError("Non-positive degrees of freedom")

    sigma0_sq = float((v.T @ last_P @ v) / dof)
    if not np.isfinite(sigma0_sq) or sigma0_sq < 0:
        raise AdjustmentError("Invalid posterior variance factor")

    Cx = sigma0_sq * last_invN
    if not np.all(np.isfinite(Cx)):
        raise AdjustmentError("Covariance contains non-finite values")

    Cx = 0.5 * (Cx + Cx.T)

    n_stations = len(bundle.stations)
    C_full = np.zeros((2 * n_stations, 2 * n_stations), dtype=float)

    for sid_i, global_i in station_to_global.items():
        if sid_i not in free_to_unknown:
            continue
        base_i = free_to_unknown[sid_i]
        g_i = 2 * global_i
        for sid_j, global_j in station_to_global.items():
            if sid_j not in free_to_unknown:
                continue
            base_j = free_to_unknown[sid_j]
            g_j = 2 * global_j
            C_full[g_i : g_i + 2, g_j : g_j + 2] = Cx[base_i : base_i + 2, base_j : base_j + 2]

    adjusted_xy = _xy_from_state(bundle, x, free_to_unknown)

    return AdjustmentResult(
        adjusted_xy=adjusted_xy,
        covariance_xy_full=C_full,
        solver_path=solver_path,
        condition_number=condition_number,
        iterations=iteration,
        converged=converged,
        posterior_variance_factor=sigma0_sq,
        residual_norm=float(np.linalg.norm(v)),
    )
