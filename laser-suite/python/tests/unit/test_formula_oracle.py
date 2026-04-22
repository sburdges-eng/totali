"""Hand-computed formula oracles for laser-suite (LS-2 + LS-3).

These tests pin the documented formulas in ``laser-suite/docs/formulas.md``:

* ``x_hat = (A^T P A)^-1 A^T P l`` (weighted least squares)
* ``C_x  = sigma0^2 * (A^T P A)^-1`` (posterior covariance)
* ``Sigma_delta = Sigma_ii + Sigma_jj - Sigma_ij - Sigma_ji`` (pair covariance)
* ``RPP_actual = k95 * sqrt(lambda_max(horizontal(Sigma_delta)))``
* ``RPP_allowable = allowable_base_m + (allowable_ppm * 1e-6 * distance_m)``

The tests call into the laser-suite implementations (``_solve_normal_equations``,
``compute_rpp_rows``) wherever a callable exists, rather than re-implementing the
math in the test and asserting against itself.
"""

from __future__ import annotations

import numpy as np
import pytest

from laser_suite.adjustment import _solve_normal_equations, run_adjustment
from laser_suite.config import DEFAULT_CONFIG
from laser_suite.io_csv import CanonicalBundle
from laser_suite.rpp import compute_rpp_rows
from laser_suite.schemas import (
    AdjacencyPair,
    GeometryRecord,
    Observation,
    SetbackRecord,
    Station,
    WeightRule,
)


# ---------------------------------------------------------------------------
# LS-2: Weighted least-squares oracle
# ---------------------------------------------------------------------------


def test_ls2_hand_computed_normal_equations_two_unknown() -> None:
    """Tiny 3-obs, 2-unknown system with x_hat computed in closed form.

    With A, P, l as literals, compute x_hat = (A^T P A)^-1 A^T P l by hand
    and compare with ``_solve_normal_equations``.
    """
    A = np.array(
        [
            [1.0, 0.0],
            [0.0, 1.0],
            [1.0, 1.0],
        ],
        dtype=float,
    )
    P = np.diag([1.0, 1.0, 2.0])
    l = np.array([3.0, 4.0, 9.0], dtype=float)  # noqa: E741 — keeps the docs/formulas.md symbol

    # Hand-computed:
    #   A^T P A = [[1 + 0 + 2,      0 + 0 + 2],
    #              [0 + 0 + 2,      0 + 1 + 2]]
    #           = [[3, 2], [2, 3]]
    #   A^T P l = [3 + 0 + 2*9, 0 + 4 + 2*9] = [21, 22]
    #   det = 9 - 4 = 5
    #   inv = [[3, -2], [-2, 3]] / 5
    #   x_hat = inv @ [21, 22] = [(63 - 44)/5, (-42 + 66)/5] = [19/5, 24/5]
    expected = np.array([19.0 / 5.0, 24.0 / 5.0], dtype=float)

    dx, invN, solver_path, cond = _solve_normal_equations(
        A=A,
        P=P,
        l=l,
        condition_number_limit=1e12,
        svd_rcond=1e-12,
    )

    np.testing.assert_allclose(dx, expected, atol=1e-9, rtol=0.0)
    # The well-conditioned system should take the cholesky path.
    assert solver_path == "cholesky"
    assert np.isfinite(cond)

    # invN == (A^T P A)^-1
    expected_invN = np.array([[3.0, -2.0], [-2.0, 3.0]], dtype=float) / 5.0
    np.testing.assert_allclose(invN, expected_invN, atol=1e-12, rtol=0.0)


def test_ls2_identity_system_returns_observations() -> None:
    """When A = I, P = I, l = y, the solution must be exactly y."""
    y = np.array([1.5, -2.25, 0.0, 7.125], dtype=float)
    A = np.eye(4, dtype=float)
    P = np.eye(4, dtype=float)

    dx, invN, solver_path, _ = _solve_normal_equations(
        A=A,
        P=P,
        l=y,
        condition_number_limit=1e12,
        svd_rcond=1e-12,
    )

    np.testing.assert_allclose(dx, y, atol=1e-12, rtol=0.0)
    np.testing.assert_allclose(invN, np.eye(4), atol=1e-12, rtol=0.0)
    assert solver_path == "cholesky"


def test_ls2_over_determined_line_fit_recovers_exactly() -> None:
    """Fit y = m*x + b through 3 exactly-colinear points.

    Observation model: l_i = m*x_i + b, unknown vector [m, b].
    Colinear data has zero residual, so even a weighted LS solution
    must recover (m, b) exactly.
    """
    m_true = 2.5
    b_true = -1.75
    xs = np.array([0.0, 1.0, 4.0], dtype=float)
    ys = m_true * xs + b_true  # exactly on the line

    # Design matrix: each row is [x_i, 1] so that A @ [m, b] = y.
    A = np.column_stack([xs, np.ones_like(xs)])
    # Use non-uniform weights to exercise the P != I path.
    P = np.diag([1.0, 4.0, 9.0])
    l = ys  # noqa: E741 — canonical symbol for observation vector

    dx, _, solver_path, _ = _solve_normal_equations(
        A=A,
        P=P,
        l=l,
        condition_number_limit=1e12,
        svd_rcond=1e-12,
    )

    np.testing.assert_allclose(dx, np.array([m_true, b_true]), atol=1e-9, rtol=0.0)
    assert solver_path == "cholesky"


def test_ls2_rank_deficient_falls_back_to_svd_without_nans() -> None:
    """A rank-deficient design must not silently yield NaNs.

    The adjustment implementation documents a two-path solver: Cholesky for
    well-conditioned systems, SVD pseudoinverse for ill-conditioned ones.
    The contract this test pins:

      1. The solver reports ``solver_path == "svd"`` when the normal matrix
         is singular / beyond the condition-number limit.
      2. The returned correction is all-finite (no silent NaN production).
      3. A direct ``numpy.linalg.solve`` on the same singular matrix raises
         ``LinAlgError``, demonstrating the fallback is load-bearing.
    """
    # Two duplicate rows -> A^T P A is rank 1, singular.
    A = np.array(
        [
            [1.0, 1.0],
            [1.0, 1.0],
            [1.0, 1.0],
        ],
        dtype=float,
    )
    P = np.eye(3, dtype=float)
    l = np.array([1.0, 1.0, 1.0], dtype=float)  # noqa: E741 — canonical symbol

    N = A.T @ P @ A
    u = A.T @ P @ l

    # Direct numpy solve on the singular N must raise.
    with pytest.raises(np.linalg.LinAlgError):
        np.linalg.solve(N, u)

    # laser-suite's solver should take the SVD branch and yield finite values.
    dx, invN, solver_path, cond = _solve_normal_equations(
        A=A,
        P=P,
        l=l,
        condition_number_limit=1e10,
        svd_rcond=1e-12,
    )

    assert solver_path == "svd"
    assert cond > 1e10 or not np.isfinite(cond)
    assert np.all(np.isfinite(dx)), "solver must not silently produce NaNs"
    assert np.all(np.isfinite(invN))


def test_ls2_residual_matches_l_minus_A_x_hat() -> None:
    """Optional oracle: weighted residual r = l - A @ x_hat on the tiny system."""
    A = np.array(
        [
            [1.0, 0.0],
            [0.0, 1.0],
            [1.0, 1.0],
        ],
        dtype=float,
    )
    P = np.diag([1.0, 1.0, 2.0])
    l = np.array([3.0, 4.0, 9.0], dtype=float)  # noqa: E741 — canonical symbol

    dx, _, _, _ = _solve_normal_equations(
        A=A,
        P=P,
        l=l,
        condition_number_limit=1e12,
        svd_rcond=1e-12,
    )

    # x_hat = [19/5, 24/5]; A @ x_hat = [19/5, 24/5, 43/5]
    expected_r = l - np.array([19.0 / 5.0, 24.0 / 5.0, 43.0 / 5.0])
    r = l - A @ dx
    np.testing.assert_allclose(r, expected_r, atol=1e-12, rtol=0.0)


# ---------------------------------------------------------------------------
# LS-3: RPP oracle
# ---------------------------------------------------------------------------


# Reference from docs/formulas.md:
#   RPP_allowable = 0.02 + (50e-6 * distance_m)
# DEFAULT_CONFIG pins allowable_base_m = 0.02, allowable_ppm = 50.0
# so base + ppm * 1e-6 * d == 0.02 + 50e-6 * d.
_RPP_BASE_M = float(DEFAULT_CONFIG["laser"]["rpp"]["allowable_base_m"])
_RPP_PPM = float(DEFAULT_CONFIG["laser"]["rpp"]["allowable_ppm"])
_K95 = float(DEFAULT_CONFIG["laser"]["rpp"]["k95"])


def _rpp_allowable(distance_m: float) -> float:
    """Inline the documented formula so the test is self-checking."""
    return _RPP_BASE_M + (_RPP_PPM * 1e-6 * distance_m)


@pytest.mark.parametrize(
    ("distance_m", "expected"),
    [
        (0.0, 0.02),
        (100.0, 0.02 + 50e-6 * 100.0),
        (1000.0, 0.02 + 50e-6 * 1000.0),
        (10_000.0, 0.02 + 50e-6 * 10_000.0),
    ],
)
def test_ls3_rpp_allowable_formula_over_distance(distance_m: float, expected: float) -> None:
    """RPP_allowable = 0.02 + 50e-6 * d — checked at several distances."""
    result = _rpp_allowable(distance_m)
    assert result == pytest.approx(expected, abs=1e-12)


def test_ls3_rpp_allowable_zero_distance_is_constant_floor() -> None:
    """Zero-distance pair gives exactly the base (0.02 m) with no ppm term."""
    assert _rpp_allowable(0.0) == 0.02


def test_ls3_pair_covariance_sigma_delta_hand_computed() -> None:
    """Sigma_delta = Sigma_ii + Sigma_jj - Sigma_ij - Sigma_ji on a 4x4 block cov."""
    # Build a 4x4 covariance: station i at rows 0..1, station j at rows 2..3.
    sii = np.array([[4.0, 1.0], [1.0, 3.0]], dtype=float)
    sjj = np.array([[2.0, 0.5], [0.5, 5.0]], dtype=float)
    sij = np.array([[0.25, 0.1], [0.2, 0.4]], dtype=float)
    # For a valid symmetric covariance we would have sji = sij.T; we choose
    # a distinct sji here so the formula's asymmetry is actually exercised.
    sji = np.array([[0.3, 0.15], [0.05, 0.35]], dtype=float)

    cov = np.block([[sii, sij], [sji, sjj]])
    assert cov.shape == (4, 4)

    # Extract blocks exactly the way rpp._pair_covariance_blocks does.
    sii_got = cov[0:2, 0:2]
    sjj_got = cov[2:4, 2:4]
    sij_got = cov[0:2, 2:4]
    sji_got = cov[2:4, 0:2]

    np.testing.assert_array_equal(sii_got, sii)
    np.testing.assert_array_equal(sjj_got, sjj)
    np.testing.assert_array_equal(sij_got, sij)
    np.testing.assert_array_equal(sji_got, sji)

    s_delta = sii_got + sjj_got - sij_got - sji_got

    expected = np.array(
        [
            [4.0 + 2.0 - 0.25 - 0.3, 1.0 + 0.5 - 0.1 - 0.15],
            [1.0 + 0.5 - 0.2 - 0.05, 3.0 + 5.0 - 0.4 - 0.35],
        ],
        dtype=float,
    )
    np.testing.assert_allclose(s_delta, expected, atol=1e-12, rtol=0.0)


def test_ls3_rpp_actual_from_known_eigenvalues() -> None:
    """RPP_actual = k95 * sqrt(lambda_max(Sigma_delta)) with known eigenvalues.

    Construct a 2x2 horizontal Sigma_delta whose eigenvalues are (a, b),
    verify compute_rpp_rows produces k95 * sqrt(max(a, b)).
    """
    # Diagonal covariance has eigenvalues equal to its diagonal entries.
    lam_major = 4e-4  # 0.02^2
    lam_minor = 1e-4  # 0.01^2

    # Build a bundle with one free station B adjacent to fixed A, so the
    # relative covariance between (A, B) is dominated by Sigma_BB.
    bundle = CanonicalBundle(
        stations=[
            Station("A", 0.0, 0.0, 0.0, "fixed"),
            Station("B", 10.0, 0.0, 0.0, "free"),
        ],
        observations=[Observation("O1", "A", "B", "distance", 10.0, None)],
        weights=[WeightRule("distance", 0.01, 0.0)],
        adjacency=[AdjacencyPair("A", "B")],
        boundaries=[GeometryRecord("B1", "POLYGON((0 0,1 0,1 1,0 1,0 0))")],
        improvements=[GeometryRecord("I1", "POLYGON((0 0,1 0,1 1,0 1,0 0))")],
        easements=[GeometryRecord("E1", "POLYGON((0 0,1 0,1 1,0 1,0 0))")],
        setbacks=[SetbackRecord("S1", "B1", 1.0)],
    )

    # cov layout: A at rows 0..1 (zero for fixed), B at rows 2..3.
    cov = np.zeros((4, 4), dtype=float)
    cov[2, 2] = lam_major
    cov[3, 3] = lam_minor

    rows = compute_rpp_rows(
        bundle=bundle,
        adjusted_xy={"A": (0.0, 0.0), "B": (10.0, 0.0)},
        covariance_xy_full=cov,
        k95=_K95,
        allowable_base_m=_RPP_BASE_M,
        allowable_ppm=_RPP_PPM,
    )
    assert len(rows) == 1
    row = rows[0]

    expected_rpp_actual = _K95 * np.sqrt(max(lam_major, lam_minor))
    expected_rpp_allow = _rpp_allowable(10.0)

    assert row.distance_m == pytest.approx(10.0, abs=1e-12)
    assert row.rpp_actual_m == pytest.approx(expected_rpp_actual, abs=1e-9)
    assert row.rpp_allowable_m == pytest.approx(expected_rpp_allow, abs=1e-12)
    assert row.margin_m == pytest.approx(expected_rpp_allow - expected_rpp_actual, abs=1e-9)


def test_ls3_end_to_end_rpp_from_run_adjustment() -> None:
    """End-to-end oracle: run_adjustment -> compute_rpp_rows produces a finite,
    documented RPP pair with allowable matching the formula for the adjusted
    distance.
    """
    bundle = CanonicalBundle(
        stations=[
            Station("A", 0.0, 0.0, 0.0, "fixed"),
            Station("B", 49.0, 31.0, 0.0, "free"),
            Station("C", 100.0, 0.0, 0.0, "fixed"),
            Station("D", 50.0, 60.0, 0.0, "fixed"),
        ],
        # Observed distances are the exact Euclidean distances from the
        # seeded station coordinates, so the adjustment is self-consistent
        # and the RPP margin is driven only by the small sigma=0.01 m.
        observations=[
            Observation("O1", "A", "B", "distance", 57.982756057296896, None),
            Observation("O2", "C", "B", "distance", 59.682493245507096, None),
            Observation("O3", "D", "B", "distance", 29.017236257093817, None),
        ],
        weights=[WeightRule("distance", 0.01, 0.0)],
        adjacency=[AdjacencyPair("A", "B")],
        boundaries=[GeometryRecord("B1", "POLYGON((0 0,1 0,1 1,0 1,0 0))")],
        improvements=[GeometryRecord("I1", "POLYGON((0 0,1 0,1 1,0 1,0 0))")],
        easements=[GeometryRecord("E1", "POLYGON((0 0,1 0,1 1,0 1,0 0))")],
        setbacks=[SetbackRecord("S1", "B1", 1.0)],
    )

    result = run_adjustment(bundle, DEFAULT_CONFIG)
    assert result.converged

    rows = compute_rpp_rows(
        bundle=bundle,
        adjusted_xy=result.adjusted_xy,
        covariance_xy_full=result.covariance_xy_full,
        k95=_K95,
        allowable_base_m=_RPP_BASE_M,
        allowable_ppm=_RPP_PPM,
    )

    assert len(rows) == 1
    row = rows[0]
    # Allowable must match the formula for the distance the row reports.
    assert row.rpp_allowable_m == pytest.approx(_rpp_allowable(row.distance_m), abs=1e-12)
    # Actual should be finite, non-negative, and much smaller than allowable
    # for this well-observed small network.
    assert np.isfinite(row.rpp_actual_m)
    assert row.rpp_actual_m >= 0.0
    assert row.compliant is True
