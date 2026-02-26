# Formulas

## Weighted Least-Squares

- Correction vector:
  - `x_hat = (A^T P A)^-1 A^T P l`
- Posterior covariance:
  - `C_x = sigma0^2 * (A^T P A)^-1`

## Pairwise Relative Positional Precision

- Relative covariance of station pair `(i, j)`:
  - `Sigma_delta = Sigma_ii + Sigma_jj - Sigma_ij - Sigma_ji`
- Actual RPP:
  - `RPP_actual = 2.448 * sqrt(lambda_max(horizontal(Sigma_delta)))`
- Allowable RPP:
  - `RPP_allowable = 0.02 + (50e-6 * distance_m)`
