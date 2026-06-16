"""Cumulative number density n(>SFR) and threshold inversion.

The number-density-matched selection needs to turn a requested mean density
nbar [(h/Mpc)^3] into the SFR threshold whose selected sample has that density.
Two routes:

* single catalog -- exact: the threshold is just the k-th largest SFR value,
  k = round(nbar * V). :func:`threshold_for_density`.
* ensemble -- the recommended default: build a fixed n(>SFR) curve averaged over
  realizations (:func:`build_ensemble_nsfr`) and invert it once, so the SAME
  threshold is applied to every realization (consistent sample definition).
"""
from __future__ import annotations

from dataclasses import dataclass
import numpy as np

from .catalog import Catalog


def cumulative_n_gt(sfr: np.ndarray, volume: float, grid: np.ndarray) -> np.ndarray:
    """n(>grid_j) = count(sfr > grid_j) / volume, evaluated on ``grid`` (ascending)."""
    s = np.sort(np.asarray(sfr, dtype=np.float64))
    # number with sfr > g  ==  N - searchsorted(s, g, 'right')
    counts = s.size - np.searchsorted(s, grid, side="right")
    return counts / float(volume)


def threshold_for_density(sfr: np.ndarray, volume: float, nbar: float) -> float:
    """Exact SFR threshold for one catalog: the value with count(>t)/V == nbar.

    Returns the k-th largest SFR (k = round(nbar*V)); selecting ``sfr > t`` then
    yields ~nbar. Raises if the requested density exceeds the catalog density.
    """
    n = np.asarray(sfr).size
    k = int(round(nbar * float(volume)))
    if k <= 0:
        raise ValueError(f"nbar={nbar:g} too small: rounds to 0 objects")
    if k > n:
        raise ValueError(f"nbar={nbar:g} needs {k} objects but catalog has {n}")
    # k-th largest -> the threshold just below it so that exactly k pass (sfr > t).
    part = np.partition(np.asarray(sfr, dtype=np.float64), n - k)
    kth = part[n - k]
    # nextafter downward so the comparison sfr > t keeps the k-th object.
    return float(np.nextafter(kth, -np.inf))


def default_grid(lo: float = 1e-4, hi: float = 1e3, num: int = 600) -> np.ndarray:
    """Log-spaced SFR grid [Msun/yr] for tabulating n(>SFR)."""
    return np.logspace(np.log10(lo), np.log10(hi), num)


@dataclass
class EnsembleNSFR:
    """Ensemble-averaged cumulative number density on a fixed SFR grid.

    ``n_mean[j]`` is the mean over realizations of n(>grid[j]); ``n_real`` is how
    many realizations were averaged. ``sfr_col`` records which column it was built
    from (``sfr_corr`` by default).
    """
    grid: np.ndarray
    n_mean: np.ndarray
    n_real: int
    sfr_col: str = "sfr_corr"

    def threshold_for_density(self, nbar: float) -> float:
        """Invert the ensemble curve: SFR threshold whose n(>t) == nbar.

        n_mean is monotone decreasing in SFR, so interpolate nbar on the reversed
        (ascending-in-density) arrays. Raises if nbar is outside the tabulated range.
        """
        n = np.asarray(self.n_mean, dtype=np.float64)
        g = np.asarray(self.grid, dtype=np.float64)
        lo, hi = n[n > 0].min(), n.max()
        if not (lo <= nbar <= hi):
            raise ValueError(
                f"nbar={nbar:g} outside ensemble n(>SFR) range [{lo:g}, {hi:g}]")
        order = np.argsort(n)            # ascending density
        return float(np.interp(nbar, n[order], g[order]))

    def density_at(self, threshold: float) -> float:
        """Forward: ensemble n(>threshold) by interpolation (for round-trip checks)."""
        return float(np.interp(threshold, self.grid, self.n_mean))


def build_ensemble_nsfr(paths, sfr_col: str = "sfr_corr",
                        grid: np.ndarray | None = None) -> EnsembleNSFR:
    """Build the ensemble n(>SFR) table from a set of catalogs.

    Averages the per-catalog cumulative density on a shared grid. Pass the full
    realization ensemble at one redshift to get the canonical table.
    """
    grid = default_grid() if grid is None else np.asarray(grid, dtype=np.float64)
    acc = np.zeros_like(grid, dtype=np.float64)
    count = 0
    for p in paths:
        with Catalog.open(p) as cat:
            acc += cumulative_n_gt(cat.column(sfr_col), cat.volume, grid)
            count += 1
    if count == 0:
        raise ValueError("no catalogs given to build_ensemble_nsfr")
    return EnsembleNSFR(grid=grid, n_mean=acc / count, n_real=count, sfr_col=sfr_col)
