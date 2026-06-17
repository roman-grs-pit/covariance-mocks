"""Selection functions: turn a catalog into a boolean mask, the same way on every
realization.

All selections expose ``mask(catalog) -> bool array`` and ``describe() -> dict`` (the
selection metadata that travels with the sample). The number-density selection fixes
its SFR threshold from the *ensemble* table by default, so the identical cut is applied
to every realization -- the property that makes the downstream covariance meaningful.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import numpy as np

from .catalog import Catalog
from .nsfr import EnsembleNSFR, threshold_for_density


class Selection:
    """Base class. Subclasses implement :meth:`mask` and :meth:`describe`."""

    def mask(self, catalog: Catalog) -> np.ndarray:
        raise NotImplementedError

    def describe(self) -> dict:
        raise NotImplementedError


@dataclass
class NumberDensity(Selection):
    """Select the ``nbar`` [(h/Mpc)^3] highest-SFR objects.

    Default (recommended): fix the threshold from ``ensemble`` so the same cut is
    applied to every realization. Set ``per_realization=True`` to instead solve the
    threshold from each catalog -- this matches nbar exactly per box but adds
    selection scatter across the ensemble (documented, not the default).
    """
    nbar: float
    ensemble: EnsembleNSFR | None = None
    sfr_col: str = "sfr_corr"
    per_realization: bool = False
    _fixed_threshold: float | None = field(default=None, init=False, repr=False)

    def threshold(self, catalog: Catalog) -> float:
        if self.per_realization:
            return threshold_for_density(catalog.column(self.sfr_col),
                                         catalog.volume, self.nbar)
        if self._fixed_threshold is None:
            if self.ensemble is None:
                raise ValueError(
                    "NumberDensity needs an ensemble n(>SFR) table (or "
                    "per_realization=True). Build one with build_ensemble_nsfr().")
            self._fixed_threshold = self.ensemble.threshold_for_density(self.nbar)
        return self._fixed_threshold

    def mask(self, catalog: Catalog) -> np.ndarray:
        return catalog.column(self.sfr_col) > self.threshold(catalog)

    def describe(self) -> dict:
        return {"kind": "number_density", "nbar": self.nbar, "sfr_col": self.sfr_col,
                "per_realization": self.per_realization,
                "ensemble_fixed": not self.per_realization}


@dataclass
class Threshold(Selection):
    """Generic ``lo <= column [< hi]`` cut (SFR or M*). ``hi=None`` => no upper edge."""
    column: str
    lo: float
    hi: float | None = None

    def mask(self, catalog: Catalog) -> np.ndarray:
        c = catalog.column(self.column)
        m = c >= self.lo
        if self.hi is not None:
            m &= c < self.hi
        return m

    def describe(self) -> dict:
        return {"kind": "threshold", "column": self.column, "lo": self.lo, "hi": self.hi}


@dataclass
class Joint(Selection):
    """Logical AND of several selections (e.g. M* x SFR)."""
    parts: list

    def mask(self, catalog: Catalog) -> np.ndarray:
        m = np.ones(len(catalog), dtype=bool)
        for p in self.parts:
            m &= p.mask(catalog)
        return m

    def describe(self) -> dict:
        return {"kind": "joint", "parts": [p.describe() for p in self.parts]}


@dataclass
class Callable(Selection):
    """Bring-your-own: ``fn(columns) -> bool mask``. ``columns`` is a dict-like that
    lazily serves any catalog column (incl. x,y,z,vx,vy,vz). ``needs`` may name the
    columns used (for metadata); otherwise the callable just pulls what it wants.
    """
    fn: object
    needs: tuple = ()
    label: str = "callable"

    def mask(self, catalog: Catalog) -> np.ndarray:
        cols = _LazyColumns(catalog)
        m = np.asarray(self.fn(cols), dtype=bool)
        if m.shape != (len(catalog),):
            raise ValueError(
                f"BYO callable returned mask shape {m.shape}, expected {(len(catalog),)}")
        return m

    def describe(self) -> dict:
        return {"kind": "callable", "label": self.label, "needs": list(self.needs)}


class _LazyColumns:
    """Dict-like over a catalog, so a BYO callable can do ``cols['sfr_corr']``."""

    def __init__(self, catalog: Catalog):
        self._cat = catalog

    def __getitem__(self, name):
        return self._cat.column(name)

    def __contains__(self, name):
        return self._cat.has(name)
