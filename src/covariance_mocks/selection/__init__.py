"""Thin selection / sample-extraction layer for the SFR-only covariance mocks.

Given one mock catalog (+ an ensemble n(>SFR) table) and a user-defined selection,
return the selected sample -- applied identically across every realization. The layer
stops at the sample: no clustering estimators, no covariance. The GRS-PIT team runs
their own statistics/covariance on the samples with their own tools.

Quickstart::

    from covariance_mocks.selection import (
        Catalog, NumberDensity, build_ensemble_nsfr, select, select_ensemble)

    ens = build_ensemble_nsfr(paths)                 # ensemble n(>sfr_corr)
    sel = NumberDensity(nbar=1e-3, ensemble=ens)     # same cut on every realization
    for sample in select_ensemble(paths, sel):       # one sample per realization
        pos, vel = sample.positions, sample.velocities   # hand off to your own tool
"""
from .catalog import Catalog
from .nsfr import (
    EnsembleNSFR, build_ensemble_nsfr, cumulative_n_gt,
    threshold_for_density, default_grid,
)
from .selection import (
    Selection, NumberDensity, Threshold, Joint, Callable,
)
from .sample import Sample, select, select_ensemble, DEFAULT_COLUMNS

__all__ = [
    "Catalog", "EnsembleNSFR", "build_ensemble_nsfr", "cumulative_n_gt",
    "threshold_for_density", "default_grid", "Selection", "NumberDensity",
    "Threshold", "Joint", "Callable", "Sample", "select",
    "select_ensemble", "DEFAULT_COLUMNS",
]
