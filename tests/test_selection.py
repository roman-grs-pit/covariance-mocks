"""Acceptance suite for the thin selection / sample-extraction layer (Dev 2).

Defines what "validated" means for the layer:
  (i)   number-density inversion round-trips, and the ensemble-fixed threshold is
        identical across realizations;
  (ii)  column / units integrity -- the returned sample is the stored columns sliced
        by the mask, with no transformation (positions/velocities untouched);
  (iii) determinism -- same selection + same catalog -> same sample.

Runs on a synthetic fixture (fast, CI). An optional test exercises a real z=1.4
catalog if one is present on NERSC.
"""
import glob
import os

import numpy as np
import pytest

from covariance_mocks.selection import (
    Catalog, NumberDensity, Threshold, Joint, Callable,
    build_ensemble_nsfr, threshold_for_density, select, select_ensemble,
)

LBOX = 100.0


def _write_catalog(path, n=200_000, seed=0):
    """Write a synthetic catalog with the real on-disk structure."""
    import h5py
    rng = np.random.default_rng(seed)
    sfr = 10 ** rng.normal(0.0, 0.6, n).astype(np.float32)          # ~Msun/yr
    mstar = 10 ** rng.uniform(9.0, 11.0, n).astype(np.float32)       # ~10^9 - 10^11 Msun
    pos = rng.uniform(0.0, LBOX, (n, 3)).astype(np.float64)
    vel = rng.normal(0.0, 300.0, (n, 3)).astype(np.float64)
    with h5py.File(path, "w") as f:
        g = f.create_group("galaxies")
        g["sfr_corr"] = sfr
        g["sfr_raw"] = (sfr * 1.1).astype(np.float32)
        g["mstar_corr"] = mstar
        g["mstar_raw"] = (mstar * 0.9).astype(np.float32)
        g["mpeak"] = (mstar * 30).astype(np.float32)
        g["pos"] = pos
        g["vel"] = vel
        f.attrs["Lbox"] = LBOX
        f.attrs["z_obs"] = 1.4
        f.attrs["n_galaxies"] = n
    return path


@pytest.fixture(scope="module")
def ensemble_paths(tmp_path_factory):
    d = tmp_path_factory.mktemp("cats")
    return [_write_catalog(os.path.join(d, f"r{i:04d}.hdf5"), seed=i) for i in range(4)]


# (i) number-density inversion round-trips ---------------------------------------
def test_number_density_roundtrip(ensemble_paths):
    ens = build_ensemble_nsfr(ensemble_paths)
    nbar = 1e-3
    thr = ens.threshold_for_density(nbar)
    assert ens.density_at(thr) == pytest.approx(nbar, rel=0.02)   # forward/back


def test_ensemble_threshold_is_identical_across_realizations(ensemble_paths):
    ens = build_ensemble_nsfr(ensemble_paths)
    sel = NumberDensity(nbar=1e-3, ensemble=ens)
    thresholds, achieved = [], []
    for s in select_ensemble(ensemble_paths, sel):
        thresholds.append(s.metadata["selection"]["threshold"])
        achieved.append(s.metadata["achieved_nbar"])
    assert len(set(thresholds)) == 1                              # SAME cut everywhere
    assert np.mean(achieved) == pytest.approx(1e-3, rel=0.05)     # right density on average


def test_per_realization_matches_nbar_exactly(ensemble_paths):
    with Catalog.open(ensemble_paths[0]) as cat:
        sel = NumberDensity(nbar=2e-3, per_realization=True)
        s = select(cat, sel)
        assert s.metadata["achieved_nbar"] == pytest.approx(2e-3, abs=1.5 / cat.volume)


# (ii) column / units integrity --------------------------------------------------
def test_column_units_integrity(ensemble_paths):
    with Catalog.open(ensemble_paths[0]) as cat:
        sel = Threshold("sfr_corr", lo=1.0)
        mask = sel.mask(cat)
        s = select(cat, sel)
        for col in ("sfr_corr", "mstar_corr", "x", "vz"):
            np.testing.assert_array_equal(s[col], cat.column(col)[mask])  # exact slice
        assert s.positions.shape == (s.n, 3)
        np.testing.assert_array_equal(s.positions[:, 0], cat.column("x")[mask])
        np.testing.assert_array_equal(s.velocities[:, 2], cat.column("vz")[mask])


# (iv) determinism ---------------------------------------------------------------
def test_determinism(ensemble_paths):
    with Catalog.open(ensemble_paths[0]) as cat:
        sel = Threshold("sfr_corr", lo=1.0)
        a = select(cat, sel)
        b = select(cat, sel)
        for col in a.columns:
            np.testing.assert_array_equal(a[col], b[col])


# BYO callable + joint -----------------------------------------------------------
def test_byo_callable_and_joint(ensemble_paths):
    with Catalog.open(ensemble_paths[0]) as cat:
        byo = Callable(lambda c: (c["sfr_corr"] > 1.0) & (c["mstar_corr"] > 1e10),
                       label="sfr&mstar")
        joint = Joint([Threshold("sfr_corr", lo=1.0), Threshold("mstar_corr", lo=1e10)])
        np.testing.assert_array_equal(byo.mask(cat), joint.mask(cat))


# optional: a real z=1.4 catalog if present -------------------------------------
_REAL = sorted(glob.glob(
    "/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/catalogs/r*/mock_z1.400.hdf5"))


@pytest.mark.skipif(not _REAL, reason="no real z=1.4 catalog available")
def test_real_catalog_number_density():
    with Catalog.open(_REAL[0]) as cat:
        assert cat.Lbox == pytest.approx(500.0)
        sel = NumberDensity(nbar=1e-3, per_realization=True)
        s = select(cat, sel)
        assert s.metadata["achieved_nbar"] == pytest.approx(1e-3, abs=2.0 / cat.volume)
        np.testing.assert_array_equal(s["x"], cat.column("pos")[sel.mask(cat)][:, 0])
