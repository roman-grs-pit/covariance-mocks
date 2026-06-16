#!/usr/bin/env python3
"""Ensemble two-pass UM-corrected SFR over a set of raw SFR-only catalogs (one redshift).

Drives the sfr_recal core across the catalogs of an ensemble at a single redshift:

  accumulate : pass 1 -- read (logMpeak, logM*, logsSFR) from every catalog, sum the
               ensemble histogram H[logMpeak, logSFR].  (parallel over catalogs)
  build      : H + UM targets (um_targets_z<z>.npz) -> f1, f2 calibration; save it.
  apply      : pass 2 -- for every catalog, write sfr_corr / mstar_corr (+ sfr_raw,
               mstar_raw, mpeak) into the file, and accumulate the ensemble n(>sfr_corr).
  run        : accumulate -> build -> apply in one go.

The corrected SFR is ensemble-consistent (one f1/f2 mapping for all realizations) and the
companion n(>sfr_corr, z) is the ensemble cumulative SFR function downstream users
abundance-match their own LF against.

Catalog raw fields (written by the SFR-only generate_single_mock):
  galaxies/logmp_t_obs   log10(Mpeak/[Msun/h])
  galaxies/logsm_t_obs   log10(M*/Msun)        (raw diffsky)
  galaxies/logssfr_t_obs log10(sSFR/yr^-1)     (raw diffsky)
  galaxies/pos, galaxies/vel                   (kept as the deliverable x/y/z, vx/vy/vz)
Added by apply (physical Msun, Msun/yr):
  galaxies/sfr_corr, galaxies/mstar_corr, galaxies/sfr_raw, galaxies/mstar_raw, galaxies/mpeak

Usage:
  python sfr_calibrate_apply.py run --z 1.400 --catalogs 'DIR/r*/mock_z1.400.hdf5' \
         --um-targets local/dashboard/refdata/um_targets_z1.400.npz --out DIR/_calib --procs 32
"""
import argparse
import glob
import os
import sys
from multiprocessing import Pool

import numpy as np
import h5py

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from covariance_mocks import sfr_recal as R

# fine SFR grid for the companion n(>sfr_corr) cumulative function [log10 Msun/yr]
LOGSFR_NGRID = np.round(np.arange(-6.0, 4.0001, 0.02), 4)


def _lbox_mpch(f):
    """Box size [Mpc/h] from the catalog (attrs, with a 500 Mpc/h fallback for small boxes)."""
    for k in ("Lbox", "lbox", "box_size", "BoxSize"):
        if k in f.attrs:
            return float(f.attrs[k])
    return 500.0


def _read_raw(path):
    """Return (logmp_h, logsm, logssfr, lbox_mpch) for one catalog."""
    with h5py.File(path, "r") as f:
        g = f["galaxies"]
        logmp = g["logmp_t_obs"][:].astype(np.float64)
        logsm = g["logsm_t_obs"][:].astype(np.float64)
        logssfr = g["logssfr_t_obs"][:].astype(np.float64)
        lbox = _lbox_mpch(f)
    return logmp, logsm, logssfr, lbox


def _accum_one(path):
    """Worker: one catalog -> (H, vphys_box, ngal)."""
    logmp, logsm, logssfr, lbox = _read_raw(path)
    H = R.empty_hist()
    R.accumulate(H, logmp, logsm, logssfr)
    vphys = (lbox / R.H_DIFFSKY) ** 3
    return H, vphys, logmp.size


def cmd_accumulate(paths, procs):
    H = R.empty_hist()
    v_ens = 0.0
    ntot = 0
    if procs > 1:
        with Pool(procs) as pool:
            for h, v, n in pool.imap_unordered(_accum_one, paths):
                H += h; v_ens += v; ntot += n
    else:
        for p in paths:
            h, v, n = _accum_one(p)
            H += h; v_ens += v; ntot += n
    print(f"[accumulate] {len(paths)} catalogs, {ntot:,} galaxies, V_ens={v_ens:.4e} Mpc^3")
    return H, v_ens, ntot


def cmd_build(H, v_ens, um_targets_path):
    um = R.load_um_targets(um_targets_path)
    f1 = R.build_f1(H, v_ens, um["sm_fine"], um["ncum"])
    f2 = R.build_f2(H, f1, um["sm_cen"], um["qgrid"], um["ssfr_quant"])
    print(f"[build] f1 finite={np.isfinite(f1).all()}  f2 rows built="
          f"{int(np.isfinite(f2[:,0]).sum())}/{f2.shape[0]}")
    return f1, f2


def _apply_one(args):
    """Worker: apply f1/f2 to one catalog, augment it, return its sfr_corr histogram + vphys."""
    path, f1, f2 = args
    with h5py.File(path, "r+") as f:
        g = f["galaxies"]
        logmp = g["logmp_t_obs"][:].astype(np.float64)
        logsm = g["logsm_t_obs"][:].astype(np.float64)
        logssfr = g["logssfr_t_obs"][:].astype(np.float64)
        lbox = _lbox_mpch(f)
        logsm_corr, logsfr_corr = R.apply_corr(logmp, logsm, logssfr, f1, f2)
        sfr_raw = 10.0 ** (logssfr + logsm)
        mstar_raw = 10.0 ** logsm
        mpeak = 10.0 ** R.mpeak_phys(logmp)          # physical Msun
        sfr_corr = 10.0 ** logsfr_corr
        mstar_corr = 10.0 ** logsm_corr
        for name, arr in (("sfr_corr", sfr_corr), ("mstar_corr", mstar_corr),
                          ("sfr_raw", sfr_raw), ("mstar_raw", mstar_raw), ("mpeak", mpeak)):
            if name in g:
                del g[name]
            g.create_dataset(name, data=arr.astype(np.float32))
    hist = np.histogram(logsfr_corr, bins=np.r_[LOGSFR_NGRID, LOGSFR_NGRID[-1] + 0.02])[0]
    return hist.astype(np.float64), (lbox / R.H_DIFFSKY) ** 3


def cmd_apply(paths, f1, f2, procs):
    sfr_hist = np.zeros(LOGSFR_NGRID.size, dtype=np.float64)
    v_ens = 0.0
    work = [(p, f1, f2) for p in paths]
    if procs > 1:
        with Pool(procs) as pool:
            for h, v in pool.imap_unordered(_apply_one, work):
                sfr_hist += h; v_ens += v
    else:
        for w in work:
            h, v = _apply_one(w)
            sfr_hist += h; v_ens += v
    n_cum = np.cumsum(sfr_hist[::-1])[::-1] / v_ens     # n(>sfr_corr) [Mpc^-3]
    print(f"[apply] augmented {len(paths)} catalogs; n(>sfr_corr) over V_ens={v_ens:.4e} Mpc^3")
    return LOGSFR_NGRID, n_cum, v_ens


def save_companion(out_path, z, logsfr_grid, n_cum, v_ens, ncat):
    with h5py.File(out_path, "w") as f:
        f.attrs["redshift"] = float(z)
        f.attrs["v_ensemble_mpc3"] = float(v_ens)
        f.attrs["n_realizations"] = int(ncat)
        f.attrs["units"] = "logsfr=log10(Msun/yr); n_cum=Mpc^-3 (physical), n(>sfr_corr)"
        f.create_dataset("logsfr_corr", data=logsfr_grid)
        f.create_dataset("n_cumulative", data=n_cum)
    print(f"[companion] saved {out_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["accumulate", "build", "apply", "run"])
    ap.add_argument("--z", required=True)
    ap.add_argument("--catalogs", required=True, help="glob for the raw catalogs at this z")
    ap.add_argument("--um-targets", required=True)
    ap.add_argument("--out", required=True, help="calibration output dir")
    ap.add_argument("--procs", type=int, default=1)
    args = ap.parse_args()

    paths = sorted(glob.glob(args.catalogs))
    if not paths:
        print(f"no catalogs match {args.catalogs}", file=sys.stderr)
        return 2
    os.makedirs(args.out, exist_ok=True)
    calib_npz = os.path.join(args.out, f"sfr_calibration_z{args.z}.npz")
    companion = os.path.join(args.out, f"n_sfr_corr_z{args.z}.hdf5")

    if args.cmd in ("accumulate", "run"):
        H, v_ens, ntot = cmd_accumulate(paths, args.procs)
        f1, f2 = cmd_build(H, v_ens, args.um_targets)
        np.savez(calib_npz, H=H, v_ens=v_ens, f1=f1, f2=f2,
                 logmp_cen=R.LOGMP_CEN, logsfr_cen=R.LOGSFR_CEN, z=float(args.z))
        print(f"[saved] {calib_npz}")
    if args.cmd in ("apply", "run"):
        d = np.load(calib_npz)
        f1, f2 = d["f1"], d["f2"]
        grid, n_cum, v_ens = cmd_apply(paths, f1, f2, args.procs)
        save_companion(companion, args.z, grid, n_cum, v_ens, len(paths))
    return 0


if __name__ == "__main__":
    sys.exit(main())
