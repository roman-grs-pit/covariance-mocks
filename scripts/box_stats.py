#!/usr/bin/env python3
"""Per-box stats sidecar — computed once when a box finishes, so the dashboard never
re-reads the 2.9 GB catalogs.

Marcelo's point (2026-06-16): don't re-read every catalog on each refresh to get ensemble
stats; compute each box's contribution at the end of its successful populate. This writes a
small sidecar `<dir>/mock_z<z>.stats.npz` (~1 MB) next to each catalog holding:

  * H_mp_sfr  -- the (logMpeak, logSFR_raw) histogram on the sfr_recal grid. Summed across
                boxes it gives the ensemble f1/f2; per box, with f1/f2, it yields that box's
                CORRECTED panel curves (n(>sfr_corr), sSFR_corr(M*), GSMF_corr, SMHM_corr)
                without re-reading the catalog.
  * raw 1-D panel curves (nsfr_raw, ssfr_raw(M*), gsmf_raw, smhm_raw) + sfrd_raw scalar --
                the box's own raw-side contribution, precomputed.
  * V (box physical volume), ngal.

The dashboard refresh reads only sidecars (a few hundred MB total, vs hundreds of GB of
catalogs). The per-object sfr_corr written INTO the catalogs is a separate, once-only
deliverable pass (sfr_calibrate_apply.py), not part of any refresh.

Usage: python box_stats.py <catalog.hdf5> [more ...]   (writes/overwrites each sidecar)
"""
import os
import sys

import numpy as np
import h5py

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from covariance_mocks import sfr_recal as R

H_D = R.H_DIFFSKY

# Panel binning (shared with the plotter so curves are consistent).
SMB = np.round(np.arange(8.0, 12.201, 0.2), 4)
SMC = 0.5 * (SMB[:-1] + SMB[1:])
MPB = np.round(np.arange(10.5, 14.301, 0.2), 4)
MPC = 0.5 * (MPB[:-1] + MPB[1:])
SFRGRID = np.round(np.arange(-2.0, 3.0001, 0.1), 4)
NMIN = 50


def _mean_ssfr_lin(logsm, sfr):
    idx = np.digitize(logsm, SMB) - 1
    ssfr = sfr / 10.0 ** logsm
    out = np.full(SMC.size, np.nan)
    for i in range(SMC.size):
        m = idx == i
        if m.sum() >= NMIN:
            out[i] = np.log10(np.mean(ssfr[m]))
    return out


def _median_in_bins(x, y, edges):
    idx = np.digitize(x, edges) - 1
    out = np.full(edges.size - 1, np.nan)
    for i in range(edges.size - 1):
        m = idx == i
        if m.sum() >= NMIN:
            out[i] = np.median(y[m])
    return out


def sidecar_path(catalog_path):
    return catalog_path[:-5] + ".stats.npz" if catalog_path.endswith(".hdf5") else catalog_path + ".stats.npz"


def compute(catalog_path):
    """Read a catalog ONCE -> the sidecar dict (raw curves + H_mp_sfr + V)."""
    with h5py.File(catalog_path, "r") as f:
        g = f["galaxies"]
        logmp = g["logmp_t_obs"][:].astype(np.float64)
        logsm = g["logsm_t_obs"][:].astype(np.float64)
        logssfr = g["logssfr_t_obs"][:].astype(np.float64)
        cen = g["upid"][:] < 0
        lbox = float(f.attrs.get("Lbox", 500.0))
    vphys = (lbox / H_D) ** 3
    sfr_raw = 10.0 ** (logssfr + logsm)
    mp_phys = R.mpeak_phys(logmp)
    lsfr = np.log10(np.clip(sfr_raw, 1e-30, None))

    H = R.empty_hist()
    R.accumulate(H, logmp, logsm, logssfr)

    nsfr_raw = np.array([(lsfr > s).sum() for s in SFRGRID], dtype=np.float64) / vphys
    out = dict(
        H_mp_sfr=H.astype(np.float32),
        v=vphys, ngal=logsm.size, lbox=lbox,
        sfrd_raw=sfr_raw.sum() / vphys,
        nsfr_raw=nsfr_raw,
        ssfr_raw=_mean_ssfr_lin(logsm, sfr_raw),
        gsmf_raw=np.histogram(logsm, bins=SMB)[0] / (vphys * 0.2),
        smhm_raw=_median_in_bins(mp_phys[cen], logsm[cen], MPB),
    )
    return out


def raw_curves(side):
    """The raw-side panel curves stored in a sidecar (no computation)."""
    return dict(nsfr=side["nsfr_raw"], sfrd=float(side["sfrd_raw"]),
                ssfr=side["ssfr_raw"], gsmf=side["gsmf_raw"], smhm=side["smhm_raw"])


def corrected_curves(side, f1, f2):
    """This box's CORRECTED panel curves from its H_mp_sfr + the ensemble f1/f2 mapping —
    pure histogram arithmetic, no catalog read. f1[mp]=logM*_corr, f2[mp,sfr]=logsSFR_corr
    on the sfr_recal grid."""
    H = np.asarray(side["H_mp_sfr"], dtype=np.float64)
    v = float(side["v"])
    mp_w = H.sum(axis=1)                                   # counts per logMpeak bin
    logsfr_corr = f2 + f1[:, None]                         # logSFR_corr per (mp, sfr) bin
    sfr_corr_lin = 10.0 ** logsfr_corr
    ssfr_corr_lin = 10.0 ** f2                             # sSFR_corr (linear) per (mp,sfr)

    # n(>sfr_corr)
    nsfr = np.array([H[logsfr_corr > s].sum() for s in SFRGRID]) / v
    sfrd = (H * sfr_corr_lin).sum() / v

    # M*_corr per mp bin = f1; GSMF_corr = weighted hist of f1 into SMB
    gsmf = np.histogram(f1, bins=SMB, weights=mp_w)[0] / (v * 0.2)

    # sSFR_corr(M*_corr): weighted linear-mean sSFR_corr in M*_corr (=f1) bins
    sm_idx = np.digitize(f1, SMB) - 1
    ssfr = np.full(SMC.size, np.nan)
    num = np.zeros(SMC.size); den = np.zeros(SMC.size)
    for b in range(f1.size):
        i = sm_idx[b]
        if 0 <= i < SMC.size:
            num[i] += (H[b] * ssfr_corr_lin[b]).sum(); den[i] += H[b].sum()
    ok = den >= NMIN
    ssfr[ok] = np.log10(num[ok] / den[ok])

    # SMHM_corr: M*_corr(Mpeak) = f1 (deterministic); weighted median of f1 in MPB bins
    mp_idx = np.digitize(R.LOGMP_CEN, MPB) - 1
    smhm = np.full(MPC.size, np.nan)
    for i in range(MPC.size):
        sel = mp_idx == i
        w = mp_w[sel]
        if w.sum() >= NMIN:
            vals = f1[sel]; order = np.argsort(vals)
            cw = np.cumsum(w[order]); smhm[i] = vals[order][np.searchsorted(cw, 0.5 * cw[-1])]
    return dict(nsfr=nsfr, sfrd=sfrd, ssfr=ssfr, gsmf=gsmf, smhm=smhm)


def write(catalog_path):
    side = compute(catalog_path)
    sp = sidecar_path(catalog_path)
    np.savez(sp, **side)
    return sp


def load(sidecar):
    d = np.load(sidecar)
    return {k: d[k] for k in d.files}


def main():
    for p in sys.argv[1:]:
        try:
            sp = write(p)
            print("wrote", sp)
        except Exception as e:
            print("FAILED", p, e, file=sys.stderr)


if __name__ == "__main__":
    main()
