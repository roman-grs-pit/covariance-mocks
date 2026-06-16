#!/usr/bin/env python3
"""Validate the production ensemble sfr_recal core against the single-box diagnostic.

Runs sfr_recal (histogram-based, ensemble two-pass) on the one z=1.4 diffsky box used by
the diagnostic and checks it reproduces the diagnostic's per-galaxy f1/f2 results:
  * SFRD_corr / SFRD_UM ~ 1.0 (the recal preserves the UM SFRD by construction)
  * mean sSFR(M*) corr matches the diagnostic's stored curve within interpolation error.

Faithfulness target: the diagnostic uses exact per-galaxy sorts; this core uses ensemble
histograms. Agreement to ~0.01-0.02 dex confirms the histogram approximation is sub-dominant.
"""
import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from covariance_mocks import sfr_recal as R

HERE = os.path.dirname(os.path.abspath(__file__))
DASH = os.path.join(HERE, "..", "local", "dashboard")
CACHE = os.path.join(DASH, "reports", "_cache_diffsky_pergal_z1.400_n1.npz")
UM_TARGETS = os.path.join(DASH, "refdata", "um_targets_z1.400.npz")
DIAG = os.path.join(DASH, "reports", "_cache_sfrmatch_z1.400.npz")

VPHYS_D = (500.0 / R.H_DIFFSKY) ** 3   # one diffsky box, physical Mpc^3

# M* bins for the mean sSFR(M*) comparison (match the diagnostic's SMB/SMC)
SMB = np.round(np.arange(6.0, 12.501, 0.1), 4)
SMC = 0.5 * (SMB[:-1] + SMB[1:])


def mean_ssfr(logsm, logssfr, min_n=200):
    idx = np.digitize(logsm, SMB) - 1
    lin = 10.0 ** logssfr
    out = np.full(SMC.size, np.nan)
    for i in range(SMC.size):
        m = idx == i
        if m.sum() >= min_n:
            out[i] = np.log10(np.mean(lin[m]))
    return out


def main():
    print("[load] diffsky cache + UM targets")
    d = np.load(CACHE)
    logsm = d["logsm"].astype(np.float64)
    logssfr = d["logssfr"].astype(np.float64)
    logmp_h = d["logmp"].astype(np.float64)        # log10(Msun/h)
    um = R.load_um_targets(UM_TARGETS)

    # --- pass 1: accumulate the ensemble histogram (one box here) and build f1, f2 ---
    H = R.empty_hist()
    R.accumulate(H, logmp_h, logsm, logssfr)
    f1 = R.build_f1(H, VPHYS_D, um["sm_fine"], um["ncum"])
    f2 = R.build_f2(H, f1, um["sm_cen"], um["qgrid"], um["ssfr_quant"])

    # --- pass 2: apply ---
    logsm_corr, logsfr_corr = R.apply_corr(logmp_h, logsm, logssfr, f1, f2)
    logssfr_corr = logsfr_corr - logsm_corr
    logsfr_raw = logssfr + logsm

    # --- SFRD preservation ---
    sfrd_raw = (10.0 ** logsfr_raw).sum() / VPHYS_D
    sfrd_corr = (10.0 ** logsfr_corr).sum() / VPHYS_D

    diag = np.load(DIAG)
    sfrd_um = float(diag["sfrd_um"])
    sfrd_d_rec_diag = float(diag["sfrd_d_rec"])

    print("\n=== SFRD [Msun/yr/Mpc^3] ===")
    print(f"  UM target            : {sfrd_um:.4e}  (log {np.log10(sfrd_um):+.3f})")
    print(f"  diffsky raw          : {sfrd_raw:.4e}  ({np.log10(sfrd_raw/sfrd_um):+.3f} dex vs UM)")
    print(f"  sfr_corr (this core) : {sfrd_corr:.4e}  ({np.log10(sfrd_corr/sfrd_um):+.3f} dex vs UM)")
    print(f"  sfr_corr (diagnostic): {sfrd_d_rec_diag:.4e}  ({np.log10(sfrd_d_rec_diag/sfrd_um):+.3f} dex vs UM)")
    print(f"  core vs diagnostic   : {np.log10(sfrd_corr/sfrd_d_rec_diag):+.4f} dex")

    # --- mean sSFR(M*) corr vs diagnostic stored curve ---
    mean_corr = mean_ssfr(logsm_corr, logssfr_corr)
    d_mean_rec_diag = diag["d_mean_rec"]
    smc_diag = diag["smc"]
    print("\n=== mean sSFR(M*): this core vs diagnostic (vs UM) ===")
    print("  logM*   core    diag    UM     core-diag")
    um_mean = diag["um_mean"]
    for lm in (9.5, 10.0, 10.5, 11.0, 11.4):
        i = np.argmin(np.abs(SMC - lm))
        j = np.argmin(np.abs(smc_diag - lm))
        c, g, u = mean_corr[i], d_mean_rec_diag[j], um_mean[j]
        dd = (c - g) if (np.isfinite(c) and np.isfinite(g)) else np.nan
        print(f"  {lm:5.1f}  {c:7.3f} {g:7.3f} {u:7.3f}   {dd:+.4f}")

    # --- verdict ---
    sfrd_ok = abs(np.log10(sfrd_corr / sfrd_um)) < 0.03
    rng = (SMC >= 9.5) & (SMC <= 11.4)
    diffs = []
    for i in np.where(rng)[0]:
        j = np.argmin(np.abs(smc_diag - SMC[i]))
        if np.isfinite(mean_corr[i]) and np.isfinite(d_mean_rec_diag[j]):
            diffs.append(mean_corr[i] - d_mean_rec_diag[j])
    max_dev = np.max(np.abs(diffs)) if diffs else np.nan
    print(f"\n=== verdict ===")
    print(f"  SFRD_corr within 0.03 dex of UM : {sfrd_ok}  ({np.log10(sfrd_corr/sfrd_um):+.4f} dex)")
    print(f"  max |mean sSFR core-diag| 9.5-11.4: {max_dev:.4f} dex")
    ok = sfrd_ok and (max_dev < 0.03)
    print(f"  PASS: {ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
