"""Ensemble UM-corrected SFR (sfr_corr) — the two-pass recalibration core.

The SFR-only deliverable (PLAN 2026-06-16 amendment) ships, per object, a UM-corrected
SFR ``sfr_corr`` alongside the raw diffsky SFR ``sfr_raw``. ``sfr_corr`` is built by the
two abundance-matching steps of the recalibration diagnostic, promoted here from a
single-box experiment to an **ensemble-wide, two-pass production calibration**:

  f1  rank-match diffsky Mpeak -> UM/Behroozi+2019 GSMF  [ n_sim(>Mpeak) = n_UM(>M*) ]
      => M*_corr reproduces the UM number density per stellar mass.
  f2  conditional-quantile match of sSFR at fixed M*_corr: within each M* bin, map each
      diffsky galaxy's sSFR percentile onto UM's sSFR distribution at that percentile.
      => imposes UM's full P(sSFR | M*); the recalibrated joint (M*, SFR) -- and hence the
         SFRD and mean/median sSFR(M*) -- reproduce UM by construction.

Why ensemble + two-pass (Marcelo, work thread 2026-06-16): f1/f2 must be ONE universal
mapping at fixed z, not a per-box rank (which would send the same Mpeak to slightly
different M*_corr across realizations -- sample noise). The companion ``n(>sfr_corr, z)``
table is itself an ensemble quantity, so an ensemble pass is required regardless.

Mechanics. The mappings are built from ensemble HISTOGRAMS, not per-galaxy sorts (65e9
galaxies across 1883 boxes cannot be held in memory):

  Pass 1 (accumulate): for every raw catalog, add to a single 2-D histogram
      H[logMpeak_bin, logSFR_bin] over the ensemble (logMpeak in physical Msun, logSFR raw).
      The logMpeak marginal gives n_sim(>Mpeak) -> f1; the per-logMpeak-bin logSFR
      distribution gives the diffsky conditional CDF -> f2. Binning the f2 conditional by
      logMpeak rather than M*_corr is equivalent because f1 is monotonic; the sSFR rank
      fraction is invariant under the constant M*-relabel shift within a (fine) bin.
  Build f1, f2 once from H + the UM targets (um_targets_z<...>.npz: ncum, qgrid, ssfr_quant).
  Pass 2 (apply): for every catalog, map (logMpeak, logSFR_raw) -> (M*_corr, sfr_corr)
      pointwise and accumulate n(>sfr_corr).

Faithfulness to the diagnostic (local/dashboard/match_sfr_distribution.py): f1 reproduces
``sham`` with the (rank+0.5)/V abundance-match convention via mid-bin counts; f2 reproduces
the conditional-quantile remap with the same (rank+0.5)/n fractions via mid-bin CDFs and the
same nearest-valid-UM-bin rule. With the bin widths below the histogram approximation is
well under the interpolation error of the diagnostic itself.
"""
import numpy as np

# diffsky/Abacus little-h: cache/catalog logMpeak is log10(Msun/h); UM masses are physical
# Msun (h=0.68 in the catalog but masses are not h-scaled), so convert Mpeak by -log10(h).
H_DIFFSKY = 0.6774

# Ensemble accumulation grids. logMpeak fine (0.02 dex) so f1 and the f2 conditioning are
# smooth; logSFR fine (0.02 dex) so the f2 conditional CDF resolves the sSFR quantiles.
LOGMP_EDGES = np.round(np.arange(10.0, 15.0001, 0.02), 4)
LOGMP_CEN = 0.5 * (LOGMP_EDGES[:-1] + LOGMP_EDGES[1:])
LOGSFR_EDGES = np.round(np.arange(-6.0, 4.0001, 0.02), 4)
LOGSFR_CEN = 0.5 * (LOGSFR_EDGES[:-1] + LOGSFR_EDGES[1:])
N_MP = LOGMP_CEN.size
N_SFR = LOGSFR_CEN.size

# Minimum ensemble counts in a logMpeak bin before its f2 conditional CDF is trusted.
NMIN_F2_BIN = 200


def mpeak_phys(logmp_h):
    """log10 Mpeak [Msun/h]  ->  log10 Mpeak [physical Msun] (UM mass convention)."""
    return np.asarray(logmp_h, dtype=np.float64) - np.log10(H_DIFFSKY)


def empty_hist():
    """A zeroed ensemble accumulation histogram H[logMpeak_bin, logSFR_bin]."""
    return np.zeros((N_MP, N_SFR), dtype=np.float64)


def accumulate(H, logmp_h, logsm_raw, logssfr_raw):
    """Add one catalog's galaxies to the ensemble histogram H (in place).

    logmp_h is log10(Mpeak/[Msun/h]); logsm_raw, logssfr_raw are the raw diffsky
    log10(M*/Msun) and log10(sSFR/yr^-1). logSFR_raw = logssfr_raw + logsm_raw.
    """
    lmp = mpeak_phys(logmp_h)
    lsfr = np.asarray(logssfr_raw, dtype=np.float64) + np.asarray(logsm_raw, dtype=np.float64)
    h, _, _ = np.histogram2d(lmp, lsfr, bins=[LOGMP_EDGES, LOGSFR_EDGES])
    H += h
    return H


def build_f1(H, v_ensemble, um_sm_fine, um_ncum):
    """Build the f1 mapping table f1[logMpeak_bin] = log10(M*_corr).

    Abundance match n_sim(>Mpeak) = n_UM(>M*). The mid-bin cumulative count reproduces the
    diagnostic's (rank+0.5)/V convention: a logMpeak bin's mid count is (counts in bins
    strictly above) + 0.5*(counts in the bin). v_ensemble is the TOTAL physical volume of
    all accumulated boxes [Mpc^3]; um_ncum is the UM cumulative GSMF n(>M*) on um_sm_fine.
    """
    n_mp = H.sum(axis=1)                                   # counts per logMpeak bin
    cum_from_top = np.cumsum(n_mp[::-1])[::-1]             # counts in this bin and above
    mid_count = cum_from_top - 0.5 * n_mp                  # mid-bin cumulative count
    n_above = mid_count / v_ensemble                       # n_sim(>Mpeak) at bin center
    n_above = np.clip(n_above, um_ncum.min(), um_ncum.max())
    # invert the UM cumulative GSMF: M*_corr at the same number density (both arrays
    # descending in M*, so reverse for np.interp's ascending-x requirement)
    f1 = np.interp(n_above, um_ncum[::-1], um_sm_fine[::-1])
    # Empty Mpeak bins (interior gaps + the sparse high-mass tail) leave holes; fill them by
    # monotonic interpolation over the populated bins so f1 is finite everywhere (a NaN here
    # would propagate into every galaxy's M*_corr at apply time). f1 over valid bins is
    # monotonic, so np.interp fills gaps and clamps the edges consistently.
    valid = n_mp > 0
    if valid.any():
        f1 = np.interp(LOGMP_CEN, LOGMP_CEN[valid], f1[valid])
    return f1


def build_f2(H, f1, um_smb, um_qgrid, um_ssfr_quant):
    """Build the f2 table f2[logMpeak_bin, logSFR_bin] = log10(sSFR_corr).

    For each logMpeak bin: its M*_corr = f1[bin] selects the nearest valid UM M* bin; the
    per-bin diffsky logSFR distribution gives the mid-bin rank fraction of each logSFR bin;
    sSFR_corr = UM sSFR quantile at that fraction. um_smb are the UM M* bin centers,
    um_qgrid the quantile grid, um_ssfr_quant[um_bin, q] the UM sSFR quantiles (NaN where
    a UM bin had too few galaxies).
    """
    f2 = np.full((N_MP, N_SFR), np.nan, dtype=np.float64)
    um_valid = np.isfinite(um_ssfr_quant[:, 0])
    valid_um_bins = np.where(um_valid)[0]
    if valid_um_bins.size == 0:
        raise ValueError("no valid UM sSFR-quantile bins in target")
    for b in range(N_MP):
        row = H[b]
        tot = row.sum()
        if tot < NMIN_F2_BIN or not np.isfinite(f1[b]):
            continue
        # mid-bin rank fraction (matches the diagnostic's (rank+0.5)/n)
        rfrac = (np.cumsum(row) - 0.5 * row) / tot
        ub = valid_um_bins[np.argmin(np.abs(um_smb[valid_um_bins] - f1[b]))]
        f2[b] = np.interp(rfrac, um_qgrid, um_ssfr_quant[ub])
    # Sparse Mpeak bins (below the count floor) get no CDF; fill their rows from the nearest
    # populated Mpeak bin so they stay on the UM quantiles rather than falling back to raw
    # sSFR (which would low-bias the high-mass tail). At ensemble scale (1883 boxes) almost
    # every bin clears the floor and this fill is a no-op; it only matters for sparse tails.
    built = np.isfinite(f2[:, 0])
    if built.any():
        bvec = np.where(built)[0]
        for b in range(N_MP):
            if not built[b] and np.isfinite(f1[b]):
                f2[b] = f2[bvec[np.argmin(np.abs(bvec - b))]]
    return f2


def apply_corr(logmp_h, logsm_raw, logssfr_raw, f1, f2):
    """Map (Mpeak, raw SFR) -> (log10 M*_corr, log10 SFR_corr) pointwise.

    Returns (logsm_corr, logsfr_corr). logsm_corr = f1(Mpeak); sSFR_corr = f2(Mpeak bin,
    raw SFR bin); logsfr_corr = sSFR_corr + logsm_corr (sSFR is recomputed against the
    relabeled M*, exactly as in the diagnostic: ssfr_in = logSFR_raw - M*_corr).
    """
    lmp = mpeak_phys(logmp_h)
    lsfr_raw = np.asarray(logssfr_raw, dtype=np.float64) + np.asarray(logsm_raw, dtype=np.float64)
    logsm_corr = np.interp(lmp, LOGMP_CEN, f1)
    b = np.clip(np.digitize(lmp, LOGMP_EDGES) - 1, 0, N_MP - 1)
    l = np.clip(np.digitize(lsfr_raw, LOGSFR_EDGES) - 1, 0, N_SFR - 1)
    ssfr_corr = f2[b, l]
    # bins below the f2 trust floor (NaN): fall back to the raw sSFR (no correction)
    raw_ssfr = lsfr_raw - logsm_corr
    bad = ~np.isfinite(ssfr_corr)
    ssfr_corr = np.where(bad, raw_ssfr, ssfr_corr)
    logsfr_corr = ssfr_corr + logsm_corr
    return logsm_corr, logsfr_corr


def load_um_targets(npz_path):
    """Load the UM target file (built by build_um_targets.py) into the arrays f1/f2 need."""
    d = np.load(npz_path)
    return dict(sm_fine=d["sm_fine"], ncum=d["ncum"], sm_cen=d["sm_cen"],
                qgrid=d["qgrid"], ssfr_quant=d["ssfr_quant"])
