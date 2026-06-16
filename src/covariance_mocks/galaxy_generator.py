"""
Galaxy Generator Module

Handles galaxy generation coordination with the rgrspit_diffsky package.
Manages batch processing and random key management for galaxy population.

Coordinates galaxy population using rgrspit_diffsky with batch processing.
"""

from jax import random as jran
from . import CURRENT_Z_OBS, LGMP_MIN

# Lean SFR-only output (2026-06-16 amendment). The deliverable carries no line
# luminosities; the emission-line step is dropped from production. We keep only
# the raw per-galaxy fields the SFR-only data model needs — positions/velocities,
# instantaneous stellar mass / sSFR / peak halo mass, the central/sat ID, and
# scalar metadata — and drop everything else: the formation histories (sfh_table,
# log_mah_table) and parameter blocks (mah_params, sfh_params, ...) that dominate
# file size (~80-90%) but are redundant (the instantaneous SFR = 10**logssfr_t_obs
# · 10**logsm_t_obs reproduces them). The UM-corrected sfr_corr / mstar_corr are
# added by the downstream ensemble two-pass calibration, not here. An allowlist
# (rather than a denylist) is robust to upstream version drift adding new arrays.
KEEP_GALAXY_KEYS = frozenset((
    "pos", "vel",
    "logsm_t_obs", "logssfr_t_obs", "logmp_t_obs",
    "upid",
    "z_obs", "t_obs", "t0", "t_table",
))


def generate_galaxies(logmhost, halo_radius, halo_pos, halo_vel, Lbox, rank=0, z_obs=None):
    """
    Generate galaxies for given halos using rgrspit_diffsky.
    
    Populates halos with galaxies using the rgrspit_diffsky package with consistent
    random key generation for reproducible results across MPI ranks.
    
    Parameters
    ----------
    logmhost : jax.numpy.ndarray, shape (N_halos,)
        Log10 host halo masses
    halo_radius : jax.numpy.ndarray, shape (N_halos,)
        Halo virial radii in Mpc/h
    halo_pos : jax.numpy.ndarray, shape (N_halos, 3)
        Halo positions in Mpc/h
    halo_vel : jax.numpy.ndarray, shape (N_halos, 3)
        Halo velocities in km/s
    Lbox : float
        Simulation box size in Mpc/h
    rank : int, optional
        MPI rank for logging (default: 0)
    z_obs : float, optional
        Observational redshift for galaxy physics calculations.
        If None, uses CURRENT_Z_OBS from constants (default: None)
        
    Returns
    -------
    dict
        Galaxy catalog from mc_galpop_synthetic_subs containing:
        - 'pos' : galaxy positions (N_galaxies, 3)
        - 'vel' : galaxy velocities (N_galaxies, 3) 
        - 'stellar_mass' : galaxy stellar masses (N_galaxies,)
        - Other galaxy properties from rgrspit_diffsky
        
    Notes
    -----
    - Uses fixed random key (0) for reproducible galaxy generation across all MPI ranks
    - Applies minimum halo mass threshold LGMP_MIN for galaxy population
    - Uses z_obs parameter if provided, otherwise falls back to CURRENT_Z_OBS constant
    - Galaxy catalog includes satellite galaxies via synthetic subhalo population
    """
    from dsps.cosmology import DEFAULT_COSMOLOGY
    from rgrspit_diffsky import mc_galpop
    
    # Use provided redshift or fall back to constant
    redshift = z_obs if z_obs is not None else CURRENT_Z_OBS
    
    # Generate random key (same as baseline for reproducibility)
    ran_key = jran.key(0)
    
    # Generate mock galaxy catalog for selected halos
    galcat = mc_galpop.mc_galpop_synthetic_subs(
        ran_key,
        logmhost,
        halo_radius,
        halo_pos,
        halo_vel,
        redshift,
        LGMP_MIN,
        DEFAULT_COSMOLOGY,
        Lbox,
    )
    
    print(f"Rank {rank}: generated mock with {len(galcat['pos'])} galaxies from {len(logmhost)} halos")

    # SFR-only deliverable: no emission-line step. The raw SFR (and the downstream
    # UM-corrected sfr_corr built ensemble-wide in the two-pass calibration) come
    # from logssfr_t_obs / logsm_t_obs, kept by the allowlist below.

    # Apply the lean output filter: keep only deliverable fields, freeing the
    # large history/param arrays before the write (also reduces peak memory).
    dropped = [k for k in list(galcat.keys()) if k not in KEEP_GALAXY_KEYS]
    for k in dropped:
        del galcat[k]
    if dropped:
        print(f"Rank {rank}: lean output — dropped {dropped}")

    return galcat
