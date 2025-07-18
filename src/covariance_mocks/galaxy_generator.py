"""
Galaxy Generator Module

Handles galaxy generation coordination with the rgrspit_diffsky package.
Manages batch processing and random key management for galaxy population.

Coordinates galaxy population using rgrspit_diffsky with batch processing.
"""

from jax import random as jran
from . import CURRENT_Z_OBS, LGMP_MIN
from .emission_lines import add_emission_lines


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
    
    # Add emission line luminosities
    galcat = add_emission_lines(galcat)
    print(f"Rank {rank}: added emission line luminosities (OII and H-alpha)")
    
    return galcat
