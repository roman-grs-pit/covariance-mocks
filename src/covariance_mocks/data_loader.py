"""
Data Loader Module

Handles loading and filtering of halo catalogs from AbacusSummit.
Implements slab decomposition and mass filtering for distributed processing.

Slab decomposition and mass filtering for distributed halo processing.
"""

import os
import numpy as np
import jax.numpy as jnp
from . import LGMP_MIN


def build_abacus_path(base_path, suite, box, phase, redshift):
    """
    Build full path to AbacusSummit halo catalog directory.
    
    Parameters
    ----------
    base_path : str
        Root directory containing AbacusSummit simulations
    suite : str  
        Simulation suite name (e.g., "AbacusSummit")
    box : str
        Box identifier (e.g., "small_c000")
    phase : str
        Phase identifier (e.g., "ph3000")
    redshift : str
        Redshift string (e.g., "z1.100")
        
    Returns
    -------
    str
        Full path to halo catalog directory
        
    Example
    -------
    >>> path = build_abacus_path("/data", "AbacusSummit", "small_c000", "ph3000", "z1.100")
    >>> print(path)
    /data/AbacusSummit/small_c000_ph3000/halos/z1.100
    """
    return os.path.join(base_path, suite, f"{box}_{phase}", "halos", redshift)


def load_and_filter_halos(catalog_path, rank=0, size=1, n_gen=None):
    """
    Load halo catalog and apply filtering for this MPI rank.
    
    Loads AbacusSummit halo catalog, applies mass filtering, optional test mode 
    limitation, and performs slab decomposition for distributed processing.
    
    Parameters
    ----------
    catalog_path : str
        Path to the AbacusSummit halo catalog directory
    rank : int, optional
        MPI rank for slab decomposition (default: 0)
    size : int, optional  
        Total number of MPI processes (default: 1)
    n_gen : int, optional
        Test mode - select only N halos with smallest x-coordinates
        
    Returns
    -------
    tuple
        (logmhost, halo_radius, halo_pos, halo_vel, Lbox) where:
        - logmhost : jax.numpy.ndarray, shape (N_halos,)
            Log10 halo masses for this rank's slab
        - halo_radius : jax.numpy.ndarray, shape (N_halos,)  
            Halo virial radii in Mpc/h
        - halo_pos : jax.numpy.ndarray, shape (N_halos, 3)
            Halo positions in [0, Lbox] coordinates (Mpc/h)
        - halo_vel : jax.numpy.ndarray, shape (N_halos, 3)
            Halo velocities in km/s
        - Lbox : float
            Simulation box size in Mpc/h
            
    Raises
    ------
    ValueError
        If no halos are found above the minimum mass threshold
        
    Notes
    -----
    - Applies minimum mass filter: log10(M) >= LGMP_MIN (default 10.0)
    - Converts halo positions from [-Lbox/2, Lbox/2] to [0, Lbox] 
    - In test mode, selects N halos with smallest x-coordinates before slab decomposition
    - Slab decomposition splits halos by y-coordinate: rank gets [rank*Lbox/size, (rank+1)*Lbox/size)
    - Returns JAX arrays with float32 dtype for GPU compatibility
    """
    from rgrspit_diffsky.data_loaders import load_abacus
    
    # Load halo catalog (all ranks load the same data initially)
    halo_catalog = load_abacus.load_abacus_halo_catalog(catalog_path)

    # Extract required variables from catalog
    mass = halo_catalog['mass']
    
    # Filter out zero masses and apply minimum mass cut
    min_mass = 10**LGMP_MIN
    valid_mask = (mass > 0) & (mass >= min_mass)
    
    if np.sum(valid_mask) == 0:
        raise ValueError(f"No halos above minimum mass {min_mass:.2e}")
    
    logmhost = np.log10(mass[valid_mask])
    halo_radius = halo_catalog['radius'][valid_mask]
    halo_pos = halo_catalog['pos'][valid_mask]
    halo_vel = halo_catalog['vel'][valid_mask]
    Lbox = halo_catalog['lbox']
    
    # Convert halo positions from [-Lbox/2, Lbox/2] to [0, Lbox]
    halo_pos = halo_pos + Lbox/2
    
    if rank == 0:
        print(f"Loaded {len(logmhost)} halos above mass threshold from {len(mass)} total halos")
    
    # Apply test mode limitation BEFORE slab decomposition (original logic)
    # Select the n_gen halos with smallest x-coordinates from the full catalog
    if n_gen is not None:
        if rank == 0:
            print(f"Test mode: selecting {n_gen} halos with smallest x-coordinates")
        
        # Sort by x-coordinate and take the first n_gen halos
        x_coords = halo_pos[:, 0]
        sorted_indices = np.argsort(x_coords)
        test_indices = sorted_indices[:n_gen]
        
        # Apply test mode limitation to all arrays
        logmhost = logmhost[test_indices]
        halo_radius = halo_radius[test_indices]
        halo_pos = halo_pos[test_indices]
        halo_vel = halo_vel[test_indices]
        
        if rank == 0:
            print(f"Test mode: reduced to {len(logmhost)} halos, x-range [{halo_pos[:, 0].min():.1f}, {halo_pos[:, 0].max():.1f}]")
    
    # Implement slab decomposition based on y-coordinate
    # Each MPI rank gets a slab: [rank * Lbox/size, (rank+1) * Lbox/size)
    y_min = rank * Lbox / size
    y_max = (rank + 1) * Lbox / size
    
    # Select halos in this rank's slab
    slab_mask = (halo_pos[:, 1] >= y_min) & (halo_pos[:, 1] < y_max)
    
    # Handle the last rank to include the boundary
    if rank == size - 1:
        slab_mask = (halo_pos[:, 1] >= y_min) & (halo_pos[:, 1] <= y_max)

    # Extract halos for this rank
    rank_logmhost = logmhost[slab_mask]
    rank_halo_radius = halo_radius[slab_mask]
    rank_halo_pos = halo_pos[slab_mask]
    rank_halo_vel = halo_vel[slab_mask]

    print(f"Rank {rank}: processing {len(rank_logmhost)} halos in y-slab [{y_min:.1f}, {y_max:.1f}]")
    print(f"Rank {rank}: using {len(rank_logmhost)} halos for galaxy generation")
    
    # Convert to JAX arrays with proper dtypes
    rank_logmhost = jnp.asarray(rank_logmhost, dtype=jnp.float32)
    rank_halo_radius = jnp.asarray(rank_halo_radius, dtype=jnp.float32)
    rank_halo_pos = jnp.asarray(rank_halo_pos, dtype=jnp.float32)
    rank_halo_vel = jnp.asarray(rank_halo_vel, dtype=jnp.float32)
    Lbox = float(Lbox)
    
    return rank_logmhost, rank_halo_radius, rank_halo_pos, rank_halo_vel, Lbox
