"""
Utilities Module

Common utility functions for the covariance mocks pipeline.
"""

import os

# AbacusSummit data configuration
ABACUS_BASE_PATH = "/global/cfs/cdirs/desi/public/cosmosim/AbacusSummit"
SIMULATION_SUITE = "small"


def validate_catalog_path(catalog_path):
    """
    Verify that the catalog path exists and is accessible.
    
    Parameters
    ----------
    catalog_path : str
        Path to AbacusSummit halo catalog directory
        
    Returns
    -------
    bool
        True if path exists and is accessible
        
    Raises
    ------
    FileNotFoundError
        If catalog path does not exist or is not a directory
    """
    if not os.path.isdir(catalog_path):
        raise FileNotFoundError(f"AbacusSummit catalog not found: {catalog_path}")
    return True


def generate_output_filename(simulation_box, phase, redshift, n_gen=None):
    """
    Generate standardized output filename for mock catalogs.
    
    Parameters
    ----------
    simulation_box : str
        Simulation box identifier (e.g., "AbacusSummit_small_c000")
    phase : str
        Phase identifier (e.g., "ph3000")  
    redshift : str
        Redshift string (e.g., "z1.100")
    n_gen : int, optional
        Number of halos for test mode (adds test suffix)
        
    Returns
    -------
    str
        Standardized HDF5 filename following naming convention
        
    Examples
    --------
    >>> generate_output_filename("AbacusSummit_small_c000", "ph3000", "z1.100")
    'mock_AbacusSummit_small_c000_ph3000_z1.100.hdf5'
    
    >>> generate_output_filename("AbacusSummit_small_c000", "ph3000", "z1.100", n_gen=5000)
    'mock_AbacusSummit_small_c000_ph3000_z1.100_test5000.hdf5'
    """
    if n_gen is not None:
        return f"mock_{simulation_box}_{phase}_{redshift}_test{n_gen}.hdf5"
    else:
        return f"mock_{simulation_box}_{phase}_{redshift}.hdf5"
