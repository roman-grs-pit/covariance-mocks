#!/usr/bin/env python3
"""
Generate Single Mock Catalog

Modular pipeline for generating galaxy catalogs with MPI parallelization.

Usage:
    python generate_single_mock.py nersc /path/to/output/directory [--test N]
"""

import sys
import argparse
import os

# Add src to path for module imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from covariance_mocks import (
    initialize_mpi_jax, finalize_mpi,
    load_and_filter_halos, build_abacus_path,
    generate_galaxies,
    write_parallel_hdf5, write_single_hdf5,
    validate_catalog_path, generate_output_filename,
    CURRENT_PHASE, CURRENT_REDSHIFT, SIMULATION_BOX
)
from covariance_mocks.utils import ABACUS_BASE_PATH


def generate_mock_for_catalog(catalog_path, output_path, n_gen=None, z_obs=None):
    """Generate mock galaxy catalog for a single AbacusSummit halo catalog using MPI if available"""
    
    # Initialize MPI and JAX
    comm, rank, size, MPI_AVAILABLE = initialize_mpi_jax()
    
    try:
        # Load and filter halos for this rank
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = load_and_filter_halos(
            catalog_path, rank, size, n_gen
        )
        
        # Generate galaxies
        galcat = generate_galaxies(logmhost, halo_radius, halo_pos, halo_vel, Lbox, rank, z_obs)
        
        # Write output using appropriate method
        if MPI_AVAILABLE and comm is not None and size > 1:
            # Parallel HDF5 writing for multiple ranks
            write_parallel_hdf5(
                galcat, logmhost, halo_radius, halo_pos, halo_vel, 
                output_path, rank, size, comm, Lbox, z_obs
            )  
        else:
            # Single process - write directly to output file
            write_single_hdf5(
                galcat, logmhost, halo_radius, halo_pos, halo_vel, 
                output_path, Lbox, z_obs
            )
        
        print(f"Galaxy catalog saved to: {output_path}")
        
        return galcat
        
    finally:
        # Clean MPI finalization
        finalize_mpi(comm, rank, size, MPI_AVAILABLE)


def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(
        description="Generate mock galaxy catalog from AbacusSummit halo data"
    )

    parser.add_argument(
        "machine", 
        help="Machine name where script is run", 
        choices=["nersc", "poboy"]
    )

    parser.add_argument(
        "drnout", 
        help="Output directory"
    )
    
    parser.add_argument(
        "--test", 
        type=int, 
        help="Test mode: use only N halos with smallest x coordinates"
    )
    
    parser.add_argument(
        "--realization",
        type=str,
        help="Realization number (e.g., '3000')"
    )
    
    parser.add_argument(
        "--redshift",
        type=str,
        help="Redshift value (e.g., '0.1')"
    )

    args = parser.parse_args()
    machine = args.machine
    drnout = args.drnout
    n_gen = args.test
    realization = args.realization
    redshift = args.redshift

    if machine == "nersc":
        # Build path for current configuration using dynamic parameters
        phase = f"ph{realization}" if realization else CURRENT_PHASE
        z_str = f"z{float(redshift):.3f}" if redshift else CURRENT_REDSHIFT
        # Build path directly since AbacusSummit uses specific directory structure
        catalog_path = os.path.join(
            ABACUS_BASE_PATH, f"AbacusSummit_small_c000_{phase}", "halos", z_str
        )
        
        # Verify path exists
        validate_catalog_path(catalog_path)
        
        # Generate output filename using dynamic parameters
        output_filename = generate_output_filename(
            SIMULATION_BOX, phase, z_str, n_gen
        )
        output_path = os.path.join(drnout, output_filename)
        
        # Generate galaxy catalog
        z_obs_float = float(redshift) if redshift else None
        galcat = generate_mock_for_catalog(catalog_path, output_path, n_gen, z_obs_float)
        print(f"Generated {len(galcat['pos'])} galaxies total")
        
    elif machine == "poboy":
        raise NotImplementedError("poboy machine not yet implemented")


if __name__ == "__main__":
    main()
