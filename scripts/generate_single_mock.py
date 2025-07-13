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


def generate_mock_for_catalog(catalog_path, output_path, n_gen=None):
    """Generate mock galaxy catalog for a single AbacusSummit halo catalog using MPI if available"""
    
    # Initialize MPI and JAX
    comm, rank, size, MPI_AVAILABLE = initialize_mpi_jax()
    
    try:
        # Load and filter halos for this rank
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = load_and_filter_halos(
            catalog_path, rank, size, n_gen
        )
        
        # Generate galaxies
        galcat = generate_galaxies(logmhost, halo_radius, halo_pos, halo_vel, Lbox, rank)
        
        # Write output using appropriate method
        if MPI_AVAILABLE and comm is not None and size > 1:
            # Parallel HDF5 writing for multiple ranks
            write_parallel_hdf5(
                galcat, logmhost, halo_radius, halo_pos, halo_vel, 
                output_path, rank, size, comm, Lbox
            )  
        else:
            # Single process - write directly to output file
            write_single_hdf5(
                galcat, logmhost, halo_radius, halo_pos, halo_vel, 
                output_path, Lbox
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

    args = parser.parse_args()
    machine = args.machine
    drnout = args.drnout
    n_gen = args.test

    if machine == "nersc":
        # Build path for current configuration
        catalog_path = build_abacus_path(
            ABACUS_BASE_PATH, "small", SIMULATION_BOX, 
            CURRENT_PHASE, CURRENT_REDSHIFT
        )
        
        # Verify path exists
        validate_catalog_path(catalog_path)
        
        # Generate output filename
        output_filename = generate_output_filename(
            SIMULATION_BOX, CURRENT_PHASE, CURRENT_REDSHIFT, n_gen
        )
        output_path = os.path.join(drnout, output_filename)
        
        # Generate galaxy catalog
        galcat = generate_mock_for_catalog(catalog_path, output_path, n_gen)
        print(f"Generated {len(galcat['pos'])} galaxies total")
        
    elif machine == "poboy":
        raise NotImplementedError("poboy machine not yet implemented")


if __name__ == "__main__":
    main()
