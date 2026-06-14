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
import time

# Phase-timing instrumentation. Prints are flushed so the last line before any
# stall is always visible (a block-buffered stream would otherwise hide where a
# job hung). _T0 is captured before the heavy imports so the import phase itself
# is measured — that is the suspected startup-stall window under concurrent load.
_T0 = time.time()
_PROCID = os.environ.get("SLURM_PROCID", "?")


def _stamp(phase):
    print(f"[TIMING rank={_PROCID} +{time.time() - _T0:8.1f}s] {phase}", flush=True)


_stamp("script-start (pre-import)")

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

_stamp("imports-done")


def generate_mock_for_catalog(catalog_path, output_path, n_gen=None, z_obs=None):
    """Generate mock galaxy catalog for a single AbacusSummit halo catalog using MPI if available"""
    
    # Initialize MPI and JAX
    comm, rank, size, MPI_AVAILABLE = initialize_mpi_jax()
    _stamp("mpi-jax-init-done")

    try:
        # Load and filter halos for this rank
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = load_and_filter_halos(
            catalog_path, rank, size, n_gen
        )
        _stamp("halos-loaded")

        # Generate galaxies
        galcat = generate_galaxies(logmhost, halo_radius, halo_pos, halo_vel, Lbox, rank, z_obs)
        _stamp("galaxies-generated (compute-done)")

        # Write output using appropriate method
        _stamp("write-start")
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
        _stamp("file-closed")

        print(f"Galaxy catalog saved to: {output_path}")

        return galcat

    finally:
        # Clean MPI finalization
        _stamp("finalize-start")
        finalize_mpi(comm, rank, size, MPI_AVAILABLE)
        _stamp("finalize-done")


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
        _stamp("work-done (hard exit)")

        # The science, write, and MPI finalize are all complete here. Exit
        # immediately rather than fall through to interpreter shutdown: under
        # concurrent load the implicit atexit MPI teardown could stall for
        # minutes and get the job wall-killed seconds after a complete write
        # (the observed TIMEOUT-at-the-finish-line mode). Flush first so no
        # output is lost, then bypass atexit/GC entirely.
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)

    elif machine == "poboy":
        raise NotImplementedError("poboy machine not yet implemented")


if __name__ == "__main__":
    main()
