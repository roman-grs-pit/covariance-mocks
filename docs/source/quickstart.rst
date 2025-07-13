Quick Start on Perlmutter (NERSC)
=================================

Basic Usage for Roman GRS PIT
------------------------------

Generate a mock galaxy catalog for the Roman Galaxy Redshift Survey on Perlmutter:

.. code-block:: bash

   # Load environment on Perlmutter
   source scripts/load_env.sh
   
   # Run mock generation (Roman GRS PIT account)
   python scripts/generate_single_mock.py nersc /path/to/output/directory

The script will:

1. Load AbacusSummit halo catalogs
2. Apply filtering and slab decomposition for MPI
3. Generate galaxies using rgrspit_diffsky
4. Write results to HDF5 format

Testing
-------

**Quick Development Testing** (< 5 minutes):

.. code-block:: bash

   # Load environment
   source scripts/load_env.sh
   
   # Fast development tests only
   pytest -m "unit or (system and not slow)" -v

**Long Validation Testing** (background execution):

.. code-block:: bash

   # Load environment
   source scripts/load_env.sh
   
   # Run validation tests in background
   nohup pytest -m "slow or validation" -v --timeout=1800 > validation.log 2>&1 &
   
   # Monitor progress
   tail -f validation.log


Example Workflow
----------------

A typical workflow for generating mock catalogs:

.. code-block:: python

   from covariance_mocks import (
       initialize_mpi_jax, finalize_mpi,
       load_and_filter_halos, generate_galaxies,
       write_parallel_hdf5, build_abacus_path
   )
   
   # Initialize MPI/JAX
   comm, rank, size, MPI_AVAILABLE = initialize_mpi_jax()
   
   # Build catalog path
   catalog_path = build_abacus_path(
       "/data", "AbacusSummit", "small_c000", "ph3000", "z1.100"
   )
   
   # Load and filter halos (each rank gets its slab)
   logmhost, halo_radius, halo_pos, halo_vel, Lbox = load_and_filter_halos(
       catalog_path, rank, size
   )
   
   # Generate galaxies for this rank's halos
   galcat = generate_galaxies(logmhost, halo_radius, halo_pos, halo_vel, Lbox, rank)
   
   # Write output using parallel HDF5
   write_parallel_hdf5(galcat, logmhost, halo_radius, halo_pos, halo_vel,
                       "output.hdf5", rank, size, comm, Lbox)
   
   # Finalize MPI
   finalize_mpi(comm, rank, size, MPI_AVAILABLE)