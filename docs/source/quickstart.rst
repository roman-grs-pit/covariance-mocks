Quick Start on Perlmutter (NERSC)
=================================

Installation and Setup
-----------------------

First, clone the repository and set up the environment:

.. code-block:: bash

   # Clone the repository
   git clone https://github.com/roman-grs-pit/covariance-mocks.git
   cd covariance-mocks
   
   # Load environment on Perlmutter
   source scripts/load_env.sh

Basic Usage for Roman GRS PIT
------------------------------

Generate a single mock galaxy catalog for the Roman Galaxy Redshift Survey on Perlmutter:

.. code-block:: bash

   # Run single mock generation (Roman GRS PIT account)
   python scripts/generate_single_mock.py nersc /path/to/output/directory

For large-scale campaigns with thousands of jobs, use the campaign management system:

.. code-block:: bash

   # Initialize campaign configuration
   python scripts/run_campaign.py init my_campaign config/examples/production_campaign.yaml
   
   # Submit campaign jobs to SLURM
   python scripts/run_campaign.py submit my_campaign
   
   # Monitor campaign progress  
   python scripts/run_campaign.py status my_campaign

The single mock generation script will:

1. Load AbacusSummit halo catalogs
2. Apply filtering and slab decomposition for MPI
3. Generate galaxies using rgrspit_diffsky
4. Write results to HDF5 format

The campaign management system will:

1. Parse YAML configuration files
2. Create SQLite database for job tracking
3. Submit SLURM array jobs with specified parameters
4. Monitor job progress and handle failures
5. Organize output files by campaign version

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