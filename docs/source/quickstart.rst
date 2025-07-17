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

For large-scale productions with thousands of jobs, use the production management system:

.. code-block:: bash

   # Install CLI tool (one-time setup)
   pip install -e .
   
   # List available productions
   production-manager list
   
   # Initialize production
   production-manager init alpha
   
   # Submit production jobs to SLURM
   production-manager submit alpha
   
   # Monitor production progress  
   production-manager monitor alpha

**Configuration Workflow:**

1. **Copy template**: ``cp config/examples/covariance_template.yaml config/productions/my_production.yaml``
2. **Edit config**: Modify production name, redshifts, and parameters
3. **Run production**: ``production-manager init my_production``

**Git Tagging for Reproducibility:**

The system automatically creates git tags for every production:

- **Clean working tree**: ``production-manager init alpha`` → ``production/alpha_v1.0_20250717_143022``
- **Development mode**: ``production-manager init alpha --allow-dirty`` → ``production/alpha_v1.0_allow_dirty_20250717_143022``
- **Version control**: ``production-manager init alpha --version v2.0`` → ``production/alpha_v2.0_20250717_143022``

The single mock generation script will:

1. Load AbacusSummit halo catalogs
2. Apply filtering and slab decomposition for MPI
3. Generate galaxies using rgrspit_diffsky
4. Write results to HDF5 format

The production management system will:

1. Parse YAML configuration files from ``config/productions/``
2. Create SQLite database for job tracking
3. Submit SLURM array jobs with specified parameters
4. Monitor job progress and handle failures
5. Organize output files by production name

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