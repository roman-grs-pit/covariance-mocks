User Guide
==========

Pipeline Overview
-----------------

The **Roman Galaxy Redshift Survey (GRS)** covariance mocks pipeline generates mock galaxy catalogs from AbacusSummit N-body simulations as part of the **Roman GRS Project Infrastructure Team (PIT)** analysis framework. The pipeline consists of modular components that handle different aspects of the mock generation process and runs on the **Perlmutter system at NERSC**.

Architecture
~~~~~~~~~~~~

The modular pipeline consists of 5 core modules:

* **data_loader** - Halo catalog loading and filtering with MPI slab decomposition
* **galaxy_generator** - Galaxy population modeling using rgrspit_diffsky  
* **hdf5_writer** - Parallel HDF5 output operations for data storage
* **mpi_setup** - MPI and JAX initialization for distributed computing
* **utils** - Common utility functions for path validation and filename generation

The campaign management system adds:

* **campaign_config** - YAML configuration validation and hierarchical inheritance
* **campaign_manager** - SQLite job tracking and SLURM array orchestration

Configuration
-------------

Default Configuration Constants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The pipeline uses the following default configuration constants defined in ``covariance_mocks.__init__``:

* ``CURRENT_PHASE = "ph3000"`` - Default phase identifier
* ``CURRENT_REDSHIFT = "z1.100"`` - Default redshift string  
* ``CURRENT_Z_OBS = 1.1`` - Observational redshift value
* ``LGMP_MIN = 10.0`` - Minimum log10 halo mass threshold
* ``SIMULATION_BOX = "AbacusSummit_small_c000"`` - Default simulation box

These can be overridden by modifying the constants or passing different values to the pipeline functions.

Data Processing
---------------

Halo Catalog Loading
~~~~~~~~~~~~~~~~~~~~

The pipeline loads AbacusSummit halo catalogs and applies several processing steps:

1. **Path Construction**: Builds full paths to halo catalog directories
2. **Mass Filtering**: Applies minimum mass threshold (default: log10(M) >= 10.0)
3. **Coordinate Transformation**: Converts positions from [-Lbox/2, Lbox/2] to [0, Lbox]
4. **Slab Decomposition**: Distributes halos across MPI ranks by y-coordinate
5. **Test Mode Support**: Optional limitation to N halos for testing

Galaxy Generation
~~~~~~~~~~~~~~~~~

Galaxy population uses the rgrspit_diffsky package:

1. **Reproducible Random Seeds**: Fixed random key (0) ensures consistent results
2. **Halo Population**: Populates halos with central and satellite galaxies
3. **Stellar Mass Assignment**: Assigns stellar masses based on halo properties
4. **Subhalo Modeling**: Includes synthetic subhalo population for satellites

Output Management
~~~~~~~~~~~~~~~~~

The pipeline supports both single-process and parallel HDF5 output:

**Single Process**:
- Direct HDF5 file creation
- All data written by single rank

**Parallel MPI**:
- Collective I/O operations
- Coordinated writes across all ranks
- Efficient handling of large datasets

HPC Integration
---------------

Single Mock SLURM Job Submission
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The pipeline runs on HPC environments with SLURM job scheduling. Example job script for single mock generation:

.. code-block:: bash

   #!/bin/bash
   #SBATCH --job-name=covariance_mocks
   #SBATCH --nodes=4
   #SBATCH --ntasks-per-node=32
   #SBATCH --time=02:00:00
   #SBATCH --output=mock_generation_%j.out
   
   source scripts/load_env.sh
   
   srun python scripts/generate_single_mock.py nersc /output/path

Campaign SLURM Array Jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~

For large-scale campaigns, the system uses SLURM array jobs:

.. code-block:: bash

   #!/bin/bash
   #SBATCH --job-name=campaign_mock_gen
   #SBATCH --array=1-500
   #SBATCH --nodes=1
   #SBATCH --ntasks-per-node=32
   #SBATCH --time=02:00:00
   #SBATCH --output=campaign_logs/job_%A_%a.out
   
   source scripts/load_env.sh
   
   # Campaign manager handles job parameters based on array index
   python scripts/run_campaign.py execute my_campaign $SLURM_ARRAY_TASK_ID

MPI Scaling
~~~~~~~~~~~

The pipeline scales across multiple nodes:

* **Slab Decomposition**: Halos distributed by spatial coordinates
* **Independent Processing**: Each rank processes its assigned halos
* **Collective Output**: Coordinated parallel HDF5 writes
* **Memory Usage**: Only loads data needed for each rank's slab

Troubleshooting
---------------

Common Issues
~~~~~~~~~~~~~

**Environment Problems**:
- Ensure ``source scripts/load_env.sh`` is run before execution
- Verify ``CONDA_ENV`` environment variable is set
- Check that all required modules are loaded

**MPI Issues**:
- Verify MPI implementation is available (OpenMPI/MPICH)
- Check that h5py is compiled with parallel HDF5 support
- Ensure consistent JAX configuration across ranks

**Memory Issues**:
- Large halo catalogs may require more memory per rank
- Consider reducing the number of MPI ranks per node
- Use test mode (``n_gen`` parameter) for smaller datasets

**File I/O Problems**:
- Verify write permissions to output directory
- Check available disk space
- Ensure parallel file system supports concurrent writes

Performance Optimization
~~~~~~~~~~~~~~~~~~~~~~~~

**MPI Configuration**:
- Use appropriate number of ranks per node based on memory requirements
- Consider NUMA topology for optimal performance
- Test different slab decomposition strategies

**JAX Optimization**:
- Enable GPU acceleration when available
- Configure JAX memory allocation settings
- Use appropriate precision settings (float32 vs float64)

**I/O Configuration**:
- Use parallel file systems (Lustre, GPFS)
- Configure HDF5 chunking and compression
- Consider collective I/O vs independent writes