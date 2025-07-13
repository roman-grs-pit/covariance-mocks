Installation
============

Environment Setup
-----------------

Load the conda environment and HPC modules:

.. code-block:: bash

   source scripts/load_env.sh
   echo $CONDA_ENV

This will load the conda environment with all necessary dependencies and configure the HPC modules for parallel processing.

Dependencies
------------

Core dependencies include:

* ``rgrspit_diffsky`` package for galaxy modeling (external dependency)
* MPI libraries for parallel processing  
* HDF5 with parallel support for collective I/O operations
* SLURM for job management on HPC systems
* Conda environment with scientific computing stack

The conda environment contains all necessary packages including:

* JAX for GPU acceleration and automatic differentiation
* NumPy for numerical computations
* Specialized astrophysics packages
* h5py with MPI support for parallel HDF5 operations

System Requirements
-------------------

* Linux HPC environment with SLURM job scheduler
* MPI implementation (OpenMPI or MPICH)
* Conda package manager
* Access to AbacusSummit simulation data

Verification
------------

Verify the installation by running:

.. code-block:: bash

   # Load environment
   source scripts/load_env.sh
   
   # Test MPI functionality
   python scripts/test_mpi_minimal.py
   
   # Run basic tests
   pytest