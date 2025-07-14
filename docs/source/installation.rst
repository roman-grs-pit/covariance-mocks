Installation
============

Clone Repository
----------------

First, clone the repository on Perlmutter:

.. code-block:: bash

   # Clone the repository
   git clone https://github.com/roman-grs-pit/covariance-mocks.git
   cd covariance-mocks

System Requirements
-------------------

This pipeline is designed specifically for the **Roman Galaxy Redshift Survey (GRS) Project Infrastructure Team (PIT)** and requires:

* **NERSC Perlmutter system** with GPU nodes
* **Roman GRS PIT account access** (m4943)
* Access to Roman GRS PIT data and computational resources

Environment Setup at NERSC
---------------------------

Load the conda environment and HPC modules on Perlmutter:

.. code-block:: bash

   source scripts/load_env.sh
   echo $CONDA_ENV

This will load the conda environment with all necessary dependencies and configure the HPC modules for parallel processing on the Perlmutter system.

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

Technical Requirements
---------------------

* **NERSC Perlmutter system** with SLURM job scheduler
* MPI implementation (OpenMPI or MPICH)
* Conda package manager (provided by NERSC)
* Access to AbacusSummit simulation data through Roman GRS PIT allocation

Verification
------------

Verify the installation with quick tests:

.. code-block:: bash

   # Load environment (provides pytest and all dependencies)
   source scripts/load_env.sh
   
   # Run fast verification tests (< 5 minutes)
   pytest -m "unit or (system and not slow)" -v
   
   # Test shell script functionality
   ./scripts/make_mocks.sh --test

For comprehensive validation (optional, run in background):

.. code-block:: bash

   # Run full validation tests in background (30+ minutes)
   nohup pytest -m "slow or validation" -v --timeout=1800 > validation.log 2>&1 &