#!/bin/bash
# Environment loading script for covariance mocks

# Load HDF5 module for MPI support
module load cray-hdf5-parallel

# Load Python module
module load python

# Activate conda environment
mamba activate /global/cfs/cdirs/m4943/Simulations/covariance_mocks/conda_envs/grspit

# Set CONDA_ENV variable
export CONDA_ENV="/global/cfs/cdirs/m4943/Simulations/covariance_mocks/conda_envs/grspit"

# JAX memory optimization
export XLA_PYTHON_CLIENT_PREALLOCATE=false
export XLA_PYTHON_CLIENT_ALLOCATOR=platform

