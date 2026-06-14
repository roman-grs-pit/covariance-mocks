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

# --- Startup-stall mitigations (concurrent Python launch off CFS) ---
# Diagnosed failure mode: under many concurrent job starts, the Python/JAX
# import phase stalled on the shared filesystem and burned the whole walltime
# before generating anything. These reduce that contention and make any
# remaining stall observable.

# Unbuffered stdout/stderr: the last progress line before a stall is always
# flushed, so logs pinpoint where a job hung instead of going dark.
export PYTHONUNBUFFERED=1

# Keep the bytecode cache off CFS. Pointing __pycache__ writes at node-local
# storage avoids a metadata write-storm when thousands of ranks import the same
# env at once. Falls back to /tmp if the SLURM tmp dir is unset.
export PYTHONPYCACHEPREFIX="${SLURM_TMPDIR:-/tmp}/covmocks_pycache"

# Spread the import load: under a batch job, sleep a bounded, per-job-random
# interval before launching so a dispatched wave of jobs does not hit the env
# filesystem in lockstep. No-op outside SLURM (interactive use is unaffected).
if [ -n "$SLURM_JOB_ID" ]; then
    _stagger=$(( (SLURM_JOB_ID + ${SLURM_ARRAY_TASK_ID:-0}) % 30 ))
    sleep "$_stagger"
fi

