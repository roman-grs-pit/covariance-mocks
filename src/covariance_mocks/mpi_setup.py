"""
MPI Setup Module

Handles MPI and JAX initialization patterns for distributed computing.
This module standardizes the environment-dependent device configuration
and manages single vs multi-process modes.

Standardizes environment-dependent device configuration and process management.
"""

import os


def initialize_mpi_jax():
    """
    Initialize MPI and JAX with proper device configuration.
    
    Sets up MPI communication and configures JAX for distributed computing
    with environment-dependent device configuration.
    
    Returns
    -------
    tuple
        (comm, rank, size, MPI_AVAILABLE) where:
        - comm : MPI.Comm or None
            MPI communicator for inter-process communication (None if MPI unavailable)
        - rank : int
            Process rank (0-based, 0 for single process)
        - size : int  
            Total number of MPI processes (1 for single process)
        - MPI_AVAILABLE : bool
            Whether MPI is available and initialized
            
    Notes
    -----
    - Attempts to import and initialize mpi4py for parallel execution
    - Falls back to single-process mode if MPI unavailable
    - Configures JAX environment variables for distributed use
    - Initializes JAX distributed backend for multi-process execution
    - Reports JAX backend and available devices for each rank
    - Handles GPU device configuration automatically
    """
    # MPI setup
    try:
        from mpi4py import MPI
        MPI_AVAILABLE = True
    except ImportError:
        MPI_AVAILABLE = False

    # Initialize MPI if available
    if MPI_AVAILABLE:
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()
        
        if rank == 0:
            print(f"Starting MPI job with {size} processes")

    else:
        rank = 0
        size = 1
        comm = None
        print("Running in single-process mode")

    # Configure JAX for distributed use if needed
    if MPI_AVAILABLE and size > 1:
        # Configure JAX for distributed use
        os.environ['JAX_PLATFORMS'] = ''
        os.environ['JAX_DISTRIBUTED_INITIALIZE'] = 'false'
    
    # Import JAX after MPI setup
    import jax
    from jax import random as jran
    
    if MPI_AVAILABLE and size > 1:
        jax.distributed.initialize()
    
    # Report JAX configuration
    print(f"Rank {rank}: JAX backend: {jax.default_backend()}")
    print(f"Rank {rank}: JAX devices: {jax.devices()}")
    
    return comm, rank, size, MPI_AVAILABLE


def finalize_mpi(comm, rank, size, MPI_AVAILABLE):
    """
    Properly finalize MPI communication.
    
    Ensures clean shutdown of MPI processes with proper synchronization.
    
    Parameters
    ----------
    comm : MPI.Comm or None
        MPI communicator from initialize_mpi_jax()
    rank : int
        Process rank
    size : int
        Total number of MPI processes  
    MPI_AVAILABLE : bool
        Whether MPI was successfully initialized
        
    Notes
    -----
    - Only performs finalization if MPI is available and multi-process
    - Uses MPI barrier to synchronize all ranks before finalization
    - Provides per-rank logging for debugging distributed shutdown
    - Safe to call even if MPI not available (no-op)
    """
    # Explicit MPI cleanup to ensure clean exit
    if MPI_AVAILABLE and comm is not None and size > 1:
        print(f"Rank {rank}: Starting MPI finalization")
        comm.Barrier()  # Ensure all ranks finish
        print(f"Rank {rank}: MPI finalization complete")
