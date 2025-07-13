# Covariance Mocks API Documentation

This document provides comprehensive API documentation for the covariance mocks modular pipeline.

## Overview

The modular pipeline consists of 5 core modules that handle different aspects of mock galaxy catalog generation:

- **`data_loader`** - Halo catalog loading and filtering
- **`galaxy_generator`** - Galaxy population modeling  
- **`hdf5_writer`** - Parallel HDF5 output operations
- **`mpi_setup`** - MPI and JAX initialization
- **`utils`** - Common utility functions

## Module Reference

### `covariance_mocks.data_loader`

Functions for loading and filtering AbacusSummit halo catalogs with MPI slab decomposition.

#### `build_abacus_path(base_path, suite, box, phase, redshift)`

Build full path to AbacusSummit halo catalog directory.

**Parameters:**
- `base_path` (str): Root directory containing AbacusSummit simulations
- `suite` (str): Simulation suite name (e.g., "AbacusSummit")
- `box` (str): Box identifier (e.g., "small_c000")
- `phase` (str): Phase identifier (e.g., "ph3000")
- `redshift` (str): Redshift string (e.g., "z1.100")

**Returns:**
- `str`: Full path to halo catalog directory

**Example:**
```python
path = build_abacus_path("/data", "AbacusSummit", "small_c000", "ph3000", "z1.100")
# Returns: /data/AbacusSummit/small_c000_ph3000/halos/z1.100
```

#### `load_and_filter_halos(catalog_path, rank=0, size=1, n_gen=None)`

Load halo catalog and apply filtering for this MPI rank.

Loads AbacusSummit halo catalog, applies mass filtering, optional test mode limitation, and performs slab decomposition for distributed processing.

**Parameters:**
- `catalog_path` (str): Path to the AbacusSummit halo catalog directory
- `rank` (int, optional): MPI rank for slab decomposition (default: 0)
- `size` (int, optional): Total number of MPI processes (default: 1)
- `n_gen` (int, optional): Test mode - select only N halos with smallest x-coordinates

**Returns:**
- `tuple`: (logmhost, halo_radius, halo_pos, halo_vel, Lbox) where:
  - `logmhost`: Log10 halo masses for this rank's slab
  - `halo_radius`: Halo virial radii in Mpc/h
  - `halo_pos`: Halo positions in [0, Lbox] coordinates (Mpc/h)
  - `halo_vel`: Halo velocities in km/s
  - `Lbox`: Simulation box size in Mpc/h

**Raises:**
- `ValueError`: If no halos are found above the minimum mass threshold

**Notes:**
- Applies minimum mass filter: log10(M) >= LGMP_MIN (default 10.0)
- Converts halo positions from [-Lbox/2, Lbox/2] to [0, Lbox]
- In test mode, selects N halos with smallest x-coordinates before slab decomposition
- Slab decomposition splits halos by y-coordinate: rank gets [rank*Lbox/size, (rank+1)*Lbox/size)
- Returns JAX arrays with float32 dtype for GPU compatibility

### `covariance_mocks.galaxy_generator`

Functions for populating halos with galaxies using the rgrspit_diffsky package.

#### `generate_galaxies(logmhost, halo_radius, halo_pos, halo_vel, Lbox, rank=0)`

Generate galaxies for given halos using rgrspit_diffsky.

Populates halos with galaxies using the rgrspit_diffsky package with consistent random key generation for reproducible results across MPI ranks.

**Parameters:**
- `logmhost` (jax.numpy.ndarray): Log10 host halo masses, shape (N_halos,)
- `halo_radius` (jax.numpy.ndarray): Halo virial radii in Mpc/h, shape (N_halos,)
- `halo_pos` (jax.numpy.ndarray): Halo positions in Mpc/h, shape (N_halos, 3)
- `halo_vel` (jax.numpy.ndarray): Halo velocities in km/s, shape (N_halos, 3)
- `Lbox` (float): Simulation box size in Mpc/h
- `rank` (int, optional): MPI rank for logging (default: 0)

**Returns:**
- `dict`: Galaxy catalog from mc_galpop_synthetic_subs containing:
  - `'pos'`: galaxy positions (N_galaxies, 3)
  - `'vel'`: galaxy velocities (N_galaxies, 3)
  - `'stellar_mass'`: galaxy stellar masses (N_galaxies,)
  - Other galaxy properties from rgrspit_diffsky

**Notes:**
- Uses fixed random key (0) for reproducible galaxy generation across all MPI ranks
- Applies minimum halo mass threshold LGMP_MIN for galaxy population
- Uses current observational redshift CURRENT_Z_OBS
- Galaxy catalog includes satellite galaxies via synthetic subhalo population

### `covariance_mocks.hdf5_writer`

Functions for writing galaxy catalogs to HDF5 files with single-process and parallel MPI support.

#### `write_single_hdf5(galcat, plot_logmhost, plot_halo_radius, plot_halo_pos, plot_halo_vel, output_path, Lbox)`

Write galaxy catalog to HDF5 file for single process.

**Parameters:**
- `galcat` (dict): Galaxy catalog from rgrspit_diffsky containing galaxy properties
- `plot_logmhost` (array_like): Log10 halo masses used for galaxy generation
- `plot_halo_radius` (array_like): Halo virial radii in Mpc/h
- `plot_halo_pos` (array_like): Halo positions in Mpc/h, shape (N_halos, 3)
- `plot_halo_vel` (array_like): Halo velocities in km/s, shape (N_halos, 3)
- `output_path` (str): Full path for output HDF5 file
- `Lbox` (float): Simulation box size in Mpc/h

**Notes:**
- Creates directory structure if it doesn't exist
- Saves galaxy properties under 'galaxies/' group
- Saves halo properties under 'halos/' group
- Includes metadata attributes: Lbox, z_obs, lgmp_min, n_halos, n_galaxies
- Handles structured data by saving components separately

#### `write_parallel_hdf5(galcat, plot_logmhost, plot_halo_radius, plot_halo_pos, plot_halo_vel, output_path, rank, size, comm, Lbox)`

Write galaxy catalog using parallel HDF5 for multiple MPI ranks.

Coordinates collective I/O operations across MPI ranks to write a single HDF5 file containing galaxies and halos from all processes.

**Parameters:**
- `galcat` (dict): Galaxy catalog from rgrspit_diffsky for this rank
- `plot_logmhost` (array_like): Log10 halo masses for this rank
- `plot_halo_radius` (array_like): Halo virial radii for this rank in Mpc/h
- `plot_halo_pos` (array_like): Halo positions for this rank in Mpc/h, shape (N_halos, 3)
- `plot_halo_vel` (array_like): Halo velocities for this rank in km/s, shape (N_halos, 3)
- `output_path` (str): Full path for output HDF5 file
- `rank` (int): MPI rank of this process
- `size` (int): Total number of MPI processes
- `comm` (MPI.Comm): MPI communicator for collective operations
- `Lbox` (float): Simulation box size in Mpc/h

**Notes:**
- Uses MPI collective operations to coordinate writes
- Gathers counts and calculates offsets for contiguous data layout
- All ranks write to same file using parallel HDF5
- Rank 0 writes metadata and creates file structure
- Includes temporary rank files cleanup after successful write
- Handles galaxy and halo data with proper offset calculations

### `covariance_mocks.mpi_setup`

Functions for initializing and finalizing MPI and JAX for distributed computing.

#### `initialize_mpi_jax()`

Initialize MPI and JAX with proper device configuration.

Sets up MPI communication and configures JAX for distributed computing with environment-dependent device configuration.

**Returns:**
- `tuple`: (comm, rank, size, MPI_AVAILABLE) where:
  - `comm`: MPI communicator for inter-process communication (None if MPI unavailable)
  - `rank`: Process rank (0-based, 0 for single process)
  - `size`: Total number of MPI processes (1 for single process)
  - `MPI_AVAILABLE`: Whether MPI is available and initialized

**Notes:**
- Attempts to import and initialize mpi4py for parallel execution
- Falls back to single-process mode if MPI unavailable
- Configures JAX environment variables for distributed use
- Initializes JAX distributed backend for multi-process execution
- Reports JAX backend and available devices for each rank
- Handles GPU device configuration automatically

#### `finalize_mpi(comm, rank, size, MPI_AVAILABLE)`

Properly finalize MPI communication.

Ensures clean shutdown of MPI processes with proper synchronization.

**Parameters:**
- `comm` (MPI.Comm or None): MPI communicator from initialize_mpi_jax()
- `rank` (int): Process rank
- `size` (int): Total number of MPI processes
- `MPI_AVAILABLE` (bool): Whether MPI was successfully initialized

**Notes:**
- Only performs finalization if MPI is available and multi-process
- Uses MPI barrier to synchronize all ranks before finalization
- Provides per-rank logging for debugging distributed shutdown
- Safe to call even if MPI not available (no-op)

### `covariance_mocks.utils`

Common utility functions for path validation and filename generation.

#### `validate_catalog_path(catalog_path)`

Verify that the catalog path exists and is accessible.

**Parameters:**
- `catalog_path` (str): Path to AbacusSummit halo catalog directory

**Returns:**
- `bool`: True if path exists and is accessible

**Raises:**
- `FileNotFoundError`: If catalog path does not exist or is not a directory

#### `generate_output_filename(simulation_box, phase, redshift, n_gen=None)`

Generate standardized output filename for mock catalogs.

**Parameters:**
- `simulation_box` (str): Simulation box identifier (e.g., "AbacusSummit_small_c000")
- `phase` (str): Phase identifier (e.g., "ph3000")
- `redshift` (str): Redshift string (e.g., "z1.100")
- `n_gen` (int, optional): Number of halos for test mode (adds test suffix)

**Returns:**
- `str`: Standardized HDF5 filename following naming convention

**Examples:**
```python
generate_output_filename("AbacusSummit_small_c000", "ph3000", "z1.100")
# Returns: 'mock_AbacusSummit_small_c000_ph3000_z1.100.hdf5'

generate_output_filename("AbacusSummit_small_c000", "ph3000", "z1.100", n_gen=5000)
# Returns: 'mock_AbacusSummit_small_c000_ph3000_z1.100_test5000.hdf5'
```

## Configuration Constants

The following constants are defined in `covariance_mocks.__init__`:

- `CURRENT_PHASE = "ph3000"`: Default phase identifier
- `CURRENT_REDSHIFT = "z1.100"`: Default redshift string
- `CURRENT_Z_OBS = 1.1`: Observational redshift value
- `LGMP_MIN = 10.0`: Minimum log10 halo mass threshold
- `SIMULATION_BOX = "AbacusSummit_small_c000"`: Default simulation box

## Usage Examples

### Basic Single-Process Pipeline

```python
from covariance_mocks import (
    initialize_mpi_jax, finalize_mpi,
    load_and_filter_halos, generate_galaxies,
    write_single_hdf5, build_abacus_path
)

# Initialize MPI/JAX
comm, rank, size, MPI_AVAILABLE = initialize_mpi_jax()

# Build catalog path
catalog_path = build_abacus_path("/data", "AbacusSummit", "small_c000", "ph3000", "z1.100")

# Load and filter halos
logmhost, halo_radius, halo_pos, halo_vel, Lbox = load_and_filter_halos(
    catalog_path, rank, size
)

# Generate galaxies
galcat = generate_galaxies(logmhost, halo_radius, halo_pos, halo_vel, Lbox, rank)

# Write output
write_single_hdf5(galcat, logmhost, halo_radius, halo_pos, halo_vel, 
                  "output.hdf5", Lbox)

# Finalize
finalize_mpi(comm, rank, size, MPI_AVAILABLE)
```

### Multi-Process MPI Pipeline

```python
from covariance_mocks import (
    initialize_mpi_jax, finalize_mpi,
    load_and_filter_halos, generate_galaxies,
    write_parallel_hdf5, build_abacus_path
)

# Initialize MPI/JAX
comm, rank, size, MPI_AVAILABLE = initialize_mpi_jax()

# Build catalog path
catalog_path = build_abacus_path("/data", "AbacusSummit", "small_c000", "ph3000", "z1.100")

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
```