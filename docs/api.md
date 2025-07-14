# Roman GRS Covariance Mocks API Documentation

**📖 Complete API Documentation**: [https://grs-pit-covariance-mocks.readthedocs.io/en/latest/api.html](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/api.html)

This document provides a reference for the **Roman Galaxy Redshift Survey (GRS)** covariance mocks modular pipeline, built for the **Roman GRS Project Infrastructure Team (PIT)** on **Perlmutter at NERSC**. For complete documentation with examples, usage patterns, and detailed parameter descriptions, visit the full documentation on Read the Docs.

## Quick Reference

The modular pipeline consists of 7 core modules:

- **[`data_loader`](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/api.html#module-covariance_mocks.data_loader)** - Halo catalog loading and filtering
- **[`galaxy_generator`](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/api.html#module-covariance_mocks.galaxy_generator)** - Galaxy population modeling  
- **[`hdf5_writer`](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/api.html#module-covariance_mocks.hdf5_writer)** - Parallel HDF5 output operations
- **[`mpi_setup`](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/api.html#module-covariance_mocks.mpi_setup)** - MPI and JAX initialization
- **[`utils`](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/api.html#module-covariance_mocks.utils)** - Common utility functions
- **[`campaign_config`](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/api.html#module-covariance_mocks.campaign_config)** - YAML configuration validation and hierarchical inheritance
- **[`campaign_manager`](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/api.html#module-covariance_mocks.campaign_manager)** - SQLite job tracking and SLURM array orchestration

## Essential Functions

### Data Loading
```python
from covariance_mocks.data_loader import build_abacus_path, load_and_filter_halos

# Build path to halo catalog
catalog_path = build_abacus_path(base_path, suite, box, phase, redshift)

# Load halos with MPI slab decomposition  
logmhost, halo_radius, halo_pos, halo_vel, Lbox = load_and_filter_halos(
    catalog_path, rank, size, n_gen=None
)
```

### Galaxy Generation
```python
from covariance_mocks.galaxy_generator import generate_galaxies

# Generate galaxies for halos using rgrspit_diffsky
galcat = generate_galaxies(logmhost, halo_radius, halo_pos, halo_vel, Lbox, rank)
```

### HDF5 Output
```python
from covariance_mocks.hdf5_writer import write_single_hdf5, write_parallel_hdf5

# Single process output
write_single_hdf5(galcat, logmhost, halo_radius, halo_pos, halo_vel, output_path, Lbox)

# Multi-process parallel output
write_parallel_hdf5(galcat, logmhost, halo_radius, halo_pos, halo_vel, 
                    output_path, rank, size, comm, Lbox)
```

### MPI Setup
```python
from covariance_mocks.mpi_setup import initialize_mpi_jax, finalize_mpi

# Initialize
comm, rank, size, MPI_AVAILABLE = initialize_mpi_jax()

# Finalize
finalize_mpi(comm, rank, size, MPI_AVAILABLE)
```

### Campaign Management
```python
from covariance_mocks.campaign_config import CampaignConfig
from covariance_mocks.campaign_manager import CampaignManager

# Load and validate campaign configuration
config = CampaignConfig.from_yaml('config/examples/production_campaign.yaml')

# Initialize campaign manager
manager = CampaignManager(config)

# Submit campaign jobs
manager.submit_campaign()

# Monitor campaign status
status = manager.get_campaign_status()
```

**📖 For complete documentation with detailed parameters, examples, and usage patterns, visit [Read the Docs](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/api.html).**