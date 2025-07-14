# Roman Galaxy Redshift Survey (GRS) Covariance Mocks

Modular pipeline for generating mock galaxy catalogs from AbacusSummit halo catalogs using the rgrspit_diffsky package. This code generates large-scale covariance mocks for the **Roman Galaxy Redshift Survey (GRS)** as part of the **Roman GRS Project Infrastructure Team (PIT)** analysis pipeline.

**System Requirements**: Designed to run on the **Perlmutter system at NERSC** under the Roman GRS PIT account (m4943).

**📖 Full Documentation**: [https://grs-pit-covariance-mocks.readthedocs.io](https://grs-pit-covariance-mocks.readthedocs.io)

## Architecture

The pipeline uses a **modular architecture** with core functionality organized in `src/covariance_mocks/`:

- **`data_loader.py`** - Halo catalog loading and filtering
- **`galaxy_generator.py`** - Galaxy population modeling via rgrspit_diffsky
- **`hdf5_writer.py`** - Parallel HDF5 output with MPI collective I/O
- **`mpi_setup.py`** - MPI initialization and domain decomposition
- **`utils.py`** - Shared utilities and configuration

## Quick Start on Perlmutter (NERSC)

### Generate Mock Catalog and Figure

```bash
# Load environment and generate mock catalog
source scripts/load_env.sh
./scripts/make_mocks.sh

# For test mode (5000 halos, faster)
./scripts/make_mocks.sh --test

# Force regeneration of existing catalogs
./scripts/make_mocks.sh --force
```

### Manual Pipeline Steps

```bash
# 1. Load environment
source scripts/load_env.sh

# 2. Generate mock catalog
srun -n 6 --gpus-per-node=3 -c 32 --qos=interactive -N 2 --time=15 -C gpu -A m4943 python scripts/generate_single_mock.py nersc /path/to/output/directory

# 3. Create visualization
python scripts/plot_mock_catalog.py /path/to/output/mock_catalog.hdf5
```

## Pipeline Overview

- **Input**: AbacusSummit halo catalogs (small box: 500 Mpc/h)
- **Galaxy Model**: rgrspit_diffsky package with star formation history modeling
- **Output**: HDF5 files containing galaxy positions, masses, and properties
- **Visualization**: Two-panel scatter plots showing galaxy and halo distributions

## Key Features

- **MPI Parallelization**: Scales across multiple GPUs/nodes
- **Slab Decomposition**: Efficient domain decomposition for large catalogs
- **Parallel HDF5**: Collective I/O operations for performance
- **Production Scale**: Designed for 40,000+ catalog generation runs

## Output Files

By default, output files are written to `$SCRATCH/grspit/covariance_mocks/data/`. To use a different directory, set the `GRS_COV_MOCKS_DIR` environment variable:

```bash
export GRS_COV_MOCKS_DIR="/path/to/your/output/directory"
```

Output files include:
- `mock_*.hdf5`: Galaxy catalog with positions, masses, and properties
- `mock_*.png`: Visualization showing galaxy and halo distributions
- `mocks.log`: Pipeline execution log

### Example Output Visualization

The pipeline generates two-panel scatter plots showing halo and galaxy distributions with zoom-in insets:

![Example Output](examples/halo_galaxy_scatter_AbacusSummit_small_c000_ph3000_z1.100.png)

*Two-panel visualization: Left panel shows halos colored by mass with r100 virial radius scaling. Right panel shows galaxies colored by stellar mass. Both panels include zoom-in insets of a selected region.*

## Documentation

For comprehensive information, see the **[full documentation on Read the Docs](https://grs-pit-covariance-mocks.readthedocs.io)**, including:

- **[Installation Guide](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/installation.html)** - Environment setup and dependencies
- **[User Guide](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/user_guide.html)** - Detailed usage instructions and examples
- **[API Reference](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/api.html)** - Complete function and module documentation
- **[Development Guide](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/development.html)** - Contributing and development setup

## Testing

The project includes comprehensive testing with pytest integration and SLURM validation:

### Quick Development Testing (< 5 minutes)
```bash
source scripts/load_env.sh
pytest -m "unit or (system and not slow)" -v
```

### Long Validation Testing (Background)
```bash
# Load environment
source scripts/load_env.sh

# Run validation tests in background (30+ minutes)
nohup pytest -m "slow or validation" -v --timeout=1800 > validation.log 2>&1 &

# Monitor progress
tail -f validation.log

# Test against reference catalog
python scripts/run_validation.py generate /tmp/validation_test
```

### Test Categories
- **Unit Tests**: Fast tests with mocked SLURM calls (< 1 minute)
- **System Tests**: SLURM integration tests (5-15 minutes)
- **Validation Tests**: Compare against reference catalogs (30+ minutes)
- **Shell Script Tests**: Verify workflow compatibility

### Development Workflow
- **Daily development**: `pytest -m unit -v` (< 1 minute)
- **Pre-commit**: `pytest -m "unit or (system and not slow)" -v` (< 5 minutes)
- **Before releases**: Run validation tests in background

See `TESTING.md` for detailed testing procedures and `CLAUDE.md` for development workflow.

## Environment

The pipeline is specifically designed for the **Roman GRS Project Infrastructure Team (PIT)** and requires:
- **NERSC Perlmutter system** with GPU nodes (account m4943)
- Conda environment with JAX, rgrspit_diffsky, and scientific Python stack
- MPI support with parallel HDF5
- Access to Roman GRS PIT data and computational resources

See the [Installation Guide](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/installation.html) for detailed setup instructions.
