# Roman Galaxy Redshift Survey (GRS) Covariance Mocks

Modular pipeline for generating mock galaxy catalogs from AbacusSummit halo catalogs using the rgrspit_diffsky package. This code generates large-scale covariance mocks for the **Roman Galaxy Redshift Survey (GRS)** as part of the **Roman GRS Project Infrastructure Team (PIT)** analysis pipeline.

**System Requirements**: Runs on the **Perlmutter system at NERSC** under the Roman GRS PIT account (m4943).

**📖 Full Documentation**: [https://grs-pit-covariance-mocks.readthedocs.io](https://grs-pit-covariance-mocks.readthedocs.io)

## Architecture

The pipeline uses a **modular architecture** with core functionality organized in `src/covariance_mocks/`:

### Core Pipeline Components
- **`data_loader.py`** - Halo catalog loading and filtering
- **`galaxy_generator.py`** - Galaxy population modeling via rgrspit_diffsky
- **`hdf5_writer.py`** - Parallel HDF5 output with MPI collective I/O
- **`mpi_setup.py`** - MPI initialization and domain decomposition
- **`utils.py`** - Shared utilities and configuration

### Production Management System
- **`production_config.py`** - YAML configuration validation and hierarchical inheritance
- **`production_manager.py`** - Three-stage workflow orchestration with SQLite job tracking
- **`scripts/run_production.py`** - CLI interface for production management

### Three-Stage Production Workflow
1. **INIT**: Production initialization with configuration validation
2. **STAGED**: SLURM script generation for inspection 
3. **SUBMITTED**: Job submission and execution tracking

## Installation

### Clone Repository

```bash
# Clone the repository
git clone https://github.com/roman-grs-pit/covariance-mocks.git
cd covariance-mocks

# Load environment and install CLI tools
source scripts/load_env.sh
pip install -e .
```

## Quick Start on Perlmutter (NERSC)

### Single Mock Generation

```bash
# Load environment and generate mock catalog
source scripts/load_env.sh
./scripts/make_mocks.sh

# For test mode (5000 halos, faster)
./scripts/make_mocks.sh --test

# Force regeneration of existing catalogs
./scripts/make_mocks.sh --force
```

### Production Management (Large Scale)

The production system provides a **CLI tool** with name-based production management:

```bash
# Load environment
source scripts/load_env.sh

# Install CLI tool (one-time setup)
pip install -e .

# List available productions
production-manager list

# Stage 1: Initialize production using name
production-manager init alpha

# Stage 2: Generate and inspect SLURM scripts (optional)
production-manager stage alpha

# Stage 3: Submit jobs to SLURM
production-manager submit alpha

# Monitor progress with live updates
production-manager monitor alpha

# Quick status check
production-manager status alpha

# Retry failed jobs
production-manager retry alpha
```

**CLI Features:**
- **Name-based lookup**: Use production names like `alpha` instead of config file paths
- **Live monitoring**: Real-time status updates with production path display
- **Registry system**: Automatic mapping of production names to configurations
- **Three-stage workflow**: Init → Stage → Submit with script inspection
- **Automatic git tagging**: Creates reproducible tags for every production

**Configuration Workflow:**
1. **Copy template**: `cp config/examples/covariance_template.yaml config/productions/my_production.yaml`
2. **Edit config**: Modify production name, redshifts, and parameters
3. **Run production**: `production-manager init my_production`

**Directory Structure:**
- **`config/productions/`** - Active production configurations (git-tracked)
- **`config/examples/`** - Template configurations for creating new productions
- Productions organized as `/productions/{name}/` (e.g., `/productions/alpha/`)
- Separate directories for catalogs, logs, metadata, and scripts

## Git Tagging for Scientific Reproducibility

The production system automatically creates git tags for every production to ensure scientific reproducibility and traceability.

### Automatic Tagging Workflow

When you run `production-manager init`, the system:

1. **Checks working tree status** - Ensures reproducible state
2. **Creates production tag** - Format: `production/{name}_{version}_{timestamp}`
3. **Records metadata** - Includes config hash, file lists, and environment info
4. **Validates reproducibility** - Warns about uncommitted changes

```bash
# Clean working tree (recommended for production)
production-manager init alpha
# Creates tag: production/alpha_v1.0_20250717_143022

# Development with uncommitted changes
production-manager init alpha --allow-dirty
# Creates tag: production/alpha_v1.0_allow_dirty_20250717_143022
```

### Tag Format and Metadata

**Tag Format**: `production/{name}_{version}_{timestamp}`
- `name`: Production name from config
- `version`: Version from config or CLI `--version` flag
- `timestamp`: Creation time (YYYYMMDD_HHMMSS)

**Tag Metadata Includes**:
- Production configuration hash
- Working tree status (clean/dirty)
- List of modified/untracked files
- Environment and dependency information
- Reproducibility warnings if applicable

### Best Practices for Scientific Reproducibility

**For Production Runs:**
- Always use clean working tree (no `--allow-dirty`)
- Commit all changes before running productions
- Use semantic versioning for production configs
- Document production purpose in git commit messages

**For Development/Testing:**
- Use `--allow-dirty` flag for development iterations
- Clean up development tags before merging branches
- Test with clean working tree before production runs

**Version Management:**
```bash
# Specify version explicitly
production-manager init alpha --version v2.0

# Use config version (default)
production-manager init alpha  # Uses version from config file
```

### Viewing Production Tags

```bash
# List all production tags
git tag -l "production/*"

# View tag metadata
git show production/alpha_v1.0_20250717_143022

# Clean up development tags
git tag -d production/alpha_v1.0_allow_dirty_test_20250717_143022
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
- **Slab Decomposition**: Domain decomposition for large catalogs
- **Parallel HDF5**: Collective I/O operations
- **Production Scale**: Supports 40,000+ catalog generation runs

## Output Files

### Single Mock Generation
By default, output files are written to `$SCRATCH/grspit/covariance_mocks/data/`. To use a different directory, set the `GRS_COV_MOCKS_DIR` environment variable:

```bash
export GRS_COV_MOCKS_DIR="/path/to/your/output/directory"
```

Output files include:
- `mock_*.hdf5`: Galaxy catalog with positions, masses, and properties
- `mock_*.png`: Visualization showing galaxy and halo distributions
- `mocks.log`: Pipeline execution log

### Production Output
Productions use organized directory structure:

```
/productions/{version}_{name}/
├── catalogs/           # Generated HDF5 catalogs
├── logs/              # SLURM job logs and scripts
├── metadata/          # Production configuration and tracking
│   ├── production_config.yaml
│   └── production.db  # SQLite job tracking database
└── qa/                # Quality assurance outputs
```

**Example**: Alpha production (`alpha`) creates `/productions/alpha/` with organized structure.

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

The project includes testing with pytest integration and SLURM validation:

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

See `TESTING.md` for detailed testing procedures.

## Environment

The pipeline is built for the **Roman GRS Project Infrastructure Team (PIT)** and requires:
- **NERSC Perlmutter system** with GPU nodes (account m4943)
- Conda environment with JAX, rgrspit_diffsky, and scientific Python stack
- MPI support with parallel HDF5
- Access to Roman GRS PIT data and computational resources

See the [Installation Guide](https://grs-pit-covariance-mocks.readthedocs.io/en/latest/installation.html) for detailed setup instructions.
