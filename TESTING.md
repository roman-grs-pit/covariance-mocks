# Testing Guide

This document provides a reference for the testing procedures in the covariance-mocks project, including production management testing.

## Prerequisites

Load the environment and install CLI tools (provides pytest and all dependencies):
```bash
source scripts/load_env.sh
pip install -e .  # Installs production-manager CLI
```

## Quick Development Testing (< 5 minutes)

For development feedback, run only fast tests:

```bash
# Load environment
source scripts/load_env.sh

# Fast tests only (unit + fast system tests)
pytest -m "unit or (system and not slow)" -v
```

## Long Validation Testing (Background Execution)

For validation, run long tests in background:

```bash
# Load environment
source scripts/load_env.sh

# Run long tests in background with logging
nohup pytest -m "slow or validation" -v --timeout=1800 > validation_tests.log 2>&1 &

# Monitor progress
tail -f validation_tests.log

# Check if still running
ps aux | grep pytest
```

## Testing Hierarchy

### 1. Unit Tests (Fast - No SLURM Required)
Test core logic with mocked SLURM calls:
```bash
# Test shared core logic functions
pytest tests/test_integration_core.py -m unit -v

# All unit tests across project  
pytest -m unit -v
```
**Goal**: Validation of core logic without HPC resources

### 2. System Tests (Requires SLURM)
Test with actual SLURM job submission:
```bash
# Fast system tests (test mode only)
pytest tests/test_system_integration.py -m "system and not slow" -v

# All system tests (including production mode - very slow)
pytest tests/test_system_integration.py -m system -v
```

### 3. Validation Tests (Requires SLURM + Reference)
Compare generated catalogs to validated reference:
```bash
# Pytest validation tests
pytest tests/test_catalog_validation.py -m validation -v

# Standalone validation tool
python scripts/run_validation.py generate /tmp/validation_test
python scripts/run_validation.py info  # Show reference catalog details
```

### 4. Production Management Tests
Test the CLI system and production workflows:
```bash
# Test CLI installation and registry
production-manager list

# Test production initialization (dry run)
production-manager init test_basic
production-manager status test_basic

# Test production workflow with small scale
production-manager stage test_basic
production-manager submit test_basic
production-manager monitor test_basic
```

### 5. Shell Script Compatibility Tests
Verify shell script uses same underlying logic:
```bash
# Test updated shell script interface
./scripts/make_mocks.sh --test

# Test direct Python call
python tests/integration_core.py /tmp/direct_test --test
```

## Test Categories and Markers

- **`@pytest.mark.unit`**: Fast tests without SLURM dependency
- **`@pytest.mark.system`**: Tests requiring SLURM resources
- **`@pytest.mark.slow`**: Long-running tests (production mode) 
- **`@pytest.mark.validation`**: Tests comparing against reference catalogs

## Production Management Testing

The CLI system includes specific tests for production workflows:

### CLI Testing
```bash
# Test CLI installation and registry
production-manager list
production-manager status --help

# Test name resolution
production-manager status alpha
```

### Production Workflow Testing
```bash
# Test complete workflow with test production
production-manager init test_basic     # Should create 120 jobs (10 realizations × 12 redshifts)
production-manager stage test_basic    # Should create batch scripts
production-manager status test_basic   # Should show staged jobs
production-manager submit test_basic   # Should submit to SLURM
production-manager monitor test_basic  # Should show live updates
```

### Git Tagging Workflow Testing
```bash
# Test clean working tree tagging
git status  # Should be clean
production-manager init test_basic
git tag -l "production/*"  # Should show new tag

# Test development workflow with --allow-dirty
echo "test" > temp_file
production-manager init test_basic --allow-dirty
git tag -l "production/*"  # Should show tag with 'allow_dirty' marker

# Test version specification
production-manager init test_basic --version v2.0
git tag -l "production/*"  # Should show tag with v2.0

# Clean up test tags
git tag -d $(git tag -l "production/test_basic*")
rm -f temp_file
```

## Common Testing Commands

### Development Workflow (Fast)
```bash
# Unit tests (< 1 minute)
pytest -m unit -v

# Fast development testing (< 5 minutes) 
pytest -m "unit or (system and not slow)" -v

# Save fast test output to log file
pytest -m "unit or (system and not slow)" -v 2>&1 | tee dev_tests.log

# Test CLI functionality
production-manager list
production-manager status test_basic
```

### Validation Workflow (Long - Run in Background)
```bash
# All validation tests (30+ minutes - background execution)
nohup pytest -m "slow or validation" -v --timeout=1800 > validation_tests.log 2>&1 &

# SLURM-only tests (background execution)
nohup pytest -m "system or validation" -v --timeout=1800 > slurm_tests.log 2>&1 &

# Complete test suite (very long - overnight execution)
nohup pytest -v --timeout=1800 > full_tests.log 2>&1 &
```

## Development Workflow

### Daily Development
- **During development**: `pytest -m unit -v` (< 1 minute)
- **Pre-commit**: `pytest -m "unit or (system and not slow)" -v` (< 5 minutes)
- **CLI testing**: `production-manager list && production-manager status test_basic` (< 30 seconds)
- **Git tagging**: Test clean/dirty workflows with `production-manager init test_basic --allow-dirty`

### Validation Testing
- **Before releases**: Run validation tests in background
  ```bash
  nohup pytest -m "slow or validation" -v --timeout=1800 > validation.log 2>&1 &
  ```
- **Manual validation**: Use `scripts/run_validation.py` for detailed catalog comparison
- **CI Integration**: Fast tests only, validation tests run separately

### Monitoring Long Tests
```bash
# Check test progress
tail -f validation.log

# Check if tests are still running
ps aux | grep pytest

# Kill long tests if needed
pkill -f "pytest.*slow"
```

## Validation Reference

**Reference catalog**: `/global/cfs/cdirs/m4943/Simulations/covariance_mocks/validation/validated/mock_AbacusSummit_small_c000_ph3000_z1.100.hdf5`

Validation checks:
- **Dataset structure**: Same dataset names, shapes, and dtypes
- **Exact equality**: For integer data
- **Tolerance-based comparison**: For floating point data (configurable tolerance)
- **Reproducibility**: Multiple runs produce identical results

## Files and Architecture

### Testing Architecture
The testing system uses a **shared core logic approach** enabling both pytest and shell script workflows:

### Core Testing Infrastructure
- **`tests/integration_core.py`**: Shared SLURM execution logic used by both pytest and shell scripts
- **`tests/conftest.py`**: Pytest configuration and fixtures with validation path updates
- **`pyproject.toml`**: Test configuration with markers for unit/system/validation tests

### Test Files
- **`tests/test_integration_core.py`**: Unit tests for core functions (fast, mocked SLURM)
- **`tests/test_system_integration.py`**: System tests for full SLURM integration
- **`tests/test_catalog_validation.py`**: Validation tests against reference data using `/validation/` paths

### Tools and Scripts
- **`scripts/run_validation.py`**: Standalone catalog validation tool
- **`scripts/make_mocks.sh`**: Updated shell script using Python shared core logic

### Validation Infrastructure Updates
- **Path migration**: Updated from `/data/` to `/validation/` directory structure
- **Reference catalog**: Now located at `/validation/validated/` for improved organization
- **HDF5 dataset comparison**: Exact equality for integers, tolerance-based for floating point
- **Reproducibility testing**: Multiple runs produce identical results

## Troubleshooting

**If pytest is not found**:
```bash
source scripts/load_env.sh
```

**If tests fail due to missing dependencies**:
- Check that `h5py` is available for validation tests
- Ensure SLURM environment is properly configured for system tests
- Verify reference catalog path exists for validation tests

For detailed troubleshooting, refer to the test output logs and error messages.