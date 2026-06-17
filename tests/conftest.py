"""Pytest configuration and shared fixtures."""

import os
import tempfile
import yaml
import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest

from tests.integration_core import MockGenerationConfig, run_full_pipeline


@pytest.fixture
def temp_output_dir():
    """Provide a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_config(temp_output_dir):
    """Provide a test configuration for mock generation."""
    return MockGenerationConfig(
        output_dir=str(temp_output_dir),
        test_mode=True,
        n_gen=100,  # Very small for testing
        time_limit=5,  # Short time limit for testing
    )


@pytest.fixture
def production_config(temp_output_dir):
    """Provide a production configuration for mock generation."""
    return MockGenerationConfig(
        output_dir=str(temp_output_dir),
        test_mode=False,
        time_limit=10,
    )


@pytest.fixture
def mock_environment(monkeypatch):
    """Mock environment variables for testing."""
    test_env = {
        "CONDA_ENV": "/test/conda/env",
        "SCRATCH": "/test/scratch",
        "GRS_COV_MOCKS_DIR": "/test/output",
    }
    
    for key, value in test_env.items():
        monkeypatch.setenv(key, value)
    
    return test_env


@pytest.fixture(scope="session") 
def shared_catalog():
    """Generate a single production catalog once per session for all tests."""
    # Use shared filesystem location for MPI-HDF5 compatibility  
    base_dir = Path("/global/cfs/cdirs/m4943/Simulations/covariance_mocks/validation/tmp")
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Create session-specific directory
    import uuid
    session_dir = base_dir / f"shared_catalog_{uuid.uuid4().hex[:8]}"
    session_dir.mkdir(parents=True, exist_ok=True)
    
    config = MockGenerationConfig(
        output_dir=str(session_dir),
        test_mode=False,  # Production mode for comprehensive testing
        force_run=True,
        time_limit=10
    )
    
    # Generate catalog once - used by all tests  
    success, messages = run_full_pipeline(config)
    if not success:
        pytest.fail(f"Shared catalog generation failed: {messages}")
    
    try:
        yield config
    finally:
        # Clean up session directory
        import shutil
        if session_dir.exists():
            shutil.rmtree(session_dir, ignore_errors=True)


# Configuration Testing Fixtures
@pytest.fixture
def sample_production_config():
    """Standard production configuration for testing."""
    return {
        "production": {
            "name": "test_production",
            "version": "v1.0", 
            "description": "Test production for unit testing",
            "dependencies": {
                "rgrspit_diffsky": "0.1.dev84+g1609408.d20250710"
            }
        },
        "science": {
            "cosmology": "AbacusSummit",
            "redshifts": [0.5, 1.0, 1.4],
            "realizations": {
                "start": 3000,
                "count": 2,
                "step": 1
            }
        },
        "execution": {
            "job_type": "balanced",
            "batch_size": 2,
            "timeout_hours": 1.0,
            "retry_policy": {
                "max_retries": 2,
                "backoff_multiplier": 1.5,
                "initial_delay_minutes": 1.0
            }
        },
        "resources": {
            "account": "test_account",
            "partition": "test_partition",
            "constraint": "cpu",
            "nodes_per_job": 1,
            "tasks_per_node": 2,
            "memory_gb": 32.0
        },
        "outputs": {
            "base_path": "/tmp/test_production",
            "structure": "hierarchical",
            "compression": "gzip",
            "cleanup_policy": {
                "keep_logs_days": 30,
                "keep_intermediate": False,
                "archive_completed": False
            }
        }
    }


@pytest.fixture 
def sample_production_schema():
    """Standard production schema for validation testing."""
    return {
        "production": {
            "type": "object",
            "required": ["name", "description"],
            "properties": {
                "name": {"type": "string", "pattern": "^[a-zA-Z][a-zA-Z0-9_-]*$"},
                "version": {"type": "string"},
                "description": {"type": "string"}
            }
        },
        "science": {
            "type": "object", 
            "required": ["redshifts", "realizations"],
            "properties": {
                "redshifts": {"type": "array", "items": {"type": "number"}},
                "realizations": {
                    "type": "object",
                    "required": ["start", "count"],
                    "properties": {
                        "start": {"type": "integer"},
                        "count": {"type": "integer"},
                        "step": {"type": "integer", "default": 1}
                    }
                }
            }
        }
    }


@pytest.fixture
def test_config_dir(sample_production_config, sample_production_schema):
    """Create temporary config directory structure with test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_dir = Path(temp_dir)
        
        # Create directory structure
        (config_dir / "config" / "schemas").mkdir(parents=True)
        (config_dir / "config" / "defaults").mkdir(parents=True)
        (config_dir / "config" / "productions").mkdir()
        (config_dir / "examples").mkdir()
        
        # Create production schema
        schema_file = config_dir / "config" / "schemas" / "production_schema.yaml"
        with open(schema_file, 'w') as f:
            yaml.dump(sample_production_schema, f)
        
        # Create test production config
        prod_config_file = config_dir / "config" / "productions" / "test.yaml"
        with open(prod_config_file, 'w') as f:
            yaml.dump(sample_production_config, f)
            
        # Create defaults
        defaults_file = config_dir / "config" / "defaults" / "nersc.yaml" 
        defaults_content = {
            "machine": "nersc",
            "resources": {
                "account": "m4943",
                "partition": "gpu",
                "constraint": "gpu"
            }
        }
        with open(defaults_file, 'w') as f:
            yaml.dump(defaults_content, f)
        
        yield config_dir


# Production Manager Testing Fixtures
@pytest.fixture
def test_job_database():
    """Create temporary SQLite database for job testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = Path(f.name)
    
    try:
        from covariance_mocks.production_manager import JobDatabase
        db = JobDatabase(db_path)
        yield db
    finally:
        if db_path.exists():
            db_path.unlink()


@pytest.fixture
def sample_job_specs():
    """Sample job specifications for testing."""
    from covariance_mocks.production_manager import JobSpec, JobStatus
    return [
        JobSpec(
            job_id="r3000_z0.500",
            realization=3000,
            redshift=0.5,
            output_path="/tmp/test/r3000_z0.500.hdf5",
            status=JobStatus.PENDING
        ),
        JobSpec(
            job_id="r3000_z1.000", 
            realization=3000,
            redshift=1.0,
            output_path="/tmp/test/r3000_z1.000.hdf5",
            status=JobStatus.PENDING
        ),
        JobSpec(
            job_id="r3001_z0.500",
            realization=3001,
            redshift=0.5,
            output_path="/tmp/test/r3001_z0.500.hdf5", 
            status=JobStatus.COMPLETED
        )
    ]


# Mock Fixtures for External Dependencies
@pytest.fixture
def mock_slurm_environment(monkeypatch):
    """Mock SLURM environment variables and commands."""
    # Set environment variables
    test_env = {
        "SLURM_JOB_ID": "12345",
        "SLURM_PROCID": "0", 
        "SLURM_NTASKS": "4",
        "SLURM_CPUS_PER_TASK": "1"
    }
    for key, value in test_env.items():
        monkeypatch.setenv(key, value)
    
    # Mock subprocess calls
    with patch('subprocess.run') as mock_run:
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "Job submitted successfully"
        yield mock_run


@pytest.fixture
def mock_mpi_environment():
    """Mock MPI environment for testing."""
    with patch('covariance_mocks.mpi_setup.initialize_mpi_jax') as mock_init:
        mock_comm = MagicMock()
        mock_comm.Get_rank.return_value = 0
        mock_comm.Get_size.return_value = 1
        mock_init.return_value = (mock_comm, 0, 1, False)
        yield mock_init


@pytest.fixture 
def mock_rgrspit_diffsky():
    """Mock rgrspit_diffsky package calls."""
    with patch('rgrspit_diffsky.mc_galpop.mc_galpop_synthetic_subs') as mock_galpop:
        # Mock galaxy catalog output
        mock_catalog = {
            'pos': [[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]],
            'vel': [[0.0, 0.0, 0.0], [10.0, 10.0, 10.0]], 
            'stellar_mass': [1e10, 1.5e10],
            'sfr': [1.0, 2.0]
        }
        mock_galpop.return_value = mock_catalog
        yield mock_galpop


# Data Processing Testing Fixtures
@pytest.fixture
def sample_halo_data():
    """Sample halo data for testing data loading functions."""
    import numpy as np
    return {
        'mass': np.array([10**12.0, 10**12.5, 10**13.0]),  # Linear masses from log masses
        'radius': np.array([0.5, 0.7, 1.0]),
        'pos': np.array([[-250.0, -250.0, -250.0], [-240.0, -240.0, -240.0], [-230.0, -230.0, -230.0]]),  # [-Lbox/2, Lbox/2] format
        'vel': np.array([[0.0, 0.0, 0.0], [100.0, 100.0, 100.0], [200.0, 200.0, 200.0]]),
        'lbox': 500.0  # lowercase 'l' to match expected key
    }


@pytest.fixture
def sample_galaxy_catalog():
    """Sample galaxy catalog for testing HDF5 writing and validation."""
    import numpy as np
    return {
        'pos': np.array([[0.0, 0.0, 0.0], [1.0, 1.0, 1.0], [2.0, 2.0, 2.0]]),
        'vel': np.array([[0.0, 0.0, 0.0], [10.0, 10.0, 10.0], [20.0, 20.0, 20.0]]),
        'stellar_mass': np.array([1e10, 1.5e10, 2e10]),
        'sfr': np.array([1.0, 2.0, 3.0]),
        'metallicity': np.array([0.01, 0.02, 0.03]),
        'oii_luminosity': np.array([1e40, 1.5e40, 2e40]),
        'halpha_luminosity': np.array([2e40, 3e40, 4e40])
    }


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "unit: Unit tests that do not require SLURM"
    )
    config.addinivalue_line(
        "markers", "system: System tests that require SLURM resources"
    )
    config.addinivalue_line(
        "markers", "slow: Tests that take a long time to run"
    )
    config.addinivalue_line(
        "markers", "validation: Tests that compare against reference catalogs"
    )