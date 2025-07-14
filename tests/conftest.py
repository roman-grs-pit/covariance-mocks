"""Pytest configuration and shared fixtures."""

import os
import tempfile
from pathlib import Path

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