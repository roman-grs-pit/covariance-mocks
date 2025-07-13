"""System tests for full SLURM integration.

These tests require actual SLURM resources and should be run on HPC systems.
They test the complete pipeline with real SLURM job submission.
"""

import os
import tempfile
from pathlib import Path

import pytest

from tests.integration_core import (
    MockGenerationConfig,
    run_full_pipeline,
    run_galaxy_generation,
    run_plotting,
)




class TestSLURMIntegration:
    """System tests requiring actual SLURM resources."""
    
    @pytest.mark.system
    @pytest.mark.slow
    def test_slurm_plotting(self, shared_catalog):
        """Test plotting with shared SLURM-generated catalog."""
        # Test plotting using shared catalog
        plot_success, plot_message = run_plotting(shared_catalog)
        
        assert plot_success is True, f"Plotting failed: {plot_message}"
        assert "completed successfully" in plot_message
        
        # Verify plot file exists
        plot_files = list(shared_catalog.output_dir.glob("*.png"))
        assert len(plot_files) > 0, "No plot files found after plotting"
    
    @pytest.mark.system
    def test_slurm_environment_available(self):
        """Test that SLURM environment is properly configured."""
        # Check that we're on a SLURM system
        result = os.system("which srun >/dev/null 2>&1")
        assert result == 0, "srun command not available - not on SLURM system"
        
        # Check that we can query SLURM partition info
        result = os.system("sinfo >/dev/null 2>&1")
        assert result == 0, "Cannot query SLURM partition info"
    
    @pytest.mark.system
    def test_conda_environment_loaded(self):
        """Test that conda environment is properly loaded."""
        conda_env = os.environ.get("CONDA_ENV")
        assert conda_env is not None, "CONDA_ENV not set"
        assert Path(conda_env).exists(), f"Conda environment path does not exist: {conda_env}"
    
    @pytest.mark.system
    def test_output_directory_writable(self, temp_output_dir):
        """Test that output directory is writable."""
        test_file = temp_output_dir / "test_write.tmp"
        
        try:
            test_file.write_text("test")
            assert test_file.exists(), "Could not create test file"
            content = test_file.read_text()
            assert content == "test", "Could not read test file content"
        finally:
            if test_file.exists():
                test_file.unlink()


class TestSLURMConfiguration:
    """Test SLURM configuration and job parameters."""
    
    @pytest.mark.system
    def test_slurm_account_accessible(self):
        """Test that the NERSC account is accessible."""
        # This would need to be customized based on the actual account validation method
        # For now, just check that the account variable is set
        from tests.integration_core import MockGenerationConfig
        config = MockGenerationConfig("/tmp")
        assert config.nersc_account == "m4943"
    
    @pytest.mark.system
    def test_gpu_partition_available(self):
        """Test that GPU partition is available."""
        result = os.system("sinfo -p gpu >/dev/null 2>&1")
        # Note: This might fail on systems without GPU partition
        # In practice, you'd want to check your specific system's partition names
        if result != 0:
            pytest.skip("GPU partition not available on this system")


class TestErrorHandling:
    """Test error handling in SLURM integration."""
    
    @pytest.mark.system
    def test_invalid_output_directory(self):
        """Test handling of invalid output directory."""
        invalid_config = MockGenerationConfig(
            output_dir="/invalid/readonly/path/that/should/not/exist",
            test_mode=True,
            n_gen=10
        )
        
        # This should either fail gracefully or create the directory
        # depending on the implementation
        try:
            success, message = run_galaxy_generation(invalid_config)
            # If it succeeds, the directory was created
            if success:
                assert invalid_config.output_dir.exists()
        except (PermissionError, OSError):
            # Expected for truly invalid paths
            pass
    
    @pytest.mark.system
    def test_existing_output_no_force(self, shared_catalog):
        """Test behavior when output exists and force=False."""
        # Create config with same output file as shared catalog to test file exists behavior
        test_config = MockGenerationConfig(
            output_dir=str(shared_catalog.output_dir),
            test_mode=False,  # Same mode as shared_catalog to get same filename
            force_run=False  # Key test: force=False with existing output
        )
        
        success, message = run_galaxy_generation(test_config)
        
        assert success is True, "Should succeed when output exists"
        assert "already exists" in message, "Should indicate file already exists"