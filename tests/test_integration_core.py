"""Unit tests for integration_core module."""

import os
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from tests.integration_core import (
    MockGenerationConfig,
    build_srun_command,
    get_script_dir,
    load_environment,
    parse_shell_args,
    run_galaxy_generation,
    run_plotting,
    run_full_pipeline,
)


class TestMockGenerationConfig:
    """Test MockGenerationConfig class."""
    
    @pytest.mark.unit
    def test_init_test_mode(self, temp_output_dir):
        """Test configuration initialization in test mode."""
        config = MockGenerationConfig(
            output_dir=str(temp_output_dir),
            test_mode=True,
            n_gen=100
        )
        
        assert config.test_mode is True
        assert config.n_gen == 100
        assert config.output_catalog == "mock_AbacusSummit_small_c000_ph3000_z1.100_test100.hdf5"
        assert "test100" in config.output_plot
        assert config.output_dir == temp_output_dir
    
    @pytest.mark.unit
    def test_init_production_mode(self, temp_output_dir):
        """Test configuration initialization in production mode."""
        config = MockGenerationConfig(
            output_dir=str(temp_output_dir),
            test_mode=False
        )
        
        assert config.test_mode is False
        assert config.output_catalog == "mock_AbacusSummit_small_c000_ph3000_z1.100.hdf5"
        assert "test" not in config.output_plot
        assert config.output_dir == temp_output_dir
    
    @pytest.mark.unit
    def test_catalog_path_property(self, temp_output_dir):
        """Test catalog_path property."""
        config = MockGenerationConfig(output_dir=str(temp_output_dir))
        expected_path = temp_output_dir / config.output_catalog
        assert config.catalog_path == expected_path
    
    @pytest.mark.unit
    def test_creates_output_directory(self, temp_output_dir):
        """Test that output directory is created if it doesn't exist."""
        subdir = temp_output_dir / "new_subdir"
        config = MockGenerationConfig(output_dir=str(subdir))
        assert subdir.exists()


class TestUtilityFunctions:
    """Test utility functions."""
    
    @pytest.mark.unit
    def test_get_script_dir(self):
        """Test get_script_dir returns correct path."""
        script_dir = get_script_dir()
        assert script_dir.name == "scripts"
        assert (script_dir / "generate_single_mock.py").exists()
    
    @pytest.mark.unit
    def test_parse_shell_args_empty(self):
        """Test parsing empty arguments."""
        force_run, test_mode, verbose = parse_shell_args([])
        assert force_run is False
        assert test_mode is False
        assert verbose is False
    
    @pytest.mark.unit
    def test_parse_shell_args_force(self):
        """Test parsing --force argument."""
        force_run, test_mode, verbose = parse_shell_args(["--force"])
        assert force_run is True
        assert test_mode is False
        assert verbose is False
    
    @pytest.mark.unit
    def test_parse_shell_args_test(self):
        """Test parsing --test argument."""
        force_run, test_mode, verbose = parse_shell_args(["--test"])
        assert force_run is False
        assert test_mode is True
        assert verbose is False
    
    @pytest.mark.unit
    def test_parse_shell_args_both(self):
        """Test parsing both arguments."""
        force_run, test_mode, verbose = parse_shell_args(["--force", "--test"])
        assert force_run is True
        assert test_mode is True
        assert verbose is False
    
    @pytest.mark.unit
    def test_parse_shell_args_invalid(self):
        """Test parsing invalid arguments raises error."""
        with pytest.raises(ValueError, match="Unknown argument"):
            parse_shell_args(["--invalid"])


class TestSrunCommand:
    """Test SLURM command building."""
    
    @pytest.mark.unit
    def test_build_srun_command_test_mode(self, test_config):
        """Test building srun command for test mode."""
        cmd = build_srun_command(test_config)
        
        assert "srun" in cmd
        assert "-n" in cmd and "1" in cmd  # Single process for test
        assert "--gpus-per-node=1" in cmd  # Single GPU for test
        assert "python" in cmd
        assert "nersc" in cmd
        assert "--test" in cmd
        assert str(test_config.n_gen) in cmd
    
    @pytest.mark.unit
    def test_build_srun_command_production_mode(self, production_config):
        """Test building srun command for production mode."""
        cmd = build_srun_command(production_config)
        
        assert "srun" in cmd
        assert "-n" in cmd and "6" in cmd  # Multiple processes for production
        assert "--gpus-per-node=3" in cmd  # Multiple GPUs for production
        assert "python" in cmd
        assert "nersc" in cmd
        assert "--test" not in cmd


class TestEnvironmentLoading:
    """Test environment loading."""
    
    @pytest.mark.unit
    @patch('subprocess.run')
    def test_load_environment_success(self, mock_run):
        """Test successful environment loading."""
        mock_run.return_value = Mock(
            returncode=0,
            stdout="PATH=/test/path\nCONDA_ENV=/test/conda\n"
        )
        
        env_vars = load_environment()
        
        assert env_vars["PATH"] == "/test/path"
        assert env_vars["CONDA_ENV"] == "/test/conda"
        mock_run.assert_called_once()
    
    @pytest.mark.unit
    @patch('subprocess.run')
    def test_load_environment_failure(self, mock_run):
        """Test environment loading failure."""
        mock_run.return_value = Mock(
            returncode=1,
            stderr="Environment loading failed"
        )
        
        with pytest.raises(RuntimeError, match="Failed to load environment"):
            load_environment()


class TestPipelineExecution:
    """Test pipeline execution functions."""
    
    @pytest.mark.unit
    @patch('tests.integration_core.load_environment')
    @patch('subprocess.run')
    def test_run_galaxy_generation_already_exists(self, mock_run, mock_load_env, test_config):
        """Test galaxy generation when output already exists."""
        # Create the output file
        test_config.catalog_path.touch()
        
        success, message = run_galaxy_generation(test_config)
        
        assert success is True
        assert "already exists" in message
        mock_run.assert_not_called()
    
    @pytest.mark.unit
    @patch('tests.integration_core.load_environment')
    @patch('subprocess.run')
    def test_run_galaxy_generation_force_run(self, mock_run, mock_load_env, test_config):
        """Test galaxy generation with force run."""
        test_config.force_run = True
        test_config.catalog_path.touch()  # File exists but should be regenerated
        
        mock_load_env.return_value = {"PATH": "/test"}
        mock_run.return_value = Mock(returncode=0, stderr="", stdout="")
        
        # Create the catalog file to simulate successful generation
        def create_catalog(*args, **kwargs):
            test_config.catalog_path.touch()
            return Mock(returncode=0, stderr="", stdout="")
        
        mock_run.side_effect = create_catalog
        
        success, message = run_galaxy_generation(test_config)
        
        assert success is True
        assert "completed successfully" in message
        mock_run.assert_called_once()
    
    @pytest.mark.unit
    @patch('tests.integration_core.load_environment')
    @patch('subprocess.run')
    def test_run_plotting_success(self, mock_run, mock_load_env, test_config):
        """Test successful plotting execution."""
        mock_load_env.return_value = {"PATH": "/test"}
        mock_run.return_value = Mock(returncode=0, stderr="", stdout="")
        
        success, message = run_plotting(test_config)
        
        assert success is True
        assert "completed successfully" in message
        mock_run.assert_called_once()
    
    @pytest.mark.unit
    @patch('tests.integration_core.load_environment')
    @patch('subprocess.run')
    def test_run_plotting_failure(self, mock_run, mock_load_env, test_config):
        """Test plotting execution failure."""
        mock_load_env.return_value = {"PATH": "/test"}
        mock_run.return_value = Mock(returncode=1, stderr="Plotting error", stdout="")
        
        success, message = run_plotting(test_config)
        
        assert success is False
        assert "failed" in message
        assert "Plotting error" in message
    
    @pytest.mark.unit
    @patch('tests.integration_core.run_galaxy_generation')
    @patch('tests.integration_core.run_plotting')
    def test_run_full_pipeline_success(self, mock_plot, mock_gen, test_config):
        """Test successful full pipeline execution."""
        mock_gen.return_value = (True, "Generation successful")
        mock_plot.return_value = (True, "Plotting successful")
        
        success, messages = run_full_pipeline(test_config)
        
        assert success is True
        assert len(messages) == 2
        assert "Generation successful" in messages[0]
        assert "Plotting successful" in messages[1]
    
    @pytest.mark.unit
    @patch('tests.integration_core.run_galaxy_generation')
    @patch('tests.integration_core.run_plotting')
    def test_run_full_pipeline_generation_failure(self, mock_plot, mock_gen, test_config):
        """Test full pipeline with generation failure."""
        mock_gen.return_value = (False, "Generation failed")
        
        success, messages = run_full_pipeline(test_config)
        
        assert success is False
        assert len(messages) == 1
        assert "Generation failed" in messages[0]
        mock_plot.assert_not_called()  # Should not call plotting if generation fails