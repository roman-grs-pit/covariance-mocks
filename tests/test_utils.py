"""Unit tests for Utils module."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from covariance_mocks.utils import (
    validate_catalog_path, generate_output_filename,
    ABACUS_BASE_PATH, SIMULATION_SUITE
)


class TestValidateCatalogPath:
    """Test validate_catalog_path function."""
    
    @pytest.mark.unit
    def test_validate_existing_directory(self):
        """Test validation of existing directory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = validate_catalog_path(tmp_dir)
            assert result is True
    
    @pytest.mark.unit
    def test_validate_nonexistent_path(self):
        """Test validation of nonexistent path."""
        nonexistent_path = "/nonexistent/path/that/does/not/exist"
        
        with pytest.raises(FileNotFoundError, match="AbacusSummit catalog not found"):
            validate_catalog_path(nonexistent_path)
    
    @pytest.mark.unit
    def test_validate_file_instead_of_directory(self):
        """Test validation when path is a file, not directory."""
        with tempfile.NamedTemporaryFile() as tmp_file:
            with pytest.raises(FileNotFoundError, match="AbacusSummit catalog not found"):
                validate_catalog_path(tmp_file.name)
    
    @pytest.mark.unit
    @patch('covariance_mocks.utils.os.path.isdir')
    def test_validate_with_mocked_isdir(self, mock_isdir):
        """Test validation with mocked os.path.isdir."""
        mock_isdir.return_value = True
        
        result = validate_catalog_path("/some/path")
        
        assert result is True
        mock_isdir.assert_called_once_with("/some/path")
    
    @pytest.mark.unit
    def test_validate_empty_string(self):
        """Test validation with empty string path."""
        with pytest.raises(FileNotFoundError, match="AbacusSummit catalog not found"):
            validate_catalog_path("")
    
    @pytest.mark.unit
    def test_validate_relative_path(self):
        """Test validation of relative path."""
        # Create a temporary directory in current working directory
        with tempfile.TemporaryDirectory(dir=".") as tmp_dir:
            # Get just the directory name (relative path)
            relative_path = os.path.basename(tmp_dir)
            result = validate_catalog_path(relative_path)
            assert result is True


class TestGenerateOutputFilename:
    """Test generate_output_filename function."""
    
    @pytest.mark.unit
    def test_generate_production_filename(self):
        """Test generation of production filename (no test suffix)."""
        result = generate_output_filename(
            "AbacusSummit_small_c000", 
            "ph3000", 
            "z1.100"
        )
        
        expected = "mock_AbacusSummit_small_c000_ph3000_z1.100.hdf5"
        assert result == expected
    
    @pytest.mark.unit
    def test_generate_test_filename(self):
        """Test generation of test filename (with test suffix)."""
        result = generate_output_filename(
            "AbacusSummit_small_c000", 
            "ph3000", 
            "z1.100", 
            n_gen=5000
        )
        
        expected = "mock_AbacusSummit_small_c000_ph3000_z1.100_test5000.hdf5"
        assert result == expected
    
    @pytest.mark.unit
    def test_generate_filename_different_parameters(self):
        """Test filename generation with different parameters."""
        result = generate_output_filename(
            "AbacusSummit_large_c001", 
            "ph2000", 
            "z0.500"
        )
        
        expected = "mock_AbacusSummit_large_c001_ph2000_z0.500.hdf5"
        assert result == expected
    
    @pytest.mark.unit
    def test_generate_filename_zero_ngen(self):
        """Test filename generation with n_gen=0."""
        result = generate_output_filename(
            "AbacusSummit_small_c000", 
            "ph3000", 
            "z1.100", 
            n_gen=0
        )
        
        expected = "mock_AbacusSummit_small_c000_ph3000_z1.100_test0.hdf5"
        assert result == expected
    
    @pytest.mark.unit
    def test_generate_filename_large_ngen(self):
        """Test filename generation with large n_gen value."""
        result = generate_output_filename(
            "AbacusSummit_small_c000", 
            "ph3000", 
            "z1.100", 
            n_gen=1000000
        )
        
        expected = "mock_AbacusSummit_small_c000_ph3000_z1.100_test1000000.hdf5"
        assert result == expected
    
    @pytest.mark.unit
    def test_generate_filename_none_ngen(self):
        """Test filename generation with None n_gen (production mode)."""
        result = generate_output_filename(
            "AbacusSummit_small_c000", 
            "ph3000", 
            "z1.100", 
            n_gen=None
        )
        
        expected = "mock_AbacusSummit_small_c000_ph3000_z1.100.hdf5"
        assert result == expected
    
    @pytest.mark.unit
    def test_generate_filename_with_special_characters(self):
        """Test filename generation with special characters in inputs."""
        result = generate_output_filename(
            "AbacusSummit_small_c000", 
            "ph3000", 
            "z1.100", 
            n_gen=123
        )
        
        expected = "mock_AbacusSummit_small_c000_ph3000_z1.100_test123.hdf5"
        assert result == expected
    
    @pytest.mark.unit
    def test_generate_filename_string_consistency(self):
        """Test that filename generation is consistent across calls."""
        params = ("AbacusSummit_small_c000", "ph3000", "z1.100")
        
        result1 = generate_output_filename(*params)
        result2 = generate_output_filename(*params)
        
        assert result1 == result2
    
    @pytest.mark.unit
    def test_generate_filename_with_underscores(self):
        """Test filename generation preserves underscores in parameters."""
        result = generate_output_filename(
            "AbacusSummit_small_c000", 
            "ph_3000", 
            "z_1.100"
        )
        
        expected = "mock_AbacusSummit_small_c000_ph_3000_z_1.100.hdf5"
        assert result == expected
    
    @pytest.mark.unit
    def test_generate_filename_empty_strings(self):
        """Test filename generation with empty string parameters."""
        result = generate_output_filename("", "", "")
        
        expected = "mock___.hdf5"
        assert result == expected
    
    @pytest.mark.unit
    def test_generate_filename_format_consistency(self):
        """Test that generated filenames follow expected format."""
        result = generate_output_filename(
            "AbacusSummit_small_c000", 
            "ph3000", 
            "z1.100", 
            n_gen=100
        )
        
        # Should start with "mock_"
        assert result.startswith("mock_")
        
        # Should end with ".hdf5"
        assert result.endswith(".hdf5")
        
        # Should contain test suffix
        assert "_test100" in result
        
        # Should contain all input parameters
        assert "AbacusSummit_small_c000" in result
        assert "ph3000" in result
        assert "z1.100" in result


class TestModuleConstants:
    """Test module-level constants."""
    
    @pytest.mark.unit
    def test_abacus_base_path_constant(self):
        """Test ABACUS_BASE_PATH constant."""
        assert ABACUS_BASE_PATH == "/global/cfs/cdirs/desi/public/cosmosim/AbacusSummit/small"
        assert isinstance(ABACUS_BASE_PATH, str)
    
    @pytest.mark.unit
    def test_simulation_suite_constant(self):
        """Test SIMULATION_SUITE constant."""
        assert SIMULATION_SUITE == "small"
        assert isinstance(SIMULATION_SUITE, str)
    
    @pytest.mark.unit
    def test_constants_immutability(self):
        """Test that constants are proper strings (not mutable)."""
        # These should be immutable string constants
        assert isinstance(ABACUS_BASE_PATH, str)
        assert isinstance(SIMULATION_SUITE, str)


class TestUtilsIntegration:
    """Test integration between utils functions."""
    
    @pytest.mark.unit
    def test_validate_and_generate_workflow(self):
        """Test typical workflow of validation and filename generation."""
        # Create temporary directory for validation
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Validate the catalog path
            validation_result = validate_catalog_path(tmp_dir)
            assert validation_result is True
            
            # Generate filename for successful validation
            filename = generate_output_filename(
                "AbacusSummit_small_c000", 
                "ph3000", 
                "z1.100", 
                n_gen=1000
            )
            
            # Combine path and filename
            full_path = os.path.join(tmp_dir, filename)
            
            # Should be valid path format
            assert os.path.dirname(full_path) == tmp_dir
            assert os.path.basename(full_path) == filename
            assert full_path.endswith(".hdf5")
    
    @pytest.mark.unit
    def test_error_handling_integration(self):
        """Test error handling in integrated workflow."""
        # Try to validate nonexistent path
        with pytest.raises(FileNotFoundError):
            validate_catalog_path("/nonexistent/path")
        
        # Filename generation should still work independently
        filename = generate_output_filename(
            "AbacusSummit_small_c000", 
            "ph3000", 
            "z1.100"
        )
        
        assert filename == "mock_AbacusSummit_small_c000_ph3000_z1.100.hdf5"


class TestUtilsEdgeCases:
    """Test edge cases and boundary conditions."""
    
    @pytest.mark.unit
    def test_generate_filename_boundary_values(self):
        """Test filename generation with boundary values."""
        # Test with very small n_gen
        result = generate_output_filename(
            "test", "test", "test", n_gen=1
        )
        assert "test1" in result
        
        # Test with negative n_gen (should still work)
        result = generate_output_filename(
            "test", "test", "test", n_gen=-1
        )
        assert "test-1" in result
    
    @pytest.mark.unit
    def test_path_validation_permissions(self):
        """Test path validation with different permission scenarios."""
        # Test with readable directory
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Make directory readable
            os.chmod(tmp_dir, 0o755)
            result = validate_catalog_path(tmp_dir)
            assert result is True
    
    @pytest.mark.unit
    def test_filename_character_handling(self):
        """Test filename generation with various character inputs."""
        # Test with numeric strings
        result = generate_output_filename("123", "456", "789")
        assert result == "mock_123_456_789.hdf5"
        
        # Test with mixed case
        result = generate_output_filename("ABC", "def", "GhI")
        assert result == "mock_ABC_def_GhI.hdf5"