"""Unit tests for Config Registry."""

import tempfile
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

import pytest

from covariance_mocks.config_registry import (
    ConfigRegistry, get_registry, resolve_config
)
from covariance_mocks.production_config import ConfigurationError


class TestConfigRegistry:
    """Test ConfigRegistry class."""
    
    @pytest.mark.unit
    @patch('covariance_mocks.config_registry.Path.exists')
    @patch('covariance_mocks.config_registry.Path.glob')
    def test_init_with_config_dir(self, mock_glob, mock_exists):
        """Test registry initialization with custom config directory."""
        mock_exists.return_value = True
        mock_glob.return_value = []
        
        config_dir = Path("/custom/config")
        registry = ConfigRegistry(config_dir)
        
        assert registry.config_dir == config_dir
        assert registry._registry == {}
    
    @pytest.mark.unit
    @patch('covariance_mocks.config_registry.Path.exists')
    @patch('covariance_mocks.config_registry.Path.glob')
    def test_init_default_config_dir(self, mock_glob, mock_exists):
        """Test registry initialization with default config directory."""
        mock_exists.return_value = True
        mock_glob.return_value = []
        
        registry = ConfigRegistry()
        
        # Should use project config directory
        assert registry.config_dir.name == "config"
        assert "covariance-mocks" in str(registry.config_dir)
    
    @pytest.mark.unit
    @patch('covariance_mocks.config_registry.Path.exists')
    def test_scan_configs_no_examples_dir(self, mock_exists):
        """Test config scanning when examples directory doesn't exist."""
        mock_exists.return_value = False
        
        registry = ConfigRegistry(Path("/tmp"))
        
        assert registry._registry == {}
    
    @pytest.mark.unit
    @patch('covariance_mocks.config_registry.Path.exists')
    @patch('covariance_mocks.config_registry.Path.glob')
    @patch('builtins.open', new_callable=mock_open)
    @patch('covariance_mocks.config_registry.yaml.safe_load')
    def test_scan_configs_valid_files(self, mock_yaml_load, mock_file, mock_glob, mock_exists):
        """Test scanning valid configuration files."""
        mock_exists.return_value = True
        
        # Mock config files
        config_file1 = Path("/tmp/config/productions/alpha.yaml")
        config_file2 = Path("/tmp/config/productions/beta.yaml")
        mock_glob.return_value = [config_file1, config_file2]
        
        # Mock YAML content
        mock_yaml_load.side_effect = [
            {"production": {"name": "alpha"}},
            {"production": {"name": "beta"}}
        ]
        
        registry = ConfigRegistry(Path("/tmp/config"))
        
        # Should register only the production names
        assert "alpha" in registry._registry
        assert "beta" in registry._registry
        assert registry._registry["alpha"] == config_file1
        assert registry._registry["beta"] == config_file2
        assert len(registry._registry) == 2
    
    @pytest.mark.unit
    @patch('covariance_mocks.config_registry.Path.exists')
    @patch('covariance_mocks.config_registry.Path.glob')
    @patch('builtins.open', new_callable=mock_open)
    @patch('covariance_mocks.config_registry.yaml.safe_load')
    def test_scan_configs_invalid_files(self, mock_yaml_load, mock_file, mock_glob, mock_exists):
        """Test scanning with invalid configuration files."""
        mock_exists.return_value = True
        
        config_file = Path("/tmp/config/productions/invalid.yaml")
        mock_glob.return_value = [config_file]
        
        # Mock YAML errors
        mock_yaml_load.side_effect = yaml.YAMLError("Invalid YAML")
        
        registry = ConfigRegistry(Path("/tmp/config"))
        
        # Should skip invalid files
        assert registry._registry == {}
    
    @pytest.mark.unit
    @patch('covariance_mocks.config_registry.Path.exists')
    @patch('covariance_mocks.config_registry.Path.glob')
    @patch('builtins.open', new_callable=mock_open)
    @patch('covariance_mocks.config_registry.yaml.safe_load')
    def test_scan_configs_missing_fields(self, mock_yaml_load, mock_file, mock_glob, mock_exists):
        """Test scanning files with missing required fields."""
        mock_exists.return_value = True
        
        config_file = Path("/tmp/config/productions/incomplete.yaml")
        mock_glob.return_value = [config_file]
        
        # Mock config with missing production name (only field required now)
        mock_yaml_load.return_value = {"production": {}}
        
        registry = ConfigRegistry(Path("/tmp/config"))
        
        # Should skip files missing production name
        assert registry._registry == {}
    
    @pytest.mark.unit
    def test_get_config_path_existing_file(self):
        """Test resolving existing file path."""
        with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False) as tmp:
            tmp_path = Path(tmp.name)
            
            registry = ConfigRegistry(Path("/tmp"))
            result = registry.get_config_path(str(tmp_path))
            
            assert result == tmp_path.resolve()
            tmp_path.unlink()  # Clean up
    
    @pytest.mark.unit
    def test_get_config_path_nonexistent_absolute(self):
        """Test resolving nonexistent absolute path."""
        registry = ConfigRegistry(Path("/tmp"))
        
        with pytest.raises(ConfigurationError, match="Configuration file not found"):
            registry.get_config_path("/nonexistent/path.yaml")
    
    @pytest.mark.unit
    def test_get_config_path_production_name(self):
        """Test resolving production name."""
        registry = ConfigRegistry(Path("/tmp"))
        config_path = Path("/tmp/config/productions/alpha.yaml")
        registry._registry["alpha"] = config_path
        
        result = registry.get_config_path("alpha")
        
        assert result == config_path.resolve()
    
    @pytest.mark.unit
    def test_get_config_path_not_found(self):
        """Test resolving nonexistent production name."""
        registry = ConfigRegistry(Path("/tmp"))
        registry._registry = {"alpha": Path("/tmp/alpha.yaml")}
        
        with pytest.raises(ConfigurationError, match="Production 'nonexistent' not found"):
            registry.get_config_path("nonexistent")
    
    @pytest.mark.unit
    @pytest.mark.skip(reason="Complex path mocking - skip for now")
    def test_get_config_path_relative_from_cwd(self):
        """Test resolving relative path from current directory."""
        pass
    
    @pytest.mark.unit
    @pytest.mark.skip(reason="Complex path mocking - skip for now")
    def test_get_config_path_relative_from_config_dir(self):
        """Test resolving relative path from config directory."""
        pass
    
    @pytest.mark.unit
    def test_list_productions(self):
        """Test listing all productions."""
        registry = ConfigRegistry(Path("/tmp"))
        test_registry = {
            "alpha": Path("/tmp/alpha.yaml"),
            "beta": Path("/tmp/beta.yaml")
        }
        registry._registry = test_registry
        
        result = registry.list_productions()
        
        assert result == test_registry
        # Should return copy, not reference
        assert result is not registry._registry
    
    @pytest.mark.unit
    @patch('covariance_mocks.config_registry.Path.exists')
    @patch('covariance_mocks.config_registry.Path.glob')
    def test_refresh(self, mock_glob, mock_exists):
        """Test refreshing the registry."""
        mock_exists.return_value = True
        mock_glob.return_value = []
        
        registry = ConfigRegistry(Path("/tmp"))
        registry._registry = {"old": Path("/tmp/old.yaml")}
        
        registry.refresh()
        
        # Should clear and rescan
        assert registry._registry == {}


class TestGlobalRegistry:
    """Test global registry functions."""
    
    @pytest.mark.unit
    @patch('covariance_mocks.config_registry.ConfigRegistry')
    def test_get_registry_singleton(self, mock_config_registry):
        """Test that get_registry returns singleton instance."""
        mock_instance = Mock()
        mock_config_registry.return_value = mock_instance
        
        # Clear global registry
        import covariance_mocks.config_registry
        covariance_mocks.config_registry._global_registry = None
        
        result1 = get_registry()
        result2 = get_registry()
        
        assert result1 is result2
        assert result1 is mock_instance
        mock_config_registry.assert_called_once()
    
    @pytest.mark.unit
    @patch('covariance_mocks.config_registry.get_registry')
    def test_resolve_config_convenience(self, mock_get_registry):
        """Test resolve_config convenience function."""
        mock_registry = Mock()
        mock_registry.get_config_path.return_value = Path("/tmp/config.yaml")
        mock_get_registry.return_value = mock_registry
        
        result = resolve_config("alpha")
        
        assert result == Path("/tmp/config.yaml")
        mock_registry.get_config_path.assert_called_once_with("alpha")


class TestConfigRegistryEdgeCases:
    """Test edge cases and error conditions."""
    
    @pytest.mark.unit
    @patch('covariance_mocks.config_registry.Path.exists')
    @patch('covariance_mocks.config_registry.Path.glob')
    @patch('builtins.open', new_callable=mock_open)
    @patch('covariance_mocks.config_registry.yaml.safe_load')
    def test_name_conflict_resolution(self, mock_yaml_load, mock_file, mock_glob, mock_exists):
        """Test handling of name conflicts (last file wins)."""
        mock_exists.return_value = True
        
        config_file1 = Path("/tmp/config/productions/alpha_v1.yaml")
        config_file2 = Path("/tmp/config/productions/alpha_v2.yaml")
        mock_glob.return_value = [config_file1, config_file2]
        
        # Both have same name (versions no longer create separate identifiers)
        mock_yaml_load.side_effect = [
            {"production": {"name": "alpha"}},
            {"production": {"name": "alpha"}}
        ]
        
        registry = ConfigRegistry(Path("/tmp/config"))
        
        # Should only have one entry for "alpha" (last file wins)
        assert "alpha" in registry._registry
        assert registry._registry["alpha"] == config_file2
        assert len(registry._registry) == 1
    
    @pytest.mark.unit
    @patch('covariance_mocks.config_registry.Path.exists')
    @patch('covariance_mocks.config_registry.Path.glob')
    @patch('builtins.open', side_effect=IOError("Cannot read file"))
    def test_io_error_handling(self, mock_file, mock_glob, mock_exists):
        """Test handling of I/O errors during file reading."""
        mock_exists.return_value = True
        
        config_file = Path("/tmp/config/productions/unreadable.yaml")
        mock_glob.return_value = [config_file]
        
        # Should not raise exception, just skip the file
        registry = ConfigRegistry(Path("/tmp/config"))
        
        assert registry._registry == {}
    
    @pytest.mark.unit
    def test_get_config_path_with_yml_extension(self):
        """Test resolving files with .yml extension."""
        with tempfile.NamedTemporaryFile(suffix='.yml', delete=False) as tmp:
            tmp_path = Path(tmp.name)
            
            registry = ConfigRegistry(Path("/tmp"))
            result = registry.get_config_path(str(tmp_path))
            
            assert result == tmp_path.resolve()
            tmp_path.unlink()  # Clean up