"""Tests for production configuration validation and loading."""

import pytest
import yaml
import tempfile
from pathlib import Path

from covariance_mocks.production_config import (
    ProductionConfigValidator, 
    ProductionConfigLoader,
    ConfigurationError,
    ValidationError
)


@pytest.fixture
def test_config_dir():
    """Create temporary config directory structure."""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_dir = Path(temp_dir)
        
        # Create directory structure
        (config_dir / "schemas").mkdir()
        (config_dir / "config" / "defaults").mkdir(parents=True)
        (config_dir / "examples").mkdir()
        
        # Create minimal schema for testing
        test_schema = config_dir / "schemas" / "production_schema.yaml"
        schema_content = {
            "production": {
                "type": "object",
                "required": ["name", "version"],
                "properties": {
                    "name": {"type": "string", "pattern": "^[a-zA-Z][a-zA-Z0-9_-]*$"},
                    "version": {"type": "string"}
                }
            },
            "science": {
                "type": "object",
                "required": ["redshifts"],
                "properties": {
                    "redshifts": {"type": "array", "items": {"type": "number"}}
                }
            }
        }
        with open(test_schema, 'w') as f:
            yaml.dump(schema_content, f)
        
        # Create test defaults
        defaults_content = {
            "resources": {
                "account": "test_account",
                "partition": "test_partition"
            },
            "execution": {
                "job_type": "balanced",
                "batch_size": 10
            }
        }
        
        with open(config_dir / "config" / "defaults" / "test_machine.yaml", 'w') as f:
            yaml.dump(defaults_content, f)
        
        yield config_dir


@pytest.fixture
def validator(test_config_dir):
    """Create validator with test schema."""
    schema_path = test_config_dir / "schemas" / "production_schema.yaml"
    return ProductionConfigValidator(schema_path)


@pytest.fixture
def config_loader(test_config_dir):
    """Create config loader with test directory."""
    loader = ProductionConfigLoader(test_config_dir)
    # Use test schema instead of production schema
    schema_path = test_config_dir / "schemas" / "production_schema.yaml"
    loader.validator = ProductionConfigValidator(schema_path)
    return loader


class TestProductionConfigValidator:
    """Test configuration validation."""
    
    def test_valid_config_passes(self, validator):
        """Test that valid configuration passes validation."""
        config = {
            "production": {
                "name": "test_campaign",
                "version": "v1.0",
                "description": "Test campaign for validation testing"
            },
            "science": {
                "cosmology": "AbacusSummit",
                "redshifts": [1.0, 2.0],
                "realizations": {
                    "start": 0,
                    "count": 10
                }
            },
            "execution": {
                "job_type": "balanced",
                "batch_size": 5
            },
            "outputs": {
                "base_path": "/tmp/test_campaign"
            }
        }
        
        errors = validator.validate(config)
        assert len(errors) == 0
    
    def test_missing_required_section_fails(self, validator):
        """Test that missing required section fails validation."""
        config = {
            "production": {
                "name": "test_campaign",
                "version": "v1.0"
            }
            # Missing required 'science' section
        }
        
        errors = validator.validate(config)
        assert len(errors) > 0
        assert any("science" in error.message for error in errors)
    
    def test_invalid_name_pattern_fails(self, validator):
        """Test that invalid name pattern fails validation."""
        config = {
            "production": {
                "name": "123invalid",  # Starts with number
                "version": "v1.0"
            },
            "science": {
                "redshifts": [1.0, 2.0]
            }
        }
        
        errors = validator.validate(config)
        assert len(errors) > 0
        assert any("pattern" in error.message for error in errors)
    
    def test_invalid_type_fails(self, validator):
        """Test that invalid types fail validation."""
        config = {
            "production": {
                "name": "test_campaign",
                "version": "v1.0"
            },
            "science": {
                "redshifts": "not_an_array"  # Should be array
            }
        }
        
        errors = validator.validate(config)
        assert len(errors) > 0
        assert any("array" in error.message for error in errors)


class TestProductionConfigLoader:
    """Test configuration loading and merging."""
    
    def test_load_production_config_success(self, config_loader, test_config_dir):
        """Test successful configuration loading."""
        # Create test production config
        production_config = {
            "production": {
                "name": "test_campaign",
                "version": "v1.0"
            },
            "science": {
                "redshifts": [1.0, 2.0]
            },
            "execution": {
                "batch_size": 20  # Override default
            }
        }
        
        config_path = test_config_dir / "test_campaign.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(production_config, f)
        
        # Load and validate
        merged_config = config_loader.load_production_config(config_path, "test_machine")
        
        # Check that defaults were merged
        assert merged_config["resources"]["account"] == "test_account"
        assert merged_config["execution"]["job_type"] == "balanced"
        
        # Check that campaign overrides were applied
        assert merged_config["execution"]["batch_size"] == 20
        
        # Check that campaign-specific values are preserved
        assert merged_config["production"]["name"] == "test_campaign"
        assert merged_config["science"]["redshifts"] == [1.0, 2.0]
    
    def test_missing_machine_defaults_fails(self, config_loader, test_config_dir):
        """Test that missing machine defaults fails."""
        production_config = {
            "production": {
                "name": "test_campaign",
                "version": "v1.0"
            },
            "science": {
                "redshifts": [1.0, 2.0]
            }
        }
        
        config_path = test_config_dir / "test_campaign.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(production_config, f)
        
        with pytest.raises(ConfigurationError, match="No defaults found"):
            config_loader.load_production_config(config_path, "nonexistent_machine")
    
    def test_invalid_production_config_fails(self, config_loader, test_config_dir):
        """Test that invalid production config fails validation."""
        production_config = {
            "production": {
                "name": "123invalid",  # Invalid name pattern
                "version": "v1.0"
            },
            "science": {
                "redshifts": "not_an_array"  # Invalid type
            }
        }
        
        config_path = test_config_dir / "test_campaign.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(production_config, f)
        
        with pytest.raises(ConfigurationError, match="Configuration validation failed"):
            config_loader.load_production_config(config_path, "test_machine")


@pytest.mark.unit
class TestRealConfigFiles:
    """Test with actual configuration files from the repository."""
    
    def test_real_schema_loads(self):
        """Test that real schema file loads without errors."""
        repo_root = Path(__file__).parent.parent
        schema_path = repo_root / "config" / "schemas" / "production_schema.yaml"
        
        if schema_path.exists():
            validator = ProductionConfigValidator(schema_path)
            assert validator.schema is not None
    
    def test_real_defaults_load(self):
        """Test that real defaults files load without errors."""
        repo_root = Path(__file__).parent.parent
        defaults_path = repo_root / "config" / "defaults" / "nersc.yaml"
        
        if defaults_path.exists():
            loader = ProductionConfigLoader(repo_root)
            defaults = loader._load_machine_defaults("nersc")
            assert "resources" in defaults
            assert "execution" in defaults
    
    def test_example_configs_validate(self):
        """Test that example configurations validate successfully."""
        repo_root = Path(__file__).parent.parent
        examples_dir = repo_root / "config" / "examples"
        
        if examples_dir.exists():
            loader = ProductionConfigLoader(repo_root)
            
            for config_file in examples_dir.glob("*.yaml"):
                try:
                    merged_config = loader.load_production_config(config_file, "nersc")
                    assert "production" in merged_config
                    assert "science" in merged_config
                except ConfigurationError as e:
                    pytest.fail(f"Example config {config_file.name} failed validation: {e}")