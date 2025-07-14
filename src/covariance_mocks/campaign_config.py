"""Campaign configuration validation and management.

This module provides YAML schema validation for campaign configurations
and hierarchical configuration loading with machine-specific defaults.
"""

import os
import re
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass


class ConfigurationError(Exception):
    """Raised when configuration validation fails."""
    pass


@dataclass
class ValidationError:
    """Individual validation error details."""
    path: str
    message: str
    value: Any = None


class CampaignConfigValidator:
    """Validates campaign configuration against schema."""
    
    def __init__(self, schema_path: Optional[Path] = None):
        """Initialize validator with schema file.
        
        Args:
            schema_path: Path to schema YAML file. If None, uses default.
        """
        if schema_path is None:
            # Default to schema in same repository
            repo_root = Path(__file__).parent.parent.parent
            schema_path = repo_root / "config" / "schemas" / "campaign_schema.yaml"
        
        self.schema_path = schema_path
        self.schema = self._load_schema()
    
    def _load_schema(self) -> Dict[str, Any]:
        """Load and parse schema file."""
        try:
            with open(self.schema_path) as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise ConfigurationError(f"Failed to load schema from {self.schema_path}: {e}")
    
    def validate(self, config: Dict[str, Any]) -> List[ValidationError]:
        """Validate configuration against schema.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Validate top-level sections
        for section_name, section_schema in self.schema.items():
            if section_name in config:
                section_errors = self._validate_section(
                    config[section_name], 
                    section_schema, 
                    section_name
                )
                errors.extend(section_errors)
            elif section_schema.get("required", False):
                errors.append(ValidationError(
                    path=section_name,
                    message=f"Required section '{section_name}' is missing"
                ))
        
        return errors
    
    def _validate_section(self, data: Any, schema: Dict[str, Any], path: str) -> List[ValidationError]:
        """Validate a configuration section."""
        errors = []
        
        if schema.get("type") == "object":
            if not isinstance(data, dict):
                errors.append(ValidationError(
                    path=path,
                    message=f"Expected object, got {type(data).__name__}",
                    value=data
                ))
                return errors
            
            # Check required properties
            required = schema.get("required", [])
            for req_prop in required:
                if req_prop not in data:
                    errors.append(ValidationError(
                        path=f"{path}.{req_prop}",
                        message=f"Required property '{req_prop}' is missing"
                    ))
            
            # Validate properties
            properties = schema.get("properties", {})
            for prop_name, prop_value in data.items():
                if prop_name in properties:
                    prop_errors = self._validate_property(
                        prop_value,
                        properties[prop_name],
                        f"{path}.{prop_name}"
                    )
                    errors.extend(prop_errors)
        
        return errors
    
    def _validate_property(self, value: Any, prop_schema: Dict[str, Any], path: str) -> List[ValidationError]:
        """Validate individual property."""
        errors = []
        
        prop_type = prop_schema.get("type")
        
        # Type validation
        if prop_type == "string":
            if not isinstance(value, str):
                errors.append(ValidationError(
                    path=path,
                    message=f"Expected string, got {type(value).__name__}",
                    value=value
                ))
                return errors
            
            # Pattern validation
            if "pattern" in prop_schema:
                pattern = prop_schema["pattern"]
                if not re.match(pattern, value):
                    errors.append(ValidationError(
                        path=path,
                        message=f"Value '{value}' does not match pattern '{pattern}'",
                        value=value
                    ))
            
            # Length validation
            if "minLength" in prop_schema and len(value) < prop_schema["minLength"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"String too short (min {prop_schema['minLength']})",
                    value=value
                ))
            
            if "maxLength" in prop_schema and len(value) > prop_schema["maxLength"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"String too long (max {prop_schema['maxLength']})",
                    value=value
                ))
            
            # Enum validation
            if "enum" in prop_schema and value not in prop_schema["enum"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"Value '{value}' not in allowed values: {prop_schema['enum']}",
                    value=value
                ))
        
        elif prop_type == "integer":
            if not isinstance(value, int):
                errors.append(ValidationError(
                    path=path,
                    message=f"Expected integer, got {type(value).__name__}",
                    value=value
                ))
                return errors
            
            # Range validation
            if "minimum" in prop_schema and value < prop_schema["minimum"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"Value {value} below minimum {prop_schema['minimum']}",
                    value=value
                ))
            
            if "maximum" in prop_schema and value > prop_schema["maximum"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"Value {value} above maximum {prop_schema['maximum']}",
                    value=value
                ))
        
        elif prop_type == "number":
            if not isinstance(value, (int, float)):
                errors.append(ValidationError(
                    path=path,
                    message=f"Expected number, got {type(value).__name__}",
                    value=value
                ))
                return errors
            
            # Range validation
            if "minimum" in prop_schema and value < prop_schema["minimum"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"Value {value} below minimum {prop_schema['minimum']}",
                    value=value
                ))
            
            if "maximum" in prop_schema and value > prop_schema["maximum"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"Value {value} above maximum {prop_schema['maximum']}",
                    value=value
                ))
        
        elif prop_type == "array":
            if not isinstance(value, list):
                errors.append(ValidationError(
                    path=path,
                    message=f"Expected array, got {type(value).__name__}",
                    value=value
                ))
                return errors
            
            # Array length validation
            if "minItems" in prop_schema and len(value) < prop_schema["minItems"]:
                errors.append(ValidationError(
                    path=path,
                    message=f"Array too short (min {prop_schema['minItems']} items)",
                    value=value
                ))
            
            # Item validation
            if "items" in prop_schema:
                for i, item in enumerate(value):
                    item_errors = self._validate_property(
                        item,
                        prop_schema["items"],
                        f"{path}[{i}]"
                    )
                    errors.extend(item_errors)
        
        elif prop_type == "object":
            errors.extend(self._validate_section(value, prop_schema, path))
        
        return errors


class CampaignConfigLoader:
    """Loads and merges campaign configurations with defaults."""
    
    def __init__(self, repo_root: Optional[Path] = None):
        """Initialize loader with repository root.
        
        Args:
            repo_root: Path to repository root. If None, auto-detects.
        """
        if repo_root is None:
            repo_root = Path(__file__).parent.parent.parent
        
        self.repo_root = Path(repo_root)
        self.defaults_dir = self.repo_root / "config" / "defaults"
        self.validator = CampaignConfigValidator()
    
    def load_campaign_config(self, config_path: Union[str, Path], machine: str = "nersc") -> Dict[str, Any]:
        """Load and validate campaign configuration.
        
        Args:
            config_path: Path to campaign configuration file
            machine: Machine name for defaults (default: "nersc")
            
        Returns:
            Merged and validated configuration dictionary
            
        Raises:
            ConfigurationError: If validation fails
        """
        # Load machine defaults
        machine_defaults = self._load_machine_defaults(machine)
        
        # Load campaign configuration
        campaign_config = self._load_yaml_file(config_path)
        
        # Merge configurations (campaign overrides defaults)
        merged_config = self._merge_configs(machine_defaults, campaign_config)
        
        # Apply job type overrides if specified
        if "job_type_overrides" in machine_defaults and "execution" in merged_config:
            job_type = merged_config["execution"].get("job_type")
            if job_type and job_type in machine_defaults["job_type_overrides"]:
                overrides = machine_defaults["job_type_overrides"][job_type]
                if "resources" not in merged_config:
                    merged_config["resources"] = {}
                merged_config["resources"].update(overrides)
        
        # Validate merged configuration
        errors = self.validator.validate(merged_config)
        if errors:
            error_msg = "Configuration validation failed:\n" + "\n".join(
                f"  {error.path}: {error.message}" for error in errors
            )
            raise ConfigurationError(error_msg)
        
        return merged_config
    
    def _load_machine_defaults(self, machine: str) -> Dict[str, Any]:
        """Load machine-specific default configuration."""
        defaults_path = self.defaults_dir / f"{machine}.yaml"
        if not defaults_path.exists():
            raise ConfigurationError(f"No defaults found for machine '{machine}' at {defaults_path}")
        
        return self._load_yaml_file(defaults_path)
    
    def _load_yaml_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """Load YAML file with error handling."""
        try:
            with open(file_path) as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise ConfigurationError(f"Failed to load YAML file {file_path}: {e}")
    
    def _merge_configs(self, defaults: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge configuration dictionaries."""
        result = defaults.copy()
        
        for key, value in overrides.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        
        return result


def validate_campaign_config(config_path: Union[str, Path], machine: str = "nersc") -> Dict[str, Any]:
    """Convenience function to load and validate campaign configuration.
    
    Args:
        config_path: Path to campaign configuration file
        machine: Machine name for defaults
        
    Returns:
        Validated configuration dictionary
        
    Raises:
        ConfigurationError: If validation fails
    """
    loader = CampaignConfigLoader()
    return loader.load_campaign_config(config_path, machine)