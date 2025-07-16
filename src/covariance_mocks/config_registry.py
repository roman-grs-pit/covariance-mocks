"""Configuration registry for mapping production names to config files."""

import os
from pathlib import Path
from typing import Dict, Optional, List
import yaml

from .production_config import ProductionConfigLoader, ConfigurationError


class ConfigRegistry:
    """Registry for mapping production names to configuration files."""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize registry with configuration directory.
        
        Args:
            config_dir: Directory containing config files. Defaults to project config/
        """
        if config_dir is None:
            # Default to project config directory
            project_root = Path(__file__).parent.parent.parent
            config_dir = project_root / "config"
        
        self.config_dir = Path(config_dir)
        self._registry: Dict[str, Path] = {}
        self._scan_configs()
    
    def _scan_configs(self):
        """Scan config directory for production files and build name mapping."""
        self._registry.clear()
        
        # Look for config files in examples directory
        examples_dir = self.config_dir / "examples"
        if not examples_dir.exists():
            return
            
        for config_file in examples_dir.glob("*.yaml"):
            try:
                # Load just the production section to get name and version
                with open(config_file, 'r') as f:
                    config = yaml.safe_load(f)
                
                production = config.get('production', {})
                if 'name' in production and 'version' in production:
                    name = production['name']
                    version = production['version']
                    
                    # Create {version}_{name} identifier
                    production_id = f"{version}_{name}"
                    self._registry[production_id] = config_file
                    
                    # Also register just the name for backwards compatibility
                    # (but {version}_{name} takes precedence if there are conflicts)
                    if name not in self._registry:
                        self._registry[name] = config_file
                    
            except (yaml.YAMLError, IOError, KeyError) as e:
                # Skip files that can't be parsed or don't have required fields
                continue
    
    def get_config_path(self, name_or_path: str) -> Path:
        """Resolve production name or path to config file path.
        
        Args:
            name_or_path: Either a production name or file path
            
        Returns:
            Path to configuration file
            
        Raises:
            ConfigurationError: If name not found or path doesn't exist
        """
        # Check if it's already a path
        path = Path(name_or_path)
        if path.exists() and path.suffix in ['.yaml', '.yml']:
            return path.resolve()
        
        # Check if it's an absolute path that doesn't exist
        if path.is_absolute():
            raise ConfigurationError(f"Configuration file not found: {path}")
        
        # Try as relative path from current directory
        if (Path.cwd() / path).exists():
            return (Path.cwd() / path).resolve()
        
        # Try as relative path from config directory
        config_path = self.config_dir / path
        if config_path.exists():
            return config_path.resolve()
        
        # Finally, try as production name
        if name_or_path in self._registry:
            return self._registry[name_or_path].resolve()
        
        # Not found anywhere
        available = list(self._registry.keys())
        raise ConfigurationError(
            f"Production '{name_or_path}' not found. "
            f"Available productions: {', '.join(available)}"
        )
    
    def list_productions(self) -> Dict[str, Path]:
        """Get all available production names and their config paths.
        
        Returns:
            Dictionary mapping production names to config file paths
        """
        return self._registry.copy()
    
    def refresh(self):
        """Refresh the registry by re-scanning config files."""
        self._scan_configs()


# Global registry instance
_global_registry: Optional[ConfigRegistry] = None


def get_registry() -> ConfigRegistry:
    """Get the global config registry instance."""
    global _global_registry
    if _global_registry is None:
        _global_registry = ConfigRegistry()
    return _global_registry


def resolve_config(name_or_path: str) -> Path:
    """Convenience function to resolve production name or path to config file.
    
    Args:
        name_or_path: Either a production name or file path
        
    Returns:
        Path to configuration file
    """
    return get_registry().get_config_path(name_or_path)