#!/usr/bin/env python3
"""Command-line interface for production management."""

import sys
import argparse
from pathlib import Path

# Import the existing CLI functions
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))
from run_production import (
    initialize_production, stage_jobs, submit_jobs, check_status,
    retry_failed, monitor_production
)

from .config_registry import resolve_config, get_registry
from .production_config import ConfigurationError


def list_productions(args):
    """List all available productions."""
    try:
        registry = get_registry()
        productions = registry.list_productions()
        
        if not productions:
            print("No productions found in config/examples/")
            return 0
        
        print("Available productions:")
        for name, config_path in sorted(productions.items()):
            rel_path = config_path.relative_to(Path.cwd()) if config_path.is_relative_to(Path.cwd()) else config_path
            print(f"  {name:20} -> {rel_path}")
        
        return 0
        
    except Exception as e:
        print(f"Error listing productions: {e}", file=sys.stderr)
        return 1


def wrap_with_config_resolution(func):
    """Wrapper to resolve config names to paths before calling original function."""
    def wrapper(args):
        try:
            # Resolve production name to config path
            config_path = resolve_config(args.config)
            # Replace the config argument with resolved path
            args.config = config_path
            return func(args)
        except ConfigurationError as e:
            print(f"Configuration error: {e}", file=sys.stderr)
            return 1
    return wrapper


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Production management for covariance mock generation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available productions
  production-manager list

  # Use production name (recommended)
  production-manager status alpha
  production-manager monitor alpha --interval 30
  production-manager init test_basic

  # Or use full config path
  production-manager status config/examples/alpha_production.yaml
  production-manager monitor config/examples/alpha_production.yaml --verbose
        """
    )
    
    parser.add_argument(
        "--machine",
        default="nersc",
        help="Machine configuration to use (default: nersc)"
    )
    
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="Override work directory (default: auto-generated from config)"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # List command
    list_parser = subparsers.add_parser("list", help="List available productions")
    list_parser.set_defaults(func=list_productions)
    
    # Initialize command
    init_parser = subparsers.add_parser("init", help="Initialize new production")
    init_parser.add_argument("config", help="Production name or configuration file path")
    init_parser.set_defaults(func=wrap_with_config_resolution(initialize_production))
    
    # Stage command
    stage_parser = subparsers.add_parser("stage", help="Stage pending jobs")
    stage_parser.add_argument("config", help="Production name or configuration file path")
    stage_parser.set_defaults(func=wrap_with_config_resolution(stage_jobs))
    
    # Submit command
    submit_parser = subparsers.add_parser("submit", help="Submit staged jobs")
    submit_parser.add_argument("config", help="Production name or configuration file path")
    submit_parser.set_defaults(func=wrap_with_config_resolution(submit_jobs))
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Check production status")
    status_parser.add_argument("config", help="Production name or configuration file path")
    status_parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed information")
    status_parser.set_defaults(func=wrap_with_config_resolution(check_status))
    
    # Retry command
    retry_parser = subparsers.add_parser("retry", help="Retry failed jobs")
    retry_parser.add_argument("config", help="Production name or configuration file path")
    retry_parser.add_argument("--submit", action="store_true", help="Submit retry jobs immediately")
    retry_parser.set_defaults(func=wrap_with_config_resolution(retry_failed))
    
    # Monitor command
    monitor_parser = subparsers.add_parser("monitor", help="Monitor production progress")
    monitor_parser.add_argument("config", help="Production name or configuration file path")
    monitor_parser.add_argument("--interval", type=int, default=60, help="Update interval in seconds (default: 60)")
    monitor_parser.set_defaults(func=wrap_with_config_resolution(monitor_production))
    
    args = parser.parse_args()
    
    if not hasattr(args, 'func'):
        parser.print_help()
        return 1
    
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())