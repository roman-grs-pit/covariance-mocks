#!/usr/bin/env python3
"""Production runner script for launching and managing large-scale productions.

This script provides a command-line interface for the production management system,
allowing users to initialize, submit, monitor, and retry production jobs.
"""

import sys
import argparse
import json
import time
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from covariance_mocks.production_manager import ProductionManager, JobStatus
from covariance_mocks.production_config import ConfigurationError


def initialize_production(args):
    """Initialize a new production."""
    try:
        manager = ProductionManager(args.config, args.machine, args.work_dir)
        
        print(f"Initializing production from {args.config}")
        print(f"Machine: {args.machine}")
        print(f"Work directory: {manager.work_dir}")
        
        jobs_created = manager.initialize_production()
        print(f"Created {jobs_created} jobs")
        
        # Save production info
        summary = manager.get_production_summary()
        print(f"\nProduction Summary:")
        print(f"  Name: {summary['production']['name']}")
        print(f"  Version: {summary['production']['version']}")
        print(f"  Description: {summary['production']['description']}")
        print(f"  Total jobs: {summary['statistics']['total_jobs']}")
        
        return 0
        
    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error initializing production: {e}", file=sys.stderr)
        return 1


def stage_jobs(args):
    """Stage pending jobs by creating SLURM scripts."""
    try:
        manager = ProductionManager(args.config, args.machine, args.work_dir)
        
        print(f"Staging pending jobs for production in {manager.work_dir}")
        
        staged_batches = manager.stage_jobs()
        
        if staged_batches:
            print(f"Staged {len(staged_batches)} batches:")
            for batch_id in staged_batches:
                print(f"  - {batch_id}")
            print(f"\nGenerated SLURM scripts in: {manager.logs_dir}")
            print("Review scripts before submitting with: python run_production.py submit")
        else:
            print("No pending jobs to stage")
        
        # Show updated statistics
        stats = manager.check_job_status()
        print(f"\nProduction Status:")
        for status, count in stats.items():
            if count > 0:
                print(f"  {status}: {count}")
        
        return 0
        
    except Exception as e:
        print(f"Error staging jobs: {e}", file=sys.stderr)
        return 1


def submit_jobs(args):
    """Submit staged jobs to SLURM."""
    try:
        manager = ProductionManager(args.config, args.machine, args.work_dir)
        
        print(f"Submitting staged jobs for production in {manager.work_dir}")
        
        submitted_batches = manager.submit_staged_jobs()
        
        if submitted_batches:
            print(f"Submitted {len(submitted_batches)} batches:")
            for batch_id in submitted_batches:
                print(f"  - {batch_id}")
        else:
            print("No staged jobs to submit")
            print("Stage jobs first with: python run_production.py stage")
        
        # Show updated statistics
        stats = manager.check_job_status()
        print(f"\nProduction Status:")
        for status, count in stats.items():
            if count > 0:
                print(f"  {status}: {count}")
        
        return 0
        
    except Exception as e:
        print(f"Error submitting jobs: {e}", file=sys.stderr)
        return 1


def check_status(args):
    """Check production status."""
    try:
        manager = ProductionManager(args.config, args.machine, args.work_dir)
        
        print(f"Checking status for production in {manager.work_dir}")
        
        stats = manager.check_job_status()
        summary = manager.get_production_summary()
        
        print(f"\nProduction: {summary['production']['name']} {summary['production']['version']}")
        print(f"Path: {manager.work_dir}")
        print(f"Total jobs: {summary['statistics']['total_jobs']}")
        print(f"Success rate: {summary['statistics']['success_rate']:.1%}")
        
        print(f"\nStatus breakdown:")
        for status, count in stats.items():
            if count > 0:
                print(f"  {status}: {count}")
        
        if args.verbose:
            print(f"\nConfiguration:")
            print(f"  Machine: {summary['configuration']['machine']}")
            print(f"  Job type: {summary['configuration']['job_type']}")
            print(f"  Batch size: {summary['configuration']['batch_size']}")
            print(f"  Redshifts: {len(summary['configuration']['redshifts'])}")
            print(f"  Realizations: {summary['configuration']['realizations']['count']}")
        
        return 0
        
    except Exception as e:
        print(f"Error checking status: {e}", file=sys.stderr)
        return 1


def retry_failed(args):
    """Retry failed jobs."""
    try:
        manager = ProductionManager(args.config, args.machine, args.work_dir)
        
        print(f"Retrying failed jobs for production in {manager.work_dir}")
        
        retried_count = manager.retry_failed_jobs()
        
        if retried_count > 0:
            print(f"Marked {retried_count} failed jobs for retry")
            
            # Optionally submit immediately
            if args.submit:
                print("Submitting retry jobs...")
                submitted_batches = manager.submit_pending_jobs()
                print(f"Submitted {len(submitted_batches)} retry batches")
        else:
            print("No failed jobs to retry")
        
        return 0
        
    except Exception as e:
        print(f"Error retrying jobs: {e}", file=sys.stderr)
        return 1


def monitor_production(args):
    """Monitor production progress continuously."""
    try:
        manager = ProductionManager(args.config, args.machine, args.work_dir)
        
        print(f"Monitoring production in {manager.work_dir}")
        print(f"Update interval: {args.interval} seconds")
        print("Press Ctrl+C to stop monitoring\n")
        
        try:
            while True:
                stats = manager.check_job_status()
                summary = manager.get_production_summary()
                
                # Clear screen and show status
                print("\033[2J\033[H", end="")  # Clear screen
                print(f"Production Monitor - {time.strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 60)
                print(f"Production: {summary['production']['name']} {summary['production']['version']}")
                print(f"Path: {manager.work_dir}")
                print(f"Total jobs: {summary['statistics']['total_jobs']}")
                print(f"Success rate: {summary['statistics']['success_rate']:.1%}")
                print()
                
                for status, count in stats.items():
                    if count > 0:
                        print(f"  {status.upper()}: {count}")
                
                print("\n" + "=" * 60)
                print(f"Next update in {args.interval} seconds...")
                
                time.sleep(args.interval)
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
            return 0
        
    except Exception as e:
        print(f"Error monitoring production: {e}", file=sys.stderr)
        return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Production management for covariance mock generation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize a new production
  python run_production.py init config/examples/test_production.yaml

  # Stage pending jobs (create scripts)
  python run_production.py stage config/examples/test_production.yaml

  # Submit staged jobs to SLURM
  python run_production.py submit config/examples/test_production.yaml

  # Check production status
  python run_production.py status config/examples/test_production.yaml --verbose

  # Retry failed jobs and submit immediately
  python run_production.py retry config/examples/test_production.yaml --submit

  # Monitor production progress
  python run_production.py monitor config/examples/test_production.yaml --interval 30
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
    
    # Initialize command
    init_parser = subparsers.add_parser("init", help="Initialize new production")
    init_parser.add_argument("config", type=Path, help="Production configuration file")
    init_parser.set_defaults(func=initialize_production)
    
    # Stage command
    stage_parser = subparsers.add_parser("stage", help="Stage pending jobs")
    stage_parser.add_argument("config", type=Path, help="Production configuration file")
    stage_parser.set_defaults(func=stage_jobs)
    
    # Submit command
    submit_parser = subparsers.add_parser("submit", help="Submit staged jobs")
    submit_parser.add_argument("config", type=Path, help="Production configuration file")
    submit_parser.set_defaults(func=submit_jobs)
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Check production status")
    status_parser.add_argument("config", type=Path, help="Production configuration file")
    status_parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed information")
    status_parser.set_defaults(func=check_status)
    
    # Retry command
    retry_parser = subparsers.add_parser("retry", help="Retry failed jobs")
    retry_parser.add_argument("config", type=Path, help="Production configuration file")
    retry_parser.add_argument("--submit", action="store_true", help="Submit retry jobs immediately")
    retry_parser.set_defaults(func=retry_failed)
    
    # Monitor command
    monitor_parser = subparsers.add_parser("monitor", help="Monitor production progress")
    monitor_parser.add_argument("config", type=Path, help="Production configuration file")
    monitor_parser.add_argument("--interval", type=int, default=60, help="Update interval in seconds (default: 60)")
    monitor_parser.set_defaults(func=monitor_production)
    
    args = parser.parse_args()
    
    if not hasattr(args, 'func'):
        parser.print_help()
        return 1
    
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())