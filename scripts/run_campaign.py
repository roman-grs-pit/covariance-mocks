#!/usr/bin/env python3
"""Campaign runner script for launching and managing large-scale campaigns.

This script provides a command-line interface for the campaign management system,
allowing users to initialize, submit, monitor, and retry campaign jobs.
"""

import sys
import argparse
import json
import time
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from covariance_mocks.campaign_manager import CampaignManager, JobStatus
from covariance_mocks.campaign_config import ConfigurationError


def initialize_campaign(args):
    """Initialize a new campaign."""
    try:
        manager = CampaignManager(args.config, args.machine, args.work_dir)
        
        print(f"Initializing campaign from {args.config}")
        print(f"Machine: {args.machine}")
        print(f"Work directory: {manager.work_dir}")
        
        jobs_created = manager.initialize_campaign()
        print(f"Created {jobs_created} jobs")
        
        # Save campaign info
        summary = manager.get_campaign_summary()
        print(f"\nCampaign Summary:")
        print(f"  Name: {summary['campaign']['name']}")
        print(f"  Version: {summary['campaign']['version']}")
        print(f"  Description: {summary['campaign']['description']}")
        print(f"  Total jobs: {summary['statistics']['total_jobs']}")
        
        return 0
        
    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error initializing campaign: {e}", file=sys.stderr)
        return 1


def submit_jobs(args):
    """Submit pending jobs to SLURM."""
    try:
        manager = CampaignManager(args.config, args.machine, args.work_dir)
        
        print(f"Submitting pending jobs for campaign in {manager.work_dir}")
        
        submitted_batches = manager.submit_pending_jobs()
        
        if submitted_batches:
            print(f"Submitted {len(submitted_batches)} batches:")
            for batch_id in submitted_batches:
                print(f"  - {batch_id}")
        else:
            print("No pending jobs to submit")
        
        # Show updated statistics
        stats = manager.check_job_status()
        print(f"\nCampaign Status:")
        for status, count in stats.items():
            if count > 0:
                print(f"  {status}: {count}")
        
        return 0
        
    except Exception as e:
        print(f"Error submitting jobs: {e}", file=sys.stderr)
        return 1


def check_status(args):
    """Check campaign status."""
    try:
        manager = CampaignManager(args.config, args.machine, args.work_dir)
        
        print(f"Checking status for campaign in {manager.work_dir}")
        
        stats = manager.check_job_status()
        summary = manager.get_campaign_summary()
        
        print(f"\nCampaign: {summary['campaign']['name']} {summary['campaign']['version']}")
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
        manager = CampaignManager(args.config, args.machine, args.work_dir)
        
        print(f"Retrying failed jobs for campaign in {manager.work_dir}")
        
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


def monitor_campaign(args):
    """Monitor campaign progress continuously."""
    try:
        manager = CampaignManager(args.config, args.machine, args.work_dir)
        
        print(f"Monitoring campaign in {manager.work_dir}")
        print(f"Update interval: {args.interval} seconds")
        print("Press Ctrl+C to stop monitoring\n")
        
        try:
            while True:
                stats = manager.check_job_status()
                summary = manager.get_campaign_summary()
                
                # Clear screen and show status
                print("\033[2J\033[H", end="")  # Clear screen
                print(f"Campaign Monitor - {time.strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 60)
                print(f"Campaign: {summary['campaign']['name']} {summary['campaign']['version']}")
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
        print(f"Error monitoring campaign: {e}", file=sys.stderr)
        return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Campaign management for covariance mock generation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Initialize a new campaign
  python run_campaign.py init config/examples/test_campaign.yaml

  # Submit pending jobs
  python run_campaign.py submit config/examples/test_campaign.yaml

  # Check campaign status
  python run_campaign.py status config/examples/test_campaign.yaml --verbose

  # Retry failed jobs and submit immediately
  python run_campaign.py retry config/examples/test_campaign.yaml --submit

  # Monitor campaign progress
  python run_campaign.py monitor config/examples/test_campaign.yaml --interval 30
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
    init_parser = subparsers.add_parser("init", help="Initialize new campaign")
    init_parser.add_argument("config", type=Path, help="Campaign configuration file")
    init_parser.set_defaults(func=initialize_campaign)
    
    # Submit command
    submit_parser = subparsers.add_parser("submit", help="Submit pending jobs")
    submit_parser.add_argument("config", type=Path, help="Campaign configuration file")
    submit_parser.set_defaults(func=submit_jobs)
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Check campaign status")
    status_parser.add_argument("config", type=Path, help="Campaign configuration file")
    status_parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed information")
    status_parser.set_defaults(func=check_status)
    
    # Retry command
    retry_parser = subparsers.add_parser("retry", help="Retry failed jobs")
    retry_parser.add_argument("config", type=Path, help="Campaign configuration file")
    retry_parser.add_argument("--submit", action="store_true", help="Submit retry jobs immediately")
    retry_parser.set_defaults(func=retry_failed)
    
    # Monitor command
    monitor_parser = subparsers.add_parser("monitor", help="Monitor campaign progress")
    monitor_parser.add_argument("config", type=Path, help="Campaign configuration file")
    monitor_parser.add_argument("--interval", type=int, default=60, help="Update interval in seconds (default: 60)")
    monitor_parser.set_defaults(func=monitor_campaign)
    
    args = parser.parse_args()
    
    if not hasattr(args, 'func'):
        parser.print_help()
        return 1
    
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())