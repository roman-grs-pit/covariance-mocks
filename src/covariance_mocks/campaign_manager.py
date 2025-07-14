"""Campaign management system for batch job orchestration.

This module provides the CampaignManager class for managing large-scale
campaigns with thousands of individual jobs, including job tracking,
failure recovery, and batch execution via SLURM.
"""

import os
import sqlite3
import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Iterator, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
import subprocess

from .campaign_config import CampaignConfigLoader, ConfigurationError


class JobStatus(Enum):
    """Job execution status."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


@dataclass
class JobSpec:
    """Individual job specification."""
    job_id: str
    realization: int
    redshift: float
    output_path: str
    status: JobStatus = JobStatus.PENDING
    submit_count: int = 0
    slurm_job_id: Optional[int] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()


@dataclass
class BatchSpec:
    """SLURM batch specification."""
    batch_id: str
    job_ids: List[str]
    slurm_array_id: Optional[int] = None
    status: JobStatus = JobStatus.PENDING
    created_at: Optional[str] = None
    submitted_at: Optional[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()


class JobDatabase:
    """SQLite database for persistent job tracking."""
    
    def __init__(self, db_path: Path):
        """Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    realization INTEGER NOT NULL,
                    redshift REAL NOT NULL,
                    output_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    submit_count INTEGER DEFAULT 0,
                    slurm_job_id INTEGER,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    error_message TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    job_ids TEXT NOT NULL,
                    slurm_array_id INTEGER,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    submitted_at TEXT
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_realization ON jobs(realization)
            """)
    
    def insert_job(self, job: JobSpec):
        """Insert job into database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO jobs 
                (job_id, realization, redshift, output_path, status, submit_count,
                 slurm_job_id, created_at, started_at, completed_at, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job.job_id, job.realization, job.redshift, job.output_path,
                job.status.value, job.submit_count, job.slurm_job_id,
                job.created_at, job.started_at, job.completed_at, job.error_message
            ))
    
    def insert_batch(self, batch: BatchSpec):
        """Insert batch into database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO batches
                (batch_id, job_ids, slurm_array_id, status, created_at, submitted_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                batch.batch_id, json.dumps(batch.job_ids), batch.slurm_array_id,
                batch.status.value, batch.created_at, batch.submitted_at
            ))
    
    def get_jobs_by_status(self, status: JobStatus) -> List[JobSpec]:
        """Get all jobs with specified status."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT job_id, realization, redshift, output_path, status, submit_count,
                       slurm_job_id, created_at, started_at, completed_at, error_message
                FROM jobs WHERE status = ?
                ORDER BY created_at
            """, (status.value,))
            
            jobs = []
            for row in cursor:
                jobs.append(JobSpec(
                    job_id=row[0],
                    realization=row[1],
                    redshift=row[2],
                    output_path=row[3],
                    status=JobStatus(row[4]),
                    submit_count=row[5],
                    slurm_job_id=row[6],
                    created_at=row[7],
                    started_at=row[8],
                    completed_at=row[9],
                    error_message=row[10]
                ))
            
            return jobs
    
    def update_job_status(self, job_id: str, status: JobStatus, **kwargs):
        """Update job status and optional fields."""
        fields = {"status": status.value}
        fields.update(kwargs)
        
        set_clause = ", ".join(f"{key} = ?" for key in fields.keys())
        values = list(fields.values()) + [job_id]
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(f"UPDATE jobs SET {set_clause} WHERE job_id = ?", values)
    
    def get_campaign_stats(self) -> Dict[str, int]:
        """Get campaign statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT status, COUNT(*) FROM jobs GROUP BY status
            """)
            
            stats = {status.value: 0 for status in JobStatus}
            for status, count in cursor:
                stats[status] = count
            
            return stats


class CampaignManager:
    """Manages campaign execution with batch job orchestration."""
    
    def __init__(self, campaign_config_path: Path, machine: str = "nersc", 
                 work_dir: Optional[Path] = None):
        """Initialize campaign manager.
        
        Args:
            campaign_config_path: Path to campaign configuration file
            machine: Machine name for defaults
            work_dir: Working directory for campaign files (default: auto-generated)
        """
        self.config_path = Path(campaign_config_path)
        self.machine = machine
        
        # Load and validate configuration
        self.config_loader = CampaignConfigLoader()
        self.config = self.config_loader.load_campaign_config(campaign_config_path, machine)
        
        # Set up working directory
        if work_dir is None:
            campaign_name = self.config["campaign"]["name"]
            campaign_version = self.config["campaign"]["version"]
            base_path = Path(self.config["outputs"]["base_path"])
            work_dir = base_path / f"{campaign_version}_{campaign_name}"
        
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize job database
        db_path = self.work_dir / "campaign.db"
        self.job_db = JobDatabase(db_path)
        
        # Create output subdirectories
        self.catalogs_dir = self.work_dir / "catalogs"
        self.metadata_dir = self.work_dir / "metadata"
        self.logs_dir = self.work_dir / "logs"
        self.qa_dir = self.work_dir / "qa"
        
        for directory in [self.catalogs_dir, self.metadata_dir, self.logs_dir, self.qa_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Save campaign configuration
        config_file = self.metadata_dir / "campaign_config.yaml"
        with open(config_file, 'w') as f:
            import yaml
            yaml.dump(self.config, f, default_flow_style=False)
    
    def initialize_campaign(self) -> int:
        """Initialize campaign by creating all job specifications.
        
        Returns:
            Number of jobs created
        """
        jobs_created = 0
        
        science_config = self.config["science"]
        realizations = science_config["realizations"]
        
        for realization in range(realizations["start"], 
                                realizations["start"] + realizations["count"], 
                                realizations.get("step", 1)):
            for redshift in science_config["redshifts"]:
                job_id = f"r{realization:04d}_z{redshift:.3f}"
                
                # Generate output path
                if self.config["outputs"]["structure"] == "hierarchical":
                    output_path = self.catalogs_dir / f"r{realization:04d}" / f"mock_z{redshift:.3f}.hdf5"
                else:
                    output_path = self.catalogs_dir / f"mock_r{realization:04d}_z{redshift:.3f}.hdf5"
                
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                job = JobSpec(
                    job_id=job_id,
                    realization=realization,
                    redshift=redshift,
                    output_path=str(output_path)
                )
                
                self.job_db.insert_job(job)
                jobs_created += 1
        
        return jobs_created
    
    def submit_pending_jobs(self) -> List[str]:
        """Submit pending jobs in batches to SLURM.
        
        Returns:
            List of submitted batch IDs
        """
        pending_jobs = self.job_db.get_jobs_by_status(JobStatus.PENDING)
        if not pending_jobs:
            return []
        
        batch_size = self.config["execution"]["batch_size"]
        submitted_batches = []
        
        # Group jobs into batches
        for i in range(0, len(pending_jobs), batch_size):
            batch_jobs = pending_jobs[i:i + batch_size]
            batch_id = f"batch_{i//batch_size:04d}_{int(time.time())}"
            
            # Create batch specification
            batch = BatchSpec(
                batch_id=batch_id,
                job_ids=[job.job_id for job in batch_jobs]
            )
            
            # Submit SLURM array job
            try:
                slurm_job_id = self._submit_slurm_batch(batch, batch_jobs)
                batch.slurm_array_id = slurm_job_id
                batch.status = JobStatus.QUEUED
                batch.submitted_at = datetime.utcnow().isoformat()
                
                # Update job statuses
                for job in batch_jobs:
                    self.job_db.update_job_status(
                        job.job_id, 
                        JobStatus.QUEUED,
                        submit_count=job.submit_count + 1
                    )
                
                submitted_batches.append(batch_id)
                
            except Exception as e:
                # Mark batch as failed
                batch.status = JobStatus.FAILED
                for job in batch_jobs:
                    self.job_db.update_job_status(
                        job.job_id,
                        JobStatus.FAILED,
                        error_message=str(e)
                    )
            
            self.job_db.insert_batch(batch)
        
        return submitted_batches
    
    def _submit_slurm_batch(self, batch: BatchSpec, jobs: List[JobSpec]) -> int:
        """Submit a batch of jobs as SLURM array job.
        
        Args:
            batch: Batch specification
            jobs: List of jobs in the batch
            
        Returns:
            SLURM job ID
        """
        # Create job script
        script_path = self.logs_dir / f"{batch.batch_id}.sh"
        
        resources = self.config["resources"]
        execution = self.config["execution"]
        
        script_content = f"""#!/bin/bash
#SBATCH --job-name={batch.batch_id}
#SBATCH --account={resources["account"]}
#SBATCH --partition={resources["partition"]}
#SBATCH --constraint={resources["constraint"]}
#SBATCH --nodes={resources["nodes_per_job"]}
#SBATCH --ntasks-per-node={resources["tasks_per_node"]}
#SBATCH --cpus-per-task={resources["cpus_per_task"]}
#SBATCH --mem={resources["memory_gb"]}GB
#SBATCH --time={execution["timeout_hours"]:02.0f}:00:00
#SBATCH --array=0-{len(jobs)-1}
#SBATCH --output={self.logs_dir}/{batch.batch_id}_%a.out
#SBATCH --error={self.logs_dir}/{batch.batch_id}_%a.err

# Load environment
source {Path(__file__).parent.parent.parent / "scripts" / "load_env.sh"}

# Job array mapping
declare -a JOB_IDS=(
"""
        
        for job in jobs:
            script_content += f'    "{job.job_id}"\n'
        
        script_content += f""")

declare -a REALIZATIONS=(
"""
        
        for job in jobs:
            script_content += f'    "{job.realization}"\n'
        
        script_content += f""")

declare -a REDSHIFTS=(
"""
        
        for job in jobs:
            script_content += f'    "{job.redshift}"\n'
        
        script_content += f""")

declare -a OUTPUT_PATHS=(
"""
        
        for job in jobs:
            script_content += f'    "{job.output_path}"\n'
        
        script_content += f""")

# Get job parameters for this array task
JOB_ID="${{JOB_IDS[$SLURM_ARRAY_TASK_ID]}}"
REALIZATION="${{REALIZATIONS[$SLURM_ARRAY_TASK_ID]}}"
REDSHIFT="${{REDSHIFTS[$SLURM_ARRAY_TASK_ID]}}"
OUTPUT_PATH="${{OUTPUT_PATHS[$SLURM_ARRAY_TASK_ID]}}"

echo "Starting job $JOB_ID: realization $REALIZATION, redshift $REDSHIFT"

# Run the mock generation
python {Path(__file__).parent.parent.parent / "scripts" / "generate_single_mock.py"} \\
    {self.machine} \\
    "$OUTPUT_PATH" \\
    --realization "$REALIZATION" \\
    --redshift "$REDSHIFT"

EXIT_CODE=$?

echo "Job $JOB_ID completed with exit code $EXIT_CODE"
exit $EXIT_CODE
"""
        
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Submit to SLURM
        result = subprocess.run(
            ["sbatch", str(script_path)],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Parse job ID from sbatch output
        output_lines = result.stdout.strip().split('\n')
        for line in output_lines:
            if "Submitted batch job" in line:
                return int(line.split()[-1])
        
        raise RuntimeError(f"Could not parse SLURM job ID from: {result.stdout}")
    
    def check_job_status(self) -> Dict[str, int]:
        """Check status of all jobs and update database.
        
        Returns:
            Updated campaign statistics
        """
        # Query SLURM for running jobs
        try:
            result = subprocess.run(
                ["squeue", "-u", os.getenv("USER", ""), "--format=%i,%T", "--noheader"],
                capture_output=True,
                text=True,
                check=True
            )
            
            slurm_jobs = {}
            for line in result.stdout.strip().split('\n'):
                if line:
                    job_id, status = line.split(',')
                    slurm_jobs[int(job_id)] = status
                    
        except (subprocess.CalledProcessError, ValueError):
            slurm_jobs = {}
        
        # Update job statuses based on SLURM state
        queued_jobs = self.job_db.get_jobs_by_status(JobStatus.QUEUED)
        running_jobs = self.job_db.get_jobs_by_status(JobStatus.RUNNING)
        
        for job in queued_jobs + running_jobs:
            if job.slurm_job_id and job.slurm_job_id in slurm_jobs:
                slurm_status = slurm_jobs[job.slurm_job_id]
                
                if slurm_status in ["RUNNING", "R"]:
                    if job.status == JobStatus.QUEUED:
                        self.job_db.update_job_status(
                            job.job_id,
                            JobStatus.RUNNING,
                            started_at=datetime.utcnow().isoformat()
                        )
                elif slurm_status in ["COMPLETED", "CD"]:
                    # Check if output file exists to confirm completion
                    if Path(job.output_path).exists():
                        self.job_db.update_job_status(
                            job.job_id,
                            JobStatus.COMPLETED,
                            completed_at=datetime.utcnow().isoformat()
                        )
                    else:
                        self.job_db.update_job_status(
                            job.job_id,
                            JobStatus.FAILED,
                            error_message="Output file not found after SLURM completion"
                        )
                elif slurm_status in ["FAILED", "F", "TIMEOUT", "TO", "CANCELLED", "CA"]:
                    self.job_db.update_job_status(
                        job.job_id,
                        JobStatus.FAILED,
                        error_message=f"SLURM status: {slurm_status}"
                    )
            elif job.slurm_job_id:
                # Job not in SLURM queue anymore, check output
                if Path(job.output_path).exists():
                    self.job_db.update_job_status(
                        job.job_id,
                        JobStatus.COMPLETED,
                        completed_at=datetime.utcnow().isoformat()
                    )
                else:
                    self.job_db.update_job_status(
                        job.job_id,
                        JobStatus.FAILED,
                        error_message="Job disappeared from SLURM queue without output"
                    )
        
        return self.job_db.get_campaign_stats()
    
    def retry_failed_jobs(self) -> int:
        """Retry failed jobs according to retry policy.
        
        Returns:
            Number of jobs marked for retry
        """
        failed_jobs = self.job_db.get_jobs_by_status(JobStatus.FAILED)
        retry_policy = self.config["execution"]["retry_policy"]
        max_retries = retry_policy["max_retries"]
        
        retried_count = 0
        
        for job in failed_jobs:
            if job.submit_count <= max_retries:
                # Mark job for retry
                self.job_db.update_job_status(
                    job.job_id,
                    JobStatus.PENDING,
                    error_message=None
                )
                retried_count += 1
        
        return retried_count
    
    def get_campaign_summary(self) -> Dict[str, Any]:
        """Get comprehensive campaign summary.
        
        Returns:
            Campaign summary with statistics and configuration
        """
        stats = self.job_db.get_campaign_stats()
        total_jobs = sum(stats.values())
        
        completed = stats.get("completed", 0)
        failed = stats.get("failed", 0)
        
        summary = {
            "campaign": self.config["campaign"],
            "work_dir": str(self.work_dir),
            "statistics": {
                "total_jobs": total_jobs,
                "completed": completed,
                "failed": failed,
                "success_rate": completed / total_jobs if total_jobs > 0 else 0.0,
                "status_breakdown": stats
            },
            "configuration": {
                "machine": self.machine,
                "batch_size": self.config["execution"]["batch_size"],
                "job_type": self.config["execution"]["job_type"],
                "redshifts": self.config["science"]["redshifts"],
                "realizations": self.config["science"]["realizations"]
            }
        }
        
        return summary