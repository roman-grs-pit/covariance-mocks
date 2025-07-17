"""Production management system for batch job orchestration.

This module provides the ProductionManager class for managing large-scale
productions with thousands of individual jobs, including job tracking,
failure recovery, and batch execution via SLURM.
"""

import os
import sqlite3
import json
import time
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Optional, Iterator, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
import subprocess

from .production_config import ProductionConfigLoader, ConfigurationError


class JobStatus(Enum):
    """Job execution status."""
    PENDING = "pending"
    STAGED = "staged"
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
    
    def get_batches_by_status(self, status: JobStatus) -> List[BatchSpec]:
        """Get all batches with specified status."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT batch_id, job_ids, slurm_array_id, status, created_at, submitted_at
                FROM batches WHERE status = ?
                ORDER BY created_at
            """, (status.value,))
            
            batches = []
            for row in cursor:
                batches.append(BatchSpec(
                    batch_id=row[0],
                    job_ids=json.loads(row[1]),
                    slurm_array_id=row[2],
                    status=JobStatus(row[3]),
                    created_at=row[4],
                    submitted_at=row[5]
                ))
            
            return batches
    
    def update_batch_status(self, batch_id: str, status: JobStatus, slurm_array_id: Optional[int] = None):
        """Update batch status and optionally SLURM array ID."""
        with sqlite3.connect(self.db_path) as conn:
            if slurm_array_id is not None:
                conn.execute("""
                    UPDATE batches 
                    SET status = ?, slurm_array_id = ?, submitted_at = ?
                    WHERE batch_id = ?
                """, (status.value, slurm_array_id, datetime.utcnow().isoformat(), batch_id))
            else:
                conn.execute("""
                    UPDATE batches 
                    SET status = ?
                    WHERE batch_id = ?
                """, (status.value, batch_id))
    
    def update_job_status(self, job_id: str, status: JobStatus, **kwargs):
        """Update job status and optional fields."""
        fields = {"status": status.value}
        fields.update(kwargs)
        
        set_clause = ", ".join(f"{key} = ?" for key in fields.keys())
        values = list(fields.values()) + [job_id]
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(f"UPDATE jobs SET {set_clause} WHERE job_id = ?", values)
    
    def get_production_stats(self) -> Dict[str, int]:
        """Get production statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT status, COUNT(*) FROM jobs GROUP BY status
            """)
            
            stats = {status.value: 0 for status in JobStatus}
            for status, count in cursor:
                stats[status] = count
            
            return stats


class ProductionManager:
    """Manages production execution with batch job orchestration."""
    
    def __init__(self, production_config_path: Path, machine: str = "nersc", 
                 work_dir: Optional[Path] = None, dry_run: bool = False, 
                 version: Optional[str] = None, allow_dirty: bool = False):
        """Initialize production manager.
        
        Args:
            production_config_path: Path to production configuration file
            machine: Machine name for defaults
            work_dir: Working directory for production files (default: auto-generated)
            dry_run: If True, create scripts and directories but don't submit to SLURM
            version: Runtime version override (e.g., 'v1.0', 'v2.1')
            allow_dirty: If True, allow tagging with uncommitted changes
        """
        self.config_path = Path(production_config_path)
        self.machine = machine
        self.dry_run = dry_run
        self.allow_dirty = allow_dirty
        
        # Load and validate configuration
        self.config_loader = ProductionConfigLoader()
        self.config = self.config_loader.load_production_config(production_config_path, machine)
        
        # Handle runtime version (CLI override or config default or fallback)
        self.production_version = (
            version or 
            self.config.get("production", {}).get("version") or 
            "v1.0"
        )
        
        # Set up working directory
        if work_dir is None:
            production_name = self.config["production"]["name"]
            base_path = Path(self.config["outputs"]["base_path"])
            work_dir = base_path / "productions" / f"{self.production_version}_{production_name}"
        
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize job database
        db_path = self.work_dir / "production.db"
        self.job_db = JobDatabase(db_path)
        
        # Create output subdirectories
        self.catalogs_dir = self.work_dir / "catalogs"
        self.metadata_dir = self.work_dir / "metadata"
        self.logs_dir = self.work_dir / "logs"
        self.qa_dir = self.work_dir / "qa"
        
        for directory in [self.catalogs_dir, self.metadata_dir, self.logs_dir, self.qa_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Save production configuration
        config_file = self.metadata_dir / "production_config.yaml"
        with open(config_file, 'w') as f:
            import yaml
            yaml.dump(self.config, f, default_flow_style=False)
        
        # Log dependency version information
        self._log_dependency_versions()
        
        # Create git tag for this production (unless dry run)
        if not dry_run:
            self._create_production_tag()
    
    def _log_dependency_versions(self):
        """Log dependency version information for reproducibility."""
        dependencies = self.config.get("production", {}).get("dependencies", {})
        
        if "rgrspit_diffsky" in dependencies:
            print(f"Production initialized with rgrspit_diffsky version: {dependencies['rgrspit_diffsky']}")
        
        # Log to metadata file
        version_info = {
            "production_version": self.production_version,
            "dependencies": dependencies,
            "initialized_at": datetime.utcnow().isoformat()
        }
        
        # Save version info to metadata directory
        version_file = self.metadata_dir / "version_info.json"
        with open(version_file, 'w') as f:
            json.dump(version_info, f, indent=2)
    
    def _check_working_tree_status(self):
        """Check git working tree status and return detailed status information.
        
        Returns:
            Dict with status information:
            - is_clean: bool, True if working tree is clean
            - modified_files: List of modified files
            - staged_files: List of staged files
            - untracked_files: List of untracked files
            - status_summary: String summary of status
        """
        try:
            # Get git status in porcelain format
            result = subprocess.run(
                ["git", "status", "--porcelain"],
                capture_output=True,
                text=True,
                check=True
            )
            
            status_lines = result.stdout.strip().split('\n') if result.stdout.strip() else []
            
            modified_files = []
            staged_files = []
            untracked_files = []
            
            for line in status_lines:
                if len(line) < 3:
                    continue
                    
                status_code = line[:2]
                filename = line[3:]
                
                # Parse git status codes
                if status_code[0] in ['M', 'A', 'D', 'R', 'C']:  # Staged changes
                    staged_files.append(filename)
                if status_code[1] in ['M', 'D']:  # Modified in working tree
                    modified_files.append(filename)
                if status_code == '??':  # Untracked
                    untracked_files.append(filename)
            
            is_clean = not (modified_files or staged_files or untracked_files)
            
            # Generate status summary
            if is_clean:
                status_summary = "Working tree clean"
            else:
                parts = []
                if staged_files:
                    parts.append(f"{len(staged_files)} staged")
                if modified_files:
                    parts.append(f"{len(modified_files)} modified")
                if untracked_files:
                    parts.append(f"{len(untracked_files)} untracked")
                status_summary = f"Working tree dirty: {', '.join(parts)} files"
            
            return {
                'is_clean': is_clean,
                'modified_files': modified_files,
                'staged_files': staged_files,
                'untracked_files': untracked_files,
                'status_summary': status_summary
            }
            
        except subprocess.CalledProcessError:
            # If git status fails, assume we're not in a repo or other issue
            return {
                'is_clean': True,  # Default to clean if we can't determine status
                'modified_files': [],
                'staged_files': [],
                'untracked_files': [],
                'status_summary': "Git status unavailable"
            }
    
    def _create_production_tag(self):
        """Create a git tag for this production initialization."""
        try:
            # Check if we're in a git repository
            result = subprocess.run(
                ["git", "rev-parse", "--git-dir"],
                capture_output=True,
                text=True,
                check=True
            )
        except subprocess.CalledProcessError:
            print("Warning: Not in a git repository, skipping tag creation")
            return
        
        # Check working tree status
        git_status = self._check_working_tree_status()
        
        # Handle dirty working tree based on policy
        if not git_status['is_clean']:
            if not self.allow_dirty:
                print(f"ERROR: {git_status['status_summary']}")
                print("Production tagging requires a clean working tree for reproducibility.")
                print("Options:")
                print("  1. Commit or stash your changes")
                print("  2. Use --allow-dirty flag to force tagging with dirty tree")
                print("Skipping tag creation.")
                return
            else:
                print(f"WARNING: {git_status['status_summary']}")
                print("Creating tag with dirty working tree - reproducibility may be compromised!")
        
        # Generate tag components
        production_name = self.config["production"]["name"]
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Create base tag name
        base_tag = f"production/{production_name}_{self.production_version}_{timestamp}"
        tag_name = base_tag
        
        # Check for tag conflicts and handle them
        counter = 1
        while self._tag_exists(tag_name):
            tag_name = f"{base_tag}_{counter:02d}"
            counter += 1
            if counter > 99:  # Safety limit
                print(f"Warning: Too many tag conflicts for {base_tag}, skipping tag creation")
                return
        
        # Create tag message with production metadata
        tag_message = self._generate_tag_message(git_status)
        
        try:
            # Create the git tag
            subprocess.run(
                ["git", "tag", "-a", tag_name, "-m", tag_message],
                capture_output=True,
                text=True,
                check=True
            )
            print(f"Created git tag: {tag_name}")
            
            # Save tag name to metadata
            tag_file = self.metadata_dir / "git_tag.txt"
            with open(tag_file, 'w') as f:
                f.write(tag_name)
                
        except subprocess.CalledProcessError as e:
            print(f"Warning: Failed to create git tag {tag_name}: {e.stderr}")
    
    def _tag_exists(self, tag_name: str) -> bool:
        """Check if a git tag already exists."""
        try:
            subprocess.run(
                ["git", "rev-parse", "--verify", f"refs/tags/{tag_name}"],
                capture_output=True,
                text=True,
                check=True
            )
            return True
        except subprocess.CalledProcessError:
            return False
    
    def _generate_tag_message(self, git_status: Dict[str, Any]) -> str:
        """Generate a comprehensive tag message with production metadata."""
        production = self.config["production"]
        science = self.config["science"]
        
        # Calculate config file hash for reproducibility
        config_hash = self._calculate_config_hash()
        
        # Get current git commit
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                check=True
            )
            commit_hash = result.stdout.strip()[:8]
        except subprocess.CalledProcessError:
            commit_hash = "unknown"
        
        # Build working tree status section
        if not git_status['is_clean']:
            dirty_details = []
            if git_status['staged_files']:
                dirty_details.append(f"  Staged files ({len(git_status['staged_files'])}): {', '.join(git_status['staged_files'][:5])}")
                if len(git_status['staged_files']) > 5:
                    dirty_details.append(f"    ... and {len(git_status['staged_files']) - 5} more")
            if git_status['modified_files']:
                dirty_details.append(f"  Modified files ({len(git_status['modified_files'])}): {', '.join(git_status['modified_files'][:5])}")
                if len(git_status['modified_files']) > 5:
                    dirty_details.append(f"    ... and {len(git_status['modified_files']) - 5} more")
            if git_status['untracked_files']:
                # Filter out common development files
                relevant_untracked = [f for f in git_status['untracked_files'] 
                                     if not f.startswith('.') and not f.endswith(('.log', '.tmp', '.pyc'))]
                if relevant_untracked:
                    dirty_details.append(f"  Untracked files ({len(relevant_untracked)}): {', '.join(relevant_untracked[:5])}")
                    if len(relevant_untracked) > 5:
                        dirty_details.append(f"    ... and {len(relevant_untracked) - 5} more")
            
            working_tree_section = f"\nWORKING TREE STATUS: {git_status['status_summary']}\n" + "\n".join(dirty_details)
            if not git_status['is_clean']:
                working_tree_section += "\nWARNING: This production was created with uncommitted changes!"
                working_tree_section += "\nReproducibility may be compromised. Ensure changes are committed for exact reproduction."
        else:
            working_tree_section = f"\nWORKING TREE STATUS: {git_status['status_summary']}"

        message = f"""Production: {production['name']} {self.production_version}

Description: {production.get('description', 'No description provided')}

Configuration:
- Redshifts: {len(science['redshifts'])} values ({min(science['redshifts']):.3f} to {max(science['redshifts']):.3f})
- Realizations: {science['realizations']['count']} ({science['realizations']['start']} to {science['realizations']['start'] + science['realizations']['count'] - 1})
- Total jobs: {len(science['redshifts']) * science['realizations']['count']}
{working_tree_section}

Technical details:
- Config file: {self.config_path.name}
- Config hash: {config_hash}
- Machine: {self.machine}
- Git commit: {commit_hash}
- Initialized: {datetime.utcnow().isoformat()}Z
- Work directory: {self.work_dir}"""
        
        return message
    
    def _calculate_config_hash(self) -> str:
        """Calculate SHA256 hash of the configuration file for reproducibility."""
        try:
            with open(self.config_path, 'rb') as f:
                content = f.read()
            return hashlib.sha256(content).hexdigest()[:12]
        except Exception:
            return "unknown"
    
    def initialize_production(self) -> int:
        """Initialize production by creating all job specifications.
        
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
    
    def submit_staged_jobs(self) -> List[str]:
        """Submit staged jobs to SLURM.
        
        Returns:
            List of submitted batch IDs
        """
        staged_jobs = self.job_db.get_jobs_by_status(JobStatus.STAGED)
        if not staged_jobs:
            return []
        
        # Get existing staged batches from database
        staged_batches = self.job_db.get_batches_by_status(JobStatus.STAGED)
        submitted_batches = []
        
        for batch in staged_batches:
            batch_jobs = [job for job in staged_jobs if job.job_id in batch.job_ids]
            if not batch_jobs:
                continue
                
            # Submit existing SLURM script
            try:
                script_path = self.logs_dir / f"{batch.batch_id}.sh"
                if not script_path.exists():
                    raise FileNotFoundError(f"Batch script not found: {script_path}")
                
                slurm_job_id = self._submit_slurm_batch_script(script_path)
                
                # Update batch status
                batch.slurm_array_id = slurm_job_id
                batch.status = JobStatus.QUEUED
                batch.submitted_at = datetime.utcnow().isoformat()
                self.job_db.update_batch_status(batch.batch_id, JobStatus.QUEUED, slurm_job_id)
                
                # Update job statuses
                for job in batch_jobs:
                    self.job_db.update_job_status(
                        job.job_id, 
                        JobStatus.QUEUED,
                        submit_count=job.submit_count + 1
                    )
                
                submitted_batches.append(batch.batch_id)
                print(f"Submitted batch {batch.batch_id} as SLURM job {slurm_job_id}")
                
            except Exception as e:
                # Mark batch as failed
                batch.status = JobStatus.FAILED
                self.job_db.update_batch_status(batch.batch_id, JobStatus.FAILED)
                for job in batch_jobs:
                    self.job_db.update_job_status(
                        job.job_id,
                        JobStatus.FAILED,
                        error_message=str(e)
                    )
                print(f"Failed to submit batch {batch.batch_id}: {e}")
        
        return submitted_batches
    
    def submit_pending_jobs(self) -> List[str]:
        """Legacy method: Submit pending jobs in batches to SLURM.
        
        This method stages jobs and submits them in one operation.
        For better control, use stage_jobs() followed by submit_staged_jobs().
        
        Returns:
            List of submitted batch IDs
        """
        staged_batches = self.stage_jobs()
        if not staged_batches:
            return []
        return self.submit_staged_jobs()
    
    def stage_jobs(self) -> List[str]:
        """Stage pending jobs by creating SLURM scripts and directories.
        
        Returns:
            List of staged batch IDs
        """
        pending_jobs = self.job_db.get_jobs_by_status(JobStatus.PENDING)
        if not pending_jobs:
            return []
        
        batch_size = self.config["execution"]["batch_size"]
        staged_batches = []
        
        # Group jobs into batches
        for i in range(0, len(pending_jobs), batch_size):
            batch_jobs = pending_jobs[i:i + batch_size]
            batch_id = f"batch_{i//batch_size:04d}_{int(time.time())}"
            
            # Create batch specification
            batch = BatchSpec(
                batch_id=batch_id,
                job_ids=[job.job_id for job in batch_jobs]
            )
            
            # Create SLURM script (but don't submit)
            try:
                script_path = self._create_slurm_batch_script(batch, batch_jobs)
                batch.status = JobStatus.STAGED
                batch.created_at = datetime.utcnow().isoformat()
                
                # Update job statuses to STAGED
                for job in batch_jobs:
                    self.job_db.update_job_status(
                        job.job_id, 
                        JobStatus.STAGED
                    )
                
                # Save batch to database
                self.job_db.insert_batch(batch)
                staged_batches.append(batch_id)
                
                print(f"Staged batch {batch_id} with {len(batch_jobs)} jobs")
                
            except Exception as e:
                # Mark batch as failed
                batch.status = JobStatus.FAILED
                for job in batch_jobs:
                    self.job_db.update_job_status(
                        job.job_id,
                        JobStatus.FAILED,
                        error_message=str(e)
                    )
                print(f"Failed to stage batch {batch_id}: {e}")
        
        return staged_batches
    
    def _create_slurm_batch_script(self, batch: BatchSpec, jobs: List[JobSpec]) -> Path:
        """Create SLURM batch script for a set of jobs.
        
        Args:
            batch: Batch specification
            jobs: List of jobs in the batch
            
        Returns:
            Path to created script file
        """
        script_path = self.logs_dir / f"{batch.batch_id}.sh"
        
        resources = self.config["resources"]
        execution = self.config["execution"]
        
        # Generate simple script for single jobs, job array for multiple jobs
        if len(jobs) == 1:
            job = jobs[0]
            script_content = f"""#!/bin/bash
#SBATCH --job-name={batch.batch_id}
#SBATCH --account={resources["account"]}
#SBATCH --qos=regular
#SBATCH --constraint={resources["constraint"]}
#SBATCH --nodes={resources["nodes_per_job"]}
#SBATCH --ntasks-per-node={resources["tasks_per_node"]}
#SBATCH --cpus-per-task={resources["cpus_per_task"]}"""
            # Only add GPU directive if GPUs are configured
            if "gpus_per_node" in resources and resources["gpus_per_node"] > 0:
                script_content += f"\n#SBATCH --gpus-per-node={resources['gpus_per_node']}"
            script_content += f"""
#SBATCH --time={int(execution["timeout_hours"] * 60):02d}:00
#SBATCH --output={self.logs_dir}/{batch.batch_id}.out
#SBATCH --error={self.logs_dir}/{batch.batch_id}.err

# Load environment
source {Path(__file__).parent.parent.parent / "scripts" / "load_env.sh"}

echo "Starting job {job.job_id}: realization {job.realization}, redshift {job.redshift}"

# Run the mock generation with MPI
srun -n 8 python {Path(__file__).parent.parent.parent / "scripts" / "generate_single_mock.py"} \\
    {self.machine} \\
    "{job.output_path}" \\
    --realization "{job.realization}" \\
    --redshift "{job.redshift}"

EXIT_CODE=$?

echo "Job {job.job_id} completed with exit code $EXIT_CODE"
exit $EXIT_CODE
"""
        else:
            script_content = f"""#!/bin/bash
#SBATCH --job-name={batch.batch_id}
#SBATCH --account={resources["account"]}
#SBATCH --qos=regular
#SBATCH --constraint={resources["constraint"]}
#SBATCH --nodes={resources["nodes_per_job"]}
#SBATCH --ntasks-per-node={resources["tasks_per_node"]}
#SBATCH --cpus-per-task={resources["cpus_per_task"]}"""
            # Only add GPU directive if GPUs are configured
            if "gpus_per_node" in resources and resources["gpus_per_node"] > 0:
                script_content += f"\n#SBATCH --gpus-per-node={resources['gpus_per_node']}"
            script_content += f"""
#SBATCH --time={int(execution["timeout_hours"] * 60):02d}:00
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
        
        # Make script executable
        script_path.chmod(0o755)
        
        return script_path
    
    def _submit_slurm_batch_script(self, script_path: Path) -> int:
        """Submit an existing SLURM batch script.
        
        Args:
            script_path: Path to the batch script to submit
            
        Returns:
            SLURM job ID
        """
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
    
    def _submit_slurm_batch(self, batch: BatchSpec, jobs: List[JobSpec]) -> int:
        """Submit a batch of jobs as SLURM array job (legacy method).
        
        Args:
            batch: Batch specification
            jobs: List of jobs in the batch
            
        Returns:
            SLURM job ID
        """
        script_path = self._create_slurm_batch_script(batch, jobs)
        return self._submit_slurm_batch_script(script_path)
    
    def check_job_status(self) -> Dict[str, int]:
        """Check status of all jobs and update database.
        
        Returns:
            Updated production statistics
        """
        # First, ensure we have jobs in the database
        stats = self.job_db.get_production_stats()
        total_jobs = sum(stats.values())
        
        if total_jobs == 0:
            # Database is empty, need to reinitialize
            print("Warning: No jobs found in database, reinitializing...")
            self.initialize_production()
            stats = self.job_db.get_production_stats()
        
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
        
        # Check all jobs in database for completion based on output files
        all_jobs = []
        for status in [JobStatus.PENDING, JobStatus.STAGED, JobStatus.QUEUED, JobStatus.RUNNING]:
            all_jobs.extend(self.job_db.get_jobs_by_status(status))
        
        for job in all_jobs:
            # Check if output file exists
            if Path(job.output_path).exists():
                # Job completed - update status
                if job.status != JobStatus.COMPLETED:
                    self.job_db.update_job_status(
                        job.job_id,
                        JobStatus.COMPLETED,
                        completed_at=datetime.utcnow().isoformat()
                    )
                continue
            
            # No output file - check SLURM status if we have job ID
            if job.slurm_job_id and job.slurm_job_id in slurm_jobs:
                slurm_status = slurm_jobs[job.slurm_job_id]
                
                if slurm_status in ["RUNNING", "R"]:
                    if job.status == JobStatus.QUEUED:
                        self.job_db.update_job_status(
                            job.job_id,
                            JobStatus.RUNNING,
                            started_at=datetime.utcnow().isoformat()
                        )
                elif slurm_status in ["FAILED", "F", "TIMEOUT", "TO", "CANCELLED", "CA"]:
                    self.job_db.update_job_status(
                        job.job_id,
                        JobStatus.FAILED,
                        error_message=f"SLURM status: {slurm_status}"
                    )
            elif job.slurm_job_id and job.slurm_job_id not in slurm_jobs:
                # Job not in SLURM queue anymore and no output file - failed
                if job.status in [JobStatus.QUEUED, JobStatus.RUNNING]:
                    self.job_db.update_job_status(
                        job.job_id,
                        JobStatus.FAILED,
                        error_message="Job disappeared from SLURM queue without output"
                    )
        
        return self.job_db.get_production_stats()
    
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
    
    def get_production_summary(self) -> Dict[str, Any]:
        """Get comprehensive production summary.
        
        Returns:
            Production summary with statistics and configuration
        """
        stats = self.job_db.get_production_stats()
        total_jobs = sum(stats.values())
        
        completed = stats.get("completed", 0)
        failed = stats.get("failed", 0)
        
        summary = {
            "production": self.config["production"],
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