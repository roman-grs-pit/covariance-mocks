"""Tests for production manager functionality."""

import pytest
import tempfile
import sqlite3
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock

from covariance_mocks.production_manager import (
    ProductionManager,
    JobDatabase,
    JobSpec,
    BatchSpec,
    JobStatus
)


@pytest.fixture
def test_production_config():
    """Create test production configuration."""
    return {
        "production": {
            "name": "test_production",
            "version": "v1.0",
            "description": "Test production for unit testing"
        },
        "science": {
            "cosmology": "AbacusSummit",
            "redshifts": [1.0, 2.0],
            "realizations": {
                "start": 0,
                "count": 4,
                "step": 1
            }
        },
        "execution": {
            "job_type": "balanced",
            "batch_size": 2,
            "timeout_hours": 1.0,
            "retry_policy": {
                "max_retries": 2,
                "backoff_multiplier": 1.5,
                "initial_delay_minutes": 1.0
            }
        },
        "resources": {
            "account": "test_account",
            "partition": "test_partition",
            "constraint": "cpu",
            "nodes_per_job": 1,
            "tasks_per_node": 4,
            "cpus_per_task": 1,
            "memory_gb": 8.0
        },
        "outputs": {
            "base_path": "/tmp/test_productions",
            "structure": "hierarchical",
            "compression": "gzip"
        }
    }


@pytest.fixture
def temp_config_file(test_production_config):
    """Create temporary configuration file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(test_production_config, f)
        yield Path(f.name)
    
    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def temp_work_dir():
    """Create temporary work directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


class TestJobDatabase:
    """Test job database functionality."""
    
    def test_database_initialization(self, temp_work_dir):
        """Test database creation and schema initialization."""
        db_path = temp_work_dir / "test.db"
        db = JobDatabase(db_path)
        
        # Check that database file was created
        assert db_path.exists()
        
        # Check that tables were created
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cursor}
            assert "jobs" in tables
            assert "batches" in tables
    
    def test_job_insert_and_retrieve(self, temp_work_dir):
        """Test inserting and retrieving jobs."""
        db_path = temp_work_dir / "test.db"
        db = JobDatabase(db_path)
        
        # Create test job
        job = JobSpec(
            job_id="test_job_001",
            realization=0,
            redshift=1.0,
            output_path="/tmp/test_output.hdf5"
        )
        
        # Insert job
        db.insert_job(job)
        
        # Retrieve jobs by status
        pending_jobs = db.get_jobs_by_status(JobStatus.PENDING)
        assert len(pending_jobs) == 1
        assert pending_jobs[0].job_id == "test_job_001"
        assert pending_jobs[0].realization == 0
        assert pending_jobs[0].redshift == 1.0
    
    def test_job_status_update(self, temp_work_dir):
        """Test updating job status."""
        db_path = temp_work_dir / "test.db"
        db = JobDatabase(db_path)
        
        # Insert job
        job = JobSpec(
            job_id="test_job_001",
            realization=0,
            redshift=1.0,
            output_path="/tmp/test_output.hdf5"
        )
        db.insert_job(job)
        
        # Update status
        db.update_job_status("test_job_001", JobStatus.RUNNING, slurm_job_id=12345)
        
        # Check update
        running_jobs = db.get_jobs_by_status(JobStatus.RUNNING)
        assert len(running_jobs) == 1
        assert running_jobs[0].slurm_job_id == 12345
    
    def test_production_stats(self, temp_work_dir):
        """Test production statistics calculation."""
        db_path = temp_work_dir / "test.db"
        db = JobDatabase(db_path)
        
        # Insert jobs with different statuses
        jobs = [
            JobSpec("job_001", 0, 1.0, "/tmp/out1.hdf5", JobStatus.PENDING),
            JobSpec("job_002", 1, 1.0, "/tmp/out2.hdf5", JobStatus.RUNNING),
            JobSpec("job_003", 2, 1.0, "/tmp/out3.hdf5", JobStatus.COMPLETED),
            JobSpec("job_004", 3, 1.0, "/tmp/out4.hdf5", JobStatus.FAILED)
        ]
        
        for job in jobs:
            db.insert_job(job)
        
        # Get statistics
        stats = db.get_production_stats()
        assert stats["pending"] == 1
        assert stats["running"] == 1
        assert stats["completed"] == 1
        assert stats["failed"] == 1


class TestProductionManager:
    """Test production manager functionality."""
    
    @patch('covariance_mocks.production_manager.ProductionConfigLoader')
    def test_manager_initialization(self, mock_loader, temp_config_file, temp_work_dir, test_production_config):
        """Test production manager initialization."""
        # Mock config loader
        mock_loader_instance = MagicMock()
        mock_loader_instance.load_production_config.return_value = test_production_config
        mock_loader.return_value = mock_loader_instance
        
        # Create manager
        manager = ProductionManager(temp_config_file, "test_machine", temp_work_dir)
        
        # Check initialization
        assert manager.work_dir == temp_work_dir
        assert manager.machine == "test_machine"
        assert manager.config == test_production_config
        
        # Check directory creation
        assert (temp_work_dir / "catalogs").exists()
        assert (temp_work_dir / "metadata").exists()
        assert (temp_work_dir / "logs").exists()
        assert (temp_work_dir / "qa").exists()
        
        # Check config file save
        config_file = temp_work_dir / "metadata" / "production_config.yaml"
        assert config_file.exists()
    
    @patch('covariance_mocks.production_manager.ProductionConfigLoader')
    def test_production_initialization(self, mock_loader, temp_config_file, temp_work_dir, test_production_config):
        """Test production job creation."""
        # Mock config loader
        mock_loader_instance = MagicMock()
        mock_loader_instance.load_production_config.return_value = test_production_config
        mock_loader.return_value = mock_loader_instance
        
        # Create manager and initialize production
        manager = ProductionManager(temp_config_file, "test_machine", temp_work_dir)
        jobs_created = manager.initialize_production()
        
        # Should create 4 realizations × 2 redshifts = 8 jobs
        assert jobs_created == 8
        
        # Check jobs in database
        pending_jobs = manager.job_db.get_jobs_by_status(JobStatus.PENDING)
        assert len(pending_jobs) == 8
        
        # Check job IDs follow expected pattern
        job_ids = {job.job_id for job in pending_jobs}
        expected_ids = {
            "r0000_z1.000", "r0000_z2.000",
            "r0001_z1.000", "r0001_z2.000", 
            "r0002_z1.000", "r0002_z2.000",
            "r0003_z1.000", "r0003_z2.000"
        }
        assert job_ids == expected_ids
    
    @patch('covariance_mocks.production_manager.ProductionConfigLoader')
    @patch('subprocess.run')
    def test_submit_pending_jobs(self, mock_subprocess, mock_loader, temp_config_file, temp_work_dir, test_production_config):
        """Test job submission to SLURM."""
        # Mock config loader
        mock_loader_instance = MagicMock()
        mock_loader_instance.load_production_config.return_value = test_production_config
        mock_loader.return_value = mock_loader_instance
        
        # Mock subprocess (sbatch)
        mock_subprocess.return_value.stdout = "Submitted batch job 12345\n"
        
        # Create manager and initialize
        manager = ProductionManager(temp_config_file, "test_machine", temp_work_dir)
        manager.initialize_production()
        
        # Submit jobs
        submitted_batches = manager.submit_pending_jobs()
        
        # With batch_size=2 and 8 jobs, should create 4 batches
        assert len(submitted_batches) == 4
        
        # Check that sbatch was called for each batch
        assert mock_subprocess.call_count == 4
        
        # Check that jobs were marked as queued
        queued_jobs = manager.job_db.get_jobs_by_status(JobStatus.QUEUED)
        assert len(queued_jobs) == 8
    
    @patch('covariance_mocks.production_manager.ProductionConfigLoader')
    def test_retry_failed_jobs(self, mock_loader, temp_config_file, temp_work_dir, test_production_config):
        """Test retrying failed jobs."""
        # Mock config loader
        mock_loader_instance = MagicMock()
        mock_loader_instance.load_production_config.return_value = test_production_config
        mock_loader.return_value = mock_loader_instance
        
        # Create manager
        manager = ProductionManager(temp_config_file, "test_machine", temp_work_dir)
        
        # Create some failed jobs
        failed_jobs = [
            JobSpec("job_001", 0, 1.0, "/tmp/out1.hdf5", JobStatus.FAILED, submit_count=1),
            JobSpec("job_002", 1, 1.0, "/tmp/out2.hdf5", JobStatus.FAILED, submit_count=2),
            JobSpec("job_003", 2, 1.0, "/tmp/out3.hdf5", JobStatus.FAILED, submit_count=3)  # Max retries exceeded
        ]
        
        for job in failed_jobs:
            manager.job_db.insert_job(job)
        
        # Retry failed jobs (max_retries = 2)
        retried_count = manager.retry_failed_jobs()
        
        # Should retry first 2 jobs (submit_count < max_retries)
        assert retried_count == 2
        
        # Check that jobs were marked as pending
        pending_jobs = manager.job_db.get_jobs_by_status(JobStatus.PENDING)
        assert len(pending_jobs) == 2
        
        # Third job should still be failed
        failed_jobs = manager.job_db.get_jobs_by_status(JobStatus.FAILED)
        assert len(failed_jobs) == 1
        assert failed_jobs[0].job_id == "job_003"
    
    @patch('covariance_mocks.production_manager.ProductionConfigLoader')
    def test_production_summary(self, mock_loader, temp_config_file, temp_work_dir, test_production_config):
        """Test production summary generation."""
        # Mock config loader
        mock_loader_instance = MagicMock()
        mock_loader_instance.load_production_config.return_value = test_production_config
        mock_loader.return_value = mock_loader_instance
        
        # Create manager and initialize
        manager = ProductionManager(temp_config_file, "test_machine", temp_work_dir)
        manager.initialize_production()
        
        # Mark some jobs as completed
        manager.job_db.update_job_status("r0000_z1.000", JobStatus.COMPLETED)
        manager.job_db.update_job_status("r0001_z1.000", JobStatus.COMPLETED)
        
        # Get summary
        summary = manager.get_production_summary()
        
        # Check summary structure
        assert "production" in summary
        assert "statistics" in summary
        assert "configuration" in summary
        
        # Check statistics
        stats = summary["statistics"]
        assert stats["total_jobs"] == 8
        assert stats["completed"] == 2
        assert stats["success_rate"] == 0.25  # 2/8
        
        # Check configuration
        config = summary["configuration"]
        assert config["machine"] == "test_machine"
        assert config["batch_size"] == 2
        assert config["job_type"] == "balanced"


@pytest.mark.unit
class TestJobSpecAndBatchSpec:
    """Test job and batch specification classes."""
    
    def test_job_spec_creation(self):
        """Test JobSpec creation and default values."""
        job = JobSpec(
            job_id="test_job",
            realization=42,
            redshift=1.5,
            output_path="/tmp/test.hdf5"
        )
        
        assert job.job_id == "test_job"
        assert job.realization == 42
        assert job.redshift == 1.5
        assert job.output_path == "/tmp/test.hdf5"
        assert job.status == JobStatus.PENDING
        assert job.submit_count == 0
        assert job.created_at is not None
    
    def test_batch_spec_creation(self):
        """Test BatchSpec creation and default values."""
        batch = BatchSpec(
            batch_id="test_batch",
            job_ids=["job1", "job2", "job3"]
        )
        
        assert batch.batch_id == "test_batch"
        assert batch.job_ids == ["job1", "job2", "job3"]
        assert batch.status == JobStatus.PENDING
        assert batch.created_at is not None