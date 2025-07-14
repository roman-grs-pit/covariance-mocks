Production Management
===================

The covariance-mocks pipeline includes a production management system for large-scale mock generation productions with thousands of individual jobs. This system provides configuration management, batch job orchestration, failure recovery, and progress monitoring.

Overview
--------

A **production** is a coordinated collection of mock generation jobs that share common scientific parameters and execution settings. Productions handle the scale requirements of covariance analysis, which typically requires:

* 2,000 realizations × 20 snapshots = 40,000 individual mock runs
* Systematic batch management for SLURM job arrays
* Persistent job tracking and failure recovery
* Organized output directory structure
* Progress monitoring and reporting

Key Features
------------

**Hierarchical Configuration System**
  YAML-based configuration with machine-specific defaults and production-specific overrides

**Declarative Resource Specification**
  Abstract job types (``cpu_intensive``, ``gpu_intensive``, etc.) mapped to machine-specific resources

**SQLite Job Tracking**
  Persistent database for job state management with ACID properties

**Failure Recovery**
  Automatic retry mechanisms with exponential backoff

**Production-Centric Organization**
  Structured output directories with metadata, logs, and quality assurance

Configuration System
--------------------

Production configurations use a hierarchical YAML structure with clear separation of concerns:

Configuration Schema
~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   production:
     name: "production_identifier"
     version: "v1.0"
     description: "Human-readable description"
     tags: ["production", "covariance"]

   science:
     cosmology: "AbacusSummit"
     redshifts: [0.5, 1.0, 1.5, 2.0]
     realizations:
       start: 0
       count: 2000
       step: 1
     catalog_params:
       magnitude_limit: 25.0
       area_deg2: 5000.0

   execution:
     job_type: "cpu_intensive"
     batch_size: 200
     timeout_hours: 24.0
     retry_policy:
       max_retries: 3
       backoff_multiplier: 2.0
       initial_delay_minutes: 10.0

   outputs:
     base_path: "/global/cfs/cdirs/m4943/Simulations/covariance_mocks/productions"
     structure: "hierarchical"
     compression: "gzip"
     cleanup_policy:
       keep_logs_days: 90
       keep_intermediate: false
       archive_completed: true

Machine Defaults
~~~~~~~~~~~~~~~~

Machine-specific defaults are loaded automatically based on the target system:

.. code-block:: yaml

   # config/defaults/nersc.yaml
   resources:
     account: "m4943"
     partition: "regular"
     constraint: "cpu"
     nodes_per_job: 1
     tasks_per_node: 128
     cpus_per_task: 1
     memory_gb: 250.0

   job_type_overrides:
     cpu_intensive:
       partition: "regular"
       constraint: "cpu"
       timeout_hours: 12.0
     
     gpu_intensive:
       partition: "gpu"
       constraint: "gpu"
       gpus_per_node: 4
       timeout_hours: 6.0

Usage Examples
--------------

Creating a Test Production
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Initialize test production
   python scripts/run_production.py init config/examples/test_production.yaml

   # Submit jobs to SLURM
   python scripts/run_production.py submit config/examples/test_production.yaml

   # Monitor progress
   python scripts/run_production.py status config/examples/test_production.yaml --verbose

Production Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # 1. Initialize production 
   python scripts/run_production.py init config/examples/production.yaml

   # 2. Submit initial batch of jobs
   python scripts/run_production.py submit config/examples/production.yaml

   # 3. Monitor production in real-time
   python scripts/run_production.py monitor config/examples/production.yaml --interval 60

   # 4. Handle failures (in separate terminal)
   python scripts/run_production.py retry config/examples/production.yaml --submit

Production Management API
------------------------

The production system can also be used programmatically:

.. code-block:: python

   from covariance_mocks.production_manager import ProductionManager

   # Initialize production manager
   manager = ProductionManager("config/examples/production.yaml", machine="nersc")

   # Create all job specifications
   jobs_created = manager.initialize_production()
   print(f"Created {jobs_created} jobs")

   # Submit pending jobs in batches
   submitted_batches = manager.submit_pending_jobs()
   print(f"Submitted {len(submitted_batches)} batches")

   # Check production status
   summary = manager.get_production_summary()
   print(f"Success rate: {summary['statistics']['success_rate']:.1%}")

   # Retry failed jobs
   retried_count = manager.retry_failed_jobs()
   print(f"Retried {retried_count} failed jobs")

Output Organization
-------------------

Productions create a structured output directory hierarchy:

.. code-block:: text

   productions/v1.0_covariance_v1/
   ├── catalogs/           # Generated mock catalogs
   │   ├── r0000/
   │   │   ├── mock_z0.500.hdf5
   │   │   ├── mock_z1.000.hdf5
   │   │   └── ...
   │   ├── r0001/
   │   └── ...
   ├── metadata/           # Production configuration and tracking
   │   ├── production_config.yaml
   │   └── production.db
   ├── logs/               # SLURM job logs
   │   ├── batch_0000_*.out
   │   ├── batch_0000_*.err
   │   └── ...
   └── qa/                 # Quality assurance outputs
       ├── validation_reports/
       └── summary_plots/

Job Tracking and Recovery
-------------------------

The production system uses SQLite for persistent job tracking:

**Job States**
  * ``PENDING``: Job created but not submitted
  * ``QUEUED``: Job submitted to SLURM queue
  * ``RUNNING``: Job actively executing
  * ``COMPLETED``: Job finished successfully
  * ``FAILED``: Job failed (eligible for retry)
  * ``CANCELLED``: Job cancelled by user

**Failure Recovery**
  Jobs are automatically retried according to the retry policy:
  
  * Maximum retry attempts configurable per production
  * Exponential backoff between retry attempts
  * Jobs exceeding max retries remain in ``FAILED`` state

**Progress Monitoring**
  Real-time status checking via SLURM integration:
  
  * Automatic detection of job state changes
  * Output file validation for completion confirmation
  * Production-wide statistics and success rates

Best Practices
--------------

**Development Workflow**
  1. Start with test productions using small job counts
  2. Validate configuration and resource requirements
  3. Test failure recovery mechanisms
  4. Scale to production once validated

**Productions**
  1. Use hierarchical output organization
  2. Set appropriate timeout values for job complexity
  3. Configure retry policies for expected failure rates
  4. Monitor productions regularly during execution

**Resource Management**
  1. Use declarative job types rather than explicit resource specs
  2. Test resource requirements with small batches first
  3. Consider SLURM array size limits (typically ~1000 jobs)
  4. Balance batch size with queue wait times

**Debugging and Troubleshooting**
  1. Check SLURM logs in the production logs/ directory
  2. Use production database for detailed job history
  3. Validate configuration files before large productions
  4. Test retry mechanisms with intentionally failing jobs

Configuration Reference
-----------------------

For complete configuration schema documentation, see the schema file at ``config/schemas/production_schema.yaml``. Example configurations are available in ``config/examples/``.

The production management system integrates seamlessly with the existing pipeline infrastructure while providing the scalability and reliability required for large-scale covariance mock generation.