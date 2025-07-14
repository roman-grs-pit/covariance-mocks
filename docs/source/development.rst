Development Guide
=================

Development Protocols
----------------------

**BASELINE PROTECTION**: Maintain existing test pass rate throughout ALL development phases.

**GIT WORKFLOW**:
- Use feature branches for each development phase
- Never work directly on main branch
- Commit frequently with descriptive messages
- Include test status in commit messages

**CHANGE MANAGEMENT**:
- Maximum 200 lines changed per commit
- Maximum 5 files modified per commit
- Test after every significant change
- Rollback immediately if tests fail

Testing Structure
-----------------

**Testing Architecture** - Shared core logic approach enabling both pytest and shell script workflows:

**Core Testing Infrastructure**:

* **``tests/integration_core.py``** - Shared SLURM execution logic used by both pytest and shell scripts
* **``tests/conftest.py``** - Session-scoped fixtures that generate catalogs once per test session
* **``pyproject.toml``** - Test configuration with markers for unit/system/validation tests

**Test Categories**:

* **Unit Tests** (``tests/test_integration_core.py``) - Fast tests with mocked SLURM calls, no HPC resources required
* **System Tests** (``tests/test_system_integration.py``) - Full SLURM integration tests with shared catalog artifacts
* **Validation Tests** (``tests/test_catalog_validation.py``) - Compare generated catalogs against reference data using shared fixtures

**Pytest Markers**:

* ``@pytest.mark.unit`` - Fast tests without SLURM dependency
* ``@pytest.mark.system`` - Tests requiring SLURM resources
* ``@pytest.mark.slow`` - Long-running tests (production mode)
* ``@pytest.mark.validation`` - Tests comparing against reference catalogs

**Shared Fixture Approach**:

The testing architecture uses session-scoped fixtures to minimize pipeline executions:

* Tests share catalog artifacts generated once per session
* System tests validate shared outputs instead of regenerating catalogs
* Validation tests use shared production catalogs for reference comparison
* Reproducibility tests generate additional catalogs only when testing deterministic behavior

**Development Testing Commands** (< 5 minutes):

.. code-block:: bash

   # Load environment (provides pytest and all dependencies)
   source scripts/load_env.sh

   # Fast development tests
   pytest -m "unit or (system and not slow)" -v

   # Unit tests only (< 1 minute)
   pytest -m unit -v

**Validation Testing Commands** (background execution):

.. code-block:: bash

   # Load environment
   source scripts/load_env.sh

   # Long validation tests (30+ minutes)
   nohup pytest -m "slow or validation" -v --timeout=1800 > validation.log 2>&1 &

   # Standalone validation tool
   python scripts/run_validation.py generate /tmp/validation_test

**Reference Data Validation**:

* **Reference catalog**: ``/global/cfs/cdirs/m4943/Simulations/covariance_mocks/data/validated/mock_AbacusSummit_small_c000_ph3000_z1.100.hdf5``
* **HDF5 dataset comparison**: Exact equality for integers, tolerance-based for floating point
* **Reproducibility testing**: Multiple runs produce identical results

**CRITICAL RULE**: Always save complete test output to log files before analysis.

**Development Testing Pattern**:

.. code-block:: bash

   # 1. Load environment
   source scripts/load_env.sh
   # 2. Run fast tests with logging
   pytest -m "unit or (system and not slow)" -v 2>&1 | tee dev_tests.log
   # 3. Analyze results
   grep "FAILED" dev_tests.log

**Validation Testing Pattern**:

.. code-block:: bash

   # 1. Load environment
   source scripts/load_env.sh
   # 2. Run validation tests in background
   nohup pytest -m "slow or validation" -v --timeout=1800 > validation.log 2>&1 &
   # 3. Monitor progress
   tail -f validation.log

**MANDATORY ITERATIVE TESTING**: When fixing test failures, continue iterating until ALL tests pass:

1. **Run tests** → **Identify failures** → **Fix issues** → **Immediately re-run tests**
2. **Never stop after a single fix** - keep cycling until 100% pass rate
3. **Don't declare success until full test suite passes**

**Testing Workflow**:

* **During development**: ``pytest -m unit -v`` (< 1 minute)
* **Pre-commit**: ``pytest -m "unit or (system and not slow)" -v`` (< 5 minutes)
* **Before releases**: Background validation tests

  .. code-block:: bash

     nohup pytest -m "slow or validation" -v --timeout=1800 > validation.log 2>&1 &

* **Manual verification**: ``./scripts/make_mocks.sh --test``


Contributing
------------

When contributing to the project:

1. **Fork the repository** and create a feature branch
2. **Follow coding standards** - maintain consistent style and formatting
3. **Write comprehensive tests** for new functionality
4. **Update documentation** for API changes
5. **Test thoroughly** - ensure all tests pass
6. **Submit pull request** with clear description

Module Development
------------------

When adding new modules:

1. **Follow existing patterns** - look at current module structure
2. **Add comprehensive docstrings** - use NumPy style formatting
3. **Include type hints** - for better code clarity
4. **Write unit tests** - test all public functions
5. **Update API documentation** - add to docs/source/api.rst

Performance Considerations
--------------------------

**Memory Management**:
- Use JAX arrays for GPU compatibility
- Consider memory usage with large halo catalogs
- Implement efficient slab decomposition

**MPI Optimization**:
- Minimize communication between ranks
- Use collective operations where appropriate
- Balance computation load across ranks

**I/O Efficiency**:
- Use parallel HDF5 for large datasets
- Implement proper chunking strategies
- Consider compression for storage efficiency

Debugging
---------

**MPI Debugging**:
- Use rank-specific logging for distributed debugging
- Implement barriers for synchronization testing
- Test with single rank first, then scale up

**JAX Debugging**:
- Enable JAX debug mode for detailed error messages
- Check device allocation and memory usage
- Verify array shapes and dtypes

**Pipeline Debugging**:
- Use test mode (``n_gen`` parameter) for small datasets
- Implement checkpoint saves for long-running jobs
- Add timing information for performance analysis

Release Process
---------------

1. **Update version numbers** in relevant files
2. **Run full test suite** and ensure 100% pass rate
3. **Update documentation** with new features
4. **Create release notes** summarizing changes
5. **Tag release** in git with semantic versioning
6. **Deploy documentation** to Read the Docs