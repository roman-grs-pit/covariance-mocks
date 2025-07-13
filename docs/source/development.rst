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

Testing focuses on pipeline functionality and environment setup:

* ``scripts/test_mpi_minimal.py`` - Basic MPI functionality testing
* Environment validation through import testing
* Pipeline integration testing with small-scale mock generation
* Dependency verification for rgrspit_diffsky package availability

**CRITICAL RULE**: Always save complete test output to log files before analysis.

**Correct Pattern**:

.. code-block:: bash

   # 1. Save complete output to log file
   pytest 2>&1 > test_output.log
   # 2. Later, analyze the log file
   grep "FAILED" test_output.log

**MANDATORY ITERATIVE TESTING**: When fixing test failures, continue iterating until ALL tests pass:

1. **Run tests** → **Identify failures** → **Fix issues** → **Immediately re-run tests**
2. **Never stop after a single fix** - keep cycling until 100% pass rate
3. **Don't declare success until full test suite passes**

Code Quality Standards
----------------------

The project uses several code quality tools configured in ``pyproject.toml``:

.. code-block:: bash

   # Format code with black (line length: 88)
   black .

   # Sort imports with isort
   isort .

   # Lint with flake8
   flake8 .

Contributing
------------

When contributing to the project:

1. **Fork the repository** and create a feature branch
2. **Follow coding standards** - use black, isort, and flake8
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