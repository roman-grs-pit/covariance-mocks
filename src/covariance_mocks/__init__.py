"""
Covariance Mocks Pipeline

Modular pipeline for generating mock galaxy catalogs for covariance analysis.
Supports large-scale productions (40,000+ mock generations).
"""

__version__ = "0.1.0"

# Configuration constants
CURRENT_PHASE = "ph3000"
CURRENT_REDSHIFT = "z1.100"
CURRENT_Z_OBS = 1.1
LGMP_MIN = 10.0  # log10 minimum halo mass
SIMULATION_BOX = "AbacusSummit_small_c000"

# Import main modules when available
try:
    from .hdf5_writer import write_parallel_hdf5, write_single_hdf5
    from .mpi_setup import initialize_mpi_jax, finalize_mpi
    from .data_loader import load_and_filter_halos, build_abacus_path
    from .galaxy_generator import generate_galaxies
    from .utils import validate_catalog_path, generate_output_filename
except ImportError:
    # Optional module imports
    pass

# Import production management modules
try:
    from .production_config import ProductionConfigValidator, ProductionConfigLoader, validate_production_config
    from .production_manager import ProductionManager, JobDatabase, JobSpec, BatchSpec, JobStatus
except ImportError:
    # Production management modules not available
    pass
