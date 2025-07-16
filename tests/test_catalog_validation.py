"""Catalog validation tests against reference data.

These tests compare generated catalogs to validated reference catalogs
to ensure scientific correctness and reproducibility.
"""

import filecmp
import numpy as np
from pathlib import Path
import tempfile

import pytest

try:
    import h5py
    HDF5_AVAILABLE = True
except ImportError:
    HDF5_AVAILABLE = False

from tests.integration_core import MockGenerationConfig, run_full_pipeline


# Reference catalog location
REFERENCE_CATALOG = Path("/global/cfs/cdirs/m4943/Simulations/covariance_mocks/validation/validated/mock_AbacusSummit_small_c000_ph3000_z1.100.hdf5")


@pytest.fixture
def reference_catalog_path():
    """Provide path to reference catalog."""
    if not REFERENCE_CATALOG.exists():
        pytest.skip(f"Reference catalog not found: {REFERENCE_CATALOG}")
    return REFERENCE_CATALOG


@pytest.fixture
def validation_output_dir():
    """Provide output directory for validation tests."""
    # Use shared filesystem location for MPI-HDF5 compatibility
    base_dir = Path("/global/cfs/cdirs/m4943/Simulations/covariance_mocks/validation/tmp")
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Create unique temporary directory
    import uuid
    test_dir = base_dir / f"validation_test_{uuid.uuid4().hex[:8]}"
    test_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        yield test_dir
    finally:
        # Clean up test directory
        import shutil
        if test_dir.exists():
            shutil.rmtree(test_dir, ignore_errors=True)


@pytest.fixture
def validation_config(validation_output_dir):
    """Provide configuration that should produce identical results to reference."""
    return MockGenerationConfig(
        output_dir=str(validation_output_dir),
        test_mode=False,  # Production mode to match reference
        force_run=True,
        time_limit=10
    )


def load_hdf5_datasets(file_path):
    """Load all datasets from HDF5 file into dictionary."""
    if not HDF5_AVAILABLE:
        pytest.skip("h5py not available")
    
    datasets = {}
    with h5py.File(file_path, 'r') as f:
        def visit_func(name, obj):
            if isinstance(obj, h5py.Dataset):
                # Handle both scalar and array datasets
                if obj.shape == ():  # Scalar dataset
                    datasets[name] = obj[()]
                else:  # Array dataset
                    datasets[name] = obj[:]
        f.visititems(visit_func)
    return datasets


def compare_datasets(ref_data, test_data, tolerance=1e-10):
    """Compare two dataset dictionaries for equality."""
    differences = []
    
    # Check that both have same dataset names
    ref_names = set(ref_data.keys())
    test_names = set(test_data.keys())
    
    if ref_names != test_names:
        missing_in_test = ref_names - test_names
        extra_in_test = test_names - ref_names
        
        if missing_in_test:
            differences.append(f"Missing datasets in test: {missing_in_test}")
        if extra_in_test:
            differences.append(f"Extra datasets in test: {extra_in_test}")
        
        return differences
    
    # Compare each dataset
    for name in ref_names:
        ref_array = ref_data[name]
        test_array = test_data[name]
        
        # Check shapes match
        if ref_array.shape != test_array.shape:
            differences.append(f"Shape mismatch for {name}: ref={ref_array.shape}, test={test_array.shape}")
            continue
        
        # Check dtypes match
        if ref_array.dtype != test_array.dtype:
            differences.append(f"Dtype mismatch for {name}: ref={ref_array.dtype}, test={test_array.dtype}")
            continue
        
        # Compare values
        if np.issubdtype(ref_array.dtype, np.floating):
            # For floating point, use tolerance
            if not np.allclose(ref_array, test_array, rtol=tolerance, atol=tolerance):
                max_diff = np.max(np.abs(ref_array - test_array))
                differences.append(f"Value mismatch for {name}: max_diff={max_diff}, tolerance={tolerance}")
        else:
            # For integers and other types, require exact equality
            if not np.array_equal(ref_array, test_array):
                differences.append(f"Exact value mismatch for {name}")
    
    return differences


class TestCatalogValidation:
    """Test catalog generation against validated reference."""
    
    @pytest.mark.system
    @pytest.mark.slow
    @pytest.mark.skipif(not HDF5_AVAILABLE, reason="h5py not available")
    def test_catalog_identical_to_reference(self, shared_catalog, reference_catalog_path):
        """Test that shared production catalog is identical to reference catalog."""
        # Use shared production catalog instead of generating new one
        
        # Compare catalogs using simple file comparison
        # For reproducibility tests, catalogs should be byte-for-byte identical
        catalogs_identical = filecmp.cmp(reference_catalog_path, shared_catalog.catalog_path, shallow=False)
        
        # Provide helpful error message if different
        if not catalogs_identical:
            ref_size = reference_catalog_path.stat().st_size
            test_size = shared_catalog.catalog_path.stat().st_size
            error_msg = f"Generated catalog differs from reference:\n"
            error_msg += f"  Reference: {reference_catalog_path} ({ref_size:,} bytes)\n"
            error_msg += f"  Generated: {shared_catalog.catalog_path} ({test_size:,} bytes)"
            if ref_size != test_size:
                error_msg += "\n  File sizes differ - catalogs are not identical"
            else:
                error_msg += "\n  File sizes match but content differs"
        
        assert catalogs_identical, error_msg if not catalogs_identical else ""
    
    
    @pytest.mark.system
    @pytest.mark.skipif(not HDF5_AVAILABLE, reason="h5py not available")
    def test_reference_catalog_accessible(self, reference_catalog_path):
        """Test that reference catalog is accessible and valid."""
        # Check file exists and is readable
        assert reference_catalog_path.exists(), f"Reference catalog not found: {reference_catalog_path}"
        assert reference_catalog_path.is_file(), f"Reference catalog is not a file: {reference_catalog_path}"
        
        # Check it's a valid HDF5 file
        try:
            ref_data = load_hdf5_datasets(reference_catalog_path)
        except Exception as e:
            pytest.fail(f"Cannot read reference catalog: {e}")
        
        # Check it has expected datasets
        assert len(ref_data) > 0, "Reference catalog contains no datasets"
        
        # Log dataset information for debugging
        print(f"\nReference catalog datasets:")
        for name, data in ref_data.items():
            print(f"  {name}: shape={data.shape}, dtype={data.dtype}")
    
    @pytest.mark.system
    @pytest.mark.slow
    @pytest.mark.skipif(not HDF5_AVAILABLE, reason="h5py not available")
    def test_catalog_reproducibility(self, shared_catalog, validation_output_dir):
        """Test that multiple runs produce identical catalogs."""
        # Use shared catalog as first run, generate only one additional catalog
        config2 = MockGenerationConfig(
            output_dir=str(validation_output_dir / "run2"), 
            test_mode=False,
            force_run=True
        )
        
        # Run pipeline once (shared catalog serves as first run)
        success2, messages2 = run_full_pipeline(config2)
        assert success2, f"Second run failed: {messages2}"
        
        # Compare shared catalog with new catalog for reproducibility
        # For reproducibility, multiple runs should produce identical files
        catalogs_identical = filecmp.cmp(shared_catalog.catalog_path, config2.catalog_path, shallow=False)
        
        if not catalogs_identical:
            size1 = shared_catalog.catalog_path.stat().st_size
            size2 = config2.catalog_path.stat().st_size
            error_msg = f"Reproducibility test failed - catalogs differ:\n"
            error_msg += f"  Shared: {shared_catalog.catalog_path} ({size1:,} bytes)\n"
            error_msg += f"  Run 2: {config2.catalog_path} ({size2:,} bytes)"
            if size1 != size2:
                error_msg += "\n  File sizes differ - runs are not reproducible"
            else:
                error_msg += "\n  File sizes match but content differs - runs are not reproducible"
        
        assert catalogs_identical, error_msg if not catalogs_identical else ""


class TestCatalogContent:
    """Test catalog content properties."""
    
    @pytest.mark.system
    @pytest.mark.skipif(not HDF5_AVAILABLE, reason="h5py not available")
    def test_reference_catalog_properties(self, reference_catalog_path):
        """Test basic properties of reference catalog."""
        ref_data = load_hdf5_datasets(reference_catalog_path)
        
        # Basic sanity checks
        assert len(ref_data) > 0, "Reference catalog is empty"
        
        # Check that we have galaxy-related data (actual structure uses 'galaxies/' prefix)
        found_datasets = list(ref_data.keys())
        
        print(f"\nFound datasets in reference catalog: {found_datasets}")
        
        # Check for galaxies group datasets
        galaxy_datasets = [name for name in found_datasets if name.startswith('galaxies/')]
        assert len(galaxy_datasets) > 0, f"No galaxy datasets found in reference catalog. Available: {found_datasets}"
        
        # Check data ranges are reasonable
        for name, data in ref_data.items():
            if np.issubdtype(data.dtype, np.floating):
                assert np.all(np.isfinite(data)), f"Non-finite values found in {name}"
                assert data.size > 0, f"Empty dataset: {name}"


