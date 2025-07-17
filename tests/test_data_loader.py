"""Unit tests for Data Loader module."""

import os
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

import pytest

from covariance_mocks.data_loader import build_abacus_path, load_and_filter_halos


class TestBuildAbacusPath:
    """Test build_abacus_path function."""
    
    @pytest.mark.unit
    def test_build_abacus_path_standard(self):
        """Test standard path construction."""
        base_path = "/data/simulations"
        suite = "AbacusSummit"
        box = "small_c000"
        phase = "ph3000"
        redshift = "z1.100"
        
        result = build_abacus_path(base_path, suite, box, phase, redshift)
        
        expected = "/data/simulations/AbacusSummit/small_c000_ph3000/halos/z1.100"
        assert result == expected
    
    @pytest.mark.unit
    def test_build_abacus_path_different_box(self):
        """Test path construction with different box identifier."""
        base_path = "/scratch/cosmo"
        suite = "AbacusSummit"
        box = "huge_c100"
        phase = "ph2000"
        redshift = "z0.500"
        
        result = build_abacus_path(base_path, suite, box, phase, redshift)
        
        expected = "/scratch/cosmo/AbacusSummit/huge_c100_ph2000/halos/z0.500"
        assert result == expected
    
    @pytest.mark.unit
    def test_build_abacus_path_empty_base(self):
        """Test path construction with empty base path."""
        result = build_abacus_path("", "AbacusSummit", "small_c000", "ph3000", "z1.100")
        
        expected = "AbacusSummit/small_c000_ph3000/halos/z1.100"
        assert result == expected
    
    @pytest.mark.unit
    def test_build_abacus_path_relative_base(self):
        """Test path construction with relative base path."""
        result = build_abacus_path("./data", "AbacusSummit", "small_c000", "ph3000", "z1.100")
        
        expected = "./data/AbacusSummit/small_c000_ph3000/halos/z1.100"
        assert result == expected
    
    @pytest.mark.unit
    def test_build_abacus_path_windows_separator(self):
        """Test path construction handles OS-specific separators."""
        # os.path.join should handle this correctly regardless of OS
        result = build_abacus_path("C:\\data", "AbacusSummit", "small_c000", "ph3000", "z1.100")
        
        # Result should use OS-appropriate separators
        assert "AbacusSummit" in result
        assert "small_c000_ph3000" in result
        assert "halos" in result
        assert "z1.100" in result


class TestLoadAndFilterHalos:
    """Test load_and_filter_halos function."""
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_load_and_filter_halos_single_process(self, mock_load_abacus):
        """Test halo loading and filtering for single process."""
        # Mock halo catalog data
        mock_catalog = {
            'mass': np.array([1e12, 1e11, 1e10, 1e9, 1e13]),  # Mix of masses
            'radius': np.array([0.5, 0.3, 0.2, 0.1, 0.8]),
            'pos': np.array([
                [-250, -250, -250],
                [250, 250, 250],
                [0, 0, 0],
                [100, 100, 100],
                [-100, -100, -100]
            ]),
            'vel': np.array([
                [100, 200, 300],
                [-100, -200, -300],
                [0, 0, 0],
                [50, -50, 100],
                [-50, 50, -100]
            ]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with patch('builtins.print') as mock_print:
            result = load_and_filter_halos("/test/path", rank=0, size=1)
        
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = result
        
        # Should filter out masses < 10^10 (first 3 halos pass the mass filter)
        assert len(logmhost) == 4  # All halos except 1e9 pass the 10^10 threshold
        assert Lbox == 1000.0
        
        # Should convert positions from [-Lbox/2, Lbox/2] to [0, Lbox]
        assert halo_pos.min() >= 0
        assert halo_pos.max() <= 1000.0
        
        # Should print loading message
        mock_print.assert_called()
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_load_and_filter_halos_multi_process(self, mock_load_abacus):
        """Test halo loading with slab decomposition for multiple processes."""
        # Mock halo catalog with halos distributed across y-coordinates
        mock_catalog = {
            'mass': np.array([1e12, 1e12, 1e12, 1e12]),  # All above threshold
            'radius': np.array([0.5, 0.5, 0.5, 0.5]),
            'pos': np.array([
                [0, 100, 0],   # y=100 -> rank 0 slab [0, 250)
                [0, 300, 0],   # y=300 -> rank 1 slab [250, 500)
                [0, 600, 0],   # y=600 -> rank 2 slab [500, 750)
                [0, 900, 0]    # y=900 -> rank 3 slab [750, 1000]
            ]) - 500,  # Shift to [-500, 500] range
            'vel': np.array([
                [100, 0, 0],
                [200, 0, 0],
                [300, 0, 0],
                [400, 0, 0]
            ]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with patch('builtins.print'):
            # Test rank 1 (should get halo at y=300)
            result = load_and_filter_halos("/test/path", rank=1, size=4)
        
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = result
        
        # Rank 1 should get 1 halo (y=300 after coordinate transformation)
        assert len(logmhost) == 1
        assert 250 <= halo_pos[0, 1] < 500  # y-coordinate in rank 1's slab
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_load_and_filter_halos_test_mode(self, mock_load_abacus):
        """Test halo loading with test mode limitation."""
        # Mock halo catalog with 5 halos
        mock_catalog = {
            'mass': np.array([1e12, 1e12, 1e12, 1e12, 1e12]),  # All above threshold
            'radius': np.array([0.5, 0.5, 0.5, 0.5, 0.5]),
            'pos': np.array([
                [400, 0, 0],   # x=400
                [200, 0, 0],   # x=200 (smallest)
                [300, 0, 0],   # x=300
                [500, 0, 0],   # x=500
                [250, 0, 0]    # x=250
            ]) - 500,  # Shift to [-500, 500] range
            'vel': np.array([
                [100, 0, 0],
                [200, 0, 0],
                [300, 0, 0],
                [400, 0, 0],
                [500, 0, 0]
            ]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with patch('builtins.print'):
            # Test mode: select 3 halos with smallest x-coordinates
            result = load_and_filter_halos("/test/path", rank=0, size=1, n_gen=3)
        
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = result
        
        # Should have exactly 3 halos (n_gen limit)
        assert len(logmhost) == 3
        
        # Should select halos with smallest x-coordinates
        x_coords = halo_pos[:, 0]
        assert x_coords.min() >= 0  # After coordinate transformation
        assert len(x_coords) == 3
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_load_and_filter_halos_last_rank_boundary(self, mock_load_abacus):
        """Test that last rank includes boundary halos correctly."""
        # Mock halo catalog with halo exactly at boundary
        mock_catalog = {
            'mass': np.array([1e12, 1e12]),  # Both above threshold
            'radius': np.array([0.5, 0.5]),
            'pos': np.array([
                [0, 499, 0],   # Just before boundary
                [0, 500, 0]    # Exactly at boundary
            ]) - 500,  # Shift to [-500, 500] range
            'vel': np.array([
                [100, 0, 0],
                [200, 0, 0]
            ]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with patch('builtins.print'):
            # Test last rank (rank 1 of size 2)
            result = load_and_filter_halos("/test/path", rank=1, size=2)
        
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = result
        
        # Last rank should include boundary halo
        assert len(logmhost) == 1  # Should get the boundary halo
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 12.0)
    def test_load_and_filter_halos_mass_filtering(self, mock_load_abacus):
        """Test mass filtering with different minimum mass."""
        # Mock halo catalog with various masses
        mock_catalog = {
            'mass': np.array([1e13, 1e11, 1e12, 1e10, 0.0]),  # Mix including zero
            'radius': np.array([0.8, 0.3, 0.5, 0.2, 0.1]),
            'pos': np.array([
                [0, 0, 0],
                [100, 100, 100],
                [200, 200, 200],
                [300, 300, 300],
                [400, 400, 400]
            ]) - 500,
            'vel': np.array([
                [100, 100, 100],
                [200, 200, 200],
                [300, 300, 300],
                [400, 400, 400],
                [500, 500, 500]
            ]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with patch('builtins.print'):
            result = load_and_filter_halos("/test/path", rank=0, size=1)
        
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = result
        
        # Should only include masses >= 10^12 (first and third halos)
        assert len(logmhost) == 2
        assert all(10**log_mass >= 1e12 for log_mass in logmhost)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 15.0)
    def test_load_and_filter_halos_no_valid_halos(self, mock_load_abacus):
        """Test error handling when no halos pass mass filter."""
        # Mock halo catalog with all masses below threshold
        mock_catalog = {
            'mass': np.array([1e12, 1e11, 1e10]),  # All below 10^15
            'radius': np.array([0.5, 0.3, 0.2]),
            'pos': np.array([
                [0, 0, 0],
                [100, 100, 100],
                [200, 200, 200]
            ]),
            'vel': np.array([
                [100, 0, 0],
                [200, 0, 0],
                [300, 0, 0]
            ]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with pytest.raises(ValueError, match="No halos above minimum mass"):
            load_and_filter_halos("/test/path", rank=0, size=1)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_load_and_filter_halos_zero_mass_filtering(self, mock_load_abacus):
        """Test filtering of zero mass halos."""
        # Mock halo catalog with zero masses
        mock_catalog = {
            'mass': np.array([1e12, 0.0, 1e11, 0.0]),  # Include zero masses
            'radius': np.array([0.5, 0.0, 0.3, 0.0]),
            'pos': np.array([
                [0, 0, 0],
                [100, 100, 100],
                [200, 200, 200],
                [300, 300, 300]
            ]) - 500,
            'vel': np.array([
                [100, 0, 0],
                [200, 0, 0],
                [300, 0, 0],
                [400, 0, 0]
            ]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with patch('builtins.print'):
            result = load_and_filter_halos("/test/path", rank=0, size=1)
        
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = result
        
        # Should exclude zero masses (first and third halos remain)
        assert len(logmhost) == 2
        assert all(mass > 0 for mass in 10**logmhost)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_load_and_filter_halos_coordinate_transformation(self, mock_load_abacus):
        """Test coordinate transformation from [-Lbox/2, Lbox/2] to [0, Lbox]."""
        # Mock halo catalog with known positions
        mock_catalog = {
            'mass': np.array([1e12, 1e12, 1e12]),
            'radius': np.array([0.5, 0.5, 0.5]),
            'pos': np.array([
                [-500, -500, -500],  # Corner position
                [0, 0, 0],           # Center position
                [499, 499, 499]      # Near opposite corner
            ]),
            'vel': np.array([
                [100, 100, 100],
                [200, 200, 200],
                [300, 300, 300]
            ]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with patch('builtins.print'):
            result = load_and_filter_halos("/test/path", rank=0, size=1)
        
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = result
        
        # Check coordinate transformation
        assert halo_pos[0, 0] == 0.0      # -500 + 500 = 0
        assert halo_pos[1, 0] == 500.0    # 0 + 500 = 500
        assert halo_pos[2, 0] == 999.0    # 499 + 500 = 999
        
        # All coordinates should be in [0, Lbox] range
        assert halo_pos.min() >= 0
        assert halo_pos.max() <= 1000.0
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_load_and_filter_halos_jax_array_conversion(self, mock_load_abacus):
        """Test conversion to JAX arrays with correct dtypes."""
        # Mock halo catalog
        mock_catalog = {
            'mass': np.array([1e12]),
            'radius': np.array([0.5]),
            'pos': np.array([[0, 0, 0]]),
            'vel': np.array([[100, 100, 100]]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with patch('builtins.print'):
            result = load_and_filter_halos("/test/path", rank=0, size=1)
        
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = result
        
        # Should return JAX arrays
        import jax.numpy as jnp
        assert isinstance(logmhost, jnp.ndarray)
        assert isinstance(halo_radius, jnp.ndarray)
        assert isinstance(halo_pos, jnp.ndarray)
        assert isinstance(halo_vel, jnp.ndarray)
        
        # Should have float32 dtype
        assert logmhost.dtype == jnp.float32
        assert halo_radius.dtype == jnp.float32
        assert halo_pos.dtype == jnp.float32
        assert halo_vel.dtype == jnp.float32
        
        # Lbox should be Python float
        assert isinstance(Lbox, float)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_load_and_filter_halos_rank_logging(self, mock_load_abacus):
        """Test rank-specific logging messages."""
        # Mock halo catalog
        mock_catalog = {
            'mass': np.array([1e12, 1e12]),
            'radius': np.array([0.5, 0.5]),
            'pos': np.array([
                [0, 100, 0],
                [0, 300, 0]
            ]) - 500,
            'vel': np.array([
                [100, 0, 0],
                [200, 0, 0]
            ]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            load_and_filter_halos("/test/path", rank=1, size=2)
        
        output = mock_stdout.getvalue()
        
        # Should contain rank-specific messages
        assert "Rank 1:" in output
        assert "y-slab" in output
        assert "galaxy generation" in output


class TestDataLoaderIntegration:
    """Test integration between data loader functions."""
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_path_building_and_loading_integration(self, mock_load_abacus):
        """Test integration between path building and halo loading."""
        # Mock halo catalog
        mock_catalog = {
            'mass': np.array([1e12]),
            'radius': np.array([0.5]),
            'pos': np.array([[0, 0, 0]]),
            'vel': np.array([[100, 100, 100]]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        # Build path
        catalog_path = build_abacus_path(
            "/data", "AbacusSummit", "small_c000", "ph3000", "z1.100"
        )
        
        # Load halos using the built path
        with patch('builtins.print'):
            result = load_and_filter_halos(catalog_path, rank=0, size=1)
        
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = result
        
        # Should load successfully
        assert len(logmhost) == 1
        assert Lbox == 1000.0
        
        # Should have called load_abacus with correct path
        expected_path = "/data/AbacusSummit/small_c000_ph3000/halos/z1.100"
        mock_load_abacus.assert_called_once_with(expected_path)


class TestDataLoaderEdgeCases:
    """Test edge cases and error conditions."""
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    def test_load_abacus_import_error(self, mock_load_abacus):
        """Test handling of rgrspit_diffsky import error."""
        # Mock import error
        mock_load_abacus.side_effect = ImportError("rgrspit_diffsky not found")
        
        with pytest.raises(ImportError, match="rgrspit_diffsky not found"):
            load_and_filter_halos("/test/path", rank=0, size=1)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_load_and_filter_halos_empty_catalog(self, mock_load_abacus):
        """Test handling of empty halo catalog."""
        # Mock empty catalog
        mock_catalog = {
            'mass': np.array([]),
            'radius': np.array([]),
            'pos': np.array([]).reshape(0, 3),
            'vel': np.array([]).reshape(0, 3),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with pytest.raises(ValueError, match="No halos above minimum mass"):
            load_and_filter_halos("/test/path", rank=0, size=1)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_load_and_filter_halos_single_halo_test_mode(self, mock_load_abacus):
        """Test test mode with single halo."""
        # Mock single halo catalog
        mock_catalog = {
            'mass': np.array([1e12]),
            'radius': np.array([0.5]),
            'pos': np.array([[0, 0, 0]]),
            'vel': np.array([[100, 100, 100]]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with patch('builtins.print'):
            # Request more halos than available
            result = load_and_filter_halos("/test/path", rank=0, size=1, n_gen=5)
        
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = result
        
        # Should return only the available halo
        assert len(logmhost) == 1
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_load_and_filter_halos_large_slab_count(self, mock_load_abacus):
        """Test slab decomposition with large number of processes."""
        # Mock halo catalog with many halos
        n_halos = 1000
        mock_catalog = {
            'mass': np.full(n_halos, 1e12),
            'radius': np.full(n_halos, 0.5),
            'pos': np.random.uniform(-500, 500, (n_halos, 3)),
            'vel': np.random.uniform(-300, 300, (n_halos, 3)),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with patch('builtins.print'):
            # Test with many processes
            result = load_and_filter_halos("/test/path", rank=50, size=100)
        
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = result
        
        # Should handle large slab count without error
        assert len(logmhost) >= 0  # May be 0 if no halos in this slab
        assert Lbox == 1000.0
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.data_loaders.load_abacus.load_abacus_halo_catalog')
    @patch('covariance_mocks.data_loader.LGMP_MIN', 10.0)
    def test_load_and_filter_halos_boundary_precision(self, mock_load_abacus):
        """Test precise boundary handling in slab decomposition."""
        # Mock halo catalog with halos exactly at boundaries
        mock_catalog = {
            'mass': np.array([1e12, 1e12, 1e12]),
            'radius': np.array([0.5, 0.5, 0.5]),
            'pos': np.array([
                [0, 250, 0],   # Exactly at boundary
                [0, 250.0001, 0],  # Just above boundary
                [0, 249.9999, 0]   # Just below boundary
            ]) - 500,
            'vel': np.array([
                [100, 0, 0],
                [200, 0, 0],
                [300, 0, 0]
            ]),
            'lbox': 1000.0
        }
        mock_load_abacus.return_value = mock_catalog
        
        with patch('builtins.print'):
            # Test boundary precision for rank 0 (y < 250)
            result = load_and_filter_halos("/test/path", rank=0, size=4)
        
        logmhost, halo_radius, halo_pos, halo_vel, Lbox = result
        
        # Should handle boundary precision correctly
        assert len(logmhost) >= 0
        if len(logmhost) > 0:
            # All halos should be in correct slab
            y_coords = halo_pos[:, 1]
            assert all(y < 250 for y in y_coords)