"""Unit tests for Emission Lines module."""

import numpy as np
from unittest.mock import Mock, patch

import pytest

from covariance_mocks.emission_lines import add_emission_lines


class TestAddEmissionLines:
    """Test add_emission_lines function."""
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.emission_lines.oii.sfr_to_OII3727_K98')
    @patch('rgrspit_diffsky.emission_lines.halpha.sfr_to_Halpha_KTC94')
    def test_add_emission_lines_basic(self, mock_halpha, mock_oii):
        """Test basic emission line calculation."""
        # Mock input galaxy catalog
        n_galaxies = 1000
        n_time_bins = 50
        
        galcat = {
            'sfh_table': np.random.rand(n_galaxies, n_time_bins),
            't_table': np.linspace(0, 13.8, n_time_bins),
            't_obs': 10.0
        }
        
        # Mock emission line functions
        mock_oii_luminosities = np.random.rand(n_galaxies) * 1e40
        mock_halpha_luminosities = np.random.rand(n_galaxies) * 1e40
        
        mock_oii.return_value = mock_oii_luminosities
        mock_halpha.return_value = mock_halpha_luminosities
        
        result = add_emission_lines(galcat)
        
        # Should add emission line luminosities to catalog
        assert 'l_oii' in result
        assert 'l_halpha' in result
        assert np.array_equal(result['l_oii'], mock_oii_luminosities)
        assert np.array_equal(result['l_halpha'], mock_halpha_luminosities)
        
        # Should preserve original catalog entries
        assert 'sfh_table' in result
        assert 't_table' in result
        assert 't_obs' in result
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.emission_lines.oii.sfr_to_OII3727_K98')
    @patch('rgrspit_diffsky.emission_lines.halpha.sfr_to_Halpha_KTC94')
    def test_time_bin_selection(self, mock_halpha, mock_oii):
        """Test correct time bin selection for SFR extraction."""
        n_galaxies = 500
        n_time_bins = 10
        
        # Create specific time array and observation time
        t_table = np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        t_obs = 5.2  # Should select index 5 (closest to 5.2)
        
        # Create SFH table with distinct values per time bin
        sfh_table = np.zeros((n_galaxies, n_time_bins))
        for i in range(n_time_bins):
            sfh_table[:, i] = i * 10  # Values: 0, 10, 20, ..., 90
        
        galcat = {
            'sfh_table': sfh_table,
            't_table': t_table,
            't_obs': t_obs
        }
        
        mock_oii.return_value = np.ones(n_galaxies)
        mock_halpha.return_value = np.ones(n_galaxies)
        
        add_emission_lines(galcat)
        
        # Should extract SFR from time bin 5 (value 50)
        expected_sfr = np.full(n_galaxies, 50.0)
        
        mock_oii.assert_called_once()
        mock_halpha.assert_called_once()
        
        # Check that correct SFR values were passed
        sfr_arg_oii = mock_oii.call_args[0][0]
        sfr_arg_halpha = mock_halpha.call_args[0][0]
        
        np.testing.assert_array_equal(sfr_arg_oii, expected_sfr)
        np.testing.assert_array_equal(sfr_arg_halpha, expected_sfr)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.emission_lines.oii.sfr_to_OII3727_K98')
    @patch('rgrspit_diffsky.emission_lines.halpha.sfr_to_Halpha_KTC94')
    def test_edge_case_time_bins(self, mock_halpha, mock_oii):
        """Test edge cases for time bin selection."""
        n_galaxies = 100
        
        # Test with t_obs exactly matching a time bin
        t_table = np.array([0, 2, 4, 6, 8])
        t_obs = 4.0  # Exactly matches index 2
        
        sfh_table = np.random.rand(n_galaxies, 5)
        
        galcat = {
            'sfh_table': sfh_table,
            't_table': t_table,
            't_obs': t_obs
        }
        
        mock_oii.return_value = np.ones(n_galaxies)
        mock_halpha.return_value = np.ones(n_galaxies)
        
        add_emission_lines(galcat)
        
        # Should select index 2
        expected_sfr = sfh_table[:, 2]
        
        sfr_arg_oii = mock_oii.call_args[0][0]
        np.testing.assert_array_equal(sfr_arg_oii, expected_sfr)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.emission_lines.oii.sfr_to_OII3727_K98')
    @patch('rgrspit_diffsky.emission_lines.halpha.sfr_to_Halpha_KTC94')
    def test_boundary_time_selection(self, mock_halpha, mock_oii):
        """Test time bin selection at boundaries."""
        n_galaxies = 100
        
        # Test with t_obs before first time bin
        t_table = np.array([2, 4, 6, 8, 10])
        t_obs = 0.5  # Before first bin, should select index 0
        
        sfh_table = np.random.rand(n_galaxies, 5)
        
        galcat = {
            'sfh_table': sfh_table,
            't_table': t_table,
            't_obs': t_obs
        }
        
        mock_oii.return_value = np.ones(n_galaxies)
        mock_halpha.return_value = np.ones(n_galaxies)
        
        add_emission_lines(galcat)
        
        # Should select index 0 (closest to 0.5)
        expected_sfr = sfh_table[:, 0]
        
        sfr_arg_oii = mock_oii.call_args[0][0]
        np.testing.assert_array_equal(sfr_arg_oii, expected_sfr)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.emission_lines.oii.sfr_to_OII3727_K98')
    @patch('rgrspit_diffsky.emission_lines.halpha.sfr_to_Halpha_KTC94')
    def test_late_time_selection(self, mock_halpha, mock_oii):
        """Test time bin selection after last time bin."""
        n_galaxies = 100
        
        # Test with t_obs after last time bin
        t_table = np.array([2, 4, 6, 8, 10])
        t_obs = 12.0  # After last bin, should select index 4
        
        sfh_table = np.random.rand(n_galaxies, 5)
        
        galcat = {
            'sfh_table': sfh_table,
            't_table': t_table,
            't_obs': t_obs
        }
        
        mock_oii.return_value = np.ones(n_galaxies)
        mock_halpha.return_value = np.ones(n_galaxies)
        
        add_emission_lines(galcat)
        
        # Should select index 4 (closest to 12.0)
        expected_sfr = sfh_table[:, 4]
        
        sfr_arg_oii = mock_oii.call_args[0][0]
        np.testing.assert_array_equal(sfr_arg_oii, expected_sfr)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.emission_lines.oii.sfr_to_OII3727_K98')
    @patch('rgrspit_diffsky.emission_lines.halpha.sfr_to_Halpha_KTC94')
    def test_array_conversion(self, mock_halpha, mock_oii):
        """Test that inputs are properly converted to numpy arrays."""
        n_galaxies = 100
        n_time_bins = 20
        
        # Use lists instead of numpy arrays
        galcat = {
            'sfh_table': [[1.0] * n_time_bins for _ in range(n_galaxies)],
            't_table': list(range(n_time_bins)),
            't_obs': 10.0
        }
        
        mock_oii.return_value = np.ones(n_galaxies)
        mock_halpha.return_value = np.ones(n_galaxies)
        
        result = add_emission_lines(galcat)
        
        # Should work with list inputs
        assert 'l_oii' in result
        assert 'l_halpha' in result
        assert len(result['l_oii']) == n_galaxies
        assert len(result['l_halpha']) == n_galaxies
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.emission_lines.oii.sfr_to_OII3727_K98')
    @patch('rgrspit_diffsky.emission_lines.halpha.sfr_to_Halpha_KTC94')
    def test_single_galaxy(self, mock_halpha, mock_oii):
        """Test emission line calculation for single galaxy."""
        n_time_bins = 10
        
        galcat = {
            'sfh_table': np.random.rand(1, n_time_bins),
            't_table': np.linspace(0, 10, n_time_bins),
            't_obs': 5.0
        }
        
        mock_oii.return_value = np.array([1e40])
        mock_halpha.return_value = np.array([2e40])
        
        result = add_emission_lines(galcat)
        
        assert result['l_oii'].shape == (1,)
        assert result['l_halpha'].shape == (1,)
        assert result['l_oii'][0] == 1e40
        assert result['l_halpha'][0] == 2e40
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.emission_lines.oii.sfr_to_OII3727_K98')
    @patch('rgrspit_diffsky.emission_lines.halpha.sfr_to_Halpha_KTC94')
    def test_zero_sfr_handling(self, mock_halpha, mock_oii):
        """Test handling of zero star formation rates."""
        n_galaxies = 100
        n_time_bins = 10
        
        # Create SFH table with all zeros
        sfh_table = np.zeros((n_galaxies, n_time_bins))
        
        galcat = {
            'sfh_table': sfh_table,
            't_table': np.linspace(0, 10, n_time_bins),
            't_obs': 5.0
        }
        
        # Mock emission line functions to return zeros for zero SFR
        mock_oii.return_value = np.zeros(n_galaxies)
        mock_halpha.return_value = np.zeros(n_galaxies)
        
        result = add_emission_lines(galcat)
        
        # Should handle zero SFR gracefully
        assert np.all(result['l_oii'] == 0)
        assert np.all(result['l_halpha'] == 0)
        
        # Should have called emission line functions with zero SFR
        sfr_arg_oii = mock_oii.call_args[0][0]
        assert np.all(sfr_arg_oii == 0)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.emission_lines.oii.sfr_to_OII3727_K98')
    @patch('rgrspit_diffsky.emission_lines.halpha.sfr_to_Halpha_KTC94')
    def test_high_sfr_handling(self, mock_halpha, mock_oii):
        """Test handling of high star formation rates."""
        n_galaxies = 100
        n_time_bins = 10
        
        # Create SFH table with high SFR values
        sfh_table = np.full((n_galaxies, n_time_bins), 100.0)  # High SFR
        
        galcat = {
            'sfh_table': sfh_table,
            't_table': np.linspace(0, 10, n_time_bins),
            't_obs': 5.0
        }
        
        # Mock emission line functions to return high luminosities
        mock_oii.return_value = np.full(n_galaxies, 1e42)
        mock_halpha.return_value = np.full(n_galaxies, 2e42)
        
        result = add_emission_lines(galcat)
        
        # Should handle high SFR values
        assert np.all(result['l_oii'] == 1e42)
        assert np.all(result['l_halpha'] == 2e42)
        
        # Should have called emission line functions with high SFR
        sfr_arg_oii = mock_oii.call_args[0][0]
        assert np.all(sfr_arg_oii == 100.0)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.emission_lines.oii.sfr_to_OII3727_K98')
    @patch('rgrspit_diffsky.emission_lines.halpha.sfr_to_Halpha_KTC94')
    def test_function_call_order(self, mock_halpha, mock_oii):
        """Test that emission line functions are called in correct order."""
        n_galaxies = 100
        n_time_bins = 10
        
        galcat = {
            'sfh_table': np.random.rand(n_galaxies, n_time_bins),
            't_table': np.linspace(0, 10, n_time_bins),
            't_obs': 5.0
        }
        
        mock_oii.return_value = np.ones(n_galaxies)
        mock_halpha.return_value = np.ones(n_galaxies)
        
        add_emission_lines(galcat)
        
        # Both functions should be called exactly once
        mock_oii.assert_called_once()
        mock_halpha.assert_called_once()
        
        # Should be called with the same SFR array
        sfr_arg_oii = mock_oii.call_args[0][0]
        sfr_arg_halpha = mock_halpha.call_args[0][0]
        np.testing.assert_array_equal(sfr_arg_oii, sfr_arg_halpha)
    
    @pytest.mark.unit
    @patch('rgrspit_diffsky.emission_lines.oii.sfr_to_OII3727_K98')
    @patch('rgrspit_diffsky.emission_lines.halpha.sfr_to_Halpha_KTC94')
    def test_catalog_modification_in_place(self, mock_halpha, mock_oii):
        """Test that catalog is modified in place."""
        n_galaxies = 100
        n_time_bins = 10
        
        galcat = {
            'sfh_table': np.random.rand(n_galaxies, n_time_bins),
            't_table': np.linspace(0, 10, n_time_bins),
            't_obs': 5.0
        }
        
        mock_oii.return_value = np.ones(n_galaxies)
        mock_halpha.return_value = np.ones(n_galaxies)
        
        original_id = id(galcat)
        result = add_emission_lines(galcat)
        
        # Should return the same object (modified in place)
        assert id(result) == original_id
        assert result is galcat
        
        # Original catalog should now have emission lines
        assert 'l_oii' in galcat
        assert 'l_halpha' in galcat