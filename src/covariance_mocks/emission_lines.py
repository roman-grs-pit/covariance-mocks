"""
Emission Line Module

Calculates emission line luminosities from star formation rates using rgrspit_diffsky.
"""

import numpy as np


def add_emission_lines(galcat):
    """
    Add emission line luminosities to galaxy catalog.
    
    Calculates OII and H-alpha line luminosities from the star formation rate
    at the observation time using the star formation history table.
    
    Parameters
    ----------
    galcat : dict
        Galaxy catalog from rgrspit_diffsky containing:
        - 'sfh_table' : star formation history table (N_galaxies, n_time_bins) 
        - 't_table' : time array corresponding to sfh_table bins
        - 't_obs' : observation time
        
    Returns
    -------
    dict
        Modified galaxy catalog with added keys:
        - 'l_oii' : OII line luminosity (N_galaxies,)
        - 'l_halpha' : H-alpha line luminosity (N_galaxies,)
        
    Notes
    -----
    - Extracts SFR from sfh_table at the time bin closest to t_obs
    - Uses rgrspit_diffsky emission line functions for conversions
    - SFR units should be M_sun/yr for proper luminosity calculations
    """
    from rgrspit_diffsky.emission_lines.oii import sfr_to_OII3727_K98
    from rgrspit_diffsky.emission_lines.halpha import sfr_to_Halpha_KTC94
    
    # Extract arrays from galaxy catalog
    sfh_table = np.array(galcat['sfh_table'])  # (N_galaxies, n_time_bins)
    t_table = np.array(galcat['t_table'])      # (n_time_bins,)
    t_obs = float(galcat['t_obs'])             # scalar
    
    # Find the time bin closest to t_obs
    t_obs_idx = np.argmin(np.abs(t_table - t_obs))
    
    # Extract SFR at observation time for all galaxies
    sfr_t_obs = sfh_table[:, t_obs_idx]  # (N_galaxies,)
    
    # Calculate emission line luminosities
    l_oii = sfr_to_OII3727_K98(sfr_t_obs)
    l_halpha = sfr_to_Halpha_KTC94(sfr_t_obs)
    
    # Add to galaxy catalog
    galcat['l_oii'] = l_oii
    galcat['l_halpha'] = l_halpha
    
    return galcat