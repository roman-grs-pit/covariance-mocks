Overview
========

The catalogs are mock galaxy catalogs built with diffsky on the AbacusSummit_small
boxes. Each redshift comes as a set of independent realizations; each realization is one
HDF5 file. The ``covariance_mocks.selection`` interface reads a catalog and returns the
sample for a given selection.

Per-object columns:

.. list-table::
   :header-rows: 1
   :widths: 20 14 66

   * - Column
     - Units
     - Description
   * - ``sfr_corr``
     - Msun/yr
     - Calibrated star-formation rate.
   * - ``sfr_raw``
     - Msun/yr
     - Raw star-formation rate.
   * - ``mstar_corr``
     - Msun
     - Calibrated stellar mass.
   * - ``mstar_raw``
     - Msun
     - Raw stellar mass.
   * - ``mpeak``
     - Msun
     - Halo peak mass.
   * - ``pos`` (``x``, ``y``, ``z``)
     - Mpc/h
     - Comoving position.
   * - ``vel`` (``vx``, ``vy``, ``vz``)
     - as stored
     - Peculiar velocity.

The catalogs live on the m4943 allocation::

   /global/cfs/cdirs/m4943/covariance_mocks/v1/

See :doc:`quickstart` to load a catalog and select a sample.
