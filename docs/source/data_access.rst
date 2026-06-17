Data access & catalog format
============================

Layout
------

The catalogs are on the m4943 allocation, one HDF5 file per realization at each
redshift::

   /global/cfs/cdirs/m4943/covariance_mocks/
     v1/
       README.md
       catalogs/
         z1.400/  r3000.hdf5 … r4999.hdf5
         z1.700/  …
       metadata/
         manifest_z1.400.csv
         provenance_z1.400.json

``catalogs/z<redshift>/r<NNNN>.hdf5`` is realization ``NNNN`` at that redshift.
``z=1.4`` is a complete ensemble (1878 realizations). Redshifts 1.7, 2.5, 1.1, 2.0, and
3.0 are also staged but are **partial ensembles** (fewer realizations), to be completed
in a later production run. Check ``metadata/manifest_z<redshift>.csv`` for the exact
realizations present at each redshift.

Catalog format
--------------

Each file has a ``galaxies/`` group of per-object columns (all length ``n_galaxies``):

.. list-table::
   :header-rows: 1
   :widths: 18 14 68

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
   * - ``pos``
     - Mpc/h
     - Comoving position, shape ``(N, 3)``; served as ``x``, ``y``, ``z``.
   * - ``vel``
     - as stored
     - Peculiar velocity, shape ``(N, 3)``; served as ``vx``, ``vy``, ``vz``.

Box-level attributes include ``Lbox`` (= 500 Mpc/h), ``z_obs`` (the redshift),
``n_galaxies``, ``phase`` (e.g. ``ph3000``), and ``simulation_box``
(``AbacusSummit_small_c000``).

Reading a catalog
-----------------

.. code-block:: python

   from covariance_mocks.selection import Catalog

   path = "/global/cfs/cdirs/m4943/covariance_mocks/v1/catalogs/z1.400/r3000.hdf5"
   with Catalog.open(path) as cat:
       cat.redshift            # 1.4
       cat.Lbox                # 500.0
       cat.volume              # Lbox**3 in (Mpc/h)^3
       len(cat)                # n_galaxies
       cat.available()         # column names
       cat.column("sfr_corr")  # per-object array
       cat.column("x")         # pos[:, 0]

The ensemble n(>SFR) table
--------------------------

:class:`~covariance_mocks.selection.NumberDensity` with an ensemble threshold uses the
ensemble-averaged cumulative density ``n(>sfr_corr)``. Build it from a set of catalogs
with :func:`~covariance_mocks.selection.build_ensemble_nsfr` (see :doc:`quickstart`).

Metadata
--------

``metadata/manifest_z<redshift>.csv`` lists each staged realization with its byte size
and source. ``metadata/provenance_z<redshift>.json`` records the source path,
simulation, data model, and realization count.
