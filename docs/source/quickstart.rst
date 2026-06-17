Quickstart
==========

Install
-------

.. code-block:: bash

   git clone https://github.com/roman-grs-pit/covariance-mocks.git
   cd covariance-mocks
   git checkout sfr-only
   pip install -e .

The catalogs are read from ``/global/cfs/cdirs/m4943``; nothing is copied.

Load a catalog
--------------

Open one HDF5 file with :class:`~covariance_mocks.selection.Catalog`. Columns are read
lazily and returned exactly as stored.

.. code-block:: python

   from covariance_mocks.selection import Catalog

   path = "/global/cfs/cdirs/m4943/covariance_mocks/v1/catalogs/z1.400/r3000.hdf5"
   with Catalog.open(path) as cat:
       print(cat.redshift, cat.Lbox, len(cat))     # 1.4   500.0   34513407
       sfr = cat.column("sfr_corr")

Select a sample
---------------

Ask for a mean number density ``nbar`` [(h/Mpc)³] and get the ``sfr_corr`` threshold that
yields it. For a single catalog, solve the threshold from that catalog:

.. code-block:: python

   from covariance_mocks.selection import Catalog, NumberDensity, select

   with Catalog.open(path) as cat:
       sel = NumberDensity(nbar=1e-3, per_realization=True)
       sample = select(cat, sel)

   print(sample.n)                          # ~124999 selected
   print(sample.metadata["achieved_nbar"])  # 1.000e-03

On the z=1.4 catalogs this cut is ``sfr_corr > ~43`` Msun/yr.

Apply the same cut to every realization
---------------------------------------

Build the ensemble ``n(>sfr_corr)`` table once and reuse its threshold, so the same cut
is applied to every realization:

.. code-block:: python

   import glob
   from covariance_mocks.selection import build_ensemble_nsfr, NumberDensity, select_ensemble

   paths = sorted(glob.glob(
       "/global/cfs/cdirs/m4943/covariance_mocks/v1/catalogs/z1.400/r*.hdf5"))

   ens = build_ensemble_nsfr(paths)
   sel = NumberDensity(nbar=1e-3, ensemble=ens)

   for sample in select_ensemble(paths, sel):
       pos, vel = sample.positions, sample.velocities

The returned sample
-------------------

:func:`~covariance_mocks.selection.select` returns a
:class:`~covariance_mocks.selection.Sample` — the selected rows' columns plus metadata:

.. code-block:: python

   sample.n               # number of selected objects
   sample["sfr_corr"]     # any column, selected rows only
   sample.positions       # (N, 3) positions [Mpc/h]
   sample.velocities      # (N, 3) velocities
   sample.metadata        # selection, threshold, achieved_nbar, realization, redshift, Lbox
