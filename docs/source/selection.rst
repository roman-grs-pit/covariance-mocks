Selection functions
===================

A selection turns a catalog into a boolean mask.
:func:`~covariance_mocks.selection.select` applies it to one catalog and returns the
:class:`~covariance_mocks.selection.Sample`;
:func:`~covariance_mocks.selection.select_ensemble` applies the same selection across a
list of catalogs, yielding one sample each.

Selections operate on the catalog columns: ``sfr_corr``, ``sfr_raw``, ``mstar_corr``,
``mstar_raw``, ``mpeak``, and the position / velocity components ``x,y,z`` / ``vx,vy,vz``.

NumberDensity
-------------

Select the highest-SFR objects down to a mean density ``nbar`` [(h/Mpc)³].

.. code-block:: python

   from covariance_mocks.selection import NumberDensity, build_ensemble_nsfr

   ens = build_ensemble_nsfr(paths)
   sel = NumberDensity(nbar=1e-3, ensemble=ens)        # threshold fixed from the ensemble

* With ``ensemble=`` set, the threshold is solved once from the ensemble
  ``n(>sfr_corr)`` table and applied to every realization.
* ``NumberDensity(nbar=1e-3, per_realization=True)`` solves the threshold from each
  catalog instead, matching ``nbar`` exactly in every catalog.
* ``sfr_col`` defaults to ``"sfr_corr"``; pass ``sfr_col="sfr_raw"`` to use the raw SFR.

The threshold and achieved density are recorded in
``sample.metadata["selection"]["threshold"]`` and ``sample.metadata["achieved_nbar"]``.

Threshold
---------

A ``lo <= column [< hi]`` cut on any column. ``hi=None`` means no upper edge.

.. code-block:: python

   from covariance_mocks.selection import Threshold

   Threshold("sfr_corr", lo=10.0)               # sfr_corr >= 10 Msun/yr
   Threshold("sfr_corr", lo=10.0, hi=100.0)     # an SFR bin
   Threshold("mstar_corr", lo=10**10.5)         # a stellar-mass cut

Joint
-----

The logical AND of several selections.

.. code-block:: python

   from covariance_mocks.selection import Joint, Threshold

   sel = Joint([Threshold("mstar_corr", lo=10**10.0),
                Threshold("sfr_corr",   lo=10.0)])

Callable
--------

Wrap any function ``fn(columns) -> bool mask``. ``columns`` serves any catalog column;
the mask must have length ``len(catalog)``.

.. code-block:: python

   from covariance_mocks.selection import Callable

   sel = Callable(lambda c: (c["sfr_corr"] > 10.0) & (c["mstar_corr"] > 1e10))
