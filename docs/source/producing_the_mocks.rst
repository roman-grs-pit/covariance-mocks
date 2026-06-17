Producing the mocks
===================

The SFR-only ``v1`` run generates lean catalogs (diffsky on AbacusSummit_small),
calibrates the SFR, and stages the result to the m4943 allocation.

Pipeline
--------

For each ``(realization, redshift)`` box:

1. **Load halos.** Read the AbacusSummit_small halo catalog for that phase and redshift
   (``ph3000``–``ph4999``), apply the ``lgmp_min = 10.0`` mass cut, and slab-decompose
   across MPI ranks.
2. **Generate galaxies.** Populate halos with diffsky via ``rgrspit_diffsky``, producing
   raw stellar mass, sSFR, SFR, positions, and velocities.
3. **Write HDF5.** One consolidated file per realization-redshift with the ``galaxies/``
   group (raw fields + ``pos``/``vel``).
4. **Calibrate SFR.** Apply the ensemble two-pass UM correction (``f1``/``f2``) to write
   ``sfr_corr`` / ``mstar_corr`` and accumulate the ensemble ``n(>sfr_corr, z)``.
5. **Stage to m4943.** Copy the finished ensemble to the m4943 allocation.

Single-box generation
---------------------

``scripts/generate_single_mock.py`` runs one box under MPI:

.. code-block:: bash

   source scripts/load_env.sh
   srun python scripts/generate_single_mock.py nersc /path/to/output/box.hdf5

Worker pool
-----------

The run goes one redshift at a time over a self-scheduling worker pool.
``scripts/build_worklist_sfr.py <z>`` writes a worklist (one line per box);
``scripts/launch_sfr_pool.sh <z> [n_regular] [n_premium]`` submits the pool. Each worker
(``scripts/worker_sfr.sh``) claims boxes from the shared worklist by atomic ``mkdir``,
packs as many runs as fit in its allocation, and chains a successor while boxes remain.
``scripts/pool_topup.sh`` keeps the premium slots at the cap and the regular pool at a
floor.

SFR calibration
---------------

``scripts/sfr_calibrate_apply.py`` applies the ensemble UM correction over a redshift's
catalogs in two passes: **accumulate** the ensemble histogram of
``(logMpeak, logM*, logsSFR)``; **build** the ``f1``/``f2`` calibration from the per-z UM
targets (``um_targets_z<z>.npz``); **apply** it to write ``sfr_corr`` / ``mstar_corr``
into each file and accumulate the companion ``n(>sfr_corr, z)``. One mapping is used for
all realizations at a redshift.

Staging to m4943
----------------

``scripts/stage_to_m4943.sh [redshift]`` copies one redshift's ensemble from the
production output to the m4943 allocation on the NERSC ``xfer`` queue. It is resumable,
writes the versioned tree (see :doc:`data_access`), makes it group-readable, and writes
the manifest and provenance.

Output locations
----------------

* **Production output:** ``/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/``
* **Staged copy:** ``/global/cfs/cdirs/m4943/covariance_mocks/v1/``
