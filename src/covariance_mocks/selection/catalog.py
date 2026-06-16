"""Read-only access to one SFR-only mock catalog.

The on-disk model (see ``galaxies/`` in each ``mock_z<z>.hdf5``):
per-object ``sfr_corr``, ``sfr_raw``, ``mstar_corr``, ``mstar_raw``, ``mpeak``,
``pos`` (N,3), ``vel`` (N,3), plus ``logmp_t_obs``/``logsm_t_obs``/``logssfr_t_obs``
and ``upid``; box attrs ``Lbox`` and ``z_obs``.

This loader does NOT transform anything: positions and velocities are handed back
exactly as stored (the team applies its own RSD downstream). The only convenience is
exposing ``x,y,z`` / ``vx,vy,vz`` as views of ``pos`` / ``vel``.
"""
from __future__ import annotations

import os
import numpy as np
import h5py

# Columns physically stored per object, plus the position/velocity component aliases.
_VECTOR_ALIASES = {
    "x": ("pos", 0), "y": ("pos", 1), "z": ("pos", 2),
    "vx": ("vel", 0), "vy": ("vel", 1), "vz": ("vel", 2),
}


class Catalog:
    """One realization-redshift catalog. Columns are read lazily and cached.

    Use as a context manager (``with Catalog.open(path) as cat: ...``) or call
    :meth:`close` when done.
    """

    def __init__(self, path: str):
        self.path = path
        self._f = h5py.File(path, "r")
        self._g = self._f["galaxies"]
        self._cache: dict[str, np.ndarray] = {}
        self.Lbox = float(self._f.attrs.get("Lbox", self._g.get("Lbox", np.nan)))
        self.redshift = float(self._f.attrs.get("z_obs", np.nan))
        self.n = int(self._f.attrs.get("n_galaxies", self._g["sfr_corr"].shape[0]))

    @classmethod
    def open(cls, path: str) -> "Catalog":
        return cls(path)

    # --- identity ----------------------------------------------------------
    @property
    def realization(self) -> str:
        """Realization id parsed from the filename (e.g. 'r3000' or 'ph3000')."""
        base = os.path.basename(self.path)
        for tok in (base.replace(".hdf5", ""), os.path.basename(os.path.dirname(self.path))):
            if tok.startswith(("r", "ph")) and any(c.isdigit() for c in tok):
                return tok
        return str(self._f.attrs.get("phase", base))

    @property
    def volume(self) -> float:
        """Box volume in (Mpc/h)^3."""
        return float(self.Lbox) ** 3

    def __len__(self) -> int:
        return self.n

    # --- column access -----------------------------------------------------
    def available(self) -> list[str]:
        cols = [k for k in self._g.keys() if getattr(self._g[k], "ndim", 0) >= 1
                and self._g[k].shape[:1] == (self.n,)]
        return sorted(cols) + list(_VECTOR_ALIASES)

    def has(self, name: str) -> bool:
        return name in _VECTOR_ALIASES or (name in self._g and self._g[name].shape[:1] == (self.n,))

    def column(self, name: str) -> np.ndarray:
        """Return a per-object column as a numpy array, exactly as stored."""
        if name in self._cache:
            return self._cache[name]
        if name in _VECTOR_ALIASES:
            base, j = _VECTOR_ALIASES[name]
            arr = self._g[base][:, j]
        elif self.has(name):
            arr = self._g[name][...]
        else:
            raise KeyError(f"{name!r} not in catalog; available: {self.available()}")
        self._cache[name] = arr
        return arr

    def columns(self, names) -> dict[str, np.ndarray]:
        return {n: self.column(n) for n in names}

    # --- lifecycle ---------------------------------------------------------
    def close(self):
        self._cache.clear()
        self._f.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
