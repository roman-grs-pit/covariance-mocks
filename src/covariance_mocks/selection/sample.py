"""Apply a selection to a catalog and return the selected sample -- nothing more.

The sample carries the selected objects' full columns (positions and velocities
passed through untouched, so the team applies its own RSD) plus selection metadata.
The layer stops here: it does not bin, build randoms, displace coordinates, or
compute any statistic.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import numpy as np

from .catalog import Catalog
from .selection import Selection, CompletenessFloor

# Default columns returned with each sample (the documented data model).
DEFAULT_COLUMNS = (
    "sfr_corr", "sfr_raw", "mstar_corr", "mstar_raw", "mpeak",
    "x", "y", "z", "vx", "vy", "vz",
)


@dataclass
class Sample:
    """A selected sample: per-object columns (selected rows) + selection metadata."""
    columns: dict[str, np.ndarray]
    metadata: dict = field(default_factory=dict)

    def __len__(self) -> int:
        return len(next(iter(self.columns.values()))) if self.columns else 0

    @property
    def n(self) -> int:
        return len(self)

    def __getitem__(self, name) -> np.ndarray:
        return self.columns[name]

    @property
    def positions(self) -> np.ndarray:
        """(N,3) positions [Mpc/h], exactly as stored."""
        return np.column_stack([self.columns[k] for k in ("x", "y", "z")])

    @property
    def velocities(self) -> np.ndarray:
        """(N,3) peculiar velocities, exactly as stored (no RSD applied)."""
        return np.column_stack([self.columns[k] for k in ("vx", "vy", "vz")])


def select(catalog: Catalog, selection: Selection,
           floor: CompletenessFloor | None = None,
           columns=DEFAULT_COLUMNS) -> Sample:
    """Apply ``selection`` to ``catalog`` and return the :class:`Sample`.

    Columns are sliced from the stored arrays with no transformation. If a
    completeness ``floor`` is given it is enforced (flag or refuse).
    """
    floor = CompletenessFloor() if floor is None else floor
    mask = selection.mask(catalog)
    below_floor = floor.check(catalog, mask)   # raises if mode='refuse'

    cols = {name: catalog.column(name)[mask] for name in columns}

    achieved_nbar = float(mask.sum()) / catalog.volume
    meta = {
        "selection": selection.describe(),
        "realization": catalog.realization,
        "redshift": catalog.redshift,
        "Lbox": catalog.Lbox,
        "volume": catalog.volume,
        "n_selected": int(mask.sum()),
        "n_total": len(catalog),
        "achieved_nbar": achieved_nbar,
        "completeness": {
            "column": floor.column, "floor": floor.value,
            "below_floor": below_floor, "mode": floor.mode,
        },
        "source_path": catalog.path,
    }
    # surface the fixed threshold for number-density selections (audit trail)
    thr = getattr(selection, "_fixed_threshold", None)
    if thr is not None:
        meta["selection"]["threshold"] = thr
    return Sample(columns=cols, metadata=meta)


def select_ensemble(paths, selection: Selection,
                    floor: CompletenessFloor | None = None,
                    columns=DEFAULT_COLUMNS):
    """Yield one :class:`Sample` per realization, applying the SAME selection to each.

    This is the only "ensemble" convenience -- it produces samples (one per
    realization), not a covariance. With a default (ensemble-fixed) NumberDensity
    selection the threshold is solved once and reused, so the cut is identical across
    realizations.
    """
    for p in paths:
        with Catalog.open(p) as cat:
            yield select(cat, selection, floor=floor, columns=columns)
