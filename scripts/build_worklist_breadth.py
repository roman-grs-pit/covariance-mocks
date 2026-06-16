"""Breadth-first worklist for the premium workers: realization OUTER, redshift INNER.

The premium pool (5 workers, high priority) walks realizations one at a time and does ALL
target redshifts for each before moving on, so a few complete multi-z realizations land
quickly and the multi-z dashboard panels (n(>SFR,z), SFRD(z), sSFR(M*|z)) fill in across
the whole redshift range. The regular pool stays on the z=1.4 depth worklist; box outputs
are shared, so the file-existence check dedups any overlap (premium skips z=1.4 boxes the
regular pool already produced).

Target redshifts (Marcelo): GRS window first, then the rest low->high.
Output: sfr_v1.0/worklist_breadth.txt
"""
import os

ABACUS = "/global/cfs/cdirs/desi/public/cosmosim/AbacusSummit/small"
OUTROOT = "/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0"
CATDIR = os.path.join(OUTROOT, "catalogs")
Z_ALL = [1.400, 1.700, 2.500, 1.100, 2.000, 3.000, 0.200, 0.250, 0.300, 0.400, 0.500, 0.800]


def has_z(phase, z):
    return os.path.isdir(f"{ABACUS}/AbacusSummit_small_c000_ph{phase}/halos/z{z:.3f}")


def main():
    phases = sorted(int(d.split("_ph")[1]) for d in os.listdir(ABACUS) if "_c000_ph" in d)
    os.makedirs(os.path.join(OUTROOT, "claims_breadth"), exist_ok=True)
    wl = os.path.join(OUTROOT, "worklist_breadth.txt")
    n = 0
    with open(wl, "w") as f:
        for p in phases:                       # realization OUTER
            for z in Z_ALL:                    # redshift INNER (all z for this realization)
                if has_z(p, z):
                    out = os.path.join(CATDIR, f"r{p:04d}", f"mock_z{z:.3f}.hdf5")
                    f.write(f"{p} {z:.3f} {out}\n")
                    n += 1
    print(f"breadth worklist: {n} boxes over {len(phases)} phases x {len(Z_ALL)} z -> {wl}")


if __name__ == "__main__":
    main()
