"""Build the master work list for the self-scheduling worker pool (full run).

Every (realization, redshift) box for realizations with COMPLETE AbacusSummit
inputs (all 12 target redshifts present), ordered REDSHIFT-OUTER / realization-
inner in the priority order, so a full per-z ensemble lands first. One line per
box:  <realization> <redshift> <output_path>

Output is staged to the cosmosim project CFS (m4943 CFS lacks the ~78 TB room).
Usage: python build_worklist.py
"""
import os

ABACUS = "/global/cfs/cdirs/desi/public/cosmosim/AbacusSummit/small"
OUTROOT = "/global/cfs/cdirs/cosmosim/covariance_mocks/full_v1.0"
CATDIR = os.path.join(OUTROOT, "catalogs")
# redshift priority: fiducial 1.4, then alternating in-survey, then out-of-survey low->high
Z_PRIORITY = [1.4, 2.0, 0.5, 1.7, 0.8, 1.1, 0.2, 0.25, 0.3, 0.4, 2.5, 3.0]


def complete(phase):
    return all(os.path.isdir(f"{ABACUS}/AbacusSummit_small_c000_ph{phase}/halos/z{z:.3f}")
               for z in Z_PRIORITY)


def main():
    phases = sorted(int(d.split("_ph")[1]) for d in os.listdir(ABACUS) if "_c000_ph" in d)
    runnable = [p for p in phases if complete(p)]
    os.makedirs(CATDIR, exist_ok=True)
    os.makedirs(os.path.join(OUTROOT, "claims"), exist_ok=True)
    os.makedirs(os.path.join(OUTROOT, "logs"), exist_ok=True)

    wl = os.path.join(OUTROOT, "worklist.txt")
    n = 0
    with open(wl, "w") as f:
        for z in Z_PRIORITY:                      # redshift OUTER
            for p in runnable:                    # realization INNER
                out = os.path.join(CATDIR, f"r{p:04d}", f"mock_z{z:.3f}.hdf5")
                f.write(f"{p} {z:.3f} {out}\n")
                n += 1
    print(f"phases total={len(phases)}  complete={len(runnable)}  boxes={n}")
    print(f"worklist: {wl}")
    print(f"output root: {OUTROOT}")
    print(f"order: z-outer {Z_PRIORITY}")


if __name__ == "__main__":
    main()
