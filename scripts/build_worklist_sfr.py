"""Build the worker-pool worklist for the SFR-only campaign (sfr_v1.0).

One line per (realization, redshift) box:  <realization> <redshift> <output_path>
Redshift-outer / realization-inner, so a full per-z ensemble lands first.

The campaign runs one redshift at a time (Marcelo: z=1.4 first, then 1.7, 2.5, 1.1, 2.0,
3.0, then the rest low->high), so the default is a single redshift passed on the CLI.
Every AbacusSummit_small phase with the requested redshift's halo catalog present is
included (all 1883 ph3000-ph4999 have z=1.400).

Usage:  python build_worklist_sfr.py 1.400 [1.700 ...]
Output: sfr_v1.0/worklist_z<z>.txt (+ claims_z<z>/, logs/)
"""
import os
import sys

ABACUS = "/global/cfs/cdirs/desi/public/cosmosim/AbacusSummit/small"
OUTROOT = "/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0"
CATDIR = os.path.join(OUTROOT, "catalogs")


def has_z(phase, z):
    return os.path.isdir(f"{ABACUS}/AbacusSummit_small_c000_ph{phase}/halos/z{z:.3f}")


def main():
    zlist = [float(a) for a in sys.argv[1:]] or [1.400]
    phases = sorted(int(d.split("_ph")[1]) for d in os.listdir(ABACUS) if "_c000_ph" in d)
    os.makedirs(CATDIR, exist_ok=True)
    os.makedirs(os.path.join(OUTROOT, "logs"), exist_ok=True)

    for z in zlist:
        runnable = [p for p in phases if has_z(p, z)]
        claims = os.path.join(OUTROOT, f"claims_z{z:.3f}")
        os.makedirs(claims, exist_ok=True)
        wl = os.path.join(OUTROOT, f"worklist_z{z:.3f}.txt")
        with open(wl, "w") as f:
            for p in runnable:
                out = os.path.join(CATDIR, f"r{p:04d}", f"mock_z{z:.3f}.hdf5")
                f.write(f"{p} {z:.3f} {out}\n")
        print(f"z={z:.3f}: {len(runnable)}/{len(phases)} phases runnable -> {wl}")
        print(f"          claims: {claims}")


if __name__ == "__main__":
    main()
