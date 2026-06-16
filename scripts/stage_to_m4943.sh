#!/bin/bash
#SBATCH -q xfer
#SBATCH -C cron
#SBATCH -A m4943
#SBATCH -t 12:00:00
#SBATCH -J stage_z1.400
#SBATCH -o /global/cfs/cdirs/m4943/covariance_mocks/_staging_logs/stage_%j.out
#SBATCH -e /global/cfs/cdirs/m4943/covariance_mocks/_staging_logs/stage_%j.err
#
# Stage one redshift's full SFR-only ensemble cosmosim CFS -> m4943 CFS (Dev 1c).
# Non-destructive copy on the NERSC xfer queue (no compute charge). Resumable: skips
# files already copied with a matching size. Lays out a clean, group-readable, z-major
# versioned tree and writes a manifest. Source scaffolding (.stats.npz, claims, logs)
# is intentionally NOT staged.
#
# Usage: sbatch scripts/stage_to_m4943.sh [redshift]   (default 1.400)
set -u
Z="${1:-1.400}"
SRC=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/catalogs
DST=/global/cfs/cdirs/m4943/covariance_mocks/v1
ZDIR="$DST/catalogs/z${Z}"
MAN="$DST/metadata/manifest_z${Z}.csv"

mkdir -p "$ZDIR" "$DST/metadata"
# setgid on dirs so new files inherit the m4943 group; group-readable+traversable.
chmod 2750 "$DST" "$DST/catalogs" "$ZDIR" "$DST/metadata" 2>/dev/null || true

echo "realization,bytes,src_mtime,source" > "$MAN"
n=0; bytes=0; copied=0; skipped=0
for d in "$SRC"/r*/; do
    r=$(basename "$d")
    f="${d}mock_z${Z}.hdf5"
    [ -f "$f" ] || continue
    out="$ZDIR/${r}.hdf5"
    ssz=$(stat -c %s "$f")
    if [ -f "$out" ] && [ "$(stat -c %s "$out" 2>/dev/null)" = "$ssz" ]; then
        skipped=$(( skipped + 1 ))
    else
        cp -p "$f" "$out" || { echo "FAIL copy $r"; continue; }
        copied=$(( copied + 1 ))
    fi
    chmod g+r "$out" 2>/dev/null || true
    echo "${r},${ssz},$(stat -c %Y "$f"),${f}" >> "$MAN"
    n=$(( n + 1 )); bytes=$(( bytes + ssz ))
done

nsrc=$(ls "$SRC"/r*/mock_z${Z}.hdf5 2>/dev/null | wc -l)
echo "[stage z${Z}] staged=$n (copied=$copied skipped=$skipped); source present=$nsrc; bytes=$bytes"
[ "$n" -eq "$nsrc" ] && echo "[stage z${Z}] COUNT OK" || echo "[stage z${Z}] COUNT MISMATCH staged=$n src=$nsrc (run again to pick up stragglers)"

# provenance + README (overwrite each run with current state)
cat > "$DST/metadata/provenance_z${Z}.json" <<JSON
{
  "redshift": "z${Z}",
  "n_realizations": $n,
  "bytes": $bytes,
  "source": "$SRC",
  "simulation": "AbacusSummit_small_c000 (DESI-public, ph3000-ph4999)",
  "data_model": "SFR-only; galaxies/{sfr_corr,sfr_raw,mstar_corr,mstar_raw,mpeak,pos,vel,...}; Lbox=500 Mpc/h",
  "staged_by": "perlmutter-marcelos-claude (xfer queue)"
}
JSON
cat > "$DST/README.md" <<'README'
# Covariance mocks (SFR-only) — v1

Ensemble of SFR-only mock galaxy catalogs (diffsky on AbacusSummit_small) for
covariance estimation by the GRS-PIT team. One consolidated HDF5 per realization,
laid out redshift-major:

```
v1/catalogs/z<redshift>/r<NNNN>.hdf5   # realization NNNN at that redshift
v1/metadata/manifest_z<redshift>.csv   # realization, bytes, source
v1/metadata/provenance_z<redshift>.json
```

Each HDF5 has a `galaxies/` group with per-object columns: `sfr_corr`, `sfr_raw`,
`mstar_corr`, `mstar_raw`, `mpeak`, `pos` (N,3 Mpc/h), `vel` (N,3). Box size
`Lbox=500` Mpc/h (attr). The realization ensemble at a fixed redshift is the
covariance mock: select a consistent sample per realization, then estimate
statistics/covariance with your own tools.
README

echo "[stage z${Z}] manifest + provenance + README written under $DST"
