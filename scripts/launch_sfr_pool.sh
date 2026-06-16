#!/bin/bash
# Launch the SFR-only worker pool for one redshift (sfr_v1.0).
#   ./launch_sfr_pool.sh <redshift> [n_workers]
# Builds the worklist (if missing) and submits an initial wave of self-scheduling workers;
# each chains a successor while boxes remain, so the pool self-sustains at the launched size.
set -eu
REPO=/global/homes/m/malvarez/work/grspit/covariance-mocks
OUTROOT=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0
PY=/global/cfs/cdirs/m4943/Simulations/covariance_mocks/conda_envs/grspit/bin/python

Z="${1:?redshift required, e.g. 1.400}"
NREG="${2:-60}"          # regular-QOS workers (backfill surface; cap 5000)
NPREM="${3:-5}"          # premium-QOS workers (high priority; per-user cap 5)
ZS=$(printf "%.3f" "$Z")
WL="$OUTROOT/worklist_z${ZS}.txt"
CLAIMS="$OUTROOT/claims_z${ZS}"

[ -f "$WL" ] || "$PY" "$REPO/scripts/build_worklist_sfr.py" "$Z"
TOTAL=$(wc -l < "$WL")
mkdir -p "$OUTROOT/logs"
echo "worklist $WL: $TOTAL boxes; submitting $NPREM premium + $NREG regular workers"
# premium first (fast scheduling, free on Perlmutter GPU), then a regular backfill pool
for i in $(seq 1 "$NPREM"); do
    sbatch --qos=premium "$REPO/scripts/worker_sfr.sh" "$WL" "$CLAIMS" 420 premium >/dev/null 2>&1 \
        || echo "  premium submit $i hit the cap (expected past 5)"
done
for i in $(seq 1 "$NREG"); do
    sbatch --qos=regular "$REPO/scripts/worker_sfr.sh" "$WL" "$CLAIMS" 420 regular >/dev/null
done
echo "submitted for z=$ZS:"
squeue -u "$USER" -h -o "%q" | sort | uniq -c
