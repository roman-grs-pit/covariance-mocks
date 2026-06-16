#!/bin/bash
#SBATCH --account=cosmosim
#SBATCH --qos=regular
#SBATCH --constraint=cpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --time=00:30:00
#SBATCH --job-name=sfr_backfill
#SBATCH --output=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/logs/backfill_%j.out
#SBATCH --error=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/logs/backfill_%j.err
#
# One-time backfill: generate the per-box stats sidecar for every already-populated box that
# lacks one (boxes finished before the worker emitted sidecars). Read-only over the catalogs;
# on a CPU node, NOT the login node. New boxes get their sidecar at populate time (worker_sfr.sh).
set -u
REPO=/global/homes/m/malvarez/work/grspit/covariance-mocks
PY=/global/cfs/cdirs/m4943/Simulations/covariance_mocks/conda_envs/grspit/bin/python
PROD=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0
Z="${1:-1.400}"
NP="${2:-128}"

echo "[backfill $(date)] scanning for catalogs without a sidecar (z=$Z)"
# emit only where the sidecar is missing
for c in "$PROD"/catalogs/r*/mock_z${Z}.hdf5; do
    [ -e "$c" ] || continue
    s="${c%.hdf5}.stats.npz"
    [ -e "$s" ] || echo "$c"
done | xargs -P "$NP" -n 1 -r "$PY" "$REPO/scripts/box_stats.py" >/dev/null
echo "[backfill $(date)] done: $(ls "$PROD"/catalogs/r*/mock_z${Z}.stats.npz 2>/dev/null | wc -l) sidecars total"
