#!/bin/bash
#SBATCH --account=cosmosim
#SBATCH --qos=shared
#SBATCH --constraint=cpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=120G
#SBATCH --time=00:40:00
#SBATCH --job-name=sfr_refresh
#SBATCH --output=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/logs/refresh_%j.out
#SBATCH --error=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/logs/refresh_%j.err
#
# Refresh the SFR-only highlight on a CPU node (NOT the login node): run the ensemble
# calibrate/apply over all completed z=1.4 catalogs, then regenerate the 6-panel PNG.
# The dashboard push happens afterward from the login node (compute nodes have no network).
set -u
REPO=/global/homes/m/malvarez/work/grspit/covariance-mocks
DASH="$REPO/local/dashboard"
PY=/global/cfs/cdirs/m4943/Simulations/covariance_mocks/conda_envs/grspit/bin/python
PROD=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0
Z="${1:-1.400}"
NP="${2:-64}"

echo "[refresh $(date)] calibrate/apply over completed z=$Z catalogs"
$PY "$REPO/scripts/sfr_calibrate_apply.py" run --z "$Z" \
    --catalogs "$PROD/catalogs/r*/mock_z${Z}.hdf5" \
    --um-targets "$DASH/refdata/um_targets_z${Z}.npz" \
    --out "$PROD/_calib" --procs "$NP"

echo "[refresh $(date)] regenerate 6-panel"
$PY "$DASH/make_highlight_6panel.py" --prod "$PROD" --z "$Z" \
    --out "$DASH/reports/_fig_highlight6_z${Z}.png"
echo "[refresh $(date)] done — figure at $DASH/reports/_fig_highlight6_z${Z}.png"
