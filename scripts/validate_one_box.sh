#!/bin/bash
#SBATCH --account=cosmosim_g
#SBATCH --qos=regular
#SBATCH --constraint=gpu
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=32
#SBATCH --gpus-per-node=4
#SBATCH --time=00:20:00
#SBATCH --job-name=sfr_validate1
#SBATCH --output=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/_validate/logs/validate_%j.out
#SBATCH --error=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/_validate/logs/validate_%j.err
#
# One-box end-to-end validation of the SFR-only generator: populate a single
# (realization, redshift) box with the emission-line step removed, confirming the
# raw catalog writes the expected fields before the ensemble launch.
set -u
REPO=/global/homes/m/malvarez/work/grspit/covariance-mocks
GEN="$REPO/scripts/generate_single_mock.py"
source "$REPO/scripts/load_env.sh"

REAL="${1:-3000}"
Z="${2:-1.400}"
OUT="${3:-/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/_validate}"
mkdir -p "$OUT"
echo "[validate] populate r${REAL} z${Z} -> $OUT"
srun -n 8 python "$GEN" nersc "$OUT" --realization "$REAL" --redshift "$Z" </dev/null
echo "[validate] exit rc=$? ; outputs:"
ls -la "$OUT"/mock_AbacusSummit*z${Z}*.hdf5 2>/dev/null
