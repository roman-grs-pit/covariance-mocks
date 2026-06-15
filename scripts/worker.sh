#!/bin/bash
#SBATCH --account=cosmosim_g
#SBATCH --qos=regular
#SBATCH --constraint=gpu
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=32
#SBATCH --gpus-per-node=4
#SBATCH --time=01:00:00
#SBATCH --job-name=covmock_worker
#SBATCH --output=/global/cfs/cdirs/cosmosim/covariance_mocks/full_v1.0/logs/worker_%j.out
#SBATCH --error=/global/cfs/cdirs/cosmosim/covariance_mocks/full_v1.0/logs/worker_%j.err
#
# Self-scheduling worker: claims boxes from a shared worklist (atomic mkdir),
# packs as many populate runs as fit in the allocation, exits cleanly with
# margin so it never dies mid-box. Re-submittable (skips done/claimed boxes).
#
# Usage (set by the launcher): sbatch worker.sh <worklist> <claims_dir> [margin_s]

set -u
REPO=/global/homes/m/malvarez/work/grspit/covariance-mocks
GEN="$REPO/scripts/generate_single_mock.py"
source "$REPO/scripts/load_env.sh"

WL="${1:?worklist path required}"
CLAIMS="${2:?claims dir required}"
MARGIN="${3:-420}"        # seconds of headroom (observed max box 305 s + buffer)

# Allocation end time (epoch), for the remaining-walltime gate.
ENDTS=$(date -d "$(scontrol show job "$SLURM_JOB_ID" 2>/dev/null \
        | grep -oP 'EndTime=\K[0-9T:-]+')" +%s 2>/dev/null)
[ -z "${ENDTS:-}" ] && ENDTS=$(( $(date +%s) + 6*3600 ))
echo "[worker $SLURM_JOB_ID @ $(hostname)] end=$(date -d "@$ENDTS") margin=${MARGIN}s"

ndone=0; nfail=0
while IFS=' ' read -r real z out; do
    [ -z "${real:-}" ] && continue
    now=$(date +%s)
    if [ $(( ENDTS - now )) -lt "$MARGIN" ]; then
        echo "[worker $SLURM_JOB_ID] $(( ENDTS - now ))s left < ${MARGIN}s margin -> clean exit"
        break
    fi
    box="r${real}_z${z}"
    # already produced?
    compgen -G "$out/mock_AbacusSummit*.hdf5" >/dev/null 2>&1 && continue
    # atomic claim: only one worker wins the mkdir
    mkdir "$CLAIMS/$box" 2>/dev/null || continue
    compgen -G "$out/mock_AbacusSummit*.hdf5" >/dev/null 2>&1 && continue   # race recheck
    mkdir -p "$out"
    echo "[worker $SLURM_JOB_ID $(date '+%H:%M:%S')] populate $box"
    srun -n 8 python "$GEN" nersc "$out" --realization "$real" --redshift "$z"
    rc=$?
    if [ "$rc" -eq 0 ] && compgen -G "$out/mock_AbacusSummit*.hdf5" >/dev/null 2>&1; then
        ndone=$(( ndone + 1 ))
    else
        echo "[worker $SLURM_JOB_ID] $box FAILED rc=$rc -> release claim for retry"
        rmdir "$CLAIMS/$box" 2>/dev/null
        nfail=$(( nfail + 1 ))
    fi
done < "$WL"
echo "[worker $SLURM_JOB_ID] done: completed=$ndone failed=$nfail"

# Resubmit-on-timeout: chain a successor while boxes remain to claim, so the pool
# self-sustains (1-in / 1-out keeps it at the launched size). Stop when every box
# is claimed (nothing left to do) — claims released by failures get re-picked up.
total=$(wc -l < "$WL")
claimed=$(ls "$CLAIMS" 2>/dev/null | wc -l)
if [ "$claimed" -lt "$total" ]; then
    (cd "$REPO" && sbatch scripts/worker.sh "$WL" "$CLAIMS" "$MARGIN" >/dev/null 2>&1) \
        && echo "[worker $SLURM_JOB_ID] resubmitted successor (claimed $claimed/$total)"
else
    echo "[worker $SLURM_JOB_ID] all $total boxes claimed — not resubmitting"
fi
