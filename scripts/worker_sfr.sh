#!/bin/bash
#SBATCH --account=cosmosim_g
#SBATCH --qos=premium
#SBATCH --constraint=gpu
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=32
#SBATCH --gpus-per-node=4
#SBATCH --time=01:00:00
#SBATCH --job-name=sfr_worker
#SBATCH --output=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/logs/worker_%j.out
#SBATCH --error=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/logs/worker_%j.err
#
# Self-scheduling worker for the SFR-only campaign (sfr_v1.0). Claims boxes from a
# shared worklist (atomic mkdir), packs as many populate runs as fit in the allocation,
# exits cleanly with margin, and chains a successor while boxes remain. Identical pool
# logic to the original worker.sh; only the output root and successor script differ.
#
# Usage: sbatch worker_sfr.sh <worklist> <claims_dir> [margin_s]
set -u
REPO=/global/homes/m/malvarez/work/grspit/covariance-mocks
GEN="$REPO/scripts/generate_single_mock.py"
source "$REPO/scripts/load_env.sh"

WL="${1:?worklist path required}"
CLAIMS="${2:?claims dir required}"
MARGIN="${3:-420}"
QOS="${4:-premium}"     # premium (cap 5, high priority) or regular (cap 5000, backfill surface)

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
    compgen -G "$out" >/dev/null 2>&1 && continue                  # already produced
    mkdir "$CLAIMS/$box" 2>/dev/null || continue                    # atomic claim
    compgen -G "$out" >/dev/null 2>&1 && continue                   # race recheck
    outdir=$(dirname "$out"); mkdir -p "$outdir"
    echo "[worker $SLURM_JOB_ID $(date '+%H:%M:%S')] populate $box"
    srun -n 8 python "$GEN" nersc "$outdir" --realization "$real" --redshift "$z" </dev/null
    rc=$?
    if [ "$rc" -eq 0 ] && compgen -G "$outdir/mock_AbacusSummit*.hdf5" >/dev/null 2>&1; then
        # normalize the generated filename to the worklist's expected path
        gen=$(ls "$outdir"/mock_AbacusSummit*z${z}*.hdf5 2>/dev/null | head -1)
        [ -n "$gen" ] && [ "$gen" != "$out" ] && mv -f "$gen" "$out"
        # compute-as-you-go: emit the per-box stats sidecar now, while the catalog is fresh
        # in page cache, so the dashboard never re-reads the 2.9 GB catalogs on refresh.
        python "$REPO/scripts/box_stats.py" "$out" >/dev/null 2>&1 || echo "[worker $SLURM_JOB_ID] sidecar failed for $box"
        ndone=$(( ndone + 1 ))
    else
        echo "[worker $SLURM_JOB_ID] $box FAILED rc=$rc -> release claim for retry"
        rmdir "$CLAIMS/$box" 2>/dev/null
        nfail=$(( nfail + 1 ))
    fi
done < "$WL"
echo "[worker $SLURM_JOB_ID] done: completed=$ndone failed=$nfail"

total=$(wc -l < "$WL")
claimed=$(ls "$CLAIMS" 2>/dev/null | wc -l)
if [ "$claimed" -lt "$total" ]; then
    # resubmit under the same QOS this worker ran with. A premium successor may bounce off
    # the 5-job premium cap (QOSMaxSubmitJobPerUserLimit) -- harmless, the regular pool
    # sustains backfill surface and a premium slot reopens when another premium job ends.
    (cd "$REPO" && sbatch --qos="$QOS" scripts/worker_sfr.sh "$WL" "$CLAIMS" "$MARGIN" "$QOS" >/dev/null 2>&1) \
        && echo "[worker $SLURM_JOB_ID] resubmitted successor qos=$QOS (claimed $claimed/$total)"
else
    echo "[worker $SLURM_JOB_ID] all $total boxes claimed — not resubmitting"
fi
