#!/bin/bash
#SBATCH --account=cosmosim_g
#SBATCH --qos=premium
#SBATCH --constraint=gpu
#SBATCH --nodes=20
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=32
#SBATCH --gpus-per-node=4
#SBATCH --time=05:00:00
#SBATCH --job-name=sfr_worker
#SBATCH --output=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/logs/packworker_%j.out
#SBATCH --error=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0/logs/packworker_%j.err
#
# CONCURRENT-PACKING EXPERIMENT (throughput test for the 5-job premium cap).
# One big premium allocation runs NSTREAMS boxes IN PARALLEL, each on a disjoint
# NODES_PER-node slice, each slice refilling from the breadth worklist for the whole
# walltime. Tests whether 20 nodes x 10 parallel boxes beats 5 x 2-node premium jobs.
#
# Job-name is kept "sfr_worker" so the pool_topup watchdog counts it as one of the 5
# premium slots (it then holds the other 4 as normal 2-node workers). ONE-OFF: this
# worker does NOT self-resubmit a successor.
#
# Usage: sbatch worker_sfr_pack.sh <worklist> <claims_dir> [margin_s] [nstreams] [nodes_per]
set -u
REPO=/global/homes/m/malvarez/work/grspit/covariance-mocks
GEN="$REPO/scripts/generate_single_mock.py"
source "$REPO/scripts/load_env.sh"

WL="${1:?worklist path required}"
CLAIMS="${2:?claims dir required}"
MARGIN="${3:-600}"
NSTREAMS="${4:-10}"
NODES_PER="${5:-2}"
TASKS_PER=$(( NODES_PER * 4 ))

ENDTS=$(date -d "$(scontrol show job "$SLURM_JOB_ID" 2>/dev/null \
        | grep -oP 'EndTime=\K[0-9T:-]+')" +%s 2>/dev/null)
[ -z "${ENDTS:-}" ] && ENDTS=$(( $(date +%s) + 5*3600 ))

mapfile -t NODES < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
echo "[pack $SLURM_JOB_ID @ $(date '+%H:%M:%S')] ${#NODES[@]} nodes; $NSTREAMS streams x ${NODES_PER} nodes; end=$(date -d "@$ENDTS")"

# One stream: owns a fixed node slice; walks its stride of the worklist; claims and
# populates boxes serially on its slice until the time margin runs out.
run_stream() {
    local sid="$1" slice="$2"
    local lineno=-1 ndone=0 nfail=0
    while IFS=' ' read -r real z out; do
        [ -z "${real:-}" ] && continue
        lineno=$(( lineno + 1 ))
        [ $(( lineno % NSTREAMS )) -ne "$sid" ] && continue      # stride: this stream's share
        now=$(date +%s)
        if [ $(( ENDTS - now )) -lt "$MARGIN" ]; then break; fi
        box="r${real}_z${z}"
        compgen -G "$out" >/dev/null 2>&1 && continue            # already produced
        mkdir "$CLAIMS/$box" 2>/dev/null || continue             # atomic claim
        compgen -G "$out" >/dev/null 2>&1 && { rmdir "$CLAIMS/$box" 2>/dev/null; continue; }
        outdir=$(dirname "$out"); mkdir -p "$outdir"
        echo "[pack $SLURM_JOB_ID s$sid $(date '+%H:%M:%S')] populate $box on $slice"
        srun --nodelist="$slice" --nodes="$NODES_PER" --ntasks="$TASKS_PER" \
             --ntasks-per-node=4 --gpus-per-node=4 --cpus-per-task=32 --exact \
             python "$GEN" nersc "$outdir" --realization "$real" --redshift "$z" </dev/null
        rc=$?
        if [ "$rc" -eq 0 ] && compgen -G "$outdir/mock_AbacusSummit*z${z}*.hdf5" >/dev/null 2>&1; then
            gen=$(ls "$outdir"/mock_AbacusSummit*z${z}*.hdf5 2>/dev/null | head -1)
            [ -n "$gen" ] && [ "$gen" != "$out" ] && mv -f "$gen" "$out"
            python "$REPO/scripts/box_stats.py" "$out" >/dev/null 2>&1 || true
            ndone=$(( ndone + 1 ))
        else
            echo "[pack $SLURM_JOB_ID s$sid] $box FAILED rc=$rc -> release claim"
            rmdir "$CLAIMS/$box" 2>/dev/null
            nfail=$(( nfail + 1 ))
        fi
    done < "$WL"
    echo "[pack $SLURM_JOB_ID s$sid] stream done: completed=$ndone failed=$nfail"
}

for s in $(seq 0 $(( NSTREAMS - 1 )) ); do
    base=$(( s * NODES_PER ))
    slice=$(IFS=,; echo "${NODES[*]:$base:$NODES_PER}")
    run_stream "$s" "$slice" &
done
wait
echo "[pack $SLURM_JOB_ID @ $(date '+%H:%M:%S')] all $NSTREAMS streams complete — NOT resubmitting (one-off experiment)"
