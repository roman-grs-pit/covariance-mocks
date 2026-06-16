#!/bin/bash
# Self-refill watchdog for the SFR-only BREADTH premium pool (sfr_v1.0).
#
# Maintains a fixed premium configuration of concurrent-packing jobs so the pool
# sustains itself with no manual relaunching (premium per-user cap is 5 jobs):
#   PACK20_TARGET  x  20-node packs (worker_sfr_pack.sh, 10 boxes in parallel each)
#   PACK40_TARGET  x  40-node packs (worker_sfr_pack.sh, 20 boxes in parallel each)
#   NORMAL_TARGET  x  2-node single-box workers (worker_sfr.sh; fills leftover slots)
# Targets sum to <= 5. Jobs are classified by node count (squeue %D), all share the
# job-name "sfr_worker" and qos premium. Pack jobs are one-off (no self-resubmit), so
# THIS watchdog is what sustains them: when a 5 h pack ends, the next tick relaunches it.
#
# Also keeps a regular-QOS backfill floor for when the queue loosens. Idempotent:
# submits only up to each target, so it is safe to run on the cron (every 15 min).
set -u

REPO=/global/homes/m/malvarez/work/grspit/covariance-mocks
OUTROOT=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0
WL="$OUTROOT/worklist_breadth.txt"
CLAIMS="$OUTROOT/claims_breadth"

PACK20_TARGET=3    # 20-node packs (10 streams x 2 nodes)
PACK40_TARGET=1    # 40-node packs (20 streams x 2 nodes)
NORMAL_TARGET=1    # 2-node single-box premium workers (the 5th slot)
REG_TARGET=64      # regular backfill floor
PACK_MARGIN=600
NORM_MARGIN=420

# Stop refilling once every breadth box is produced. Count actual output files, not
# claim dirs: workers skip already-existing boxes without creating a claim.
total=$(wc -l < "$WL" 2>/dev/null || echo 0)
produced=$(find "$OUTROOT/catalogs" -name 'mock_z*.hdf5' 2>/dev/null | wc -l)
if [ "$total" -gt 0 ] && [ "$produced" -ge "$total" ]; then
    echo "[topup $(date -u +%FT%TZ)] breadth complete ($produced/$total produced) — nothing to refill"
    exit 0
fi

# Classify live (running+pending+completing) premium sfr_worker jobs by node count.
prem=$(squeue -u "$USER" -h -o "%j %q %D" 2>/dev/null | awk '$1=="sfr_worker" && $2=="gpu_premium"{print $3}')
n20=$(printf '%s\n' "$prem" | grep -cx 20)
n40=$(printf '%s\n' "$prem" | grep -cx 40)
n2=$( printf '%s\n' "$prem" | grep -cx 2)
nreg=$(squeue -u "$USER" -h -o "%j %q" 2>/dev/null | awk '$1=="sfr_worker" && $2=="gpu_regular"' | wc -l)

a20=$(( PACK20_TARGET - n20 )); [ "$a20" -lt 0 ] && a20=0
a40=$(( PACK40_TARGET - n40 )); [ "$a40" -lt 0 ] && a40=0
a2=$((  NORMAL_TARGET - n2  )); [ "$a2"  -lt 0 ] && a2=0
areg=$(( REG_TARGET - nreg ));  [ "$areg" -lt 0 ] && areg=0

cd "$REPO" || exit 1
for _ in $(seq 1 "$a40"); do
    sbatch --nodes=40 scripts/worker_sfr_pack.sh "$WL" "$CLAIMS" "$PACK_MARGIN" 20 2 >/dev/null 2>&1 || true
done
for _ in $(seq 1 "$a20"); do
    sbatch --nodes=20 scripts/worker_sfr_pack.sh "$WL" "$CLAIMS" "$PACK_MARGIN" 10 2 >/dev/null 2>&1 || true
done
for _ in $(seq 1 "$a2"); do
    sbatch --qos=premium scripts/worker_sfr.sh "$WL" "$CLAIMS" "$NORM_MARGIN" premium >/dev/null 2>&1 || true
done
for _ in $(seq 1 "$areg"); do
    sbatch --qos=regular scripts/worker_sfr.sh "$WL" "$CLAIMS" "$NORM_MARGIN" regular >/dev/null 2>&1 || true
done
echo "[topup $(date -u +%FT%TZ)] premium pack20 ${n20}->${PACK20_TARGET} (+${a20}), pack40 ${n40}->${PACK40_TARGET} (+${a40}), normal ${n2}->${NORMAL_TARGET} (+${a2}); regular ${nreg}->${REG_TARGET} (+${areg}); produced ${produced}/${total}"
