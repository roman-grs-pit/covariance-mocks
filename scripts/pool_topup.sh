#!/bin/bash
# Self-refill watchdog for the SFR-only BREADTH pool (sfr_v1.0).
#
# Keeps the premium pool pinned at its per-user cap and a regular backfill floor,
# both pointed at the breadth worklist (realization-outer, z-inner over ALL z), so
# the pool sustains itself with no manual relaunching. Idempotent: submits only up
# to the target, so it is safe to run on a cron (see scrontab entry `pool_topup`).
#
# Throughput model on a saturated Perlmutter GPU queue (fairshare weight 0):
# premium (free priority boost, cap 5) is the real engine; regular almost never
# backfills but is a zero-cost option that pays off when the queue loosens. So the
# dominant job of this watchdog is to keep premium at 5.
#
# Workers also self-chain (worker_sfr.sh resubmits one successor while boxes remain),
# so this watchdog is the safety net that refills slots lost to job failures/timeouts
# and tops premium back to the cap; the two do not compound (each only replaces to
# the target / 1:1, and Slurm caps premium at 5 regardless).
set -u

REPO=/global/homes/m/malvarez/work/grspit/covariance-mocks
OUTROOT=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0
WL="$OUTROOT/worklist_breadth.txt"
CLAIMS="$OUTROOT/claims_breadth"
MARGIN=420
PREM_TARGET=5      # premium per-user cap on Perlmutter GPU
REG_TARGET=64      # regular backfill floor (self-chaining sustains any surplus above this)

# Stop refilling once the whole breadth set is claimed/done.
total=$(wc -l < "$WL" 2>/dev/null || echo 0)
claimed=$(ls "$CLAIMS" 2>/dev/null | wc -l)
if [ "$total" -gt 0 ] && [ "$claimed" -ge "$total" ]; then
    echo "[topup $(date -u +%FT%TZ)] breadth complete ($claimed/$total) — nothing to refill"
    exit 0
fi

# Count live (running+pending) breadth workers by QOS. After the z1.4 depth list is
# retired, every sfr_worker is a breadth worker, so a name+QOS count is sufficient.
counts=$(squeue -u "$USER" -h -o "%j %T %q" 2>/dev/null | awk '$1=="sfr_worker"{print $3}')
nprem=$(printf '%s\n' "$counts" | grep -c gpu_premium)
nreg=$( printf '%s\n' "$counts" | grep -c gpu_regular)

add_prem=$(( PREM_TARGET - nprem )); [ "$add_prem" -lt 0 ] && add_prem=0
add_reg=$((  REG_TARGET  - nreg  )); [ "$add_reg"  -lt 0 ] && add_reg=0

cd "$REPO" || exit 1
for _ in $(seq 1 "$add_prem"); do
    sbatch --qos=premium scripts/worker_sfr.sh "$WL" "$CLAIMS" "$MARGIN" premium >/dev/null 2>&1 || true
done
for _ in $(seq 1 "$add_reg"); do
    sbatch --qos=regular scripts/worker_sfr.sh "$WL" "$CLAIMS" "$MARGIN" regular >/dev/null 2>&1 || true
done
echo "[topup $(date -u +%FT%TZ)] premium ${nprem}->${PREM_TARGET} (+${add_prem}), regular ${nreg}->${REG_TARGET} (+${add_reg}), claimed ${claimed}/${total}"
