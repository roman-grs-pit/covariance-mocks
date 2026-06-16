#!/bin/bash
# One-shot dashboard refresh (for scrontab): regenerate the 9-panel highlight + status table
# from sidecars and push if anything changed. No catalog re-reads.
set -u
REPO=/global/homes/m/malvarez/work/grspit/covariance-mocks
DASH=$REPO/local/dashboard
PY=/global/cfs/cdirs/m4943/Simulations/covariance_mocks/conda_envs/grspit/bin/python
DREPO=/global/u2/m/malvarez/.collab-ai-context-dashboards
PROD=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0
ZL="1.400 1.700 2.500 1.100 2.000 3.000 0.200 0.250 0.300 0.400 0.500 0.800"
git -C "$DREPO" pull --ff-only -q 2>/dev/null
"$PY" "$DASH/make_highlight_6panel.py" --prod "$PROD" --z $ZL --out "$DASH/reports/_fig_highlight6_z1.400.png" >/dev/null 2>&1
"$PY" "$DASH/status_sfr.py" >/dev/null 2>&1
if ! git -C "$DREPO" diff --quiet 2>/dev/null; then
    git -C "$DREPO" add -A
    git -C "$DREPO" -c user.name="Marcelo Alvarez" -c user.email="marcelo.alvarez@stanford.edu" \
        commit -q -m "dashboard: scheduled auto-refresh"
    git -C "$DREPO" push -q
fi
echo "refresh done $(date)"
