#!/bin/bash
# Auto-refresh the dashboard from sidecars (cheap) until the z=1.4 populate completes.
set -u
REPO=/global/homes/m/malvarez/work/grspit/covariance-mocks
DASH=$REPO/local/dashboard
PY=/global/cfs/cdirs/m4943/Simulations/covariance_mocks/conda_envs/grspit/bin/python
DREPO=/global/u2/m/malvarez/.collab-ai-context-dashboards
PROD=/global/cfs/cdirs/cosmosim/covariance_mocks/sfr_v1.0
Z=1.400; TOTAL=1882
for i in $(seq 1 30); do
  n=$(ls $PROD/catalogs/r*/mock_z${Z}.hdf5 2>/dev/null | wc -l)
  sc=$(ls $PROD/catalogs/r*/mock_z${Z}.stats.npz 2>/dev/null | wc -l)
  git -C "$DREPO" pull --ff-only -q 2>/dev/null
  $PY "$DASH/make_highlight_6panel.py" --prod $PROD --z 1.400 1.700 2.500 1.100 2.000 3.000 0.200 0.250 0.300 0.400 0.500 0.800 --out "$DASH/reports/_fig_highlight6_z${Z}.png" >/dev/null 2>&1
  $PY "$DASH/status_sfr.py" $Z >/dev/null 2>&1
  if ! git -C "$DREPO" diff --quiet 2>/dev/null; then
    git -C "$DREPO" add -A
    git -C "$DREPO" -c user.name="Marcelo Alvarez" -c user.email="marcelo.alvarez@stanford.edu" \
        commit -q -m "dashboard: auto-refresh — $n/$TOTAL boxes, $sc sidecars" >/dev/null 2>&1
    git -C "$DREPO" push -q >/dev/null 2>&1
  fi
  echo "[refresh $i $(date +%H:%M)] boxes=$n sidecars=$sc -> pushed"
  [ "$n" -ge "$TOTAL" ] && { echo "POPULATE COMPLETE"; break; }
  sleep 600
done
