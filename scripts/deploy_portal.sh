#!/bin/bash
# Deploy a no-frills landing page for the covariance mocks to the NERSC CFS portal.
#
# Serves /global/cfs/cdirs/m4943/www/covariance_mocks (world-readable "www" dir) at
# https://portal.nersc.gov/cfs/m4943/covariance_mocks. Writes index.html and the SFR-only
# highlight image, and cross-references the full docs on readthedocs. The data tree (v1/)
# already lives under the portal dir; this only adds the landing page.
#
# Usage: bash scripts/deploy_portal.sh
set -u
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WWW=/global/cfs/cdirs/m4943/www/covariance_mocks
RTD=https://grs-pit-covariance-mocks.readthedocs.io
PORTAL=https://portal.nersc.gov/cfs/m4943/covariance_mocks
HILITE_SRC="$REPO/local/dashboard/reports/_fig_highlight6_z1.400.png"

[ -d "$WWW" ] || { echo "portal dir not found: $WWW"; exit 1; }

# highlight image (9-panel SFR-only figure)
if [ -f "$HILITE_SRC" ]; then
    cp -p "$HILITE_SRC" "$WWW/highlight.png"
    chmod a+r "$WWW/highlight.png"
    HILITE_TAG='<img src="highlight.png" alt="SFR-only covariance mocks — highlight panels">'
else
    echo "WARN: highlight image not found ($HILITE_SRC); page will omit it"
    HILITE_TAG='<p><em>Highlight figure not available.</em></p>'
fi

# available redshifts (rows: redshift, #realizations) from the staged tree
ROWS=""
for d in "$WWW"/v1/catalogs/z*/; do
    [ -d "$d" ] || continue
    z=$(basename "$d" | sed 's/^z//')
    n=$(ls "$d" 2>/dev/null | wc -l)
    ROWS="${ROWS}      <tr><td>${z}</td><td>${n}</td></tr>
"
done
[ -n "$ROWS" ] || ROWS='      <tr><td colspan="2">(none staged yet)</td></tr>'

UPDATED=$(date -u '+%Y-%m-%d %H:%M UTC')

cat > "$WWW/index.html" <<HTML
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Roman GRS Covariance Mocks</title>
<style>
  body { font: 16px/1.5 -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
         color: #222; max-width: 900px; margin: 2rem auto; padding: 0 1rem; }
  h1 { font-size: 1.7rem; margin-bottom: 0.2rem; }
  h2 { font-size: 1.2rem; margin-top: 2rem; border-bottom: 1px solid #ddd; padding-bottom: 0.2rem; }
  code { background: #f4f4f4; padding: 0.1em 0.3em; border-radius: 3px; }
  table { border-collapse: collapse; margin: 0.5rem 0; }
  th, td { border: 1px solid #ccc; padding: 0.3rem 0.8rem; text-align: left; }
  th { background: #f4f4f4; }
  img { max-width: 100%; height: auto; border: 1px solid #ddd; margin-top: 0.5rem; }
  a { color: #1a5fb4; }
  .muted { color: #666; font-size: 0.9rem; }
</style>
</head>
<body>

<h1>Roman GRS Covariance Mocks</h1>
<p class="muted">Mock galaxy catalogs and a Python interface for selecting samples from them.</p>

<p>An ensemble of SFR-only mock galaxy catalogs (diffsky on the AbacusSummit_small boxes)
for the Roman Galaxy Redshift Survey. Each redshift is a set of independent realizations;
each realization is one HDF5 file.</p>

<p><strong>Full documentation:</strong>
<a href="$RTD">$RTD</a> &mdash; install, quickstart, the selection interface, and the
catalog format. This page is the data portal; the docs site has the how-to.</p>

<h2>Highlight</h2>
$HILITE_TAG

<h2>Data access</h2>
<p>Browse the catalogs here on the portal:
<a href="v1/">v1/</a>. On NERSC they are at
<code>/global/cfs/cdirs/m4943/covariance_mocks/v1/</code>, laid out redshift-major:</p>
<pre>v1/catalogs/z&lt;redshift&gt;/r&lt;NNNN&gt;.hdf5   # realization NNNN at that redshift
v1/metadata/manifest_z&lt;redshift&gt;.csv   # realizations present, sizes, source</pre>

<p>Available redshifts (realizations staged):</p>
<table>
  <thead><tr><th>redshift</th><th>realizations</th></tr></thead>
  <tbody>
$ROWS
  </tbody>
</table>
<p class="muted">z=1.4 is a complete ensemble; the other redshifts are partial ensembles,
to be completed in a later run. See each <code>manifest_z&lt;redshift&gt;.csv</code> for the
exact list.</p>

<h2>Catalog columns</h2>
<table>
  <thead><tr><th>column</th><th>units</th><th>description</th></tr></thead>
  <tbody>
    <tr><td>sfr_corr</td><td>Msun/yr</td><td>Calibrated star-formation rate</td></tr>
    <tr><td>sfr_raw</td><td>Msun/yr</td><td>Raw star-formation rate</td></tr>
    <tr><td>mstar_corr</td><td>Msun</td><td>Calibrated stellar mass</td></tr>
    <tr><td>mstar_raw</td><td>Msun</td><td>Raw stellar mass</td></tr>
    <tr><td>mpeak</td><td>Msun</td><td>Halo peak mass</td></tr>
    <tr><td>pos</td><td>Mpc/h</td><td>Comoving position (N,3)</td></tr>
    <tr><td>vel</td><td>as stored</td><td>Peculiar velocity (N,3)</td></tr>
  </tbody>
</table>
<p>Box size <code>Lbox = 500</code> Mpc/h. See
<a href="$RTD/en/latest/quickstart.html">the quickstart</a> for loading a catalog and
selecting a sample.</p>

<p class="muted">Portal: <a href="$PORTAL">$PORTAL</a> &middot; updated $UPDATED</p>

</body>
</html>
HTML

chmod a+r "$WWW/index.html"
echo "deployed: $WWW/index.html  ->  $PORTAL"
