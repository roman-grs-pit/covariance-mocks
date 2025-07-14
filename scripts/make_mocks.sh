#!/bin/bash

# Run the mock generation pipeline with optional flags
# Usage: ./scripts/make_mocks.sh [--force] [--test]

logfile="mocks.log"
rm -f "$logfile"  # Clear previous log file
touch "$logfile"  # Create a new log file
exec > >(tee -a "$logfile") 2>&1

printf "Mock generation pipeline invoked with:\n  %s %s\n" "$0" "$@"

SCRIPT_DIR=$( cd -- "$( dirname "$(readlink -f "$0")" )" &>/dev/null && pwd )
echo "SCRIPT DIR IS $SCRIPT_DIR"

# Set output directory from environment variable or default
if [[ -n "$GRS_COV_MOCKS_DIR" ]]; then
    OUTPUT_DIR="$GRS_COV_MOCKS_DIR"
else
    OUTPUT_DIR="$SCRATCH/grspit/covariance_mocks/data"
fi

# Load environment
source $SCRIPT_DIR/load_env.sh

# Call the Python shared core logic
python "$SCRIPT_DIR/../tests/integration_core.py" "$OUTPUT_DIR" "$@"
exit_code=$?

if [[ $exit_code -eq 0 ]]; then
    echo "Mock generation pipeline completed successfully"
else
    echo "Mock generation pipeline failed with exit code $exit_code"
fi

exit $exit_code
