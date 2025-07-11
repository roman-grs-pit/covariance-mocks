#!/bin/bash

# Run the mock generation pipeline with optional flags
# Usage: ./scripts/make_mocks.sh [--force] [--test]

logfile="mocks.log"
rm -f "$logfile"  # Clear previous log file
touch "$logfile"  # Create a new log file
exec > >(tee -a "$logfile") 2>&1

printf "Mock generation pipeline invoked with:\n  %s %s\n" "$0" "$@"

# Set default parameters
NERSC_ACCOUNT=m4943
FORCE_RUN=false
TEST_MODE=false
N_GEN=5000
TIME_LIMIT=10
SCRIPT_DIR=$( cd -- "$( dirname "$(readlink -f "$0")" )" &>/dev/null && pwd )
echo "SCRIPT DIR IS $SCRIPT_DIR"

# Set output directory from environment variable or default
if [[ -n "$GRS_COV_MOCKS_DIR" ]]; then
    OUTPUT_DIR="$GRS_COV_MOCKS_DIR"
else
    OUTPUT_DIR="$SCRATCH/grspit/covariance_mocks/data"
fi
mkdir -p "$OUTPUT_DIR"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --force)
            FORCE_RUN=true
            echo "Force run enabled - will regenerate galaxy catalog"
            shift
            ;;
        --test)
            TEST_MODE=true
            echo "Test mode enabled - using $N_GEN halos"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--force] [--test]"
            exit 1
            ;;
    esac
done

# Define output file based on test mode
if [[ "$TEST_MODE" == true ]]; then
    OUTPUT_BASE="mock_AbacusSummit_small_c000_ph3000_z1.100_test${N_GEN}"
else
    OUTPUT_BASE="mock_AbacusSummit_small_c000_ph3000_z1.100"
fi
OUTPUT_CATALOG="${OUTPUT_BASE}.hdf5"
OUTPUT_PLOT="${OUTPUT_BASE}.png"

# Load environment
source $SCRIPT_DIR/load_env.sh

# Check if output file exists and force flag is not set
if [[ -f "$OUTPUT_DIR/$OUTPUT_CATALOG" && "$FORCE_RUN" == false ]]; then
    echo "Galaxy catalog already exists: $OUTPUT_DIR/$OUTPUT_CATALOG"
    echo "Skipping galaxy generation. Use --force to regenerate."
else
    # Run galaxy generation with optional test mode
    if [[ "$TEST_MODE" == true ]]; then
        # Test mode: single GPU for quick testing
        exe="srun -n 1 -c 8 --qos=interactive -N 1 --time=15 -C gpu -A ${NERSC_ACCOUNT} --gpus-per-node=1 python $SCRIPT_DIR/make_sfh_cov_mocks.py nersc $OUTPUT_DIR --test $N_GEN"
    else
        # Production mode: multiple GPUs with MPI
        exe="stdbuf -o0 -e0 srun -n 6 --gpus-per-node=3 -c 32 --qos=interactive -N 2 --time=${TIME_LIMIT} -C gpu -A ${NERSC_ACCOUNT} python $SCRIPT_DIR/make_sfh_cov_mocks.py nersc $OUTPUT_DIR"
    fi
    printf "Running galaxy catalog generation with command:\n  %s\n" "$exe"
    $exe   

    # Check if generation was successful
    if [[ ! -f "$OUTPUT_DIR/$OUTPUT_CATALOG" ]]; then
        echo "Error: Galaxy catalog generation failed!"
        echo "missing file: $OUTPUT_DIR/$OUTPUT_CATALOG"
        exit 1
    fi
    echo "mock generation complete"
fi

# Run plotting script
echo "Creating plots from galaxy catalog..."
if [[ "$TEST_MODE" == true ]]; then
    python $SCRIPT_DIR/plot_mock_catalog.py "$OUTPUT_DIR/$OUTPUT_CATALOG" --max-halos "$N_GEN"
else
    python $SCRIPT_DIR/plot_mock_catalog.py "$OUTPUT_DIR/$OUTPUT_CATALOG"
fi

echo "- Galaxy catalog: $OUTPUT_DIR/$OUTPUT_CATALOG"
echo "- Plot: $OUTPUT_DIR/$OUTPUT_PLOT"
