#!/usr/bin/env bash

# Usage: ./run_2000.sh [LOG_LEVEL]
# LOG_LEVEL: DEBUG, INFO (default), WARNING, ERROR

# Set log level from argument or default to INFO
export SNOWMAPPER_LOG_LEVEL="${1:-INFO}"

source /Users/joel/miniconda3/etc/profile.d/conda.sh
conda activate downscaling

WDIR=/Users/joel/sim/snowmapper_2026
cd $WDIR

# Create logs directory
mkdir -p logs

# Single log file, overwritten each run
LOGFILE="logs/pipeline.log"

# Clear log file at start
> "$LOGFILE"

# Function to log messages to both console and file
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $1" | tee -a "$LOGFILE"
}

# Function to run a step with timing and error checking
run_step() {
    local step_name="$1"
    shift
    local cmd="$@"

    log "START | $step_name"
    local step_start=$(date +%s)

    if $cmd 2>&1 | tee -a "$LOGFILE"; then
        local step_end=$(date +%s)
        local step_duration=$((step_end - step_start))
        log "DONE  | $step_name (${step_duration}s)"
        return 0
    else
        local exit_code=${PIPESTATUS[0]}
        log "FAIL  | $step_name (exit code: $exit_code)"
        return $exit_code
    fi
}

# Capture start time
start_seconds=$(date +%s)
log "=========================================="
log "SnowMapper Pipeline Started"
log "Log level: $SNOWMAPPER_LOG_LEVEL"
log "=========================================="

# Run pipeline steps
run_step "Fetch IFS forecast" python /Users/joel/src/snowmapper/fetch_ifs_forecast.py || exit 1
run_step "Download ERA5 and downscale" python /Users/joel/src/snowmapper/download_era5.py "./master/" || exit 1

# Check if sim_archive already has consolidated data (skip initialization if so)
ARCHIVE_FILE="./D2000/sim_archive/HS.nc"
if [ -f "$ARCHIVE_FILE" ]; then
    log "SKIP  | Init domain and archive simulation (sim_archive already exists)"
else
    run_step "Init D2000 domain" python /Users/joel/src/snowmapper/init_domain.py ./D2000 || exit 1
    run_step "Run archive simulation" python /Users/joel/src/snowmapper/run_archive_sim.py ./D2000 || exit 1
fi

run_step "Run forecast simulation" python /Users/joel/src/snowmapper/run_forecast_sim.py ./D2000 || exit 1
run_step "Merge FSM outputs" python /Users/joel/src/snowmapper/merge_fsm_outputs.py ./D2000 || exit 1
run_step "Grid FSM to NetCDF" python /Users/joel/src/snowmapper/grid_fsm_to_netcdf.py ./D2000 || exit 1

run_step "Merge and reproject rasters" python /Users/joel/src/snowmapper/merge_reproject.py "./" "D2000" || exit 1
run_step "Compute basin statistics" python /Users/joel/src/snowmapper/compute_basin_stats.py || exit 1
run_step "Compute zonal statistics" python /Users/joel/src/snowmapper/zonal_stats.py || exit 1

# Optional: Upload to S3 (uncomment to enable)
# run_step "Upload to S3" python /Users/joel/src/snowmapper/upload_to_s3.py || exit 1

# Calculate total runtime
end_seconds=$(date +%s)
runtime_seconds=$((end_seconds - start_seconds))
hours=$((runtime_seconds / 3600))
minutes=$(((runtime_seconds % 3600) / 60))
seconds=$((runtime_seconds % 60))

log "=========================================="
log "Pipeline Complete"
printf -v runtime "%02d:%02d:%02d" $hours $minutes $seconds
log "Total runtime: $runtime"
log "=========================================="
