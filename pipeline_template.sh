#!/usr/bin/env bash

# SnowMapper Pipeline - Config-driven version
# Reads settings from snowmapper.yml

# Usage: ./pipeline.sh [LOG_LEVEL]
# LOG_LEVEL: DEBUG, INFO (default), WARNING, ERROR

set -e  # Exit on error

# Set log level from argument or config
export SNOWMAPPER_LOG_LEVEL="${1:-INFO}"

# Activate conda environment
source /Users/joel/miniconda3/etc/profile.d/conda.sh
conda activate downscaling

# Working directory
WDIR=$(pwd)

# Read config values using Python
SCRIPTS=$(python -c "import sys; sys.path.insert(0, '/Users/joel/src/snowmapper'); from config import load_config; print(load_config('$WDIR')['paths']['snowmapper_scripts'])")
CLIMATE_DIR=$(python -c "import sys; sys.path.insert(0, '/Users/joel/src/snowmapper'); from config import load_config; print(load_config('$WDIR')['paths']['climate_dir'])")
SPATIAL_DIR=$(python -c "import sys; sys.path.insert(0, '/Users/joel/src/snowmapper'); from config import load_config; print(load_config('$WDIR')['paths']['spatial_dir'])")
TABLES_DIR=$(python -c "import sys; sys.path.insert(0, '/Users/joel/src/snowmapper'); from config import load_config; print(load_config('$WDIR')['paths']['tables_dir'])")
LOGS_DIR=$(python -c "import sys; sys.path.insert(0, '/Users/joel/src/snowmapper'); from config import load_config; print(load_config('$WDIR')['paths']['logs_dir'])")

# Get enabled domains
DOMAINS=$(python -c "import sys; sys.path.insert(0, '/Users/joel/src/snowmapper'); from config import load_config, get_enabled_domains; cfg=load_config('$WDIR'); print(' '.join(d['path'] for d in get_enabled_domains(cfg)))")

# Create logs directory
mkdir -p "$LOGS_DIR"

# Log file
LOGFILE="$LOGS_DIR/pipeline.log"
> "$LOGFILE"

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $1" | tee -a "$LOGFILE"
}

# Arrays to track step results for summary
declare -a STEP_NAMES
declare -a STEP_STATUS
declare -a STEP_DURATION

# Run step with timing and error checking
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
        # Record for summary
        STEP_NAMES+=("$step_name")
        STEP_STATUS+=("✓")
        STEP_DURATION+=("$step_duration")
        return 0
    else
        local exit_code=${PIPESTATUS[0]}
        local step_end=$(date +%s)
        local step_duration=$((step_end - step_start))
        log "FAIL  | $step_name (exit code: $exit_code)"
        # Record for summary
        STEP_NAMES+=("$step_name")
        STEP_STATUS+=("✗")
        STEP_DURATION+=("$step_duration")
        return $exit_code
    fi
}

# Record skipped step
skip_step() {
    local step_name="$1"
    log "SKIP  | $step_name"
    STEP_NAMES+=("$step_name")
    STEP_STATUS+=("⊘")
    STEP_DURATION+=("0")
}

# Print summary checklist
print_summary() {
    local summary_file="$LOGS_DIR/pipeline_summary.txt"

    echo "" | tee -a "$LOGFILE"
    echo "==================== PIPELINE SUMMARY ====================" | tee -a "$LOGFILE" | tee "$summary_file"
    echo "" | tee -a "$LOGFILE" | tee -a "$summary_file"
    printf "%-4s %-40s %10s\n" "Status" "Module" "Duration" | tee -a "$LOGFILE" | tee -a "$summary_file"
    printf "%-4s %-40s %10s\n" "------" "----------------------------------------" "----------" | tee -a "$LOGFILE" | tee -a "$summary_file"

    local total_run=0
    local total_skip=0
    local total_fail=0

    for i in "${!STEP_NAMES[@]}"; do
        local name="${STEP_NAMES[$i]}"
        local status="${STEP_STATUS[$i]}"
        local duration="${STEP_DURATION[$i]}"

        # Format duration
        if [ "$duration" -eq 0 ]; then
            duration_str="--"
        else
            local mins=$((duration / 60))
            local secs=$((duration % 60))
            printf -v duration_str "%dm %02ds" $mins $secs
        fi

        printf "  %s   %-40s %10s\n" "$status" "$name" "$duration_str" | tee -a "$LOGFILE" | tee -a "$summary_file"

        # Count by status
        case "$status" in
            "✓") ((total_run++)) ;;
            "⊘") ((total_skip++)) ;;
            "✗") ((total_fail++)) ;;
        esac
    done

    echo "" | tee -a "$LOGFILE" | tee -a "$summary_file"
    echo "Legend: ✓ = completed, ⊘ = skipped, ✗ = failed" | tee -a "$LOGFILE" | tee -a "$summary_file"
    echo "Completed: $total_run | Skipped: $total_skip | Failed: $total_fail" | tee -a "$LOGFILE" | tee -a "$summary_file"
    echo "==========================================================" | tee -a "$LOGFILE" | tee -a "$summary_file"
}

# Capture start time
start_seconds=$(date +%s)
log "=========================================="
log "SnowMapper Pipeline Started"
log "Log level: $SNOWMAPPER_LOG_LEVEL"
log "Sim directory: $WDIR"
log "Scripts: $SCRIPTS"
log "Climate: $CLIMATE_DIR"
log "Domains: $DOMAINS"
log "=========================================="

# Step 1: Fetch IFS forecast (uses shared climate dir)
run_step "Fetch IFS forecast" python "$SCRIPTS/fetch_ifs_forecast.py" || exit 1

# Step 2: Download ERA5 climate data
run_step "Download ERA5" python "$SCRIPTS/download_era5.py" || exit 1

# Process each enabled domain
for DOMAIN in $DOMAINS; do
    DOMAIN_NAME=$(basename "$DOMAIN")
    log "=========================================="
    log "Processing domain: $DOMAIN_NAME"
    log "=========================================="

    # Check if sim_archive outputs exist (skip init if so)
    ARCHIVE_DIR="$DOMAIN/sim_archive/outputs"
    if [ -d "$ARCHIVE_DIR" ] && [ "$(ls -A "$ARCHIVE_DIR"/*.nc 2>/dev/null)" ]; then
        skip_step "Init $DOMAIN_NAME domain (sim_archive exists)"
        skip_step "Run archive simulation (sim_archive exists)"
    else
        run_step "Init $DOMAIN_NAME domain" python "$SCRIPTS/init_domain.py" "$DOMAIN" || exit 1
        run_step "Run archive simulation" python "$SCRIPTS/run_archive_sim.py" "$DOMAIN" || exit 1
    fi

    # Run forecast simulation
    run_step "Run forecast simulation" python "$SCRIPTS/run_forecast_sim.py" "$DOMAIN" || exit 1

    # Merge FSM outputs
    run_step "Merge FSM outputs" python "$SCRIPTS/merge_fsm_outputs.py" "$DOMAIN" || exit 1

    # Grid to NetCDF
    run_step "Grid FSM to NetCDF" python "$SCRIPTS/grid_fsm_to_netcdf.py" "$DOMAIN" || exit 1
done

# Post-processing (uses shared spatial/tables dirs)
cd "$WDIR"
run_step "Merge and reproject rasters" python "$SCRIPTS/merge_reproject.py" "./" "domains/D2000" || exit 1
run_step "Compute basin statistics" python "$SCRIPTS/compute_basin_stats.py" || exit 1
run_step "Compute zonal statistics" python "$SCRIPTS/zonal_stats.py" || exit 1

# Optional: Upload to S3
UPLOAD_ENABLED=$(python -c "import sys; sys.path.insert(0, '/Users/joel/src/snowmapper'); from config import load_config; print(load_config('$WDIR')['upload']['enabled'])")
if [ "$UPLOAD_ENABLED" = "True" ]; then
    run_step "Upload to S3" python "$SCRIPTS/upload_to_s3.py" || exit 1
else
    skip_step "Upload to S3 (disabled in config)"
fi

# Calculate total runtime
end_seconds=$(date +%s)
runtime_seconds=$((end_seconds - start_seconds))
hours=$((runtime_seconds / 3600))
minutes=$(((runtime_seconds % 3600) / 60))
seconds=$((runtime_seconds % 60))

# Print summary checklist
print_summary

log "=========================================="
log "Pipeline Complete"
printf -v runtime "%02d:%02d:%02d" $hours $minutes $seconds
log "Total runtime: $runtime"
log "Summary saved to: $LOGS_DIR/pipeline_summary.txt"
log "=========================================="
