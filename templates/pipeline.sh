#!/usr/bin/env bash
# SnowMapper Pipeline
# All logic is in pipeline_runner.py - this just activates conda and runs it

set -e

source /Users/joel/miniconda3/etc/profile.d/conda.sh
conda activate downscaling

python /Users/joel/src/snowmapper/pipeline_runner.py "$@"
