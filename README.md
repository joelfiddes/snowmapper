# SnowMapper

Operational snow mapping and forecasting system for High-Mountain Central Asia (HMCA). Generates daily high-resolution maps of snow height (HS), snow water equivalent (SWE), snow melt/runoff (ROF), and ground surface temperature (GST).

**Live output**: [snowmapper.ch](https://snowmapper.ch)

## Pipeline Overview

```
ERA5 Reanalysis + IFS Forecast
         ↓
    TopoPyScale Downscaling
         ↓
    FSM Snow Model (per cluster)
         ↓
    Spatial Gridding (500m)
         ↓
    Basin Statistics
         ↓
    S3 Upload
```

## Pipeline Modules

| Module | Description |
|--------|-------------|
| `fetch_ifs_forecast.py` | Download ECMWF IFS 10-day forecast data |
| `download_era5.py` | Download ERA5 reanalysis + run TopoPyScale downscaling |
| `init_domain.py` | Initialize domain directories and configuration |
| `run_archive_sim.py` | Run FSM simulation on historical archive data |
| `run_forecast_sim.py` | Run FSM simulation on recent + forecast data |
| `merge_fsm_outputs.py` | Merge archive and latest FSM outputs with overlap handling |
| `grid_fsm_to_netcdf.py` | Convert FSM point outputs to gridded NetCDF |
| `merge_reproject.py` | Merge tiles and reproject to output CRS |
| `compute_basin_stats.py` | Compute mean SWE/HS/ROF per basin |
| `zonal_stats.py` | Compute catchment-level zonal statistics |
| `upload_to_s3.py` | Upload daily results and forecast bundles to S3 |

### Utility Modules

| Module | Description |
|--------|-------------|
| `logging_utils.py` | Shared logging configuration with tqdm integration |
| `s3_utils.py` | S3 upload helper functions |

## Project Structure

```
snowmapper/                 # This repo - pipeline scripts
├── fetch_ifs_forecast.py
├── download_era5.py
├── init_domain.py
├── run_archive_sim.py
├── run_forecast_sim.py
├── merge_fsm_outputs.py
├── grid_fsm_to_netcdf.py
├── merge_reproject.py
├── compute_basin_stats.py
├── zonal_stats.py
├── upload_to_s3.py
├── logging_utils.py
└── s3_utils.py

simulation_dir/             # Simulation data directory
├── pipeline.sh             # Main pipeline runner
├── master/                 # Master domain (downloads ERA5)
│   ├── config.yml          # TopoPyScale configuration
│   └── inputs/climate/     # ERA5 + forecast data
├── D2000/                  # High-res domain (2000 clusters)
│   ├── config.yml
│   ├── FSM                 # FSM binary
│   ├── inputs/
│   │   ├── dem/            # Digital elevation model
│   │   └── basins/         # Basin shapefiles
│   ├── sim_archive/        # Consolidated historical results
│   ├── sim_latest/         # Current run outputs
│   ├── spatial/            # Output rasters (SWE, HS, ROF)
│   └── tables/             # Basin statistics CSVs
└── logs/                   # Pipeline logs
```

## Quick Start

```bash
# Activate environment
conda activate downscaling

# Run full pipeline
cd /path/to/simulation
bash pipeline.sh

# Run with debug logging
bash pipeline.sh DEBUG
```

## Requirements

- Python 3.10+
- TopoPyScale
- FSM (Factorial Snow Model) binary
- ECMWF API credentials (for IFS forecast)
- CDS API credentials (for ERA5)

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed setup instructions.

## Configuration

Each domain requires a `config.yml` defining:
- Geographic extent and DEM settings
- TopoSUB clustering parameters
- Climate data paths
- TopoPyScale interpolation methods

## Typical Runtime

| Step | Duration |
|------|----------|
| Fetch IFS forecast | ~10 min (or skipped if present) |
| Download ERA5 + downscale | ~3 min |
| Run forecast simulation | ~20 sec |
| Merge FSM outputs | ~5 sec |
| Grid to NetCDF | ~13 min |
| Merge/reproject | ~2 sec |
| Compute basin stats | ~10 min |
| Zonal statistics | ~6 sec |
| **Total** | **~25-30 min** |

## License

GNU General Public License v3.0
