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

## Setting Up a New Simulation

### 1. Create Simulation Directory Structure

```bash
mkdir -p my_simulation/{master,D2000}/{inputs/dem,inputs/basins}
mkdir -p my_simulation/{logs,spatial,tables}
cd my_simulation
```

### 2. Copy Pipeline Script

```bash
cp /path/to/snowmapper/pipeline.sh .
# Edit pipeline.sh to update paths if needed
```

### 3. Configure Master Domain

The master domain downloads ERA5 climate data shared by other domains.

Create `master/config.yml`:
```yaml
project:
    name: master
    description: Master domain for ERA5 download
    directory: /path/to/my_simulation/master/
    start: 2024-09-01          # Water year start
    end: 2024-12-31            # Will be updated automatically
    extent: [lat_max, lat_min, lon_min, lon_max]  # Bounding box
    climate: era5

climate:
    era5:
        path: ./inputs/climate/
        product: reanalysis
        timestep: 1H
        plevels: [300, 400, 500, 600, 700, 850, 925, 1000]
        realtime: True
        output_format: netcdf

dem:
    file: dem.tif
    epsg: 32642               # UTM zone for your region
    horizon_increments: 45
    dem_resol: 500

sampling:
    method: toposub
    toposub:
        clustering_method: minibatchkmean
        n_clusters: 300       # Number of TopoSUB clusters
        random_seed: 2
        clustering_features: {'x':1, 'y':1, 'elevation':1, 'slope':1, 'aspect_cos':1, 'aspect_sin':1, 'svf':1}

toposcale:
    interpolation_method: idw
    LW_terrain_contribution: True
```

### 4. Configure Processing Domain

The processing domain (e.g., D2000) runs FSM simulations using climate from master.

Create `D2000/config.yml`:
```yaml
project:
    name: D2000
    description: High-resolution processing domain
    directory: /path/to/my_simulation/D2000/
    start: 2024-09-01
    end: 2024-12-31
    extent: [lat_max, lat_min, lon_min, lon_max]  # Can be subset of master
    climate: era5

climate:
    era5:
        path: /path/to/my_simulation/master/inputs/climate/  # Points to master
        product: reanalysis
        timestep: 1H
        plevels: [300, 400, 500, 600, 700, 850, 925, 1000]
        realtime: False
        output_format: grib

dem:
    file: dem_500m.tif
    epsg: 32642
    horizon_increments: 45
    dem_resol: 500

sampling:
    method: toposub
    toposub:
        clustering_method: minibatchkmean
        n_clusters: 2000      # Higher resolution clustering
        random_seed: 2
        clustering_features: {'x':1, 'y':1, 'elevation':1, 'slope':1, 'aspect_cos':1, 'aspect_sin':1, 'svf':1}
```

### 5. Add Required Input Files

```bash
# DEM (GeoTIFF in projected CRS)
cp your_dem.tif D2000/inputs/dem/

# Basin shapefiles for statistics (optional)
cp basins.shp basins.shx basins.dbf D2000/inputs/basins/

# FSM binary
cp /path/to/FSM D2000/FSM
chmod +x D2000/FSM
```

### 6. Set Up API Credentials

```bash
# CDS API for ERA5 (~/.cdsapirc)
echo "url: https://cds.climate.copernicus.eu/api/v2
key: YOUR_UID:YOUR_API_KEY" > ~/.cdsapirc

# ECMWF API for IFS forecast (~/.ecmwfapirc)
echo '{"url": "https://api.ecmwf.int/v1", "key": "YOUR_KEY", "email": "your@email.com"}' > ~/.ecmwfapirc
```

### 7. Run Pipeline

```bash
conda activate downscaling
bash pipeline.sh
```

On first run, `init_domain.py` and `run_archive_sim.py` will initialize the domain. Subsequent runs skip initialization if `sim_archive/HS.nc` exists.

## Configuration Reference

Each domain requires a `config.yml` (TopoPyScale format) defining:
- **project.extent**: Geographic bounding box `[lat_max, lat_min, lon_min, lon_max]`
- **project.start/end**: Simulation time range (water year)
- **climate.era5.path**: Path to ERA5 data (master downloads, others reference)
- **dem.file**: DEM filename in `inputs/dem/`
- **dem.epsg**: Projected coordinate system (UTM recommended)
- **sampling.toposub.n_clusters**: Number of landscape clusters (more = higher resolution)

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
