"""
Basin Statistics Calculator for SnowMapper.

Computes mean values of SWE, HS, and ROF for each basin/catchment polygon
using exactextract for fast zonal statistics.

Inputs:
    - spatial/SWE_YYYYMMDD.nc  (reprojected SWE rasters)
    - spatial/HS_YYYYMMDD.nc  (reprojected HS rasters)
    - spatial/ROF_YYYYMMDD.nc  (reprojected ROF rasters)
    - inputs/basins/basins.shp  (basin polygons with REGION and CODE fields)

Outputs:
    - tables/swe_mean_values_table.csv  (catchment-level SWE)
    - tables/hs_mean_values_table.csv  (catchment-level HS)
    - tables/rof_mean_values_table.csv  (catchment-level ROF)
    - tables/swe_basin_mean_values_table.csv  (basin-level SWE)
    - tables/hs_basin_mean_values_table.csv  (basin-level HS)
    - tables/rof_basin_mean_values_table.csv  (basin-level ROF)

Usage:
    python compute_basin_stats.py

Note: Reads paths from snowmapper.yml or uses defaults.
"""
import os
import geopandas as gpd
import numpy as np
import pandas as pd
import glob
from datetime import datetime
from tqdm import tqdm
from rasterstats import zonal_stats
from logging_utils import setup_logger_with_tqdm

# Set up logging
logger = setup_logger_with_tqdm("results_table", file=False)

startTime = datetime.now()
thismonth = startTime.month
thisyear = startTime.year
year = [thisyear if thismonth in {9, 10, 11, 12} else thisyear-1][0]
logger.info(f"Processing water year: {year}")

# Try to load paths from snowmapper.yml config
try:
    from config import load_config
    cfg = load_config()
    shapefile_path = os.path.join(cfg['paths']['basins_dir'], 'basins.shp')
    directory = cfg['paths']['spatial_dir'] + "/"
    tables_dir = cfg['paths']['tables_dir']
    logger.info(f"Using config paths: basins={cfg['paths']['basins_dir']}, spatial={directory}")
except (FileNotFoundError, ImportError):
    # Fall back to default paths (new structure)
    shapefile_path = "./inputs/basins/basins.shp"
    directory = "./spatial/"
    tables_dir = "./tables"
    logger.info("No snowmapper.yml found, using default paths")

# Load the shapefile containing polygons
polygons = gpd.read_file(shapefile_path)

# Ensure polygons are in WGS84 to match the rasters
if polygons.crs is None:
    polygons = polygons.set_crs("EPSG:4326")
elif polygons.crs.to_epsg() != 4326:
    polygons = polygons.to_crs("EPSG:4326")

# Create output dir for tables
os.makedirs(tables_dir, exist_ok=True)


def extract_mean_values_rasterstats(nc_file, polygons):
    """Extract mean values from NetCDF file for all polygons using rasterstats."""
    import xarray as xr
    from rasterio.transform import from_bounds
    from affine import Affine

    try:
        # Open NC file with xarray
        ds = xr.open_dataset(nc_file)
        var_name = [v for v in ds.data_vars][0]
        da = ds[var_name]

        # Get the data as numpy array
        data = da.values
        if data.ndim == 3:
            data = data[0]  # Take first band if 3D

        # Get coordinates
        x = da.x.values if 'x' in da.coords else da.coords[list(da.coords)[0]].values
        y = da.y.values if 'y' in da.coords else da.coords[list(da.coords)[1]].values

        # Calculate transform (affine)
        x_res = abs(x[1] - x[0]) if len(x) > 1 else 0.01
        y_res = abs(y[1] - y[0]) if len(y) > 1 else 0.01

        # Affine transform: (x_res, 0, x_origin, 0, -y_res, y_origin)
        # y should be descending, so we use max(y) as origin
        affine = Affine(x_res, 0, x.min() - x_res/2, 0, -y_res, y.max() + y_res/2)

        ds.close()

        # Use rasterstats for zonal statistics
        stats = zonal_stats(
            polygons,
            data,
            affine=affine,
            stats=['mean'],
            nodata=np.nan
        )

        return [s['mean'] if s['mean'] is not None else np.nan for s in stats]

    except Exception as e:
        logger.warning(f"rasterstats failed for {nc_file}: {e}, falling back to NaN")
        return [np.nan] * len(polygons)


def get_date_from_nc_filename(filename):
    """Extract date from NC filename like SWE_20251001.nc"""
    basename = os.path.basename(filename)
    date_str = basename.split("_")[1].split(".")[0]
    return datetime.strptime(date_str, "%Y%m%d")


def get_water_year_files(directory, variable, water_year):
    """Get all NC files for a variable within the water year (Sep 1 to Aug 31)."""
    start_date = datetime(water_year, 9, 1)
    end_date = datetime(water_year + 1, 8, 31)

    pattern = os.path.join(directory, f"{variable}_*.nc")
    all_files = glob.glob(pattern)

    water_year_files = []
    for f in all_files:
        try:
            file_date = get_date_from_nc_filename(f)
            if start_date <= file_date <= end_date:
                water_year_files.append(f)
        except (ValueError, IndexError):
            continue

    water_year_files.sort(key=get_date_from_nc_filename)
    return water_year_files


def process_variable(variable, polygons, directory, tables_dir, year, id_column, output_prefix):
    """Process a single variable and save results."""
    file_list = get_water_year_files(directory, variable, year)

    if not file_list:
        logger.warning(f"No files found for {variable}")
        return None

    # Collect results
    results = []
    for filename in tqdm(file_list, desc=f"{output_prefix} {variable}"):
        timestamp = get_date_from_nc_filename(filename)
        mean_values = extract_mean_values_rasterstats(filename, polygons)
        results.append([timestamp] + mean_values)

    # Create DataFrame
    columns = ['Date'] + [str(idx) for idx in list(polygons[id_column])]
    results_df = pd.DataFrame(results, columns=columns)

    return results_df


# ===============================================================================
# Process all variables
# ===============================================================================

logger.info("Processing basin-level statistics...")

# Basin level (grouped by REGION)
for variable in ["SWE", "HS", "ROF"]:
    results_df = process_variable(variable, polygons, directory, tables_dir, year, "REGION", "Basin")

    if results_df is not None:
        # Group columns by their names and calculate the average (for duplicate regions)
        date_column = results_df['Date']
        df_no_date = results_df.drop(columns=['Date'])
        averages = df_no_date.T.groupby(df_no_date.columns).mean().T
        out = pd.concat([date_column, averages], axis=1)

        output_file = os.path.join(tables_dir, f"{variable.lower()}_basin_mean_values_table.csv")
        out.to_csv(output_file, index=False)
        logger.info(f"Saved {variable.lower()}_basin_mean_values_table.csv")

logger.info("Processing catchment-level statistics...")

# Catchment level (by CODE)
for variable in ["SWE", "HS", "ROF"]:
    results_df = process_variable(variable, polygons, directory, tables_dir, year, "CODE", "Catchment")

    if results_df is not None:
        output_file = os.path.join(tables_dir, f"{variable.lower()}_mean_values_table.csv")
        results_df.to_csv(output_file, index=False)
        logger.info(f"Saved {variable.lower()}_mean_values_table.csv")

logger.info(f"Completed in {datetime.now() - startTime}")
