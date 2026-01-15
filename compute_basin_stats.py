"""
Basin Statistics Calculator for SnowMapper.

Computes mean values of SWE, HS, and ROF for each basin/catchment polygon
by clipping reprojected NetCDF files to basin boundaries.

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
import xarray as xr
import rioxarray
import geopandas as gpd
import numpy as np
import pandas as pd
import sys
import glob
import re
from datetime import datetime
from tqdm import tqdm
from logging_utils import setup_logger_with_tqdm

# Set up logging
logger = setup_logger_with_tqdm("results_table", file=False)

startTime = datetime.now()
thismonth = startTime.month    # now
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

# create output dir for tables
os.makedirs(tables_dir, exist_ok=True)


def extract_mean_values_nc(nc_file, polygons):
    """Extract mean values from NetCDF file for each polygon."""
    ds = xr.open_dataset(nc_file)
    # Get the data variable (first non-coordinate variable)
    var_name = [v for v in ds.data_vars][0]
    da = ds[var_name]

    # Ensure CRS is set
    if da.rio.crs is None:
        da = da.rio.write_crs("EPSG:4326")

    mean_values = []
    for idx, geom in enumerate(polygons.geometry):
        try:
            clipped = da.rio.clip([geom], polygons.crs, drop=True)
            mean_value = float(clipped.mean().values)
        except Exception:
            mean_value = np.nan
        mean_values.append(mean_value)

    ds.close()
    return mean_values


def get_date_from_nc_filename(filename):
    """Extract date from NC filename like SWE_20251001.nc"""
    basename = os.path.basename(filename)
    # Pattern: VAR_YYYYMMDD.nc
    date_str = basename.split("_")[1].split(".")[0]
    return datetime.strptime(date_str, "%Y%m%d")


def get_water_year_files(directory, variable, water_year):
    """Get all NC files for a variable within the water year (Sep 1 to Aug 31)."""
    # Water year starts Sep 1 of 'water_year' and ends Aug 31 of 'water_year + 1'
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

    # Sort by date
    water_year_files.sort(key=get_date_from_nc_filename)
    return water_year_files


#===============================================================================
# Basin SWE
#===============================================================================

results_df = pd.DataFrame(columns=['Date'] + [str(idx) for idx in list(polygons['REGION'])])
file_list = get_water_year_files(directory, "SWE", year)

for filename in tqdm(file_list, desc="Basin SWE"):
    timestamp = get_date_from_nc_filename(filename)
    mean_values = extract_mean_values_nc(filename, polygons)
    results_df.loc[len(results_df)] = [timestamp] + mean_values

# Extract the 'Date' column
date_column = results_df['Date']

# Group columns by their names and calculate the average
df_no_date = results_df.drop(columns=['Date'])
averages = df_no_date.T.groupby(df_no_date.columns).mean().T

# Concatenate the 'Date' column with the resulting DataFrame
out = pd.concat([date_column, averages], axis=1)

out.to_csv(os.path.join(tables_dir, "swe_basin_mean_values_table.csv"), index=False)
logger.info("Saved swe_basin_mean_values_table.csv")

#===============================================================================
# Basin HS
#===============================================================================

results_df = pd.DataFrame(columns=['Date'] + [str(idx) for idx in list(polygons['REGION'])])
file_list = get_water_year_files(directory, "HS", year)

for filename in tqdm(file_list, desc="Basin HS"):
    timestamp = get_date_from_nc_filename(filename)
    mean_values = extract_mean_values_nc(filename, polygons)
    results_df.loc[len(results_df)] = [timestamp] + mean_values

date_column = results_df['Date']
df_no_date = results_df.drop(columns=['Date'])
averages = df_no_date.T.groupby(df_no_date.columns).mean().T
out = pd.concat([date_column, averages], axis=1)

out.to_csv(os.path.join(tables_dir, "hs_basin_mean_values_table.csv"), index=False)
logger.info("Saved hs_basin_mean_values_table.csv")


#===============================================================================
# Basin ROF
#===============================================================================

results_df = pd.DataFrame(columns=['Date'] + [str(idx) for idx in list(polygons['REGION'])])
file_list = get_water_year_files(directory, "ROF", year)

for filename in tqdm(file_list, desc="Basin ROF"):
    timestamp = get_date_from_nc_filename(filename)
    mean_values = extract_mean_values_nc(filename, polygons)
    results_df.loc[len(results_df)] = [timestamp] + mean_values

date_column = results_df['Date']
df_no_date = results_df.drop(columns=['Date'])
averages = df_no_date.T.groupby(df_no_date.columns).mean().T
out = pd.concat([date_column, averages], axis=1)

out.to_csv(os.path.join(tables_dir, "rof_basin_mean_values_table.csv"), index=False)
logger.info("Saved rof_basin_mean_values_table.csv")


#===============================================================================
# Catchment SWE
#===============================================================================

results_df = pd.DataFrame(columns=['Date'] + [str(idx) for idx in list(polygons['CODE'])])
file_list = get_water_year_files(directory, "SWE", year)

for filename in tqdm(file_list, desc="Catchment SWE"):
    timestamp = get_date_from_nc_filename(filename)
    mean_values = extract_mean_values_nc(filename, polygons)
    results_df.loc[len(results_df)] = [timestamp] + mean_values

results_df.to_csv(os.path.join(tables_dir, "swe_mean_values_table.csv"), index=False)
logger.info("Saved swe_mean_values_table.csv")

#===============================================================================
# Catchment HS
#===============================================================================

results_df = pd.DataFrame(columns=['Date'] + [str(idx) for idx in list(polygons['CODE'])])
file_list = get_water_year_files(directory, "HS", year)

for filename in tqdm(file_list, desc="Catchment HS"):
    timestamp = get_date_from_nc_filename(filename)
    mean_values = extract_mean_values_nc(filename, polygons)
    results_df.loc[len(results_df)] = [timestamp] + mean_values

results_df.to_csv(os.path.join(tables_dir, "hs_mean_values_table.csv"), index=False)
logger.info("Saved hs_mean_values_table.csv")

#===============================================================================
# Catchment ROF
#===============================================================================

results_df = pd.DataFrame(columns=['Date'] + [str(idx) for idx in list(polygons['CODE'])])
file_list = get_water_year_files(directory, "ROF", year)

for filename in tqdm(file_list, desc="Catchment ROF"):
    timestamp = get_date_from_nc_filename(filename)
    mean_values = extract_mean_values_nc(filename, polygons)
    results_df.loc[len(results_df)] = [timestamp] + mean_values

results_df.to_csv(os.path.join(tables_dir, "rof_mean_values_table.csv"), index=False)
logger.info("Saved rof_mean_values_table.csv")
