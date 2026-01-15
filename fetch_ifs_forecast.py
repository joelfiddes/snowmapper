"""
IFS Forecast Fetcher for SnowMapper.

Downloads and processes ECMWF IFS open data forecasts to fill the gap between
ERA5 reanalysis (5-6 day latency) and the current date, plus 10-day forecasts.

Inputs:
    - Config from snowmapper.yml (optional) or default paths
    - ECMWF open data via ecmwf-opendata package

Outputs:
    - inputs/climate/forecast/PLEV_FC_YYYY-MM-DD.nc  (daily hindcast files)
    - inputs/climate/forecast/SURF_FC_YYYY-MM-DD.nc  (daily hindcast files)
    - inputs/climate/forecast/PLEV_FC.nc  (merged forecast)
    - inputs/climate/forecast/SURF_FC.nc  (merged forecast)

Usage:
    python fetch_ifs_forecast.py

References:
    - https://github.com/ecmwf/ecmwf-opendata
    - https://www.ecmwf.int/en/forecasts/datasets/open-data
"""
# Steps:

# For times 00z &12z: 0 to 144 by 3, 150 to 240 by 6.
# For times 06z & 18z: 0 to 90 by 3.
# Single and Pressure Levels (hPa): 1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 50

# 6 day forecast available at 3h steps

# do i need TOA?
#  [300, 500,600, 700,800, 850, 900, 1000]

# we compute z from gh on pressure levels
# renaming of 2t, 2d
# we comput z at surfeca from  msl and sp and t

from ecmwf.opendata import Client
import glob
import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import xarray as xr
import numpy as np
from matplotlib import pyplot
import matplotlib
from tqdm import tqdm
from logging_utils import setup_logger_with_tqdm
#matplotlib.use('TkAgg')

# Set up logging
logger = setup_logger_with_tqdm("fetch_ifs", file=False)


# ============================================================================
# Helper functions
# ============================================================================

def get_last_era5_date(forecast_dir):
    """
    Find the last available ERA5 date from daily files in the forecast directory.

    Returns:
        datetime: Last ERA5 date, or None if no files found
    """
    era5_files = sorted(glob.glob(os.path.join(forecast_dir, "PLEV_20*.nc")))
    # Filter out FC files
    era5_files = [f for f in era5_files if "_FC" not in f]

    if not era5_files:
        return None

    last_file = era5_files[-1]
    date_str = os.path.basename(last_file).split('_')[1].split('.')[0]
    return datetime.strptime(date_str, "%Y%m%d")


def get_existing_forecast_dates(forecast_dir):
    """
    Find all existing forecast gap-fill files (SURF_FC_YYYY-MM-DD.nc).

    Returns:
        set: Set of datetime objects for existing forecast dates
    """
    fc_files = glob.glob(os.path.join(forecast_dir, "SURF_FC_20*.nc"))
    dates = set()

    for f in fc_files:
        # Extract date from filename like SURF_FC_2026-01-14.nc
        basename = os.path.basename(f)
        if basename.startswith("SURF_FC_") and len(basename) >= 21:
            date_str = basename[8:18]  # "2026-01-14"
            try:
                dates.add(datetime.strptime(date_str, "%Y-%m-%d"))
            except ValueError:
                pass

    return dates


def get_missing_forecast_dates(forecast_dir):
    """
    Identify missing forecast gap-fill dates between last ERA5 and today.

    Returns:
        list: List of (date, mydate_offset) tuples for missing dates
    """
    last_era5 = get_last_era5_date(forecast_dir)
    if last_era5 is None:
        logger.warning("No ERA5 files found, cannot determine gap")
        return []

    existing_fc = get_existing_forecast_dates(forecast_dir)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # We need forecast data from (last_era5 + 1 day) to (today - 1 day)
    # Today's forecast will be fetched with mydate=0
    missing = []
    current = last_era5 + timedelta(days=1)

    while current < today:
        if current not in existing_fc:
            # Calculate mydate offset: how many days ago is this date?
            days_ago = (today - current).days
            if days_ago <= 3:  # IFS only keeps 3 days of historical forecasts
                missing.append((current, -days_ago))
        current += timedelta(days=1)

    return missing


# ============================================================================
# Spatial subset and processing functions
# ============================================================================

def spatial_subset(nc_file, lat_range, lon_range):
    """Perform spatial subset on NetCDF file"""
    ds = xr.open_dataset(nc_file)
    lat = ds['lat'].values
    lon = ds['lon'].values
    lat_indices = (lat >= lat_range[0]) & (lat <= lat_range[1])
    lon_indices = (lon >= lon_range[0]) & (lon <= lon_range[1])
    subset = ds.sel(lat=lat[lat_indices], lon=lon[lon_indices])
    return subset


def calculate_geopotential(P, T, P0):
    """Calculate geopotential height"""
    R = 287  # Gas constant for dry air (J/kg/K)
    P_hpa = P/100
    P0_hpa = P0/100
    Z = (R * T) * np.log(P0_hpa / P_hpa)
    return Z


# ============================================================================
# Main forecast download and processing function
# ============================================================================

def download_and_process_forecast(mydate, fctime, tmp_path, lat_range, lon_range, save_hindcast_only=False):
    """
    Download and process IFS forecast for a given date offset.

    Parameters:
        mydate: Date offset (0=today, -1=yesterday, -2=day before, -3=3 days ago)
        fctime: Forecast time (0 or 12)
        tmp_path: Path to temporary directory
        lat_range: (min_lat, max_lat) for spatial subset
        lon_range: (min_lon, max_lon) for spatial subset
        save_hindcast_only: If True, only save the first 24h as hindcast product

    Returns:
        str: The date string of the first forecast day (YYYY-MM-DD)
    """
    os.chdir(tmp_path)

    # Clean up tmp directory
    files2delete = glob.glob("*")
    for file in files2delete:
        try:
            os.remove(file)
        except:
            pass

    logger.info(f"Downloading forecast for mydate={mydate}")

    # Download surface variables at fc steps 0-144 (3h step)
    client = Client()
    client.retrieve(
        time=fctime,
        date=mydate,
        step=[i for i in range(0, 147, 3)],
        type="fc",
        param=["2t", "sp", "2d", "ssrd", "strd", "tp", "msl"],
        target="SURF_fc1.grib2",
    )

    # Download pressure variables at fc steps 0-144 (3h step)
    client = Client()
    client.retrieve(
        time=fctime,
        date=mydate,
        step=[i for i in range(0, 147, 3)],
        type="fc",
        param=["gh", "u", "v", "r", "q", "t"],
        levelist=[1000, 925, 850, 700, 600, 500, 400, 300],
        target="PLEV_fc1.grib2",
    )

    # Download surface variables at steps 150-240 (6h step)
    client = Client()
    client.retrieve(
        time=fctime,
        date=mydate,
        step=[i for i in range(150, 241, 6)],
        type="fc",
        param=["2t", "sp", "2d", "ssrd", "strd", "tp", "msl"],
        target="SURF_fc2.grib2",
    )

    # Download pressure level variables at steps 150-240 (6h step)
    client = Client()
    client.retrieve(
        time=fctime,
        date=mydate,
        step=[i for i in range(150, 241, 6)],
        type="fc",
        param=["gh", "u", "v", "r", "q", "t"],
        levelist=[1000, 925, 850, 700, 600, 500, 400, 300],
        target="PLEV_fc2.grib2",
    )

    logger.info("Converting GRIB to NetCDF")
    os.system("cdo -f nc copy SURF_fc1.grib2 SURF_fc1.nc")
    os.system("cdo -f nc copy PLEV_fc1.grib2 PLEV_fc1.nc")
    os.system("cdo -f nc copy SURF_fc2.grib2 SURF_fc2.nc")
    os.system("cdo -f nc copy PLEV_fc2.grib2 PLEV_fc2.nc")

    # Clean up GRIB files
    for file in glob.glob("*grib2"):
        os.remove(file)

    logger.info("Spatial subsetting and deaccumulation")

    # Process SURF files
    nc_files = ['SURF_fc1.nc', 'SURF_fc2.nc']
    lasttimestep_forecast1_tp = None
    lasttimestep_forecast1_ssrd = None
    lasttimestep_forecast1_strd = None

    for nc_file in nc_files:
        subset = spatial_subset(nc_file, lat_range, lon_range)

        # Process TP (precipitation)
        try:
            accumulated_var = subset["tp"]
        except:
            accumulated_var = subset["param193.1.0"]

        if nc_file == "SURF_fc1.nc":
            lasttimestep_forecast1_tp = accumulated_var.isel(time=-1)
        if nc_file == "SURF_fc2.nc":
            accumulated_var = accumulated_var - lasttimestep_forecast1_tp

        deaccumulated_var = accumulated_var - accumulated_var.shift(time=1, fill_value=0)
        divisor = 3 if nc_file == "SURF_fc1.nc" else 6
        deaccumulated_var = deaccumulated_var / divisor

        if "tp" in subset:
            subset['tp'] = deaccumulated_var
        else:
            subset['tp'] = deaccumulated_var.rename('tp')
            subset = subset.drop_vars('param193.1.0', errors='ignore')

        # Process SSRD
        accumulated_var = subset["ssrd"]
        if nc_file == "SURF_fc1.nc":
            lasttimestep_forecast1_ssrd = accumulated_var.isel(time=-1)
        if nc_file == "SURF_fc2.nc":
            accumulated_var = accumulated_var - lasttimestep_forecast1_ssrd
        deaccumulated_var = accumulated_var - accumulated_var.shift(time=1, fill_value=0)
        deaccumulated_var = deaccumulated_var / divisor
        subset['ssrd'] = deaccumulated_var

        # Process STRD
        accumulated_var = subset["strd"]
        if nc_file == "SURF_fc1.nc":
            lasttimestep_forecast1_strd = accumulated_var.isel(time=-1)
        if nc_file == "SURF_fc2.nc":
            accumulated_var = accumulated_var - lasttimestep_forecast1_strd
        deaccumulated_var = accumulated_var - accumulated_var.shift(time=1, fill_value=0)
        deaccumulated_var = deaccumulated_var / divisor
        subset['strd'] = deaccumulated_var

        # Rename and compute geopotential
        subset = subset.rename({'lon': 'longitude', 'lat': 'latitude', '2t': 't2m', '2d': 'd2m'})
        subset['z'] = calculate_geopotential(subset['sp'], subset['t2m'], subset['msl'])
        subset = subset.drop_vars('msl')
        subset = subset.squeeze('height', drop=True)
        subset.to_netcdf(f'subset_{nc_file}')

    # Process PLEV files
    for nc_file in glob.glob("PLEV*.nc"):
        subset = spatial_subset(nc_file, lat_range, lon_range)
        subset = subset.rename({'lon': 'longitude', 'lat': 'latitude', 'plev': 'level'})
        subset['z'] = subset['gh'] * 9.81
        subset = subset.assign_coords(level=subset['level'].values / 100.)
        subset = subset.sortby('level', ascending=True)
        subset = subset.drop_vars('gh')
        subset.to_netcdf(f'subset_{nc_file}')

    # Replace original files with subsets
    os.remove("SURF_fc1.nc")
    os.rename("subset_SURF_fc1.nc", "SURF_fc1.nc")
    os.remove("SURF_fc2.nc")
    os.rename("subset_SURF_fc2.nc", "SURF_fc2.nc")
    os.remove("PLEV_fc1.nc")
    os.rename("subset_PLEV_fc1.nc", "PLEV_fc1.nc")
    os.remove("PLEV_fc2.nc")
    os.rename("subset_PLEV_fc2.nc", "PLEV_fc2.nc")

    logger.info("Interpolating 6h to 3h timestep")

    # Interpolate SURF_fc2 from 6h to 3h
    ds = xr.open_dataset("SURF_fc2.nc")
    date_string = f"{ds['time.year'][0].values}-{str(ds['time.month'][0].values).zfill(2)}-{str(ds['time.day'][0].values).zfill(2)}"
    time_string = f"{str(ds['time.hour'][0].values).zfill(2)}:{str(ds['time.minute'][0].values).zfill(2)}:{str(ds['time.second'][0].values).zfill(2)}"
    os.system(f"cdo inttime,{date_string},{time_string},3hour SURF_fc2.nc SURF_fc2_3h.nc")

    # Interpolate PLEV_fc2 from 6h to 3h
    ds = xr.open_dataset("PLEV_fc2.nc")
    date_string = f"{ds['time.year'][0].values}-{str(ds['time.month'][0].values).zfill(2)}-{str(ds['time.day'][0].values).zfill(2)}"
    time_string = f"{str(ds['time.hour'][0].values).zfill(2)}:{str(ds['time.minute'][0].values).zfill(2)}:{str(ds['time.second'][0].values).zfill(2)}"
    os.system(f"cdo inttime,{date_string},{time_string},3hour PLEV_fc2.nc PLEV_fc2_3h.nc")

    # Concatenate and interpolate to 1h
    for prefix in ['SURF', 'PLEV']:
        ds1 = xr.open_dataset(f'{prefix}_fc1.nc')
        ds2 = xr.open_dataset(f'{prefix}_fc2_3h.nc')

        # Average between last of fc1 and first of fc2
        last_ts = ds1.isel(time=-1)
        first_ts = ds2.isel(time=0)
        avg_ds = (last_ts + first_ts) / 2

        t1 = pd.to_datetime(ds1['time'])[-1].to_pydatetime()
        t2 = pd.to_datetime(ds2['time'])[0].to_pydatetime()
        avg_ts = t1 + (t2 - t1) / 2

        avg_ds['time'] = avg_ts
        avg_ds = avg_ds.assign_coords(time=avg_ts)
        ds_cat = xr.concat([ds1, avg_ds, ds2], dim='time')
        ds_cat.to_netcdf(f'{prefix}_cat.nc')

        ds1.close()
        ds2.close()

    logger.info("Interpolating 3h to 1h timestep")

    for prefix in ['SURF', 'PLEV']:
        ds = xr.open_dataset(f'{prefix}_cat.nc')
        date_string = f"{ds['time.year'][0].values}-{str(ds['time.month'][0].values).zfill(2)}-{str(ds['time.day'][0].values).zfill(2)}"
        time_string = f"{str(ds['time.hour'][0].values).zfill(2)}:{str(ds['time.minute'][0].values).zfill(2)}:{str(ds['time.second'][0].values).zfill(2)}"
        os.system(f"cdo inttime,{date_string},{time_string},1hour {prefix}_cat.nc {prefix}_cat_1h.nc")
        ds.close()

    # Move to parent directory
    os.rename("SURF_cat_1h.nc", "../SURF_FC.nc")
    os.rename("PLEV_cat_1h.nc", "../PLEV_FC.nc")

    # Create hindcast products (first 24 hours)
    ds1 = xr.open_dataset("../PLEV_FC.nc")
    thind = pd.to_datetime(ds1['time'])[0:24]
    day = str(thind[0])[0:10]
    first_24 = ds1.isel(time=slice(0, 24))
    first_24.to_netcdf(f'../PLEV_FC_{day}.nc', mode='w')
    ds1.close()

    ds1 = xr.open_dataset("../SURF_FC.nc")
    first_24 = ds1.isel(time=slice(0, 24))
    first_24.to_netcdf(f'../SURF_FC_{day}.nc', mode='w')
    ds1.close()

    logger.info(f"Saved hindcast products for {day}")

    return day


def merge_all_forecasts(tmp_path):
    """Merge all forecast files (hindcast products + current forecast)"""
    os.chdir(tmp_path)

    for prefix in ['PLEV', 'SURF']:
        # Remove old merged file
        merged_file = f"{prefix}_merged.nc"
        if os.path.exists(merged_file):
            os.remove(merged_file)

        logger.info(f"Merging {prefix} hindcast and forecast files")
        os.system(f"cdo --sortname mergetime ../{prefix}_FC* {merged_file}")

        # Remove duplicate timestamps
        ds = xr.open_dataset(merged_file)
        time_index = pd.to_datetime(ds['time'].values)
        _, unique_indices = np.unique(time_index, return_index=True)
        ds_unique = ds.isel(time=unique_indices)

        output_file = f'../{prefix}_FC.nc'
        if os.path.exists(output_file):
            os.remove(output_file)
        ds_unique.to_netcdf(output_file)
        ds.close()

        logger.info(f"Cleaned hindcast+forecast {prefix} saved")


# ============================================================================
# Main execution
# ============================================================================

if __name__ == "__main__":
    logger.info("Starting IFS forecast fetch")

    # Try to load config from snowmapper.yml
    try:
        from config import load_config
        cfg = load_config()
        directory_path = os.path.join(cfg['paths']['climate_dir'], 'forecast')
        logger.info(f"Using config climate_dir: {cfg['paths']['climate_dir']}")
    except (FileNotFoundError, ImportError):
        # Fall back to default path (new structure)
        directory_path = './inputs/climate/forecast/'
        logger.info("No snowmapper.yml found, using default path")

    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    # Remember original directory
    original_dir = os.getcwd()
    os.chdir(directory_path)
    forecast_dir = os.getcwd()

    # Create tmp directory
    tmp_path = os.path.join(forecast_dir, "tmp")
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)

    # Configuration
    fctime = 0
    lat_range = (32, 45)
    lon_range = (59, 81)

    # Print latest forecast availability
    try:
        client = Client(source="ecmwf")
        latest_info = client.latest(
            type="fc",
            step=24,
            param=["2t", "msl"],
            target=os.path.join(tmp_path, "data.grib2"),
        )
        logger.info(f"Latest forecast data available at: {latest_info}")
    except Exception as e:
        logger.warning(f"Could not check latest forecast: {e}")

    # Check for missing forecast gap-fill dates
    missing_dates = get_missing_forecast_dates(forecast_dir)

    # Check if today's forecast already exists
    today = datetime.now()
    today_fc_plev = os.path.join(forecast_dir, f"PLEV_FC_{today.strftime('%Y-%m-%d')}.nc")
    today_fc_surf = os.path.join(forecast_dir, f"SURF_FC_{today.strftime('%Y-%m-%d')}.nc")
    today_forecast_exists = os.path.exists(today_fc_plev) and os.path.exists(today_fc_surf)

    # Skip if no missing dates AND today's forecast already exists
    if not missing_dates and today_forecast_exists:
        logger.info("All forecast files already present - skipping download")
        # Still merge to ensure final files are up to date
        merge_all_forecasts(tmp_path)
        os.chdir(original_dir)
        logger.info("IFS forecast fetch complete (skipped)")
        exit(0)

    if missing_dates:
        logger.info(f"Found {len(missing_dates)} missing forecast dates to backfill")
        for date, offset in missing_dates:
            logger.info(f"  {date.strftime('%Y-%m-%d')} (mydate={offset})")

        # Download missing forecasts
        for date, offset in tqdm(missing_dates, desc="Backfilling forecasts"):
            try:
                download_and_process_forecast(
                    mydate=offset,
                    fctime=fctime,
                    tmp_path=tmp_path,
                    lat_range=lat_range,
                    lon_range=lon_range,
                    save_hindcast_only=True
                )
            except Exception as e:
                logger.error(f"Failed to download forecast for {date}: {e}")
    else:
        logger.info("No missing forecast dates to backfill")

    # Download today's forecast if not already present
    if today_forecast_exists:
        logger.info("Today's forecast already exists - skipping download")
    else:
        logger.info("Downloading today's forecast (mydate=0)")
        download_and_process_forecast(
            mydate=0,
            fctime=fctime,
            tmp_path=tmp_path,
            lat_range=lat_range,
            lon_range=lon_range,
            save_hindcast_only=False
        )

    # Merge all forecast files
    merge_all_forecasts(tmp_path)

    os.chdir(original_dir)
    logger.info("IFS forecast fetch complete")
