"""
ERA5 Climate Data Downloader and Merger for SnowMapper.

Downloads ERA5 reanalysis data from Copernicus Climate Data Store (CDS),
checks for missing days, and merges with IFS forecast data to create
continuous climate forcing files.

Inputs:
    - Domain config.yml with climate paths and spatial extent
    - ERA5 data from CDS (requires ~/.cdsapirc credentials)
    - IFS forecast files from fetch_ifs_forecast.py

Outputs:
    - inputs/climate/forecast/PLEV_YYYYMMDD.nc  (daily ERA5 pressure levels)
    - inputs/climate/forecast/SURF_YYYYMMDD.nc  (daily ERA5 surface)
    - inputs/climate/PLEV_final_merged_output.nc  (merged ERA5 + forecast)
    - inputs/climate/SURF_final_merged_output.nc  (merged ERA5 + forecast)

Usage:
    python download_era5.py <domain_path>

Example:
    python download_era5.py ./domains/D2000
"""

import os
import sys
import shutil
import pandas as pd
from datetime import datetime, timedelta
from TopoPyScale import topoclass as tc
from munch import DefaultMunch
import xarray as xr
import numpy as np
import concurrent.futures
import glob
from tqdm import tqdm
from logging_utils import setup_logger_with_tqdm, get_log_dir

# Set up module-level logger (will be configured in main())
logger = None



def load_config(config_file):
    """
    Load configuration from a YAML file.
    
    Parameters:
    - config_file (str): Path to the configuration file.
    
    Returns:
    - config (DefaultMunch): Loaded configuration object.
    """
    try:
        with open(config_file, 'r') as f:
            config = DefaultMunch.fromYAML(f)
        if config.project.directory is None:
            config.project.directory = os.getcwd() + '/'
        return config
    except IOError:
        raise FileNotFoundError(f"ERROR: config file does not exist.\n\t Current file path: {config_file}\n\t Current working directory: {os.getcwd()}")

def parse_filename(file_path):
    """
    Parse the filename to extract start and end dates.
    
    Parameters:
    - file_path (str): Path to the file.
    
    Returns:
    - start_date (pd.Timestamp): Start date extracted from the filename.
    - end_date (pd.Timestamp): End date calculated from the start date.
    """
    filename = os.path.basename(file_path)
    year, month = int(filename.split('_')[1][:4]), int(filename.split('_')[1][4:6])
    start_date = pd.Timestamp(year, month, 1, 0)
    end_date = start_date + pd.offsets.MonthEnd(0) + pd.DateOffset(hours=23)
    return start_date, end_date

def check_timesteps(file_path, error_files):
    """
    Check if all model timesteps are present in a file.

    Parameters:
    - file_path (str): Path to the file to check.
    - error_files (list): List to append files with missing timesteps.
    """
    try:
        ds = xr.open_dataset(file_path)
        actual_time_steps = pd.to_datetime(ds.time.values)
        start_date, end_date = parse_filename(file_path)
        expected_time_steps = pd.date_range(start=start_date, end=end_date, freq='1H')
        missing_time_steps = expected_time_steps[~expected_time_steps.isin(actual_time_steps)]
        if missing_time_steps.empty:
            if logger:
                logger.debug(f"All model timesteps present in {file_path}")
        else:
            if logger:
                logger.warning(f"Missing time steps in {file_path}: {len(missing_time_steps)} steps")
            error_files.append(file_path)
    except (FileNotFoundError, KeyError) as e:
        if logger:
            logger.error(f"Error processing {file_path}: {str(e)}")
        error_files.append(file_path)

def delete_files(file_paths):
    """
    Delete files listed in the provided file_paths.

    Parameters:
    - file_paths (list): A list of file paths to be deleted.
    """
    deleted = 0
    failed = 0
    for file_path in tqdm(file_paths, desc="Deleting files", disable=len(file_paths) < 5):
        try:
            os.remove(file_path)
            deleted += 1
        except Exception as e:
            if logger:
                logger.debug(f"Could not delete {file_path}: {str(e)}")
            failed += 1
    if logger:
        logger.info(f"Deleted {deleted} files" + (f", {failed} failed" if failed else ""))

def generate_file_paths(start_year, end_year, end_month, file_types):
    """
    Generate file paths for a given range of months and file types.
    
    Parameters:
    - start_year (int): Start year for generating file paths.
    - end_year (int): End year for generating file paths.
    - end_month (int): End month for generating file paths.
    - file_types (list): List of file types to generate paths for.
    
    Returns:
    - file_paths (list): List of generated file paths.
    """
    start_date = pd.Timestamp(start_year, 9, 1, 0)  # September of the start year
    end_date = pd.Timestamp(end_year, end_month, 1, 0) + pd.offsets.MonthEnd(0)
    time_vector = pd.date_range(start=start_date, end=end_date, freq='1M')
    return [f"{file_type}_{time.strftime('%Y%m')}.nc" for time in time_vector for file_type in file_types]

def trim_forecast_data(climate_file, forecast_file, output_file):
    """
    Trim forecast data to remove overlapping time steps with climate data.
    
    Parameters:
    - climate_file (str): Path to the climate data file.
    - forecast_file (str): Path to the forecast data file.
    - output_file (str): Path to save the trimmed forecast data.
    """
    ds1 = xr.open_dataset(climate_file)
    ds2 = xr.open_dataset(forecast_file)
    last_time_file1 = ds1.time[-1].values
    next_time = last_time_file1 + np.timedelta64(1, 'h')
    trimmed_ds2 = ds2.sel(time=slice(next_time, None))
    trimmed_ds2.to_netcdf(output_file)
    if logger:
        logger.debug(f"Trimmed forecast data saved to {output_file}")





def trim_forecast_data2(climate_file, forecast_file, output_file):
    """
    Trim forecast data to remove overlapping time steps with climate data.

    Ensures the climate data ends at 23:00 and the forecast data starts at 00:00.

    Parameters:
    - climate_file (str): Path to the climate data file.
    - forecast_file (str): Path to the forecast data file.
    - output_file (str): Path to save the trimmed forecast data.
    """
    try:
        # Open datasets
        ds1 = xr.open_dataset(climate_file)
        ds2 = xr.open_dataset(forecast_file)

        # Get the last time in the climate data
        last_time_file1 = ds1.time[-1].values

        # Ensure climate data ends at 23:00
        # Convert last_time to a datetime and check the hour
        if np.datetime64(last_time_file1).astype('datetime64[h]').astype(str)[-2:] != '23':
            if logger:
                logger.debug("Trimming climate data to end at 23:00")
            # Trim the climate data to end at the last occurrence of 23:00
            ds1 = ds1.sel(time=slice(None, str(last_time_file1)[:10] + 'T23:00:00'))
            last_time_file1 = ds1.time[-1].values  # Update last time after trimming

        # Calculate the next time step (start of forecast data), which should be 00:00 of the next day
        next_time = (last_time_file1 + np.timedelta64(1, 'h')).astype('datetime64[D]') + np.timedelta64(0, 'h')

        # Trim forecast data to start at 00:00 of the next day
        trimmed_ds2 = ds2.sel(time=slice(next_time, None))

        # Save the trimmed forecast data
        trimmed_ds2.to_netcdf(output_file)
        if logger:
            logger.debug(f"Trimmed forecast data saved to {output_file}")

    except KeyError as e:
        if logger:
            logger.error(f"Time coordinate not found in one of the datasets: {e}")
    except Exception as e:
        if logger:
            logger.error(f"An error occurred: {e}")
    finally:
        # Close datasets
        ds1.close()
        ds2.close()



def merge_datasets(pattern1, pattern2, output_path):
    """
    Load datasets from specified patterns, interpolate, merge, and save to a new NetCDF file.
    
    Parameters:
        pattern1 (str): File pattern for the first group of datasets.
        pattern2 (str): File pattern for the second group of datasets.
        output_path (str): Path to save the merged dataset.
        
    Returns:
        xarray.Dataset: The merged dataset.
    """
    # Find files that match the patterns
    grid1_files = glob.glob(pattern1)
    grid2_files = glob.glob(pattern2)

    # Load the files from both grids
    ds_grid1_list = [xr.open_dataset(file) for file in grid1_files]
    ds_grid2_list = [xr.open_dataset(file) for file in grid2_files]

    # Choose the grid from the first file in Grid 1 as the common grid
    common_grid = ds_grid1_list[0]  # Assuming all grid 1 files share the same grid

    # Interpolate all Grid 2 files to the common grid (Grid 1)
    ds_grid2_interp_list = [ds.interp(latitude=common_grid.latitude, longitude=common_grid.longitude) for ds in ds_grid2_list]

    # Merge all Grid 1 files into a single dataset
    ds_grid1 = xr.merge(ds_grid1_list)

    # Merge all Grid 2 (interpolated) files into a single dataset
    ds_grid2_interp = xr.merge(ds_grid2_interp_list)

    # Now merge both datasets into a single one
    ds_merged = xr.merge([ds_grid1, ds_grid2_interp])

    # Save the merged dataset to a new NetCDF file
    ds_merged.to_netcdf(output_path)

    if logger:
        logger.info(f"All files merged and saved to {output_path}")


def merge_datasets_filter(pattern1, pattern2, output_path):
    """
    Load datasets from specified patterns, interpolate, merge, and save to a new NetCDF file.
    
    Parameters:
        pattern1 (str): File pattern for the first group of datasets.
        pattern2 (str): File pattern for the second group of datasets.
        output_path (str): Path to save the merged dataset.
        
    Returns:
        xarray.Dataset: The merged dataset.
    """
    # Get today's date and calculate the cutoff date (9 days ago)
    cutoff_date = datetime.now() - timedelta(days=9)

    # Find files that match the patterns
    grid1_files = glob.glob(pattern1)
    grid2_files = glob.glob(pattern2)
    
    # Check if files were found
    if not grid1_files:
        raise FileNotFoundError(f"No files found for pattern: {pattern1}")
    if not grid2_files:
        raise FileNotFoundError(f"No files found for pattern: {pattern2}")
    
    # Filter grid1_files by date (extract the date from the filename)
    filtered_grid1_files = []
    for file in grid1_files:
        # Extract date from the filename assuming format "SURF_YYYYMMDD.nc"
        filename = os.path.basename(file)
        date_str = filename.split('_')[1].split('.')[0]  # Extract "YYYYMMDD"
        
        try:
            file_date = datetime.strptime(date_str, "%Y%m%d")
            # Only keep files that are after the cutoff date
            if file_date >= cutoff_date:
                filtered_grid1_files.append(file)
        except ValueError:
            if logger:
                logger.warning(f"Could not parse date from filename: {filename}")

    # Check if any files remain after filtering
    if not filtered_grid1_files:
        raise FileNotFoundError("No grid1 files found within the last 9 days.")
    
    # Load the files from both grids (use lazy loading for efficiency if datasets are large)
    ds_grid1_list = [xr.open_dataset(file) for file in filtered_grid1_files]
    ds_grid2_list = [xr.open_dataset(file) for file in grid2_files]
    
    # Choose the grid from the first file in Grid 1 as the common grid
    common_grid = ds_grid1_list[0]  # Assuming all grid 1 files share the same grid
    
    # Interpolate all Grid 2 files to the common grid (Grid 1)
    ds_grid2_interp_list = [
        ds.interp(latitude=common_grid.latitude, longitude=common_grid.longitude)
        for ds in ds_grid2_list
    ]
    
    # Merge all Grid 1 files into a single dataset
    ds_grid1 = xr.merge(ds_grid1_list)
    
    # Merge all Grid 2 (interpolated) files into a single dataset
    ds_grid2_interp = xr.merge(ds_grid2_interp_list)
    
    # Now merge both datasets into a single one
    ds_merged = xr.merge([ds_grid1, ds_grid2_interp]) # override”: skip comparing and pick variable from first dataset ie era5 gets prioritised if overlap exists
    
    # Save the merged dataset to a new NetCDF file
    ds_merged.to_netcdf(output_path)

    if logger:
        logger.info(f"All files merged and saved to {output_path}")




def merge_forecast_with_merged(ds_merged_path, ds_surf_fc_path, output_path):
    """
    Merge the ERA5 gapfill and forecast dataset with the previously merged dataset.
    
    Parameters:
        ds_merged_path (str): Path to the merged dataset file.
        ds_surf_fc_path (str): Path to the surf forecast dataset file.
        output_path (str): Path to save the final merged dataset.
    """
    # Load the datasets
    ds_merged = xr.open_dataset(ds_merged_path)
    ds_surf_fc = xr.open_dataset(ds_surf_fc_path)

    # Interpolate ds_surf_fc to match the grid of ds_merged
    ds_surf_fc_interp = ds_surf_fc.interp(latitude=ds_merged.latitude, longitude=ds_merged.longitude)

    # Check for overlapping time
    overlap_start = max(ds_merged.time.min(), ds_surf_fc_interp.time.min())
    overlap_end = min(ds_merged.time.max(), ds_surf_fc_interp.time.max())

    # Check if there is an overlap
    if overlap_start < overlap_end:
        if logger:
            logger.debug(f"Overlap detected from {overlap_start.values} to {overlap_end.values}")
        # Dynamically slice ds_merged to remove the overlapping time
        ds_merged_cleaned = ds_merged.sel(time=slice(None, overlap_start - pd.Timedelta(hours=1)))
    else:
        if logger:
            logger.debug("No overlapping time detected")
        ds_merged_cleaned = ds_merged  # No need to slice if there's no overlap

    # Concatenate the datasets along the 'time' dimension
    ds_final = xr.concat([ds_merged_cleaned, ds_surf_fc_interp], dim='time')

    # Save the final merged dataset
    ds_final.to_netcdf(output_path)

    if logger:
        logger.info(f"Final merged dataset saved to {output_path}")


def convert_time_units_to_ncview_compatible(dataset_path, output_path):
    """
    Convert the time units of the dataset to 'hours since' the start of the dataset.
    
    Parameters:
        dataset_path (str): Path to the input NetCDF dataset.
        output_path (str): Path to save the modified dataset.
        # Example usage
    dataset_path = '/home/joel/sim/snowmapper_2025/master/inputs/climate/SURF_final_merged_output.nc'
    output_path = '/home/joel/sim/snowmapper_2025/master/inputs/climate/SURF_fixed_time_output.nc'
    convert_time_units_to_ncview_compatible(dataset_path, output_path)
    """
    try:
        # Load the dataset
        ds = xr.open_dataset(dataset_path)

        # Ensure 'time' variable exists in the dataset
        if 'time' not in ds:
            raise ValueError("'time' variable not found in the dataset.")

        # Convert time units to 'hours since' the start of the dataset
        start_time = pd.to_datetime(ds['time'].values[0])  # Get the first time point

        # Convert time to hours since start_time
        ds['time'] = (ds['time'] - ds['time'][0]).astype('timedelta64[h]')

        # Handle encoding instead of directly modifying attributes
        ds['time'].encoding['units'] = f"hours since {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        ds['time'].encoding['calendar'] = 'proleptic_gregorian'
        
        # Set standard and long names directly in the attributes
        ds['time'].attrs['standard_name'] = 'time'
        ds['time'].attrs['long_name'] = 'time'

        # Save the dataset with the corrected time variable
        ds.to_netcdf(output_path)

        if logger:
            logger.debug(f"Saved file with corrected time to {output_path}")

    except Exception as e:
        if logger:
            logger.error(f"Error converting time units: {e}")





def check_duplicate_and_missing_times(dataset_path):
    """
    Check for duplicate and missing timestamps in the dataset.

    Parameters:
        dataset_path (str): Path to the input NetCDF dataset.
    """
    # Load the dataset
    ds = xr.open_dataset(dataset_path)

    # Convert ds.time to a pandas DatetimeIndex
    mytimeseries = pd.to_datetime(ds.time.values)

    # Create a complete time range from the start to the end of mytimeseries with an hourly frequency
    complete_time_range = pd.date_range(start=mytimeseries.min(), end=mytimeseries.max(), freq='H')

    # Find missing times by comparing the two time series
    missing_times = complete_time_range.difference(mytimeseries)

    # Check for duplicate times
    duplicate_times = mytimeseries[mytimeseries.duplicated()]

    # Log the missing times, if any
    if not missing_times.empty:
        if logger:
            logger.warning(f"Missing times found: {len(missing_times)} timestamps")
            logger.debug(f"Missing times: {missing_times}")
    else:
        if logger:
            logger.debug("No missing times in the time series")

    # Log the duplicate times, if any
    if not duplicate_times.empty:
        if logger:
            logger.warning(f"Duplicate times found: {len(duplicate_times)} timestamps")
            logger.debug(f"Duplicate times: {duplicate_times}")
    else:
        if logger:
            logger.debug("No duplicate times in the time series")


def save_daily_files(dataset_path, output_directory):
    """
    Save each day of the dataset to a separate NetCDF file.

    Parameters:
        dataset_path (str): Path to the input NetCDF dataset.
        output_directory (str): Directory to save the daily NetCDF files.
    """
    # Load the dataset
    ds = xr.open_dataset(dataset_path)

    days = ds.time.dt.strftime('%Y%m%d').values
    unique_days = list(dict.fromkeys(days))  # Preserve order, remove duplicates

    # Loop over each unique day in the 'time' dimension
    for day in tqdm(unique_days, desc="Saving daily files"):
        # Select the data for the specific day
        ds_day = ds.sel(time=day)

        # Define the output file name based on the date
        output_file = f'{output_directory}/SURF_{day}.nc'

        # Save the daily data to a new NetCDF file
        ds_day.to_netcdf(output_file)

    if logger:
        logger.info(f"Saved {len(unique_days)} daily files to {output_directory}")


def log_data_chain_summary(out_dir):
    """
    Log a summary of the forcing data chain showing ERA5 and forecast coverage.

    Parameters:
        out_dir (str): Path to the output directory containing merged climate files.
    """
    import xarray as xr
    import pandas as pd

    try:
        # Check merged PLEV file for ERA5 + forecast coverage
        merged_file = os.path.join(out_dir, "PLEV_final_merged_output.nc")
        if not os.path.exists(merged_file):
            logger.warning("Merged PLEV file not found, cannot log data chain summary")
            return

        ds = xr.open_dataset(merged_file)
        times = pd.to_datetime(ds['time'].values)
        ds.close()

        first_time = times[0]
        last_time = times[-1]
        total_hours = len(times)

        # Check for gaps
        expected_hours = int((last_time - first_time).total_seconds() / 3600) + 1
        missing_hours = expected_hours - total_hours

        logger.info("=" * 60)
        logger.info("FORCING DATA CHAIN SUMMARY")
        logger.info("=" * 60)
        logger.info(f"  First timestamp:  {first_time.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"  Last timestamp:   {last_time.strftime('%Y-%m-%d %H:%M')}")
        logger.info(f"  Total timesteps:  {total_hours} hours")

        if missing_hours == 0:
            logger.info(f"  Status:           COMPLETE (no gaps)")
        else:
            logger.warning(f"  Status:           {missing_hours} hours MISSING")

        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Failed to generate data chain summary: {e}")


def get_missing_era5_days(climate_path, latest_available):
    """
    Check for missing ERA5 days between the first downloaded data and the latest available.

    Parameters:
        climate_path (str): Path to the climate/forecast directory containing ERA5 files.
        latest_available (datetime): The latest available ERA5 date from CDS.

    Returns:
        list: List of datetime objects for missing days that need to be downloaded.
    """
    forecast_dir = os.path.join(climate_path, "forecast")

    # Find all existing ERA5 daily files (PLEV_YYYYMMDD.nc format, not FC files)
    era5_files = sorted(glob.glob(os.path.join(forecast_dir, "PLEV_20*.nc")))

    if not era5_files:
        logger.warning("No existing ERA5 files found")
        return [latest_available]

    # Get the first downloaded date (start of our data)
    first_file = era5_files[0]
    first_date_str = os.path.basename(first_file).split('_')[1].split('.')[0]
    first_downloaded = datetime.strptime(first_date_str, "%Y%m%d")

    # Find ALL missing days from first downloaded to latest available
    missing_days = []
    current = first_downloaded

    while current <= latest_available:
        # Check if BOTH PLEV and SURF files exist for this date
        plev_file = os.path.join(forecast_dir, f"PLEV_{current.strftime('%Y%m%d')}.nc")
        surf_file = os.path.join(forecast_dir, f"SURF_{current.strftime('%Y%m%d')}.nc")
        if not os.path.exists(plev_file) or not os.path.exists(surf_file):
            missing_days.append(current)
        current += timedelta(days=1)

    return missing_days


def download_missing_era5_days(mp, missing_days):
    """
    Download ERA5 data for missing days.

    Parameters:
        mp: Topoclass instance with ERA5 download methods.
        missing_days (list): List of datetime objects for days to download.
    """
    if not missing_days:
        logger.info("No missing ERA5 days to download")
        return

    logger.info(f"Found {len(missing_days)} missing ERA5 day(s): {[d.strftime('%Y-%m-%d') for d in missing_days]}")

    for day in tqdm(missing_days, desc="Downloading missing ERA5 days"):
        logger.info(f"Downloading ERA5 for {day.strftime('%Y-%m-%d')}")
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_surf = executor.submit(mp.get_era5_snowmapper, 'surf', day)
                future_plev = executor.submit(mp.get_era5_snowmapper, 'plev', day)
                concurrent.futures.wait([future_surf, future_plev])

            # Process downloaded files - unzip and remap each file individually
            plev_path = str(mp.config.climate.path) + f"/forecast/PLEV_{day.strftime('%Y%m%d')}.nc"
            surf_path = str(mp.config.climate.path) + f"/forecast/SURF_{day.strftime('%Y%m%d')}.nc"
            mp.unzip_file(plev_path)
            mp.unzip_file(surf_path)
            mp.remap_netcdf(plev_path, 'PLEV')
            mp.remap_netcdf(surf_path, 'SURF')

            # Archive any forecast files that this ERA5 data replaces
            plev_file = str(mp.config.climate.path) + f"/forecast/PLEV_{day.strftime('%Y%m%d')}.nc"
            surf_file = str(mp.config.climate.path) + f"/forecast/SURF_{day.strftime('%Y%m%d')}.nc"
            handle_forecast_file(plev_file, prefix="PLEV", archive=True)
            handle_forecast_file(surf_file, prefix="SURF", archive=True)

            logger.info(f"Successfully downloaded ERA5 for {day.strftime('%Y-%m-%d')}")
        except Exception as e:
            logger.error(f"Failed to download ERA5 for {day.strftime('%Y-%m-%d')}: {e}")


def handle_forecast_file(era5_file_path, prefix="PLEV", archive=True):
    """
    Delete or archive the corresponding forecast file when an ERA5 file is downloaded.

    Parameters:
        era5_file_path (str): The path to the downloaded ERA5 file in the format '{prefix}_YYYYMMDD.nc'.
        prefix (str): The prefix used in the filenames ('PLEV' or 'SURF').
        archive (bool): If True, move the forecast file to the 'archive_forecast' directory. If False, delete the forecast file.
    """
    # Extract the date from the ERA5 filename
    era5_filename = os.path.basename(era5_file_path)
    date_str = era5_filename.split('_')[1].split('.')[0]  # Extract "YYYYMMDD"
    
    try:
        # Convert the date to the forecast file format "YYYY-MM-DD"
        file_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
        # Construct the corresponding forecast file path
        forecast_file = era5_file_path.replace(f"{prefix}_{date_str}.nc", f"{prefix}_FC_{file_date}.nc")

        # Check if the forecast file exists
        if os.path.exists(forecast_file):
            if archive:
                # Create the archive directory if it doesn't exist
                archive_dir = os.path.join(os.path.dirname(forecast_file), 'archive_forecast')
                os.makedirs(archive_dir, exist_ok=True)

                # Move the forecast file to the archive directory
                shutil.move(forecast_file, os.path.join(archive_dir, os.path.basename(forecast_file)))
                if logger:
                    logger.debug(f"Moved forecast file to archive: {os.path.basename(forecast_file)}")
            else:
                # Delete the forecast file
                os.remove(forecast_file)
                if logger:
                    logger.debug(f"Deleted forecast file: {forecast_file}")
        else:
            if logger:
                logger.debug(f"No corresponding forecast file found for {file_date}")

    except ValueError as e:
        if logger:
            logger.error(f"Error processing the filename {era5_filename}: {e}")

# Example usage:
# handle_forecast_file("/path/to/downloaded/PLEV_20240926.nc", prefix="PLEV", archive=True)
# handle_forecast_file("/path/to/downloaded/SURF_20240926.nc", prefix="SURF", archive=False)


# https://forum.ecmwf.int/t/forthcoming-update-to-the-format-of-netcdf-files-produced-by-the-conversion-of-grib-data-on-the-cds/7772

import xarray as xr
import pandas as pd
from pathlib import Path

def merge_climate_files(data_dir, prefix, output_file):
    """
    Merge daily reanalysis, single-day forecasts, and continuous forecast for a given prefix.

    Parameters:
        data_dir (Path or str): Directory containing the NetCDF files.
        prefix (str): File prefix, e.g., "SURF" or "PLEV".
        output_file (Path or str): Path for merged output NetCDF.
    """
    data_dir = Path(data_dir)

    # 1. ERA5 daily reanalysis
    era5_files = sorted(data_dir.glob(f"{prefix}_2025*.nc"))
    ds_era5 = xr.concat([xr.open_dataset(f) for f in era5_files], dim="time")

    # 2. Single-day forecasts (fill gaps)
    fc_daily_files = sorted(data_dir.glob(f"{prefix}_FC_2025-*.nc"))

    era5_times = pd.to_datetime(ds_era5.time.values)
    expected_dates = pd.date_range(start=era5_times.min(), end=era5_times.max(), freq="D")
    missing_dates = [d for d in expected_dates if d not in era5_times]

    ds_fc_daily = None
    if missing_dates:
        ds_fc_daily = xr.concat(
            [xr.open_dataset(f) for f in fc_daily_files if pd.to_datetime(f.name.split("_")[-1].split(".")[0]) in missing_dates],
            dim="time"
        )

    # 3. Continuous 10-day forecast
    fc_cont_file = data_dir / f"{prefix}_FC.nc"
    ds_fc_cont = xr.open_dataset(fc_cont_file)

    # 4. Merge in correct order
    datasets = [ds_era5]
    if ds_fc_daily is not None:
        datasets.append(ds_fc_daily)
    datasets.append(ds_fc_cont)

    ds_merged = xr.concat(datasets, dim="time")

    # 5. Save
    ds_merged.to_netcdf(output_file)
    if logger:
        logger.info(f"[{prefix}] Merged file written to {output_file}")
        logger.info(f"[{prefix}] Time coverage: {ds_merged.time.values.min()} to {ds_merged.time.values.max()}")



import re
from pathlib import Path
import pandas as pd
import xarray as xr

def merge_climate_files2(data_dir, prefix, output_file):
    data_dir = Path(data_dir)

    # 1. ERA5 hourly reanalysis
    era5_files = sorted(data_dir.glob(f"{prefix}_2025*.nc"))
    ds_era5 = xr.open_mfdataset(era5_files, combine="by_coords")

    # 2. Find gaps in ERA5 coverage
    era5_times = pd.to_datetime(ds_era5.time.values)
    expected_times = pd.date_range(start=era5_times.min(), end=era5_times.max(), freq="H")
    missing_times = set(expected_times) - set(era5_times)

    # 3. Single-day forecasts (only where ERA5 is missing)
    fc_daily_files = sorted(data_dir.glob(f"{prefix}_FC_2025-*.nc"))
    selected_fc_files = []
    for f in fc_daily_files:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", f.name)
        if m:
            # forecast files should cover full 24h → include if any of its hours are missing
            file_date = pd.to_datetime(m.group(1))
            hours = pd.date_range(file_date, file_date + pd.Timedelta("23H"), freq="H")
            if any(h in missing_times for h in hours):
                selected_fc_files.append(f)

    ds_fc_daily = None
    if selected_fc_files:
        ds_fc_daily = xr.open_mfdataset(selected_fc_files, combine="by_coords")

    # 4. Continuous forecast
    fc_cont_file = data_dir / f"{prefix}_FC.nc"
    ds_fc_cont = xr.open_dataset(fc_cont_file)

    # 5. Merge with priority: ERA5 > daily forecast > continuous forecast
    ds_base = ds_era5
    if ds_fc_daily is not None:
        ds_base = ds_base.combine_first(ds_fc_daily)
    ds_merged = ds_base.combine_first(ds_fc_cont)

    # 6. Save
    ds_merged.to_netcdf(output_file)






import re
from pathlib import Path
import pandas as pd
import xarray as xr

def merge_climate_files3(data_dir, prefix, output_file):
    data_dir = Path(data_dir)

    # 1. ERA5 hourly reanalysis
    era5_files = sorted(data_dir.glob(f"{prefix}_20*.nc"))
    if not era5_files:
        raise FileNotFoundError("No ERA5 files found")
    ds_era5 = xr.open_mfdataset(era5_files, combine="by_coords")
    ref_lat = ds_era5.latitude
    ref_lon = ds_era5.longitude

    # 2. Find gaps in ERA5 coverage
    era5_times = pd.to_datetime(ds_era5.time.values)
    expected_times = pd.date_range(start=era5_times.min(), end=era5_times.max(), freq="H")
    missing_times = set(expected_times) - set(era5_times)

    # 3. Single-day forecasts (only where ERA5 is missing)
    fc_daily_files = sorted(data_dir.glob(f"{prefix}_FC_20*.nc"))
    selected_fc_files = []
    for f in fc_daily_files:
        m = re.search(r"(\d{4}-\d{2}-\d{2})", f.name)
        if m:
            file_date = pd.to_datetime(m.group(1))
            hours = pd.date_range(file_date, file_date + pd.Timedelta("23H"), freq="H")
            if any(h in missing_times for h in hours):
                selected_fc_files.append(f)

    ds_fc_daily = None
    if selected_fc_files:
        ds_fc_daily = xr.open_mfdataset(selected_fc_files, combine="by_coords")
        ds_fc_daily = ds_fc_daily.interp(latitude=ref_lat, longitude=ref_lon)

    # 4. Continuous forecast
    fc_cont_file = data_dir / f"{prefix}_FC.nc"
    ds_fc_cont = xr.open_dataset(fc_cont_file)
    ds_fc_cont = ds_fc_cont.interp(latitude=ref_lat, longitude=ref_lon)

    # 5. Merge with priority: ERA5 > daily forecast > continuous forecast
    ds_base = ds_era5
    if ds_fc_daily is not None:
        ds_base = ds_base.combine_first(ds_fc_daily)
    ds_merged = ds_base.combine_first(ds_fc_cont)

    # 6. Save
    ds_merged.to_netcdf(output_file)
    return ds_merged



def main():
    global logger
    start_time = datetime.now()
    mydir = sys.argv[1]
    os.chdir(mydir)

    # Set up logging
    log_dir = get_log_dir(mydir)
    logger = setup_logger_with_tqdm("run_master", file=False)

    config_file = './config.yml'

    # Initialize Topoclass and perform operations
    mp = tc.Topoclass(config_file)

    # Get latest available ERA5 date
    lastday = mp.get_lastday()
    # Handle both string and datetime return types
    if isinstance(lastday, str):
        lastday_str = lastday
        lastday = datetime.strptime(lastday, '%Y-%m-%d')
    else:
        lastday_str = lastday.strftime('%Y-%m-%d')
    logger.info(f"Latest available ERA5 date: {lastday_str}")

    # Check for and download any missing ERA5 days
    missing_days = get_missing_era5_days(str(mp.config.climate.path), lastday)
    download_missing_era5_days(mp, missing_days)

    # Use climate path from domain config
    climate_path = str(mp.config.climate.era5.path)
    data_dir = os.path.join(climate_path, "forecast")
    out_dir = climate_path
    # List of files to remove if they exist
    files_to_remove = [
        os.path.join(out_dir, "SURF_final_merged_output.nc"),
        os.path.join(out_dir, "PLEV_final_merged_output.nc")
    ]

    # Remove each file if it exists
    for f in files_to_remove:
        if os.path.exists(f):
             os.remove(f)

    
    # --- Example usage ---
    merge_climate_files3(data_dir, "SURF", out_dir + "/SURF_final_merged_output.nc")
    merge_climate_files3(data_dir, "PLEV", out_dir + "/PLEV_final_merged_output.nc")

    # Log data chain summary
    log_data_chain_summary(out_dir)



    logger.info(f"Script completed in {datetime.now() - start_time}")

if __name__ == "__main__":
    main()


