"""
Upload forecast bundle to S3.

Usage:
    python upload_to_AWS_offline_Forecast.py <date> [--bundle]

Arguments:
    date        Date in YYYYMMDD format (e.g., 20260119)
    --bundle    Create bundle from spatial/*.nc files before uploading
                Without this flag, uploads existing bundle files directly

Examples:
    # Bundle forecast files and upload
    python upload_to_AWS_offline_Forecast.py 20260119 --bundle

    # Upload existing bundle files (no bundling)
    python upload_to_AWS_offline_Forecast.py 20260119
"""
import os
import sys
import xarray as xr
import pandas as pd
from datetime import datetime
import boto3

# Handle import path for s3_utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import s3_utils as s3

# Parse arguments
if len(sys.argv) < 2:
    print(__doc__)
    sys.exit(1)

formatted_date = sys.argv[1]
do_bundle = '--bundle' in sys.argv

# Validate date format
try:
    datetime.strptime(formatted_date, "%Y%m%d")
except ValueError:
    print(f"ERROR: Invalid date format '{formatted_date}'. Use YYYYMMDD (e.g., 20260119)")
    sys.exit(1)

# AWS setup
session = boto3.Session()
credentials = session.get_credentials().get_frozen_credentials()
SNOW_MODEL = "joel-snow-model"
SNOW_MODEL_BUCKET = "snow-model-data-source"
aws_access_key_id = credentials.access_key
aws_secret_access_key = credentials.secret_key
spatial_directory = "./spatial/"


def bundle_nc_files(directory, start_date, file_class, output_file):
    """
    Bundle NetCDF files from start_date onwards into a single file with time dimension.

    Args:
        directory: Path to spatial directory containing daily NC files
        start_date: Start date in YYYYMMDD format
        file_class: Variable name (SWE, HS, ROF)
        output_file: Output filename for bundled file

    Returns:
        int: Number of files bundled
    """
    files_on_or_after = []
    start_dt = datetime.strptime(start_date, "%Y%m%d")

    # Collect all files on or after the start date for the specified class
    for file in os.listdir(directory):
        if file.startswith(file_class + '_') and file.endswith('.nc'):
            file_date_str = file.split('_')[1].replace('.nc', '')
            try:
                file_date = datetime.strptime(file_date_str, "%Y%m%d")
                if file_date >= start_dt:
                    files_on_or_after.append(file)
            except ValueError:
                continue  # Skip files with invalid date format

    if not files_on_or_after:
        print(f"WARNING: No {file_class} files found on or after {start_date}")
        return 0

    # Sort the files by date
    files_on_or_after.sort(key=lambda x: x.split('_')[1].replace('.nc', ''))

    print(f"Bundling {len(files_on_or_after)} {file_class} files:")
    for f in files_on_or_after:
        print(f"  - {f}")

    datasets = []
    times = []

    # Load each file and extract data along with its time dimension
    for file in files_on_or_after:
        file_path = os.path.join(directory, file)
        ds = xr.open_dataset(file_path)

        # Extract the date from the filename
        file_date_str = file.split('_')[1].replace('.nc', '')
        file_date = pd.to_datetime(file_date_str, format="%Y%m%d")
        times.append(file_date)

        datasets.append(ds)

    # Concatenate all datasets along a new 'time' dimension
    combined = xr.concat(datasets, dim='time')

    # Assign the times to the 'time' dimension
    combined = combined.assign_coords(time=("time", times))

    # Save the combined dataset to a new NetCDF file
    combined.to_netcdf(output_file)

    print(f"Created bundle: {output_file} ({len(files_on_or_after)} days)")
    return len(files_on_or_after)


def upload_parameter(parameter, formatted_date, do_bundle, directory):
    """Upload a single parameter (SWE, HS, or ROF) to S3."""
    output_filename_nc = f'{parameter}_{formatted_date}.nc'

    if do_bundle:
        # Bundle files from spatial directory
        n_files = bundle_nc_files(directory, formatted_date, parameter, output_filename_nc)
        if n_files == 0:
            print(f"SKIP: No files to bundle for {parameter}")
            return False
    else:
        # Check if pre-bundled file exists
        if not os.path.exists(output_filename_nc):
            print(f"ERROR: {output_filename_nc} not found. Use --bundle to create it.")
            return False

    # Upload to S3
    s3_path = s3.get_file_path(formatted_date, parameter, True)  # True = forecast path
    success = s3.upload_file(output_filename_nc, SNOW_MODEL_BUCKET, s3_path,
                             aws_access_key_id, aws_secret_access_key)

    if success:
        print(f"SUCCESS: Uploaded {s3_path}")
        if do_bundle:
            os.remove(output_filename_nc)  # Clean up temp bundle file
            print(f"Cleaned up: {output_filename_nc}")
    else:
        print(f"FAILED: Upload failed for {s3_path}")

    return success


# Main execution
print(f"{'='*60}")
print(f"Forecast Upload: {formatted_date}")
print(f"Mode: {'Bundle + Upload' if do_bundle else 'Upload existing'}")
print(f"{'='*60}")

results = {}
for param in ['SWE', 'HS', 'ROF']:
    print(f"\n--- {param} ---")
    results[param] = upload_parameter(param, formatted_date, do_bundle, spatial_directory)

# Summary
print(f"\n{'='*60}")
print("Summary:")
for param, success in results.items():
    status = "OK" if success else "FAILED"
    print(f"  {param}: {status}")
print(f"{'='*60}")
