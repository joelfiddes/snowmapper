"""
Upload forecast bundle to S3.

Usage:
    python upload_to_AWS_offline_Forecast.py <spatial_dir> <start_date> [end_date] [--bundle] [--days N]

Arguments:
    spatial_dir Absolute path to spatial directory containing NC files
    start_date  Start date in YYYYMMDD format (e.g., 20260119)
    end_date    Optional end date - processes all dates in range (inclusive)
    --bundle    Create bundle from spatial/*.nc files before uploading
                Without this flag, uploads existing bundle files directly
    --days N    Number of days to include in each bundle (default: 10)

Examples:
    # Single date: bundle 10 forecast days and upload
    python upload_to_AWS_offline_Forecast.py /home/ubuntu/sim/snowmapper/spatial 20260119 --bundle

    # Date range: process multiple dates
    python upload_to_AWS_offline_Forecast.py /home/ubuntu/sim/snowmapper/spatial 20260110 20260119 --bundle

    # Bundle 5 days per date
    python upload_to_AWS_offline_Forecast.py /home/ubuntu/sim/snowmapper/spatial 20260119 --bundle --days 5

    # Upload existing bundle files (no bundling)
    python upload_to_AWS_offline_Forecast.py /home/ubuntu/sim/snowmapper/spatial 20260119
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
if len(sys.argv) < 3:
    print(__doc__)
    sys.exit(1)

# Separate positional args from flags (skip args that follow --days)
positional_args = []
skip_next = False
for i, arg in enumerate(sys.argv[1:], 1):
    if skip_next:
        skip_next = False
        continue
    if arg == '--days':
        skip_next = True
        continue
    if not arg.startswith('--'):
        positional_args.append(arg)

do_bundle = '--bundle' in sys.argv

# Parse --days argument (default: 10)
max_days = 10
if '--days' in sys.argv:
    try:
        days_idx = sys.argv.index('--days')
        max_days = int(sys.argv[days_idx + 1])
    except (IndexError, ValueError):
        print("ERROR: --days requires a number (e.g., --days 10)")
        sys.exit(1)

# Parse spatial_dir, start_date, and optional end_date
if len(positional_args) < 2:
    print("ERROR: Must provide spatial_dir and start_date")
    print(__doc__)
    sys.exit(1)

spatial_directory = positional_args[0]
if not spatial_directory.endswith('/'):
    spatial_directory += '/'

# Validate spatial directory exists
if not os.path.isdir(spatial_directory):
    print(f"ERROR: Spatial directory not found: {spatial_directory}")
    sys.exit(1)

start_date_str = positional_args[1]
end_date_str = positional_args[2] if len(positional_args) > 2 else start_date_str

# Validate date formats
try:
    start_dt = datetime.strptime(start_date_str, "%Y%m%d")
    end_dt = datetime.strptime(end_date_str, "%Y%m%d")
except ValueError as e:
    print(f"ERROR: Invalid date format. Use YYYYMMDD (e.g., 20260119)")
    sys.exit(1)

if end_dt < start_dt:
    print(f"ERROR: End date {end_date_str} is before start date {start_date_str}")
    sys.exit(1)

# Generate list of dates to process
dates_to_process = []
current_dt = start_dt
while current_dt <= end_dt:
    dates_to_process.append(current_dt.strftime("%Y%m%d"))
    current_dt += pd.Timedelta(days=1)

# AWS setup
session = boto3.Session()
credentials = session.get_credentials().get_frozen_credentials()
SNOW_MODEL = "joel-snow-model"
SNOW_MODEL_BUCKET = "snow-model-data-source"
aws_access_key_id = credentials.access_key
aws_secret_access_key = credentials.secret_key


def bundle_nc_files(directory, start_date, file_class, output_file, max_days=10):
    """
    Bundle NetCDF files from start_date onwards into a single file with time dimension.

    Args:
        directory: Path to spatial directory containing daily NC files
        start_date: Start date in YYYYMMDD format
        file_class: Variable name (SWE, HS, ROF)
        output_file: Output filename for bundled file
        max_days: Maximum number of days to include (default: 10)

    Returns:
        int: Number of files bundled
    """
    files_on_or_after = []
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = start_dt + pd.Timedelta(days=max_days - 1)  # inclusive

    # Collect files within the date range for the specified class
    for file in os.listdir(directory):
        if file.startswith(file_class + '_') and file.endswith('.nc'):
            file_date_str = file.split('_')[1].replace('.nc', '')
            try:
                file_date = datetime.strptime(file_date_str, "%Y%m%d")
                if start_dt <= file_date <= end_dt:
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


def upload_parameter(parameter, formatted_date, do_bundle, directory, max_days=10):
    """Upload a single parameter (SWE, HS, or ROF) to S3."""
    # Save temp bundle in spatial directory (not cwd)
    output_filename_nc = os.path.join(directory, f'{parameter}_{formatted_date}_bundle.nc')

    if do_bundle:
        # Bundle files from spatial directory
        n_files = bundle_nc_files(directory, formatted_date, parameter, output_filename_nc, max_days)
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
print(f"Forecast Upload")
print(f"Spatial dir: {spatial_directory}")
print(f"Dates: {start_date_str} to {end_date_str} ({len(dates_to_process)} dates)")
print(f"Mode: {'Bundle + Upload' if do_bundle else 'Upload existing'}")
if do_bundle:
    print(f"Days per bundle: {max_days}")
print(f"{'='*60}")

all_results = {}
for formatted_date in dates_to_process:
    print(f"\n{'='*60}")
    print(f"Processing: {formatted_date}")
    print(f"{'='*60}")

    results = {}
    for param in ['SWE', 'HS', 'ROF']:
        print(f"\n--- {param} ---")
        results[param] = upload_parameter(param, formatted_date, do_bundle, spatial_directory, max_days)
    all_results[formatted_date] = results

# Summary
print(f"\n{'='*60}")
print("SUMMARY")
print(f"{'='*60}")
for date, results in all_results.items():
    statuses = [f"{p}:{'OK' if s else 'FAIL'}" for p, s in results.items()]
    print(f"  {date}: {', '.join(statuses)}")
print(f"{'='*60}")
