import xarray as xr
import rioxarray  # Import the rioxarray module for raster I/O operations
from datetime import datetime
import pyproj
import os
import rasterio
from rasterio.merge import merge
from rasterio.enums import Resampling
from rasterio.transform import rowcol
import numpy as np
import sys
import glob
from tqdm import tqdm
from logging_utils import setup_logger_with_tqdm, get_log_dir


def write_mosaic_to_netcdf(mosaic, transform, crs, output_filename, var_name, long_name, units, resolution_m):
    """Write mosaic array to NetCDF using xarray."""
    height, width = mosaic.shape[1], mosaic.shape[2]

    # Generate coordinates from transform
    cols = np.arange(width)
    rows = np.arange(height)
    xs = transform[2] + cols * transform[0]
    ys = transform[5] + rows * transform[4]

    # Create xarray DataArray
    da = xr.DataArray(
        mosaic[0],  # Take first band
        dims=['y', 'x'],
        coords={'y': ys, 'x': xs},
        name=var_name,
        attrs={
            'long_name': long_name,
            'units': units,
            'resolution_m': resolution_m,
            'crs': str(crs)
        }
    )

    # Set nodata
    da = da.where(da != -9999)

    # Write to NetCDF
    da.to_netcdf(output_filename, encoding={var_name: {'dtype': 'float32', 'zlib': True, 'complevel': 5}})

mydir = sys.argv[1]
mydomain = sys.argv[2]

# Set up logging
log_dir = get_log_dir(f"{mydir}/{mydomain}")
logger = setup_logger_with_tqdm("merge_reproj", file=False)

#year= sys.argv[1]
startTime = datetime.now()
thismonth = startTime.month    # now
thisyear = startTime.year 
year = [thisyear if thismonth in {9, 10, 11, 12} else thisyear-1][0]
fileyear = year+1

logger.info(f"Water year: {fileyear}")

# Define the target projection as longitude and latitude

# Define the target projection as longitude and latitude
target_projection = 'EPSG:4326'  # EPSG code for WGS 84 coordinate system (longitude and latitude)



#========== SWE computation ====================================================

# Directory containing merged reprojected files
spatial_directory = mydir + "/spatial/"
os.makedirs(spatial_directory, exist_ok=True) 
# Loop over years
 # Adjust the range according to your desired years
# Load the NetCDF files for each year

# file1 = f"./myproject/D1/outputs/*_SWE.nc"
# file2 = f"./myproject/D2/outputs/*_SWE.nc"
# file3 = f"./myproject/D3/outputs/*_SWE.nc"
variable_name ="SWE"
filep1 =    f"{mydir}/{mydomain}/outputs/*_{variable_name}.nc"
file1 = glob.glob(filep1)


# Open the datasets
ds1 = xr.open_dataset(file1[0])


# Loop through each timestep (change this loop to re run entire last three months
for time_idx, time_value in enumerate(tqdm(ds1.Time.values, desc="Processing SWE")):

    formatted_date = np.datetime_as_string(time_value, unit='D')
    # Now format as YYYYMMDD
    formatted_date = formatted_date.replace('-', '')

    # Construct output filename
    output_filename_nc = spatial_directory+f'SWE_{formatted_date}.nc'
    output_filename_tif = spatial_directory+f'swe_merged_reprojected_{year}_{time_idx}.tif'

    if os.path.exists(output_filename_tif):
        logger.debug(f"File {output_filename_tif} already exists. Skipping.")
        continue

    # Select data for the current timestep
    ds1_slice = ds1.isel(Time=time_idx)


    # Set spatial dimensions
    ds1_slice = ds1_slice.rename({'easting': 'x','northing': 'y'})
    ds1_slice = ds1_slice.rio.write_crs(pyproj.CRS.from_epsg(32642).to_wkt())

    # Reproject ds1 to latitude and longitude
    ds1_latlon = ds1_slice.rio.reproject(target_projection)

    # Define output filenames
    output_filename_ds1 = f'ds1_reprojected_{year}_{time_idx}.tif'

    # Write reprojected datasets to GeoTIFF files
    ds1_latlon.rio.to_raster(output_filename_ds1)

    # List of filenames of GeoTIFF files to merge
    file_list = [output_filename_ds1]  # Add more filenames as needed

    # Open each GeoTIFF file
    src_files_to_mosaic = [rasterio.open(file) for file in file_list]

    # Merge the raster datasets
    mosaic, out_trans = merge(src_files_to_mosaic, resampling=Resampling.cubic)

    if os.path.exists(output_filename_tif):
        logger.debug(f"File {output_filename_tif} already exists. Skipping.")
        continue

    # Write the merged raster to a new GeoTIFF file
    with rasterio.open(output_filename_tif, 'w', driver='GTiff',
                               width=mosaic.shape[2], height=mosaic.shape[1],
                               count=mosaic.shape[0], dtype=mosaic.dtype,
                               crs=src_files_to_mosaic[0].crs, transform=out_trans) as dest:
        dest.write(mosaic)


    # Write NetCDF using xarray
    write_mosaic_to_netcdf(mosaic, out_trans, src_files_to_mosaic[0].crs,
                           output_filename_nc, 'swe', 'snow_water_equivalent', 'mm', '500')


    # Delete input raster files
    for file in file_list:
        os.remove(file)


#========== HS computation ====================================================


# Loop over years
 # Adjust the range according to your desired years
# Load the NetCDF files for each year

variable_name ="HS"
filep1 =    f"{mydir}/{mydomain}/outputs/*_{variable_name}.nc"
file1 = glob.glob(filep1)


# Open the datasets
ds1 = xr.open_dataset(file1[0])

# Loop through each timestep
for time_idx, time_value in enumerate(tqdm(ds1.Time.values, desc="Processing HS")):

    formatted_date = np.datetime_as_string(time_value, unit='D')
    # Now format as YYYYMMDD
    formatted_date = formatted_date.replace('-', '')

    # Construct output filename
    output_filename_nc = spatial_directory+f'HS_{formatted_date}.nc'
    output_filename_tif = spatial_directory+f'hs_merged_reprojected_{year}_{time_idx}.tif'

    if os.path.exists(output_filename_tif):
        logger.debug(f"File {output_filename_tif} already exists. Skipping.")
        continue

    # Select data for the current timestep
    ds1_slice = ds1.isel(Time=time_idx)


    # Set spatial dimensions
    ds1_slice = ds1_slice.rename({'easting': 'x','northing': 'y'})
    ds1_slice = ds1_slice.rio.write_crs(pyproj.CRS.from_epsg(32642).to_wkt())


    # Reproject ds1 to latitude and longitude
    ds1_latlon = ds1_slice.rio.reproject(target_projection)

    # Define output filenames
    output_filename_ds1 = f'ds1_reprojected_{year}_{time_idx}.tif'

    # Write reprojected datasets to GeoTIFF files
    ds1_latlon.rio.to_raster(output_filename_ds1)

    # List of filenames of GeoTIFF files to merge
    file_list = [output_filename_ds1]  # Add more filenames as needed

    # Open each GeoTIFF file
    src_files_to_mosaic = [rasterio.open(file) for file in file_list]

    # Merge the raster datasets
    mosaic, out_trans = merge(src_files_to_mosaic, resampling=Resampling.cubic)

    if os.path.exists(output_filename_tif):
        logger.debug(f"File {output_filename_tif} already exists. Skipping.")
        continue

    # Write the merged raster to a new GeoTIFF file
    with rasterio.open(output_filename_tif, 'w', driver='GTiff',
                       width=mosaic.shape[2], height=mosaic.shape[1],
                       count=mosaic.shape[0], dtype=mosaic.dtype,
                       crs=src_files_to_mosaic[0].crs, transform=out_trans) as dest:
        dest.write(mosaic)

    # Write NetCDF using xarray
    write_mosaic_to_netcdf(mosaic, out_trans, src_files_to_mosaic[0].crs,
                           output_filename_nc, 'hs', 'snow_height', 'm', '500')

    # Delete input raster files
    for file in file_list:
        os.remove(file)







#========== ROF computation ====================================================


# Loop over years
 # Adjust the range according to your desired years
# Load the NetCDF files for each year

variable_name ="ROF"
filep1 =    f"{mydir}/{mydomain}/outputs/*_{variable_name}.nc"
file1 = glob.glob(filep1)


# Open the datasets
ds1 = xr.open_dataset(file1[0])

# Loop through each timestep
for time_idx, time_value in enumerate(tqdm(ds1.Time.values, desc="Processing ROF")):

    formatted_date = np.datetime_as_string(time_value, unit='D')
    # Now format as YYYYMMDD
    formatted_date = formatted_date.replace('-', '')

    # Construct output filename
    output_filename_nc = spatial_directory+f'ROF_{formatted_date}.nc'
    output_filename_tif = spatial_directory+f'ROF_merged_reprojected_{year}_{time_idx}.tif'

    if os.path.exists(output_filename_tif):
        logger.debug(f"File {output_filename_tif} already exists. Skipping.")
        continue

    # Select data for the current timestep
    ds1_slice = ds1.isel(Time=time_idx)


    # Set spatial dimensions
    ds1_slice = ds1_slice.rename({'easting': 'x','northing': 'y'})
    ds1_slice = ds1_slice.rio.write_crs(pyproj.CRS.from_epsg(32642).to_wkt())


    # Reproject ds1 to latitude and longitude
    ds1_latlon = ds1_slice.rio.reproject(target_projection)

    # Define output filenames
    output_filename_ds1 = f'ds1_reprojected_{year}_{time_idx}.tif'

    # Write reprojected datasets to GeoTIFF files
    ds1_latlon.rio.to_raster(output_filename_ds1)

    # List of filenames of GeoTIFF files to merge
    file_list = [output_filename_ds1]  # Add more filenames as needed

    # Open each GeoTIFF file
    src_files_to_mosaic = [rasterio.open(file) for file in file_list]

    # Merge the raster datasets
    mosaic, out_trans = merge(src_files_to_mosaic, resampling=Resampling.cubic)

    if os.path.exists(output_filename_tif):
        logger.debug(f"File {output_filename_tif} already exists. Skipping.")
        continue

    # Write the merged raster to a new GeoTIFF file
    with rasterio.open(output_filename_tif, 'w', driver='GTiff',
                       width=mosaic.shape[2], height=mosaic.shape[1],
                       count=mosaic.shape[0], dtype=mosaic.dtype,
                       crs=src_files_to_mosaic[0].crs, transform=out_trans) as dest:
        dest.write(mosaic)

    # Write NetCDF using xarray
    write_mosaic_to_netcdf(mosaic, out_trans, src_files_to_mosaic[0].crs,
                           output_filename_nc, 'rof', 'snow_runoff', 'mm', '500')

    # Delete input raster files
    for file in file_list:
        os.remove(file)


