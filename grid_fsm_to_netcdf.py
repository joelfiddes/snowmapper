"""
FSM to NetCDF Converter for SnowMapper.

Converts merged FSM point outputs to gridded NetCDF files by mapping
cluster values back to the original DEM grid using landform membership.

Inputs:
    - outputs/FSM_pt_*.txt  (merged FSM point outputs)
    - outputs/landform.tif  (TopoSUB cluster classification)
    - config.yml  (DEM resolution and EPSG)

Outputs:
    - outputs/*_SWE.nc  (snow water equivalent, mm)
    - outputs/*_HS.nc  (snow height, m)
    - outputs/*_ROF.nc  (runoff, mm)
    - outputs/*_GST.nc  (ground surface temperature, K)

Usage:
    python grid_fsm_to_netcdf.py <domain_path>

Example:
    python grid_fsm_to_netcdf.py ./domains/D2000
"""
import os
import sys
import gc
from munch import DefaultMunch
from TopoPyScale import sim_fsm as sim
from logging_utils import setup_logger_with_tqdm, get_log_dir

# Module-level logger
logger = None

def load_config(config_file):
    """
    Load the configuration file using DefaultMunch.

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
        if logger:
            logger.error(f'Config file does not exist: {config_file} (cwd: {os.getcwd()})')
        sys.exit(1)

def process_variable(var_name, unit, epsg, dem_res):
    """
    Process a specific variable and write it to a NetCDF file.

    Parameters:
    - var_name (str): Name of the variable to process (e.g., "swe", "snd").
    - unit (str): Unit of the variable (e.g., "mm", "m").
    - epsg (int): EPSG code for coordinate reference system.
    - dem_res (float): DEM resolution from the configuration.
    """
    output_var_map = {"swe": "SWE", "snd": "HS", "rof": "ROF", "gst": "GST"}
    output_var_name = output_var_map.get(var_name, var_name.upper())

    # Load and process data
    df = sim.agg_by_var_fsm(var=var_name)
    grid_stack, lats, lons = sim.topo_map_sim_memsafe(df, 1, 'float32', dem_res)

    # Write to NetCDF
    sim.write_ncdf(".", grid_stack, var_name, unit, epsg, dem_res, df.index.array, lats, lons, "float32", True, output_var_name)

    # Explicit cleanup to reduce peak memory
    del df, grid_stack, lats, lons

def main(mydir):
    """
    Main function to process snow variables (SWE and SND) and save them as NetCDF files.

    Parameters:
    - mydir (str): Directory containing the project.
    """
    global logger
    os.chdir(mydir)

    # Set up logging
    log_dir = get_log_dir(mydir)
    logger = setup_logger_with_tqdm("make_netcdf", file=False)

    config_file = './config.yml'
    config = load_config(config_file)

    logger.info("Processing FSM outputs to NetCDF")

    # Process each variable sequentially with explicit memory cleanup
    # This reduces peak memory from ~4.3GB to ~1.5GB
    variables = [
        ("swe", "mm"),  # Snow water equivalent
        ("snd", "m"),   # Snow depth (height)
        ("rof", "mm"),  # Runoff
        ("gst", "k"),   # Ground surface temperature
    ]

    for var_name, unit in variables:
        process_variable(var_name, unit, config.dem.epsg, config.dem.dem_resol)
        gc.collect()  # Force garbage collection after each variable

    logger.info("NetCDF conversion complete")

if __name__ == "__main__":
    mydir = sys.argv[1]
    main(mydir)
