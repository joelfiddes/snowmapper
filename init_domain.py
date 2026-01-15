"""
Domain Initialization for SnowMapper.

Initializes a TopoSUB domain by computing DEM parameters, extracting
topographic parameters, and writing landform classification files.
This is a one-time setup step for each domain.

Inputs:
    - Domain directory with config.yml
    - DEM file specified in config.yml

Outputs:
    - outputs/dem_description.nc  (processed DEM parameters)
    - outputs/landform.tif  (TopoSUB cluster classification)
    - outputs/df_centroids.csv  (cluster centroid coordinates)

Usage:
    python init_domain.py <domain_path>

Example:
    python init_domain.py ./domains/D2000
"""
from TopoPyScale import topoclass as tc
import os
import sys
mydir = sys.argv[1]
os.chdir(mydir)
config_file = './config.yml'
mp  = tc.Topoclass(config_file)
mp.compute_dem_param()
mp.extract_topo_param() # this loads existing datasets
mp.toposub.write_landform()
