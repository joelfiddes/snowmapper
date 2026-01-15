"""
Configuration loader for SnowMapper.

This module loads the central snowmapper.yml configuration file and resolves
all relative paths to absolute paths based on the simulation directory.

Usage:
    from config import load_config
    cfg = load_config()  # Uses current directory
    cfg = load_config('/path/to/sim_dir')

    # Access config values
    climate_dir = cfg['paths']['climate_dir']
    domains = cfg['domains']

Config file: snowmapper.yml in the simulation directory root.
"""
import os
import yaml
from pathlib import Path


def load_config(sim_dir=None):
    """
    Load snowmapper.yml and resolve relative paths.

    Args:
        sim_dir: Path to simulation directory containing snowmapper.yml.
                 Defaults to current working directory.

    Returns:
        dict: Configuration with resolved absolute paths.

    Raises:
        FileNotFoundError: If snowmapper.yml not found in sim_dir.
    """
    if sim_dir is None:
        sim_dir = os.getcwd()

    sim_path = Path(sim_dir).resolve()
    config_file = sim_path / "snowmapper.yml"

    with open(config_file) as f:
        cfg = yaml.safe_load(f)

    # Helper to resolve relative paths
    def resolve(p):
        path = Path(p)
        return str(path if path.is_absolute() else (sim_path / path).resolve())

    # Store sim_dir in config
    cfg['sim_dir'] = str(sim_path)

    # Resolve all path entries
    if 'paths' in cfg:
        for key in ['climate_dir', 'basins_dir', 'spatial_dir', 'tables_dir', 'logs_dir']:
            if key in cfg['paths']:
                cfg['paths'][key] = resolve(cfg['paths'][key])

    # Resolve domain paths
    for domain in cfg.get('domains', []):
        domain['path'] = resolve(domain['path'])
        domain['config_file'] = str(Path(domain['path']) / 'config.yml')

    return cfg


def get_enabled_domains(cfg):
    """Return list of enabled domain dicts."""
    return [d for d in cfg.get('domains', []) if d.get('enabled', True)]


def load_config_or_none(sim_dir=None):
    """
    Try to load config, return None if snowmapper.yml not found.
    Useful for backward compatibility during transition.
    """
    try:
        return load_config(sim_dir)
    except FileNotFoundError:
        return None
