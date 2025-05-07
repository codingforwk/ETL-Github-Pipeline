# utils/config_loader.py

import os
import yaml

def load_config(config_path=None):
    """
    Loads the YAML config file from the given path or from the default location.

    Args:
        config_path (str, optional): Path to the config.yaml file.

    Returns:
        dict: Configuration as a Python dictionary.

    Raises:
        FileNotFoundError: If the config file is not found.
        yaml.YAMLError: If the YAML is invalid.
    """
    if config_path is None:
        # Default path: <project_root>/config/config.yaml
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'config.yaml')
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_env_or_config(env_var, config_dict, config_key_path, default=None):
    """
    Returns the value from environment variable if set, otherwise from the config dictionary.

    Args:
        env_var (str): Name of the environment variable.
        config_dict (dict): Loaded config dictionary.
        config_key_path (str): Dot-separated path to the config key (e.g., "azure.file_system").
        default: Value to return if neither is set.

    Returns:
        Value from env or config, or default.
    """
    value = os.getenv(env_var)
    if value is not None:
        return value
    # Traverse nested keys
    keys = config_key_path.split('.')
    d = config_dict
    try:
        for k in keys:
            d = d[k]
        return d
    except (KeyError, TypeError):
        return default
