import yaml
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file"""
    try:
        base_path = Path(__file__).parent.parent
        full_path = base_path / config_path
        
        with open(full_path) as f:
            config = yaml.safe_load(f)
            
        logger.info("Configuration loaded successfully")
        return config
        
    except Exception as e:
        logger.error(f"Failed to load config: {str(e)}")
        raise