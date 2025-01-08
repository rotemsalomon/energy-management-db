import logging
import logging.config
import yaml # type: ignore
import os

# Load logging configuration from YAML
def setup_logging(config_path='logging_config.yaml', default_level=logging.INFO):
    if os.path.exists(config_path):
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
        logging.warning(f"Logging config file not found at {config_path}. Using basic config.")

# Fetch a logger by name
def get_logger(name):
    return logging.getLogger(name)
