import logging
import logging.config
from logging.handlers import RotatingFileHandler
import yaml #type: ignore
import os

# Load logging configuration from YAML or set up a default configuration
def setup_logging(
    config_path='logging_config.yaml',
    log_filename='/var/log/energyapp/daily_usage.log',
    max_bytes=5*1024*1024,
    backup_count=5,
    default_level=logging.INFO
):
    if os.path.exists(config_path):
        # Use YAML-based logging configuration
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file.read())
        logging.config.dictConfig(config)
    else:
        # Fallback to programmatic configuration with RotatingFileHandler
        logging.basicConfig(level=default_level)
        logger = logging.getLogger()
        logger.warning(f"Logging config file not found at {config_path}. Using fallback configuration.")

        # Set up a rotating file handler
        rotating_file_handler = RotatingFileHandler(
            log_filename, maxBytes=max_bytes, backupCount=backup_count
        )
        rotating_file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        rotating_file_handler.setLevel(logging.DEBUG)
        logger.addHandler(rotating_file_handler)

        # Add a console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)

# Fetch a logger by name
def get_logger(name):
    return logging.getLogger(name)
