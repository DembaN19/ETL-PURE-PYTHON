import logging
import os
import sys
from pathlib import Path
from datetime import datetime
from pyhocon import ConfigFactory

# Constants
config_file = 'src/config.conf'
log_dir = 'logs'

# Create "logs" directory if it doesn't exist
os.makedirs(log_dir, exist_ok=True)

# Set up project name, timestamp, and log filename
project_name = Path.cwd().name
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f'{log_dir}/log_{current_time}_{project_name}.log'

# Configure logging
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Logger instance
logger = logging.getLogger(__name__)

# Check if the config file exists and is accessible
if not os.path.isfile(config_file):
    logger.error(f'Config file not found: {config_file}')
    sys.exit()

if not os.access(config_file, os.R_OK):
    logger.error(f'Permission denied to read config file: {config_file}')
    sys.exit()

# Parse the config file
config = ConfigFactory.parse_file(config_file)
