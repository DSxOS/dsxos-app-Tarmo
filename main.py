import query_utils
import argparse
import yaml
from datetime import datetime, timezone
import Util
from logger import setup_logger


# create parser
parser = argparse.ArgumentParser(description="Run dsxos-app-test with config file")
parser.add_argument("-c", "--config", required=False, help="Path to config YAML file", default="/app/config.yaml")
args = parser.parse_args() # Read arguments
with open(args.config, "r") as f: # Open and read config-file
    raw_data = yaml.safe_load(f)
    
# Extract API URL and Token
api_url = raw_data["params"]["apiEndpoint"]
api_token = raw_data["params"]["token"]
api_headers = {"Authorization": api_token}

# Initialize logger with central logging to Loki
logger = setup_logger(
    #app_name="dsxos-app-test",
    log_file="query.log",
    loki_url="http://localhost:3100/loki/api/v1/push",  # Loki address
    loki_tags={"app_name": "dsxos-app-test"},        # add more tags if needed
    level="INFO"
)

# Initialize query_utils with URL + headers    
query_utils.init(api_url, api_headers, logger)

# Hello wolrld application
logger.info("dsxos-app-test start")

start_time = datetime.now(timezone.utc)
logger.info(f'Hello world. The time is {start_time.strftime("%H:%M:%S %d-%m-%Y")}')

logger.info("dsxos-app-test finished")