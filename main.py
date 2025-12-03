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

# Log passed arguments 
logger.info("Passed arguments: %s", raw_data)

# Hello wolrld application
logger.info("dsxos-app-test start")

start_time = datetime.now(timezone.utc)
testDP_read_val = query_utils.get_last_reading_value(raw_data["params"]["testDP_read_ID"])
logger.info(f'The last reading value for datapoint "{raw_data["params"]["testDP_read_ID"]}" at time {start_time.strftime("%H:%M:%S %d-%m-%Y")} is {testDP_read_val}')

current_second = start_time.second
testDP_readonly_val = current_second
testDP_readonly_payload = {
    "datapointId": query_utils.get_datapoint_ID(raw_data["params"]["testDP_read_only_ID"]),
    "value": testDP_readonly_val
}
logger.info(f"Response for Datapoint reading POST: {query_utils.post_datapoint_reading(testDP_readonly_payload)}")

logger.info("dsxos-app-test finished")