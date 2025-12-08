import query_utils
import argparse
import yaml
from datetime import datetime, timezone, timedelta
import pytz
import Util
import ess_scheduling
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
# logger.info(f"Response for Datapoint reading POST: {query_utils.post_datapoint_reading(testDP_readonly_payload)}")

schedule = ess_scheduling.generate_schedule(
                    query_utils.get_last_prognosis_readings('production_p_lt'), 
                    query_utils.get_last_prognosis_readings('consumption_p_lt'), 
                    query_utils.get_last_prognosis_readings('elering_nps_price'), 
                    query_utils.get_last_reading_value('elering_nps_price'), 
                    None, #query_utils.get_last_prognosis_readings('ess_e_lt'), 
                    query_utils.get_last_reading_value('ess_p') ,
                    query_utils.get_last_reading_value('ess_charge'),
                    query_utils.get_last_reading_value('ess_avg_SOC'),
                    query_utils.get_last_reading_value('ess_max_p'),
                    query_utils.get_last_reading_value('ess_max_e'),
                    ess_charge_end = 10*1000,
                    ess_soc_min = 0,
                    ess_soc_max = 0,
                    ess_safe_min = query_utils.get_last_reading_value('ess_min_batt_safe_lim')*100,
                    pccImportLimitW = 100000,
                    pccExportLimitW = -100000,
                    startTime = datetime.now(),
                    endTime = datetime.now() + timedelta(seconds=86400), # +24h
                    interval = 900, #15min
                    DAY_TARIFF = 0.07,
                    NIGHT_TARIFF = 0.05,
                    ESS_DEG_COST = 0.139,
                    local_timezone = pytz.timezone('Europe/Tallinn'),
                    logger = logger)

logger.info(f"Generated ESS schedule: {schedule}")



logger.info("dsxos-app-test finished")
