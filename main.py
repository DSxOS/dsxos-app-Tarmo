import query_utils
import argparse
import yaml
from datetime import datetime, timezone, timedelta
import pytz
import Util
import ess_scheduling
from logger import setup_logger

APP_NAME = "dsxos-app-test"

# create parser
parser = argparse.ArgumentParser(description=f"Run {APP_NAME} with config file")
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
    log_file="query.log",
    loki_url="http://localhost:3100/loki/api/v1/push",  # Loki address
    loki_tags={"app_name": APP_NAME},        # add more tags if needed
    level="INFO"
)

# Initialize query_utils with URL + headers    
query_utils.init(api_url, api_headers, logger)

# Log passed arguments 
logger.info(f"{APP_NAME} run with arguments: %s", raw_data)

'''
start_time = datetime.now(timezone.utc)
testDP_read_val = query_utils.get_last_reading_value(raw_data["params"]["testDP_read_ID"])
logger.info(f'The last reading value for datapoint "{raw_data["params"]["testDP_read_ID"]}" at time {start_time.strftime("%H:%M:%S %d-%m-%Y")} is {testDP_read_val}')

current_second = start_time.second
testDP_readonly_val = current_second
testDP_readonly_payload = {
    "datapointId": query_utils.get_datapoint_ID(raw_data["params"]["testDP_read_only_ID"]),
    "value": testDP_readonly_val
}
'''
# logger.info(f"Response for Datapoint reading POST: {query_utils.post_datapoint_reading(testDP_readonly_payload)}")

try:
    schedule = ess_scheduling.generate_schedule(
                        lastProductionPrognosis = query_utils.get_last_prognosis_readings(raw_data["params"]['production_p_lt_DP_ID']), 
                        lastConsumptionPrognosis = query_utils.get_last_prognosis_readings(raw_data["params"]['consumption_p_lt_DP_ID']), 
                        lastNpSpotPricePrognosis = query_utils.get_last_prognosis_readings(raw_data["params"]['elering_nps_price_DP_ID']), 
                        npSpotCurrentPrice = query_utils.get_last_reading_value(raw_data["params"]['elering_nps_price_DP_ID']), 
                        lastEss_e_lt = query_utils.get_last_prognosis_readings(raw_data["params"]['ess_e_lt_DP_ID']), 
                        ess_p = query_utils.get_last_reading_value(raw_data["params"]['ess_p_DP_ID']) ,
                        ess_charge = query_utils.get_last_reading_value(raw_data["params"]['ess_charge_DP_ID']),
                        ess_charge_end = query_utils.get_last_reading_value(raw_data["params"]['ess_charge_end_DP_ID']),
                        ess_soc = query_utils.get_last_reading_value(raw_data["params"]['ess_avg_SOC_DP_ID']),
                        ess_max_p = query_utils.get_last_reading_value(raw_data["params"]['ess_max_p_DP_ID']),
                        ess_max_e = query_utils.get_last_reading_value(raw_data["params"]['ess_max_e_DP_ID']),
                        ess_soc_min = raw_data["params"]['ess_soc_min'], 
                        ess_soc_max = raw_data["params"]['ess_soc_max'],
                        ess_safe_min = query_utils.get_last_reading_value(raw_data["params"]['ess_min_batt_safe_lim_DP_ID'])*100,
                        pccImportLimitW = raw_data["params"]['pccImportLimitW'], #100000,
                        pccExportLimitW = raw_data["params"]['pccExportLimitW'], #-100000,
                        startTime = datetime.now(),
                        endTime = datetime.now() + timedelta(seconds=86400), # +24h
                        interval = raw_data["params"]['interval'], #900, #15min
                        DAY_TARIFF = raw_data["params"]['DAY_TARIFF'], #0.07,
                        NIGHT_TARIFF = raw_data["params"]['NIGHT_TARIFF'], #0.05,
                        ESS_DEG_COST = raw_data["params"]['ESS_DEG_COST'], #0.139,
                        local_timezone = pytz.timezone(raw_data["params"]['timezone']),
                        logger = logger)
    
    logger.info(f"Generated ESS schedule: {schedule}")

except Exception as e:
    logger.error(f"Error generating ESS schedule: {e}")

logger.info("dsxos-app-test finished")
