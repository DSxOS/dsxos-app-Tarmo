import query_utils
import argparse
import yaml
from datetime import datetime, timezone, timedelta
import pytz
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
api_url = raw_data['params']['apiEndpoint']
api_token = raw_data['params']['token']
api_headers = {"Authorization": api_token}

# Initialize logger with central logging to Loki
logger = setup_logger(
    log_file="query.log",
    loki_url="http://localhost:3100/loki/api/v1/push",  # Loki address
    loki_tags={"app_name": APP_NAME},        # add more tags if needed
    #level=raw_data["logLevel"]
)

# Initialize query_utils with URL + headers    
query_utils.init(api_url, api_headers, logger)

# Log passed arguments 
logger.info(f"{APP_NAME} run with arguments: %s", raw_data)

# Generate ESS schedule
try:
    schedule = ess_scheduling.generate_schedule(
                        lastProductionPrognosis = query_utils.get_last_prognosis_readings(raw_data['params']['production_p_lt_DP_ID']), 
                        lastConsumptionPrognosis = query_utils.get_last_prognosis_readings(raw_data['params']['consumption_p_lt_DP_ID']), 
                        lastNpSpotPricePrognosis = query_utils.get_last_prognosis_readings(raw_data['params']['elering_nps_price_DP_ID']), 
                        npSpotCurrentPrice = query_utils.get_last_reading_value(raw_data['params']['elering_nps_price_DP_ID']), 
                        lastEss_e_lt = query_utils.get_last_prognosis_readings(raw_data['params']['ess_e_lt_DP_ID'], generate_if_missing=True), 
                        ess_p = query_utils.get_last_reading_value(raw_data['params']['ess_p_DP_ID']) ,
                        ess_charge = query_utils.get_last_reading_value(raw_data['params']['ess_charge_DP_ID']),
                        ess_charge_end = query_utils.get_last_reading_value(raw_data['params']['ess_charge_end_DP_ID']),
                        ess_soc = query_utils.get_last_reading_value(raw_data['params']['ess_avg_SOC_DP_ID']),
                        ess_max_p = query_utils.get_last_reading_value(raw_data['params']['ess_max_p_DP_ID']),
                        ess_max_e = query_utils.get_last_reading_value(raw_data['params']['ess_max_e_DP_ID']),
                        ess_soc_min = raw_data['params']['ess_soc_min'], 
                        ess_soc_max = raw_data['params']['ess_soc_max'],
                        ess_safe_min = query_utils.get_last_reading_value(raw_data['params']['ess_min_batt_safe_lim_DP_ID'])*100,
                        pccImportLimitW = raw_data['params']['pccImportLimitW'], #100000,
                        pccExportLimitW = raw_data['params']['pccExportLimitW'], #-100000,
                        startTime = datetime.now(),
                        endTime = datetime.now() + timedelta(seconds=86400), # +24h
                        interval = raw_data['params']['interval'], #900, #15min
                        DAY_TARIFF = raw_data['params']['DAY_TARIFF'], #0.07,
                        NIGHT_TARIFF = raw_data['params']['NIGHT_TARIFF'], #0.05,
                        ESS_DEG_COST = raw_data['params']['ESS_DEG_COST'], #0.139,
                        local_timezone = pytz.timezone(raw_data['params']['timezone']),
                        logger = logger)            

    if (len(schedule) == 0):
        # Handle empty schedule case
        logger.warning(f'Optimization failed - empty result. Prognosis not updated.')
    else:
        logger.info("ESS Schedule: "+", ".join(f"{dt} = {ess:.4g}" for dt, ess in zip(schedule["datetime"], schedule["ESS"])))

    # Prepare prognosis payload data based on generated schedule
    essPowerPlan =[]
    for dt, value in schedule: 
        utc_dt = datetime.fromisoformat(dt).astimezone(timezone.utc) 
        essPowerPlan.append({
            "time": utc_dt.isoformat().replace('+00:00', 'Z'),
            "value": value*1000
        })

    # Construct the prognosis payload with datapoint ID, timestamp, and payload data
    prognosis_payload = {
        "datapointId": query_utils.get_datapoint_ID(raw_data['params']['ess_e_lt_DP_ID']),
        "time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "readings":essPowerPlan
    }

    # POST datapoint prognosis
    response = query_utils.post_datapoint_prognosis(prognosis_payload)
    logger.info(f"Posted prognosis for datapoint {raw_data['params']['ess_e_lt_DP_ID']}; Response: {response}")
    
except Exception as e:
    logger.error(f'Error generating ESS schedule: {e}')

logger.info("dsxos-app-test finished")
