import logging
from Query import Query
import Util

_query_url = None
_query_headers = None
_logger = logging.getLogger(__name__)

def init(url, headers, logger=None):
    global _query_url, _query_headers, _logger
    _query_url = url
    _query_headers = headers
    if logger is not None:
        _logger = logger
    _logger.debug(f"query_utils initialized with URL: {_query_url}")
    
# Helper to create Query object
def Q():
    return Query(_query_url, headers=_query_headers, logger=_logger)

def add_header(key, value):
    global _query_headers
    if _query_headers == None:
        _query_headers = {}
    _query_headers[key] = value

###########################################################
# Authentication
###########################################################

# Get access token using client credentials flow
def get_token(client_id, api_token):
    token_data = Q()._request("POST", "/auth/token", data={ "grant_type":"client_credentials", "client_id":client_id, "client_secret":api_token })
    return token_data["access_token"]

###########################################################
# GET
###########################################################

# GET datapoint data
def get_datapoint(dp_identifier):
    dp_data = (
        Q()
        .filter(identifier__equals=dp_identifier)
        .paginate(page=0, size=1)
        .get("/datapoints")
    )
    _logger.debug("get_datapoint(%s) -> %s", dp_identifier, dp_data)
    return dp_data

# GET datapoint ID
def get_datapoint_ID(dp_identifier):
    dp_data = (
        Q()
        .filter(identifier__equals=dp_identifier)
        .paginate(page=0, size=1)
        .get("/datapoints")
    )
    return dp_data[0]["id"]

# Get datapoint last reading by identifier
def get_last_reading(dp_identifier):
    dp_data = get_datapoint(dp_identifier)
    dp_id = dp_data[0]["id"]
    last_reading = (
        Q()
        .filter(datapointId__equals=dp_id)
        .order_by("time", "desc")
        .paginate(page=0, size=1)
        .get("/readings")
    )
    return last_reading

# Get datapoint last reading value by identifier
def get_last_reading_value(dp_identifier):
    last_reading_value = get_last_reading(dp_identifier)
    return last_reading_value[0].get("value")

###########################################################
# GET readings with retrieval modes
###########################################################

# General-purpose readings fetch with full retrieval mode support.
#
# retrieval_mode    | interval_seconds | extra param      | Description
# ------------------+------------------+------------------+------------------------------
# None / "FULL"     | not used         |                  | All stored readings, no processing (default)
# "DELTA"           | not used         |                  | Only readings where the value changed;
#                   |                  |                  |   removes consecutive duplicates
# "CYCLIC"          | required         |                  | One value per interval boundary;
#                   |                  |                  |   carries forward the last known value
# "INTERPOLATED"    | required         |                  | One value per interval boundary;
#                   |                  |                  |   linearly interpolates between readings
# "BEST_FIT"        | required         |                  | Min + max reading per bucket with original
#                   |                  |                  |   timestamps (preserves trend shape)
# "AVERAGE"         | required         |                  | Arithmetic mean of sample values per bucket
# "MINIMUM"         | required         |                  | Minimum stored value per bucket
# "MAXIMUM"         | required         |                  | Maximum stored value per bucket
# "INTEGRAL"        | required         |                  | Area under value-vs-time curve (unit×seconds);
#                   |                  |                  |   divide by 3600 to convert W×s → Wh
# "SLOPE"           | required         |                  | Rate of change per bucket (Δvalue/Δsecond);
#                   |                  |                  |   multiply by 3600 for Δvalue/hour
# "COUNTER"         | required         | rollover_value   | Net cumulative delta per bucket; adds
#                   |                  |                  |   rollover_value when a step is negative
#                   |                  |                  |   (use for meters/pulse counters that wrap)
# "VALUE_STATE"     | required         |                  | Time-weighted average per bucket using
#                   |                  |                  |   step carry-forward interpolation;
#                   |                  |                  |   unlike AVERAGE, weights by time at value
# "ROUND_TRIP"      | not used         |                  | Seconds between consecutive rising edges
#                   |                  |                  |   (low→high, threshold=0)
# "EDGE_DETECTION"  | not used         | edge_type        | +1 at rising edges, -1 at falling edges
#                   |                  | LEADING/TRAILING |   (threshold=0); filter by edge_type
#                   |                  | /BOTH (default)  |
# "PREDICTIVE"      | required         |                  | Simple Linear Regression fitted on stored
#                   |                  |                  |   readings, evaluated at interval boundaries
# "START_BOUND"     | required         |                  | Last stored value at or before each boundary
#                   |                  |                  |   (look-back snapshot)
# "END_BOUND"       | required         |                  | First stored value after each boundary
#                   |                  |                  |   (look-ahead snapshot)
#
# Time params: ISO-8601 strings, e.g. "2026-05-20T00:00:00Z"
# Returns a list of {"id", "time", "value", "datapointId"} dicts, or [] on error.
def get_readings(dp_identifier, from_time=None, to_time=None,
                 retrieval_mode=None, interval_seconds=None,
                 rollover_value=None, edge_type=None,
                 page=0, size=10000):
    dp_id = get_datapoint_ID(dp_identifier)
    q = Q().filter(datapointId__equals=dp_id)
    if from_time is not None:
        q = q.filter(time__greaterThanOrEqual=from_time)
    if to_time is not None:
        q = q.filter(time__lessThan=to_time)
    if retrieval_mode is not None:
        q = q.filter(retrievalMode=retrieval_mode)
    if interval_seconds is not None:
        q = q.filter(intervalSeconds=interval_seconds)
    if rollover_value is not None:
        q = q.filter(rolloverValue=rollover_value)
    if edge_type is not None:
        q = q.filter(edgeType=edge_type)
    return q.paginate(page=page, size=size).get("/readings") or []

# Raw readings for a time range — no preprocessing.
# Use for: data export, auditing, feeding into custom analysis.
def get_readings_full(dp_identifier, from_time, to_time, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="FULL", size=size)

# Value-change events only — consecutive duplicate values are suppressed.
# Use for: state-change sensors, discrete signals, noise reduction.
def get_readings_delta(dp_identifier, from_time, to_time, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="DELTA", size=size)

# Step-interpolated resampling at fixed interval boundaries.
# Each point carries the last known value forward (no smoothing).
# Use for: chart rendering at a fixed resolution, dashboard time-series.
# interval_seconds: e.g. 900 = 15 min, 3600 = 1 hour
def get_readings_cyclic(dp_identifier, from_time, to_time, interval_seconds, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="CYCLIC",
                        interval_seconds=interval_seconds, size=size)

# Linearly interpolated resampling at fixed interval boundaries.
# Use for: smooth trend lines, when gradual change between readings is assumed.
# interval_seconds: e.g. 900 = 15 min, 3600 = 1 hour
def get_readings_interpolated(dp_identifier, from_time, to_time, interval_seconds, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="INTERPOLATED",
                        interval_seconds=interval_seconds, size=size)

# Min+max reading per bucket with original timestamps — preserves trend shape.
# Use for: downsampling dense series for display while keeping visual peaks/troughs.
# interval_seconds: bucket size, e.g. 3600 = hourly min+max pairs
def get_readings_best_fit(dp_identifier, from_time, to_time, interval_seconds, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="BEST_FIT",
                        interval_seconds=interval_seconds, size=size)

# Time-weighted arithmetic mean per bucket.
# Use for: statistical reporting, hourly/daily averages.
# interval_seconds: bucket size, e.g. 3600 = hourly averages
def get_readings_average(dp_identifier, from_time, to_time, interval_seconds, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="AVERAGE",
                        interval_seconds=interval_seconds, size=size)

# Minimum value per bucket.
# Use for: trough detection, minimum load/production per period.
def get_readings_minimum(dp_identifier, from_time, to_time, interval_seconds, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="MINIMUM",
                        interval_seconds=interval_seconds, size=size)

# Maximum value per bucket.
# Use for: peak detection, maximum load/production per period.
def get_readings_maximum(dp_identifier, from_time, to_time, interval_seconds, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="MAXIMUM",
                        interval_seconds=interval_seconds, size=size)

# Trapezoidal integral (area under the value-vs-time curve) per bucket.
# Result unit = original_unit × seconds. Divide by 3600 to get Wh from W readings.
# Use for: energy accounting, cumulative consumption/production over a period.
# interval_seconds: integration window, e.g. 3600 = 1 hour → result in W×s → /3600 = Wh
def get_readings_integral(dp_identifier, from_time, to_time, interval_seconds, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="INTEGRAL",
                        interval_seconds=interval_seconds, size=size)

# Rate of change per bucket: Δvalue / Δtime (in seconds).
# Multiply result by 3600 to express as Δvalue/hour.
# Use for: ramp detection, rate-of-change alarms, derivative analysis.
# interval_seconds: bucket size for the slope calculation
def get_readings_slope(dp_identifier, from_time, to_time, interval_seconds, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="SLOPE",
                        interval_seconds=interval_seconds, size=size)

# Net cumulative counter delta per bucket with rollover correction.
# rollover_value: counter maximum (e.g. 65536 for a 16-bit counter); None = no rollover handling.
# Use for: utility meters, pulse counters, any monotonically increasing counter that wraps.
def get_readings_counter(dp_identifier, from_time, to_time, interval_seconds, rollover_value=None, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="COUNTER",
                        interval_seconds=interval_seconds,
                        rollover_value=rollover_value, size=size)

# Time-weighted average per bucket using step (carry-forward) interpolation.
# Unlike AVERAGE (arithmetic mean of samples), VALUE_STATE weights by time spent at each level.
# Use for: discrete state signals, relay states, mode indicators.
def get_readings_value_state(dp_identifier, from_time, to_time, interval_seconds, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="VALUE_STATE",
                        interval_seconds=interval_seconds, size=size)

# Time in seconds between consecutive rising edges (low→high, threshold=0).
# Use for: cycle time analysis, heartbeat monitoring, periodic event duration.
# No interval_seconds required.
def get_readings_round_trip(dp_identifier, from_time, to_time, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="ROUND_TRIP", size=size)

# State transition detection: +1 for rising edges, -1 for falling edges (threshold=0).
# edge_type: "LEADING" (rising only), "TRAILING" (falling only), or None/"BOTH" (default).
# Use for: event counting, alarm transitions, binary signal analysis.
# No interval_seconds required.
def get_readings_edge_detection(dp_identifier, from_time, to_time, edge_type=None, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="EDGE_DETECTION",
                        edge_type=edge_type, size=size)

# Simple Linear Regression fitted on stored readings, evaluated at interval boundaries.
# Use for: trend extrapolation, gap-filling, predictive analysis.
def get_readings_predictive(dp_identifier, from_time, to_time, interval_seconds, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="PREDICTIVE",
                        interval_seconds=interval_seconds, size=size)

# For each interval boundary: value of the last stored reading at or before it.
# Use for: snapshot queries, audit trails, "what was the value at time T?" queries.
def get_readings_start_bound(dp_identifier, from_time, to_time, interval_seconds, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="START_BOUND",
                        interval_seconds=interval_seconds, size=size)

# For each interval boundary: value of the first stored reading strictly after it.
# Use for: forward-fill gaps, look-ahead queries, next-known-value retrieval.
def get_readings_end_bound(dp_identifier, from_time, to_time, interval_seconds, size=10000):
    return get_readings(dp_identifier, from_time, to_time,
                        retrieval_mode="END_BOUND",
                        interval_seconds=interval_seconds, size=size)

# Get datapoint last control command by identifier
def get_last_control(dp_identifier):
    dp_data = get_datapoint(dp_identifier)
    dp_id = dp_data[0]["id"]
    last_control_val = (
        Q()
        .filter(datapointId__equals=dp_id)
        .order_by("time", "desc")
        .paginate(page=0, size=1)
        .get("/control-values")
    )
    return last_control_val

# Get datapoint last control command value by identifier
def get_last_control_value(dp_identifier):
    last_control_value = get_last_control(dp_identifier)
    return last_control_value[0].get("value")

# Get datapoint last control command status by identifier
def get_last_control_status(dp_identifier):
    last_reading_value = get_last_control(dp_identifier)
    return last_reading_value[0].get("sent")

# Get datapoint last control command value and status by identifier
def get_last_control_value_and_status(dp_identifier):
    last_reading_value = get_last_control(dp_identifier)
    return {
        "value" : last_reading_value[0].get("value"), 
        "sent" : last_reading_value[0].get("sent")}

# GET datapoint last prognosis readings data
def get_last_prognosis_readings(dp_identifier, generate_if_missing=False):
    last_prognosis_id = get_datapoint(dp_identifier)[0].get("lastPrognosisId")
    if last_prognosis_id is not None:
        last_prognosis_readings = (
            Q()
            .filter(datapointPrognosisId__equals=last_prognosis_id)
            .get("/prognosis-readings")
        )
        if not last_prognosis_readings:
            raise RuntimeError(f"No prognosis readings found for lastPrognosisId={last_prognosis_id}")
    else:              
        _logger.warning(f"No prognosis available for datapoint {dp_identifier}.")
        if generate_if_missing:
            last_prognosis_readings = Util.generate_prognosis_entries() 
        else:
            last_prognosis_readings = []
    
    return last_prognosis_readings    
        
# GET datapoint's last datapoint prognosis
def get_datapoint_prognosis(dp_identifier):
    last_prognosis_id = get_datapoint(dp_identifier)[0].get("lastPrognosisId")
    _logger.debug("lastPrognosisId = %s", last_prognosis_id)
    if last_prognosis_id is not None:
        datapoint_prognosis = (
            Q()
            .filter(Id__equals=last_prognosis_id)
            .get("/datapoint-prognoses")
        )
        _logger.debug(f"datapoint_prognosis: {datapoint_prognosis}")
        return datapoint_prognosis
    else:
        _logger.warning(f"No prognosis available for datapoint {dp_identifier}.")
        return None
        
##########################################################        
# POST
##########################################################

# POST prognosis readings
def post_prognosis_readings(prognosis_readings_payload):
    for reading in prognosis_readings_payload:
        response = (Q().post("/prognosis-readings", json=reading))
    return response

# POST datapoint prognosis
def post_datapoint_prognosis(prognosis_payload):
    response = (Q().post("/datapoint-prognoses", json=prognosis_payload))
    
    prognosis_readings_payload = prognosis_payload["readings"]
    for dp_pr_id in prognosis_readings_payload:
        dp_pr_id["datapointPrognosisId"] = response["id"]
    post_prognosis_readings(prognosis_readings_payload)
    
    return response

# POST datapoint reading
def post_datapoint_reading(datapoint_reading_payload):    
    response = (Q().post("/readings", json=datapoint_reading_payload))
    
    return response

# POST datapoint control value
def post_datapoint_ctrl_value(datapoint_ctrl_val_payload):    
    response = (Q().post("/control-values", json=datapoint_ctrl_val_payload))
    
    return response

# POST set datapoint control value status to sent
def post_datapoint_ctrl_status_sent(ctrl_status_sent_payload):    
    response = (Q().post("/control-values/set-sent", json=ctrl_status_sent_payload))
    
    return response