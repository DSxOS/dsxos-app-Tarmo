from Query import Query
import Util

_query_url = None
_query_headers = None

def init(url, headers, logger=None):
    global _query_url, _query_headers, _logger
    _query_url = url
    _query_headers = headers
    _logger = logger
    
    logger.debug(f"query_utils initialized with URL: {_query_url}")
    
# Helper to create Query object
def Q():
    return Query(_query_url, headers=_query_headers, logger=_logger)
    
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