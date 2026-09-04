from SiemplifyJob import SiemplifyJob
from GoogleChronicleManagerV2 import GoogleChronicleManagerV2
import json
import datetime
import copy
import base64
from pympler import asizeof
from DataModels import SecOpsLogsPayload



INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "Send Audit Data to SecOps"

siemplify = SiemplifyJob()
siemplify.script_name = SCRIPT_NAME

class SecOpsLog(object):
    def __init__(self, logs, fwd_id, log_type):
        self.logs = logs
        self.fwd_id = fwd_id
        self.log_type = log_type

def batch_logs(payload, max_size_mb=2):
    """Batches logs into chunks of up to max_size_mb.

    Args:
        logs: The array of logs to batch.
        max_size_mb: The maximum size of each batch in MB (default: 20).

    Returns:
        A list of batches (arrays of logs).
    """
    max_size_bytes = max_size_mb * 1024 * 1024
    batches = []
    current_batch = []
    current_batch_size = 0

    for i in range(0, len(payload.logs), 100):  # Iterate in steps of 100
    
        chunk = payload.logs[i: i + 100]  # Get a chunk of 100 logs
        chunk_size = asizeof.asizeof(chunk)

        if current_batch_size + chunk_size <= max_size_bytes:
            current_batch.extend(chunk)
            current_batch_size += chunk_size
        else:
            batches.append(current_batch)
            current_batch = list(chunk)  # Start a new batch with the current chunk
            current_batch_size = chunk_size

    if current_batch:
        batches.append(current_batch)
    
    batch_payload = []
    for batch in batches:
        new_payload = copy.deepcopy(payload)
        new_payload.logs= batch
        batch_payload.append(new_payload)
    
    return batch_payload


def convert_ms_to_iso_format(timestamp_ms):
    """
    Converts a timestamp in milliseconds to 'YYYY-MM-DDTHH:MI:sZ' format.

    Args:
        timestamp_ms (int): The timestamp in milliseconds.

    Returns:
        str: The formatted datetime string.
    """
    # Convert milliseconds to seconds
    timestamp_seconds = timestamp_ms / 1000

    # Create a datetime object from the timestamp
    dt_object = datetime.datetime.fromtimestamp(timestamp_seconds, tz=datetime.timezone.utc)

    # Format the datetime object to the desired string format
    # The 'Z' indicates UTC, which is what .astimezone(datetime.timezone.utc) provides.
    formatted_string = dt_object.strftime('%Y-%m-%dT%H:%M:%SZ')
    return formatted_string

def create_logs_payload(records, namespace = None):
    logs = []
    for log in records:
        log_text = json.dumps(log)
        log = {
            "data" : base64.b64encode(log_text.encode('utf-8')).decode('utf-8'),
            "log_entry_time": convert_ms_to_iso_format(log.get("creationTimeUnixTimeInMs"))
        }

        if namespace:
            log["environment_namespace"] = namespace

        logs.append(log)
    
    return logs

def generate_timestamp():
    """
    Returns the timestamp in milliseconds for 1 day back from the current time.
    """
    now = datetime.datetime.now()
    yesterday = now - datetime.timedelta(days=2)
    timestamp_milliseconds = int(yesterday.timestamp() * 1000)
    return timestamp_milliseconds

def parse_records(records):
    parsed_records = []
    siemplify.LOGGER.info(len(records))
    for record in records:
        new_record = copy.deepcopy(record)
        
        if record.get("activityItem"):
            new_record["activityItem"] = json.loads(record.get("activityItem"))
        if record.get("currentActivity"):
            new_record["currentActivity"] = json.loads(record.get("currentActivity"))
        if record.get("previousActivity"):
            new_record["previousActivity"] = json.loads(record.get("previousActivity"))
        
        parsed_records.append(new_record)
    
    return  parsed_records
        

def get_audit(last_success_time=0):
    api_root = siemplify.API_ROOT
    more_records = True

    audit_records = []
    page = 0
    
    while more_records:
    
        body = {"usersNames":[],"apiKeys":[],"pageNumber": page, "auditSettingsRequestType":1}
        r = siemplify.session.post(f"{api_root}/external/v1/settings/GetAuditDataV2", json = body)
        r.raise_for_status()

        page_result = r.json()
        
        if page_result.get("auditRecords")[-1].get("creationTimeUnixTimeInMs") < last_success_time:
            siemplify.LOGGER.info(f"Last Relevant Page Reached: Page {page}")
            audit_records += [record for record in page_result.get("auditRecords") 
                if record.get("creationTimeUnixTimeInMs") > last_success_time]
            more_records = False
        
        else: 
            siemplify.LOGGER.info("more records")
            audit_records += page_result.get("auditRecords")
            page += 1

    
    return audit_records

def main():
    # In order to use the SiemplifyLogger, you must assign a name to the script.
    
    # INIT Job PARAMETERS:
    instance = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Instance Name", print_value=True)
    chronicle_sa = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Chronicle SA", print_value=False)
    verify_ssl = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Verify SSL", print_value=True, input_type=bool)
    region = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Region", print_value=False)
    fwd_id = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Forwarder ID", print_value=False).replace(' ', '')
    namespace = siemplify.extract_job_param(param_name="Namespace", print_value=True, default_value=None)
    workload_identity_email = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Workload Identity Email", print_value=False)

    creds = {}
    if chronicle_sa and chronicle_sa != 'None':
        try:
            creds = json.loads(chronicle_sa)
        except Exception as e:
            siemplify.LOGGER.error("Unable to parse credentials as JSON. Please validate creds.")
            siemplify.LOGGER.exception(e)
            raise
        
    chronicle_manager = GoogleChronicleManagerV2(instance=instance, region= region, verify_ssl=verify_ssl,
                        siemplify_logger=siemplify.LOGGER, workload_identity_email=workload_identity_email, **creds)
        
    chronicle_manager.test_connectivity()

    try:
        last_success_time = siemplify.fetch_timestamp()
        if not last_success_time:
            last_success_time = generate_timestamp()
       
        audit_records = get_audit(last_success_time = last_success_time)
        
        parsed_audit_records = parse_records(audit_records)
        siemplify.LOGGER.info(f"Found {len(parsed_audit_records)} relevant Audit Records")

        if parsed_audit_records:

            secops_logs = SecOpsLog(create_logs_payload(parsed_audit_records, namespace),
                    f"{instance}/forwarders/{fwd_id}", "CHRONICLE_SOAR_AUDIT")
            payload = SecOpsLogsPayload(secops_logs)  
            
            for batch in batch_logs(payload, max_size_mb=2):
                chronicle_manager.import_logs(batch)                   

    except Exception as e:
        siemplify.LOGGER.error("General error performing Job {}".format(SCRIPT_NAME))
        siemplify.LOGGER.exception(e)
        raise

    siemplify.end_script()


if __name__ == "__main__":
    main()