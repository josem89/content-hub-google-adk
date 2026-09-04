from SiemplifyAction import SiemplifyAction
from GoogleChronicleManagerV2 import GoogleChronicleManagerV2
from SiemplifyUtils import output_handler
from ScriptResult import EXECUTION_STATE_COMPLETED, EXECUTION_STATE_FAILED,EXECUTION_STATE_TIMEDOUT
from datetime import datetime
from constants import SEARCH_DATETIME_FORMAT, DEFAULT_DATETIME_FORMAT
from DataModels import UnstructuredLogs, UdmEvents, SecOpsLog, SecOpsLogsPayload
import json
from pympler import asizeof
import copy

INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "Import Logs"

def build_labels(labels):
    parsed_labels = {}
    if labels:
        for key,value in labels.items():
            parsed_labels[key] = {'value':value}
    return parsed_labels     

def batch_logs(payload, max_size_mb=15):
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


@output_handler
def main():
    siemplify = SiemplifyAction()
    siemplify.script_name = SCRIPT_NAME # In order to use the SiemplifyLogger, you must assign a name to the script.
    
    status = EXECUTION_STATE_COMPLETED  # used to flag back to siemplify system, the action final status
    output_message = "Logs Imported Succesfully"  # human readable message, showed in UI as the action result
    result_value = True

    siemplify.LOGGER.info(
        "================= Main - Param Init ================="
    )
     # INIT INTEGRATION CONFIGURATION:
    
    instance = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Instance Name",
                                                       print_value=True, default_value='None')
    chronicle_sa = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Chronicle SA",
                                                         print_value=False, default_value='None')
    region = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Region", print_value=False,
                                                   default_value=None)
    fwd_id = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Forwarder ID",
                                                   print_value=False, default_value=None).replace(' ', '')
    workload_identity_email = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME,
                                                                    param_name="Workload Identity Email",
                                                                    print_value=False)
    
    # INIT ACTION PARAMETERS:
    is_udm = siemplify.extract_action_param(param_name="UDM", print_value=True, default_value = False, input_type = bool)
    namespace = siemplify.extract_action_param(param_name="Namespace", print_value=True, default_value =None)
    labels_string = siemplify.extract_action_param(param_name="Ingestion Labels", print_value=True, default_value=None)
    update_times = siemplify.extract_action_param(param_name="Update Log Times", print_value=True, default_value =True, input_type = bool)
    first_time = siemplify.extract_action_param(param_name="Time Window Start Time", print_value=True, default_value = None, input_type=str)
    last_time = siemplify.extract_action_param(param_name="Time Window End Time", print_value=True, default_value = None, input_type=str)
    multiple_entries = siemplify.extract_action_param(param_name="Multiple Entries", print_value=True, default_value = False)
    log_type = siemplify.extract_action_param(param_name="Log Type", print_value=True, default_value = None, input_type=str)
    content = siemplify.extract_action_param(param_name="Content", print_value=False, default_value = None, input_type=str, is_mandatory = True)
    siemplify.LOGGER.info("----------------- Main - Started -----------------")

    try:
        
        log_data = None
        udm_data = None
        reference_time = datetime.now()
        namespace = 'Cymbal' if not namespace else namespace

        labels = None
        if labels_string:
            try:
                labels_raw = json.loads(labels_string)
                labels = build_labels(labels_raw)
            except Exception as e:
                siemplify.LOGGER.error('Ingestion labels must be valid json format.')
                raise

        if not is_udm:
            
            if not log_type:
                raise ValueError("If importing raw logs, Log Type must be provded.")
            
            if not multiple_entries:
                content = content.replace('\n',"")
            
            if update_times:
                if not first_time:
                    raise ValueError("If importing raw logs, start time must be provided")
                
                last_time = reference_time.strftime(DEFAULT_DATETIME_FORMAT) if not last_time else last_time

                log_data = UnstructuredLogs(content, log_type, reference_time, namespace, None, usecase_mapping = {"first": first_time, "last":last_time}, 
                                                usecase_name = "DemoverseImport", logger = siemplify.LOGGER)
                
                log_data.get_updated_logs()

            else:
                
                log_data = UnstructuredLogs(content, log_type, reference_time, namespace, None, usecase_mapping = None, 
                                                usecase_name = "DemoverseImport", logger = siemplify.LOGGER)
                log_data.entries = log_data.base_entries

        else:

            try:
                udm_raw = json.loads(content)
                if not isinstance(udm_raw, list):
                    udm_raw = [udm_raw]
            

                udm_data = UdmEvents(udm_raw, reference_time, namespace, "DemoverseImport", 
                                        logger = siemplify.LOGGER, is_parsed = True, labels=labels)

                if update_times:
                    udm_data.get_updated_logs()

            except Exception as e:
                siemplify.LOGGER.info("Failed processing UDM data")
                raise

        creds = {}
        if chronicle_sa and chronicle_sa != 'None':
            try:
                creds = json.loads(chronicle_sa)
            except Exception as e:
                siemplify.LOGGER.error("Unable to parse credentials as JSON. Please validate creds.")
                siemplify.LOGGER.exception(e)
                status = EXECUTION_STATE_FAILED
                result_value = False
                output_message = "Unable to parse credentials as JSON. Please validate creds."
                raise
        
        chronicle_manager = GoogleChronicleManagerV2(instance=instance, region=region, verify_ssl=True,
                                                     siemplify_logger=siemplify.LOGGER,
                                                     workload_identity_email=workload_identity_email, **creds)
        
        chronicle_manager.test_connectivity()
        
        if log_data:
            
            json_result = {"logtype":log_type, "imported_logs":0, "failed_logs":0}

            secops_logs = SecOpsLog(log_data, f"{instance}/forwarders/{fwd_id}", labels=labels)
            payload = SecOpsLogsPayload(secops_logs)                              
                
            siemplify.LOGGER.info(f"Importing {len(payload.logs)} raw logs of type {log_type}.")
            
            for batch in batch_logs(payload, max_size_mb=15):
                try:
                    chronicle_manager.import_logs(batch)
                    json_result["imported_logs"] += len(batch.logs)
                except Exception as e:
                    siemplify.LOGGER.error(f"Failed Processing batch of logtype {log_type}: {e}")
                    json_result['failed_logs'] += len(batch.logs)
            
            if json_result['imported_logs']:
                output_message += f"\nSuccesfully imported {json_result['imported_logs']} raw logs of type {log_type}."
            if json_result['failed_logs']:
                output_message +=  f"\nFailed importing {json_result['failed_logs']} raw logs of type {log_type}. "

                
        elif udm_data:

            json_result = {"logtype":"UDM", "imported_events":0, "failed_events":0}
            
            siemplify.LOGGER.info(f"Importing {len(udm_data.events)} UDM events")            

            
            try:
                chronicle_manager.import_events(udm_data.events)
                
                json_result["imported_events"] += len(udm_data.events)
                output_message += f"\nSuccesfully imported {json_result['imported_events']} udm Events."
            
            except Exception as e:
                siemplify.LOGGER.error(f"Failed Processing udm events payload : {e}")

        
        
        if json_result:
            json_result['namespace'] = namespace
            siemplify.result.add_result_json(json_result)

        siemplify.LOGGER.info("----------------- Main - Ended -----------------")

    except Exception as e:
        siemplify.LOGGER.error(f"Action Failed with error: {e}")
        
        status = EXECUTION_STATE_FAILED
        result_value = False
        output_message = f"Action Failed with error: {e}"
        siemplify.LOGGER.exception(e)



    siemplify.LOGGER.info("\n  status: {}\n  result_value: {}\n  output_message: {}".format(status,result_value, output_message))
    siemplify.end(output_message, result_value, status)


if __name__ == "__main__":
    main()
