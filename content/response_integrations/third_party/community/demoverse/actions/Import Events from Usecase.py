import Parser
from GoogleChronicleManagerV2 import GoogleChronicleManagerV2
from SiemplifyAction import SiemplifyAction
from DataModels import SecOpsLog, SecOpsLogsPayload
import json, os
import copy
from pympler import asizeof
from pathlib import Path
from datetime import datetime
from ScriptResult import EXECUTION_STATE_COMPLETED, EXECUTION_STATE_FAILED, EXECUTION_STATE_TIMEDOUT
from constants import DEFAULT_DATETIME_FORMAT


INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "Import Events from UseCase"


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
  

def main():
    siemplify = SiemplifyAction()
    siemplify.script_name = SCRIPT_NAME # In order to use the SiemplifyLogger, you must assign a name to the script.
    
    status = EXECUTION_STATE_COMPLETED  # used to flag back to siemplify system, the action final status
    output_message = "Logs from provided Use Cases where successfully imported"  # human readable message, showed in UI as the action result
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
    uc_paths_string = siemplify.extract_action_param(param_name="Usecase Paths", print_value=True)
    namespace = siemplify.extract_action_param(param_name="Namespace", print_value=False, default_value =None)
    labels_string = siemplify.extract_action_param(param_name="Labels", print_value=True)
    update_times = siemplify.extract_action_param(param_name="Update Timestamps", print_value=True, default_value =True, input_type = bool)
    first_time = siemplify.extract_action_param(param_name="Date of First Log", print_value=True, default_value = "1979-01-01T00:00:00", input_type=str)
    last_time = siemplify.extract_action_param(param_name="Date of Last Log", print_value=True, default_value = None, input_type=str)
    siemplify.LOGGER.info("----------------- Main - Started -----------------")
    
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


    try:
        
        if labels_string:
            labels_raw = json.loads(labels_string)
            labels = build_labels(labels_raw)
        else:
            labels = {}
    except Exception as e:
        siemplify.LOGGER.error("Unable to parse Labels. Please make srue to use a valid Json key value object")
        siemplify.LOGGER.exception(e)
        status = EXECUTION_STATE_FAILED
        result_value = False
        output_message = "Unable to parse Labels. Please make srue to use a valid Json key value object"
        raise
    
    
    try:
        paths = uc_paths_string.split(",")
    
    except Exception as e:
        siemplify.LOGGER.exception(e)
        status = EXECUTION_STATE_FAILED
        result_value = False
        output_message = "Provide a valid comma separated list of paths"

    
    try:      
        
        chronicle_manager = GoogleChronicleManagerV2(instance=instance, region=region, verify_ssl=True,
                                                     siemplify_logger=siemplify.LOGGER,
                                                     workload_identity_email=workload_identity_email, **creds)
        
        chronicle_manager.test_connectivity()
        
        siemplify.LOGGER.info("Processing Use Cases")
        
        chronicle_unstructured_events = []
        chronicle_udm_events = []
        json_result = {}
        if not last_time:
            time_map = {
                "first": first_time.replace('Z',''), #Removes Z timezone if present to match right format
                "last": datetime.now().strftime(DEFAULT_DATETIME_FORMAT)
            }
        else:
            time_map = {
                "first": first_time.replace('Z',''), #Removes Z timezone if present to match right format
                "last": last_time.replace('Z',''), #Removes Z timezone if present to match right format
            }


        for path in paths:
            events_files_path = os.path.join(path, "EVENTS")
            

            try:  

                use_case_base_time = None
                new_base_time = datetime.now()
                usecase_name = os.path.basename(path)
                if events_files_path:
                    if os.path.exists(events_files_path):
                        siemplify.LOGGER.info(f"Processing use_case in  {path}")
                        use_case_unstructured_events = []
                        use_case_udm_events = []
                        for event_file in os.listdir(events_files_path):
                            if event_file[-4:] == ".log":
                                siemplify.LOGGER.info(f"Parsing Events Log file {event_file}")
                                file_path = Path(os.path.join(events_files_path,event_file))
                                try:
                                    file_content = file_path.read_text()
                                except Exception as e:
                                    siemplify.LOGGER.error(f"Couldnt get content of file {path}")
                                    siemplify.LOGGER.error(e)
                                
                                event_data = Parser.parse_events(file_content, event_file.replace('.log',''), namespace, 
                                    new_base_time, None, usecase_name = usecase_name, logger=siemplify.LOGGER, usecase_mapping= time_map)
                            
                                if event_data:

                                    if not event_data.base_timestamp:
                                        siemplify.LOGGER.error(f'Couldnt match timestamp, skipping file {event_file}') 
                                        continue

                                    if not use_case_base_time:
                                        use_case_base_time = event_data.base_timestamp
                                    
                                    elif event_data.base_timestamp > use_case_base_time:
                                        use_case_base_time = event_data.base_timestamp
                            
                                
                                    if event_data.api == "unstructured":
                                        use_case_unstructured_events.append(event_data)
                                        #chronicle_unstructured_events.append(event_data)
                                    else:
                                        #chronicle_udm_events.append(event_data)
                                        print("mark1")
                                        use_case_udm_events.append(event_data)
                        
                        if update_times:
                            siemplify.LOGGER.info("Updating Timestamps")
                            if use_case_unstructured_events:
                                for event_data in use_case_unstructured_events:
                                    event_data.base_timestamp = use_case_base_time
                                    event_data.get_updated_logs()
                                    
                                chronicle_unstructured_events += use_case_unstructured_events
                            if use_case_udm_events:
                                for event_data in use_case_udm_events:
                                    event_data.base_timestamp = use_case_base_time
                                    event_data.get_updated_logs()
                                chronicle_udm_events += use_case_udm_events
                        else:
                            if use_case_unstructured_events:
                                for event_data in use_case_unstructured_events:
                                    event_data.entries = event_data.base_entries
                                chronicle_unstructured_events += use_case_unstructured_events
                            if use_case_udm_events:
                                for event_data in use_case_udm_events:
                                    event_data.events = event_data.base_events
                                chronicle_udm_events += use_case_udm_events
                               
            except Exception as e:
                siemplify.LOGGER.error(f"Failed processing Use Case {usecase_name}")
                siemplify.LOGGER.error(e)
        
        
        if chronicle_unstructured_events:

            secops_logs_payload = {}
            for batch in chronicle_unstructured_events:                

                secops_logs = SecOpsLog(batch, f"{instance}/forwarders/{fwd_id}", labels=labels)

                if secops_logs_payload.get(secops_logs.log_type):
                    secops_logs_payload.get(secops_logs.log_type).extend_logs(secops_logs.logs)
                else:
                    secops_logs_payload[secops_logs.log_type] = SecOpsLogsPayload(secops_logs)
                                
                

            for log_type,payload in secops_logs_payload.items():

                siemplify.LOGGER.info(f"Processing batch of log type {log_type} with {len(payload.logs)} logs")
                for batch in batch_logs(payload, max_size_mb=15):
                    try:
                        chronicle_manager.import_logs(batch)
                        if not json_result.get(log_type):
                            json_result[log_type] = {"replayed_logs":0}
                        json_result[log_type]["replayed_logs"] += len(batch.logs)
                    except Exception as e:
                        siemplify.LOGGER.error(f"Failed Processing batch of logtype {log_type}: {e}")

           

        else:
            siemplify.LOGGER.info("No valid Unstructured log entries to process")
        
        if chronicle_udm_events:
            siemplify.LOGGER.info(f"Found {len(chronicle_udm_events)} udm batches to replay")
            
            udm_payload = []
            for index,batch in enumerate(chronicle_udm_events):
                udm_payload.extend(batch.events)
            siemplify.LOGGER.info(f"Processing {len(udm_payload)} udm events with {len(batch.events)} events")
            
            try:
                chronicle_manager.import_events(udm_payload)
                
                if not json_result.get('UDM'):
                    print('mark3')
                    json_result['UDM'] = {"replayed_events":0}
                print('mark2')
                json_result['UDM']["replayed_events"] += len(udm_payload)
            except Exception as e:
                siemplify.LOGGER.error(f"Failed Processing udm events payload : {e}")
        else:
            siemplify.LOGGER.info("No valid UDM Events to process")
        
        if json_result:
            json_result['namespace'] = namespace
            siemplify.result.add_result_json(json_result)
        else:
            result_value = False
            output_message = "No data was replayed to Google SecOps."

        siemplify.LOGGER.info("----------------- Main - Ended -----------------")
        
    except Exception as e:
        siemplify.LOGGER.error("General error performing Job {}".format(SCRIPT_NAME))
        siemplify.LOGGER.exception(e)
        status = EXECUTION_STATE_FAILED
        result_value = False
        output_message = "Provide a valid comma separated list of paths"        
        raise e

    siemplify.end(output_message, result_value, status)


if __name__ == "__main__":
    main()