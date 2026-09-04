from GCSManager import GCSManager
from GoogleChronicleManagerV2 import GoogleChronicleManagerV2
from SiemplifyJob import SiemplifyJob
from DataModels import SecOpsLog, SecOpsLogsPayload
from constants import LOGSTORY_USE_CASES_PATH, SOAR_CONTENT_PATH, FOLDER_BLACKLIST
import json, os
import urllib.parse
import copy
from pympler import asizeof


INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "Replay Use Cases V2"



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
  

def process_whitelist(whitelist):
    """Processes a whitelist of use cases and groups them by their folder name.

    Args:
        whitelist (list): A list of strings, each containing a folder and a use case name
            separated by a colon (e.g., ['Folder:UseCaseName']).

    Returns:
        dict: A dictionary mapping folder names to lists of usecase names.
    """
    relevant_usecases = dict()
    for usecase in whitelist:
        usecase_data = usecase.split(':')

        if len(usecase_data) == 2:
            if relevant_usecases.get(usecase_data[0]):
                relevant_usecases[usecase_data[0]].append(usecase_data[1])
            else:
                relevant_usecases[usecase_data[0]]=[usecase_data[1]]
    return relevant_usecases

def simulate_case(_case, siemplify):
    """Imports a custom case into the SOAR Attacks Simulator and triggers use case generation.

    Args:
        _case (dict): The case data dictionary containing the case details and name.
        siemplify (SiemplifyJob): The Siemplify job instance.

    Returns:
        bool: True if the case was successfully simulated, False otherwise.
    """

    try:
        res = siemplify.session.post(f'{siemplify.API_ROOT}/external/v1/attackssimulator/ImportCustomCase',
                    json = _case)
        res.raise_for_status()
                        
                    
        data =  {
            "environment": "Demoverse",
            "customCases": [_case["cases"][0]["name"]],
            "kinds": [
                    0
            ]
                                
        }

        res = siemplify.session.post(f'{siemplify.API_ROOT}/external/v1/attackssimulator/GenerateUseCases',
                json = data)
        res.raise_for_status()
        return True
    except Exception as e:
        siemplify.LOGGER.error(e)
        return False

def process_simulated_cases(simulated_cases,siemplify,gcs_manager, base_time):
    """Retrieves simulated case content from SOAR, updates its timing, and returns updated cases.

    Args:
        simulated_cases (list): List of simulated case dictionaries to process.
        siemplify (SiemplifyJob): The Siemplify job instance.
        gcs_manager (GCSManager): The GCS manager instance used to update case time fields.
        base_time (datetime or str): The base time to align case events to.

    Returns:
        list: A list of updated simulated case dictionaries.
    """
    updated_simulated_cases = []
    for _case in simulated_cases:
        try:
            res = siemplify.session.get(f'{siemplify.API_ROOT}/external/v1/attackssimulator/ExportCustomCase/{urllib.parse.quote(_case.get("name"))}'),
            res[0].raise_for_status()
            content = res[0].json()

            updated_case = gcs_manager.update_case(content=content, 
            time_format= _case.get('time_format'), start_time_key=_case.get('start_time_key'),
            end_time_key=_case.get('end_time_key'), base_time=base_time)
                            
            if updated_case:
                updated_simulated_cases.append(updated_case)

        except Exception as e:
            siemplify.LOGGER.error(f'Failed to get simulated case content for {_case}')
            siemplify.LOGGER.error(e)
    
    return updated_simulated_cases

def main():
    """Main execution flow for the Replay Use Cases V2 job.
    
    Reads configuration and parameters, fetches and processes events and simulated
    cases from GCS, and imports the events to Google Chronicle.
    """
    siemplify = SiemplifyJob()
    siemplify.script_name = SCRIPT_NAME # In order to use the SiemplifyLogger, you must assign a name to the script.
    
    # INIT INTEGRATION CONFIGURATION:
    #integration_param = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME,param_name="Param A")


    # INIT ACTION PARAMETERS:
    bucket_name = siemplify.extract_job_param(param_name="GCS Bucket Name", print_value=True, default_value=None)
    prefix = siemplify.extract_job_param(param_name="GCS Prefix", print_value=True, default_value =None)
    gcs_sa_key = siemplify.extract_job_param(param_name="GCS SA Key", print_value=False, default_value=None)
    gcs_workload_identity_email = siemplify.extract_job_param(param_name="GCS Workload Identity Email", print_value=False, default_value=None)
    instance = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Instance Name", print_value=True)
    chronicle_sa = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Chronicle SA", print_value=False)
    verify_ssl = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Verify SSL", print_value=True, input_type=bool)
    use_case_white_list = siemplify.extract_job_param(param_name="Use Case Whitelist", print_value=True, default_value=None)
    region = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Region", print_value=False)
    fwd_id = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Forwarder ID", print_value=False).replace(' ', '')
    workload_identity_email = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Workload Identity Email", print_value=False)
    siemplify.LOGGER.info("Starting Job Execution")
    
    creds = {}
    if chronicle_sa and chronicle_sa != 'None':
        try:
            creds = json.loads(chronicle_sa)
        except Exception as e:
            siemplify.LOGGER.error("Unable to parse credentials as JSON. Please validate creds.")
            siemplify.LOGGER.exception(e)
            raise
    
    if use_case_white_list:
        try:
            use_case_white_list = use_case_white_list.split(",")
            relevant_usecases = process_whitelist(use_case_white_list)

            if not relevant_usecases:
                raise

        except Exception as e:
            siemplify.LOGGER.error("Unable to parse whitelist; no valid filter was provided")
            siemplify.LOGGER.exception(e)

    
    try:
        
        gcs_manager = GCSManager(bucket_name=bucket_name, prefix=prefix, siemplify=siemplify, sa_key=gcs_sa_key, workload_identity_email=gcs_workload_identity_email)
        
        chronicle_manager = GoogleChronicleManagerV2(instance=instance, region=region, verify_ssl=verify_ssl,
                                                     siemplify_logger=siemplify.LOGGER,
                                                     workload_identity_email=workload_identity_email, **creds)
        
        chronicle_manager.test_connectivity()
        
        siemplify.LOGGER.info("Reading Source Repo for available Use Cases")

        filtered_use_cases =[]
        
        for _folder in relevant_usecases.keys():
            if _folder == "LogStory":
                prefix_path = LOGSTORY_USE_CASES_PATH
            else:
                prefix_path = _folder
            
            blobs = gcs_manager.list_files(prefix=prefix_path)
            usecases = set()
            for blob_name in blobs:
                rel_path = blob_name[len(prefix_path):].strip('/')
                if rel_path:
                    parts = rel_path.split('/')
                    if parts:
                        usecases.add(parts[0])
            
            if 'ALL' in relevant_usecases.get(_folder):
                filtered_use_cases += [{'name':usecase, 'source':_folder if _folder!="Demo" else "Cymbal"} for usecase 
                                        in usecases]
            else:
                filtered_use_cases += [{'name':usecase, 'source':_folder if _folder!="Demo" else "Cymbal"} for usecase in 
                                        usecases if usecase in relevant_usecases.get(_folder) and usecase not in FOLDER_BLACKLIST]

        if filtered_use_cases:    
            siemplify.LOGGER.info("Processing Relevant Use Cases")
        
        else:
            raise ValueError("Couldnt find any relevant Use Cases to Replay")     
        
        
        chronicle_unstructured_events = []
        chronicle_udm_events = []

        
        for use_case in filtered_use_cases:
            events_files_path = None
            usecase_mapping = None
            simulated_cases = list()
            has_event_data = True
            try:  

                use_case_base_time = None
                
                if use_case.get('source') == 'LogStory':
                    events_files_path = f'{LOGSTORY_USE_CASES_PATH}/{use_case.get("name")}/EVENTS'
                elif use_case.get('source') == 'Cymbal':
                    usecase_mapping = gcs_manager.get_generic_time_mapping('Demo', use_case.get("name"))
                    if usecase_mapping:
                        events_files_path = f'Demo/{use_case.get("name")}/EVENTS'
                    try:
                        soar_content = gcs_manager.get_soar_file_content(f'Demo/{use_case.get("name")}/{SOAR_CONTENT_PATH}', optional=True)
                        if soar_content and soar_content.get('simulated_cases'):
                            simulated_cases += [_case for _case in soar_content.get('simulated_cases')]
                    except Exception:
                        pass
                else:
                    usecase_mapping = gcs_manager.get_generic_time_mapping(use_case.get("source"), use_case.get("name"))
                    if usecase_mapping:
                        events_files_path = f'{use_case.get("source")}/{use_case.get("name")}/EVENTS'
                    try:
                        soar_content = gcs_manager.get_soar_file_content(f'{use_case.get("source")}/{use_case.get("name")}/{SOAR_CONTENT_PATH}', optional=True)
                        if soar_content and soar_content.get('simulated_cases'):
                            simulated_cases += [_case for _case in soar_content.get('simulated_cases')]
                    except Exception:
                        pass

                if events_files_path:
                    siemplify.LOGGER.info(f"Processing use_case {use_case.get('source')}-{use_case.get('name')}")
                    use_case_unstructured_events = []
                    use_case_udm_events = []
                    
                    blobs = gcs_manager.list_files(prefix=events_files_path)
                    if blobs:
                        for blob_name in blobs:
                            if blob_name.endswith(".log"):
                                event_file = os.path.basename(blob_name)
                                siemplify.LOGGER.info(f"Parsing Events Log file {event_file}")
                                event_data = gcs_manager.get_file_content_as_use_case_data(blob_name, 
                                            'events', event_file, name_space = use_case.get('source'), usecase_mapping=usecase_mapping,
                                            usecase_name = use_case.get("name"))
                                
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
                                        use_case_udm_events.append(event_data)
                        
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
                        siemplify.LOGGER.info(f"No Event Data for use case {use_case.get('name')} - skipping")
                        has_event_data = False

                if simulated_cases:
                    
                    if not use_case_base_time:
                        use_case_base_time = gcs_manager.new_base_time
                    updated_simulated_cases = process_simulated_cases(simulated_cases,siemplify,gcs_manager, 
                                                use_case_base_time)

                    if updated_simulated_cases:
                        siemplify.LOGGER.info("Simulating cases in SOAR")
                        for _case in updated_simulated_cases:
                            is_case_simulated = simulate_case(_case, siemplify)
                            if is_case_simulated:
                                siemplify.LOGGER.info(f'Successfully simulated case {_case["cases"][0]["name"]}')    

                if not has_event_data:
                    continue
                   
            
            except Exception as e:
                siemplify.LOGGER.error(f"Failed processing Use Case {use_case.get('source')}-{use_case.get('name')}")
                siemplify.LOGGER.error(e)
        
        secops_logs_payload = {}

        if chronicle_unstructured_events:

            for batch in chronicle_unstructured_events:                

                secops_logs = SecOpsLog(batch, f"{instance}/forwarders/{fwd_id}")

                if secops_logs_payload.get(secops_logs.log_type):
                    secops_logs_payload.get(secops_logs.log_type).extend_logs(secops_logs.logs)
                else:
                    secops_logs_payload[secops_logs.log_type] = SecOpsLogsPayload(secops_logs)
                                
                

            for log_type,payload in secops_logs_payload.items():

                siemplify.LOGGER.info(f"Processing batch of log type {log_type} with {len(payload.logs)} logs")
                for batch in batch_logs(payload, max_size_mb=2):
                    chronicle_manager.import_logs(batch)
           

        else:
            siemplify.LOGGER.info("No valid Unstructured log entries to process")
        
        if chronicle_udm_events:
            siemplify.LOGGER.info(f"Found {len(chronicle_udm_events)} udm batches to replay")
            
            udm_payload = []
            for index,batch in enumerate(chronicle_udm_events):
                udm_payload.extend(batch.events)
            siemplify.LOGGER.info(f"Processing {len(udm_payload)} udm events with {len(batch.events)} events")
            chronicle_manager.import_events(udm_payload)
        else:
            siemplify.LOGGER.info("No valid UDM Events to process")
        

        siemplify.LOGGER.info("Finished")
        
    except Exception as e:
        siemplify.LOGGER.error("General error performing Job {}".format(SCRIPT_NAME))
        siemplify.LOGGER.exception(e)
        
        raise

    siemplify.end_script()


if __name__ == "__main__":
    main()