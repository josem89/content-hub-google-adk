from GCSManager import GCSManager
from GoogleChronicleManagerV2 import GoogleChronicleManagerV2
from SiemplifyJob import SiemplifyJob
from DataModels import SecOpsLog, SecOpsLogsPayload
from constants import LOGSTORY_USE_CASES_PATH, SOAR_CONTENT_PATH, FOLDER_BLACKLIST
import json, os, copy
from pympler import asizeof


INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "Replay Use Cases Entities V2"

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
        new_payload.logs = batch
        batch_payload.append(new_payload)
    
    return batch_payload

def process_whitelist(whitelist):
    relevant_usecases = dict()
    for usecase in whitelist:
        usecase_data = usecase.split(':')

        if len(usecase_data) == 2:
            if relevant_usecases.get(usecase_data[0]):
                relevant_usecases[usecase_data[0]].append(usecase_data[1])
            else:
                relevant_usecases[usecase_data[0]]=[usecase_data[1]]
    return relevant_usecases

def main():
    siemplify = SiemplifyJob()
    siemplify.script_name = SCRIPT_NAME # In order to use the SiemplifyLogger, you must assign a name to the script.
    
    # INIT INTEGRATION CONFIGURATION:
    #integration_param = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME,param_name="Param A")


    # INIT ACTION PARAMETERS:
    bucket_name = siemplify.extract_job_param(param_name="GCS Bucket Name", print_value=True, default_value=None)
    prefix = siemplify.extract_job_param(param_name="GCS Prefix", print_value=True, default_value=None)
    gcs_sa_key = siemplify.extract_job_param(param_name="GCS SA Key", print_value=False, default_value=None)
    gcs_workload_identity_email = siemplify.extract_job_param(param_name="GCS Workload Identity Email", print_value=False, default_value=None)
    instance = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Instance Name", print_value=True).replace(' ', '')
    chronicle_sa = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Chronicle SA", print_value=False)
    verify_ssl = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Verify SSL", print_value=True, input_type=bool)
    use_case_white_list = siemplify.extract_job_param(param_name="Use Case Whitelist", print_value=True, default_value=None)
    region = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Region", print_value=False).replace(' ', '')
    fwd_id = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Forwarder ID", print_value=False).replace(' ', '')
    workload_identity_email = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Workload Identity Email", print_value=False)
    
    siemplify.LOGGER.info("Starting Job Execution")
    siemplify.LOGGER.info("CHECKPOINT: V3.0 - GCS Migration Active")
    
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
                                        usecases if usecase in relevant_usecases.get(_folder)]

        if filtered_use_cases:    
            
            siemplify.LOGGER.info("Processing Relevant Use Cases")
        
        else:
            raise ValueError("Couldnt find any relevant Use Cases to Replay")
        
        chronicle_entities = []
        
        for use_case in filtered_use_cases:
            try:
                use_case_base_time = None
                if use_case.get('source') == 'LogStory':
                    entities_files_path = f'{LOGSTORY_USE_CASES_PATH}/{use_case.get("name")}/ENTITIES'
                elif use_case.get('source') == 'Cymbal':
                    entities_files_path = f'Demo/{use_case.get("name")}/ENTITIES'
                else:
                    entities_files_path = f'{use_case.get("source")}/{use_case.get("name")}/ENTITIES'
            
                siemplify.LOGGER.info(f"Processing use_case {use_case.get('source')}-{use_case.get('name')}")
                blobs = gcs_manager.list_files(prefix=entities_files_path)
                use_case_entities = []
                for blob_name in blobs:
                    if blob_name.endswith(".log"):
                        entity_file = os.path.basename(blob_name)
                        siemplify.LOGGER.info(f"Parsing Entities Log file {entity_file}")
                        entity_data = gcs_manager.get_file_content_as_use_case_data(blob_name, 
                                    'entities', entity_file, name_space = use_case.get('source'))
                    
                        if entity_data:                               
                            
                            if not use_case_base_time:
                                use_case_base_time = entity_data.base_timestamp

                            if entity_data.base_timestamp and use_case_base_time:   

                                if entity_data.base_timestamp > use_case_base_time:
                                    use_case_base_time = entity_data.base_timestamp
                
                            use_case_entities.append(entity_data)
                
                if use_case_entities:
                    for entity_data in use_case_entities:
                        entity_data.base_timestamp = use_case_base_time
                        entity_data.get_updated_logs()
                    chronicle_entities.extend(use_case_entities)
        
            except Exception as e:
                siemplify.LOGGER.error(f"Failed processing Use Case {use_case.get('source')}-{use_case.get('name')}")
                siemplify.LOGGER.error(e)

        aggregated_batches = {}

        if chronicle_entities:

            for batch in chronicle_entities:
                if aggregated_batches.get(batch.name_space):
                    if not aggregated_batches.get(batch.name_space).get(batch.log_type):
                        aggregated_batches[batch.name_space][batch.log_type] = batch
                    else:
                        aggregated_batches[batch.name_space][batch.log_type].entries += batch.entries
                else:
                    aggregated_batches[batch.name_space]=dict()
                    aggregated_batches[batch.name_space][batch.log_type] = batch
            
            chronicle_entities = []
            for key in aggregated_batches.keys():
                chronicle_entities += [value for value in aggregated_batches[key].values()]
            
            siemplify.LOGGER.info(f"Found {len(chronicle_entities)} batch entity logs to replay")       
            
            if chronicle_entities:
                for batch in chronicle_entities:
                    siemplify.LOGGER.info(f"Processing batch of log type {batch.log_type} with {len(batch.entries)} entries")                
                    entry_timestamp = batch.new_base_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
                    secops_logs = SecOpsLog(batch, f"{instance}/forwarders/{fwd_id}", timestamp = entry_timestamp)             
                    payload = SecOpsLogsPayload(secops_logs)
                    for chunk in batch_logs(payload, max_size_mb=2):
                        chronicle_manager.import_logs(chunk)
                
            
        else:
            siemplify.LOGGER.info("No valid Unstructured log entries to process")
        
        siemplify.LOGGER.info("Finished")
        
    except Exception as e:
        siemplify.LOGGER.error("General error performing Job {}".format(SCRIPT_NAME))
        siemplify.LOGGER.exception(e)
        raise

    siemplify.end_script()


if __name__ == "__main__":
    main()