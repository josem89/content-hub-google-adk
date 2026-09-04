from GCSManager import GCSManager
from GoogleChronicleManagerV2 import GoogleChronicleManagerV2
from SiemplifyJob import SiemplifyJob
from DataModels import SecOpsLog, SecOpsLogsPayload
from constants import LOGSTORY_USE_CASES_PATH, SOAR_CONTENT_PATH, FOLDER_BLACKLIST
from utils import calculate_time_from_batch_size, calculate_new_stream
import json, os
import urllib.parse
import copy
from pympler import asizeof
from datetime import datetime


INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "Stream Use Cases Events"


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

    # INIT ACTION PARAMETERS:
    bucket_name = siemplify.extract_job_param(param_name="GCS Bucket Name", print_value=True, default_value=None)
    prefix = siemplify.extract_job_param(param_name="GCS Prefix", print_value=True, default_value=None)
    gcs_sa_key = siemplify.extract_job_param(param_name="GCS SA Key", print_value=False, default_value=None)
    gcs_workload_identity_email = siemplify.extract_job_param(param_name="GCS Workload Identity Email", print_value=False, default_value=None)
    instance = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Instance Name", print_value=True)
    chronicle_sa = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Chronicle SA", print_value=False)
    verify_ssl = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Verify SSL", print_value=True, input_type=bool)
    use_case_white_list = siemplify.extract_job_param(param_name="Use Case Whitelist", print_value=True, default_value=None)
    region = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Region", print_value=False)
    fwd_id = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Forwarder ID", print_value=False).replace(' ', '')
    init_batchsize = siemplify.extract_job_param(param_name="Initial Batch Size", print_value=True, default_value=None).replace(' ', '')
    wait_time = siemplify.extract_job_param(param_name="Wait Time", print_value=True, default_value=None).replace(' ', '')
    workload_identity_email = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Workload Identity Email", print_value=False)

    siemplify.LOGGER.info("Starting Job Execution")
    
    try:
        creds = {}
        if chronicle_sa and chronicle_sa != 'None':
            try:
                creds = json.loads(chronicle_sa)
                siemplify.LOGGER.info("Successfully parsed Chronicle SA credentials.")
            except Exception as e:
                siemplify.LOGGER.error("Unable to parse credentials as JSON. Please validate creds.")
                siemplify.LOGGER.exception(e)
                raise
        else:
            siemplify.LOGGER.info("Chronicle SA not provided. Proceeding without it.")
    except Exception as e:
        siemplify.LOGGER.error("Unable to parse credentials as JSON. Please validate creds.")
        siemplify.LOGGER.exception(e)
        raise
    
    if use_case_white_list:
        try:
            use_case_white_list = use_case_white_list.split(",")
            relevant_usecases = process_whitelist(use_case_white_list)
            siemplify.LOGGER.info(f"Successfully parsed use case whitelist: {relevant_usecases}")

            if not relevant_usecases:
                raise ValueError("Whitelist was provided but resulted in an empty use case filter.")

        except Exception as e:
            siemplify.LOGGER.error("Unable to parse whitelist; no valid filter was provided")
            siemplify.LOGGER.exception(e)

    
    try:
        # --- Initialize Managers ---
        gcs_manager = GCSManager(bucket_name=bucket_name, prefix=prefix, siemplify=siemplify, sa_key=gcs_sa_key, workload_identity_email=gcs_workload_identity_email)
        siemplify.LOGGER.info("GCSManager initialized successfully.")
        
        chronicle_manager = GoogleChronicleManagerV2(instance=instance, region=region, verify_ssl=verify_ssl,
                                                     siemplify_logger=siemplify.LOGGER,
                                                     workload_identity_email=workload_identity_email, **creds)
        
        chronicle_manager.test_connectivity()
        siemplify.LOGGER.info("GoogleChronicleManagerV2 initialized and connectivity tested successfully.")

        # --- Use Case Discovery and Filtering ---
        siemplify.LOGGER.info("Reading Source Repo for available Use Cases")

        filtered_use_cases = []
        all_discovered_use_cases = []
        
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
                all_discovered_use_cases += [{'name':usecase, 'source':_folder if _folder!="Demo" else "Cymbal"} for usecase in usecases]
            else:
                all_discovered_use_cases += [{'name':usecase, 'source':_folder if _folder!="Demo" else "Cymbal"} for usecase in usecases if usecase in relevant_usecases.get(_folder) and usecase not in FOLDER_BLACKLIST]

        # Apply the whitelist filter
        filtered_use_cases = all_discovered_use_cases

        if filtered_use_cases:    
            siemplify.LOGGER.info(f"Found {len(filtered_use_cases)} relevant use cases to process based on the whitelist.")
        else:
            raise ValueError("Couldnt find any relevant Use Cases to Replay")     
        
        # --- State Management: Check if a stream is already in progress ---
        active_stream = siemplify.get_scoped_job_context_property("active_stream") not in ("false", None)
        reference_time = gcs_manager.new_base_time
        
        if active_stream:
            siemplify.LOGGER.info("Active Stream - Continuing usecase Streaming")
            completed_use_cases = siemplify.get_scoped_job_context_property("completed_use_cases")

            if completed_use_cases:
                completed_use_cases = json.loads(completed_use_cases)
                siemplify.LOGGER.info(f"Loaded {len(completed_use_cases)} completed use cases: {completed_use_cases}")
            else:
                completed_use_cases = []
                siemplify.LOGGER.info("No use cases were marked as completed in the previous run.")
            use_case_reference_time = json.loads(siemplify.get_scoped_job_context_property("use_case_reference_time"))
            last_usecase_timestamp = json.loads(siemplify.get_scoped_job_context_property("last_usecase_timestamp"))
            last_usecase_replay_timestamp = json.loads(siemplify.get_scoped_job_context_property("last_usecase_replay_timestamp"))
            file_byte_offsets = json.loads(siemplify.get_scoped_job_context_property("file_byte_offsets") or "{}")

        if not active_stream:
            if wait_time:
                last_successful_stream_ms = siemplify.fetch_timestamp()
                is_new_stream_valid = calculate_new_stream(last_successful_stream_ms, wait_time, reference_time)

                if not is_new_stream_valid:
                    siemplify.LOGGER.info("Wait Time between Stream iterations has not been completed")
                    siemplify.LOGGER.info("Will check again in next Job Execution")
                    siemplify.LOGGER.info("Finished")
                    siemplify.end_script()

            siemplify.set_scoped_job_context_property("completed_use_cases", "[]")
            active_stream = True
            siemplify.LOGGER.info(f"No Active Stream found. Initializing a new stream with Initial Batch Size: '{init_batchsize}' and Wait Time: '{wait_time}'.")
            
            completed_use_cases = []
            use_case_reference_time = {}
            last_usecase_replay_timestamp = {}
            last_usecase_timestamp = {}
            file_byte_offsets = {}
        
        # --- Main Processing Loop ---
        replayed_counts_per_type = {}
        filtered_use_cases = [ uc for uc in filtered_use_cases if uc.get('name') not in completed_use_cases]
        
        if filtered_use_cases:
            for use_case in filtered_use_cases:
                siemplify.LOGGER.info(f"--- Processing Use Case: {use_case.get('name')} ---")
                events_files_path = None
                usecase_mapping = None
                try:  
                    use_case_base_time = None
                    use_case_base_time_first = None
                
                    if use_case.get('source') == 'LogStory':
                        events_files_path = f'{LOGSTORY_USE_CASES_PATH}/{use_case.get("name")}/STREAM'
                    elif use_case.get('source') == 'Cymbal':
                        usecase_mapping = gcs_manager.get_generic_time_mapping('Demo', use_case.get("name"))
                        if usecase_mapping:
                            events_files_path = f'Demo/{use_case.get("name")}/STREAM'
                    else:
                        usecase_mapping = gcs_manager.get_generic_time_mapping(use_case.get("source"), use_case.get("name"))
                        if usecase_mapping:
                            events_files_path = f'{use_case.get("source")}/{use_case.get("name")}/STREAM'
                        
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
                                    
                                    file_key = f"{use_case.get('name')}_{event_file}"
                                    start_byte = file_byte_offsets.get(file_key, 0)

                                    event_data = gcs_manager.get_stream_use_case_data(blob_name, 
                                            'events', event_file, start_byte=start_byte, name_space=use_case.get('source'), usecase_mapping=usecase_mapping,
                                            usecase_name=use_case.get("name"))

                                    if event_data:
                                        event_data.file = event_file
                                        event_data.file_key = file_key

                                        if not event_data.base_timestamp_first or not event_data.base_timestamp:
                                            siemplify.LOGGER.error(f'Couldnt match timestamp, skipping file {event_file}') 
                                            continue
                                    
                                        if not use_case_base_time:
                                            use_case_base_time = event_data.base_timestamp
                                        elif event_data.base_timestamp > use_case_base_time:
                                            use_case_base_time = event_data.base_timestamp
                                    
                                        if not use_case_base_time_first:
                                            use_case_base_time_first = event_data.base_timestamp_first
                                        elif event_data.base_timestamp_first < use_case_base_time_first:
                                            use_case_base_time_first = event_data.base_timestamp_first
                            
                                        if event_data.api == "unstructured":
                                            use_case_unstructured_events.append(event_data)
                                        else:
                                            use_case_udm_events.append(event_data)
                        else:
                            siemplify.LOGGER.warn(f"Usecase '{use_case.get('name')}' has no STREAM folder or log files. Marking as complete and skipping.")
                            if use_case.get('name') not in completed_use_cases:
                                completed_use_cases.append(use_case.get('name'))
                            continue
                
                    if not use_case_base_time or not use_case_base_time_first:
                        siemplify.LOGGER.warn(f"Usecase '{use_case.get('name')}' has no valid timestamp bounds. Skipping.")
                        continue

                    use_case_reference_time[use_case.get('name')] = use_case_base_time.timestamp()

                    if not last_usecase_timestamp.get(use_case.get('name')):
                        last_usecase_timestamp[use_case.get('name')] = use_case_base_time_first.timestamp() - 1
                
                    if not last_usecase_replay_timestamp.get(use_case.get('name')):
                        last_usecase_replay_timestamp[use_case.get('name')] = calculate_time_from_batch_size(init_batchsize, reference_time)
                
                    last_file_timestamp = new_file_timestamp = datetime.fromtimestamp(last_usecase_timestamp.get(use_case.get('name')))
                    last_replayed_timestamp = new_replayed_timestamp = datetime.fromtimestamp(last_usecase_replay_timestamp.get(use_case.get('name')))
                
                    max_timestamp_to_process = last_file_timestamp + (reference_time - last_replayed_timestamp)
                    siemplify.LOGGER.info(f"Original log time window for this run: {last_file_timestamp} -> {max_timestamp_to_process}")
                    
                    if use_case_unstructured_events:                   
                        for event_data in use_case_unstructured_events:
                            siemplify.LOGGER.info(f"Streaming unstructured logs for '{use_case.get('name')}' from file '{event_data.file}' (start_byte: {event_data.start_byte}).")
                            
                            def make_unstructured_callback(log_type):
                                def callback(batch):
                                    count = len(batch.entries)
                                    replayed_counts_per_type[log_type] = replayed_counts_per_type.get(log_type, 0) + count
                                    chronicle_manager.import_logs(SecOpsLogsPayload(SecOpsLog(batch, f"{instance}/forwarders/{fwd_id}")))
                                return callback
                            
                            next_byte_offset = event_data.stream_and_process_window(reference_time, last_replayed_timestamp, last_file_timestamp, make_unstructured_callback(event_data.log_type))
                            file_byte_offsets[event_data.file_key] = next_byte_offset

                            siemplify.LOGGER.info(f"File: {event_data.last_file_time} - Replayed: {event_data.last_replayed_time} - Next Byte Offset: {next_byte_offset}")  

                            if event_data.last_file_time and event_data.last_file_time > new_file_timestamp:
                                new_file_timestamp = event_data.last_file_time
                            if event_data.last_replayed_time and event_data.last_replayed_time > new_replayed_timestamp:
                                new_replayed_timestamp = event_data.last_replayed_time
                        
                    if use_case_udm_events:        
                        for event_data in use_case_udm_events:
                            siemplify.LOGGER.info(f"Streaming UDM events for '{use_case.get('name')}' from file '{event_data.file}' (start_byte: {event_data.start_byte}).")
                            
                            def udm_callback(events_list):
                                count = len(events_list)
                                replayed_counts_per_type['UDM'] = replayed_counts_per_type.get('UDM', 0) + count
                                chronicle_manager.import_events(events_list)
                            
                            next_byte_offset = event_data.stream_and_process_window(reference_time, last_replayed_timestamp, last_file_timestamp, udm_callback)
                            file_byte_offsets[event_data.file_key] = next_byte_offset

                            if event_data.last_file_time and event_data.last_file_time > new_file_timestamp:
                                new_file_timestamp = event_data.last_file_time
                            if event_data.last_replayed_time and event_data.last_replayed_time > new_replayed_timestamp:
                                new_replayed_timestamp = event_data.last_replayed_time      
                    
                    last_usecase_timestamp[use_case.get('name')] = new_file_timestamp.timestamp()
                    last_usecase_replay_timestamp[use_case.get('name')] = new_replayed_timestamp.timestamp()            
                
                except Exception as e:
                    siemplify.LOGGER.error(f"Failed processing Use Case {use_case.get('source')}-{use_case.get('name')}")
                    siemplify.LOGGER.error(e)
            
            # --- Completion Check ---
            for usecase in use_case_reference_time.keys():
                time_difference = abs(use_case_reference_time[usecase] - last_usecase_timestamp[usecase])
                
                if time_difference <= 1:
                    if usecase not in completed_use_cases:
                        siemplify.LOGGER.info(f"Marking use case '{usecase}' as completed. Time difference: {time_difference:.2f}s.")
                        completed_use_cases.append(usecase)

            if completed_use_cases:
                siemplify.LOGGER.info(f"Streaming Completed for use cases:{','.join(completed_use_cases)}")
            
            pending_use_cases = [uc.get('name') for uc in filtered_use_cases if uc.get('name') not in completed_use_cases]
            if not pending_use_cases:
                siemplify.LOGGER.info("All UseCases Streaming Completed")
                active_stream = False
                siemplify.save_timestamp(new_timestamp = int(reference_time.timestamp()*1000))
            else:
                siemplify.LOGGER.info(f"Streaming in progress for usecases {','.join(pending_use_cases)}")
        
            # save Job context:
            siemplify.LOGGER.info("Saving job state for next execution.")
            siemplify.set_scoped_job_context_property("active_stream", active_stream)
        
            if completed_use_cases:
                siemplify.set_scoped_job_context_property("completed_use_cases", json.dumps(completed_use_cases))

            siemplify.set_scoped_job_context_property("last_usecase_timestamp", json.dumps(last_usecase_timestamp))
            siemplify.set_scoped_job_context_property("use_case_reference_time", json.dumps(use_case_reference_time))
            siemplify.set_scoped_job_context_property("last_usecase_replay_timestamp", json.dumps(last_usecase_replay_timestamp))
            siemplify.set_scoped_job_context_property("file_byte_offsets", json.dumps(file_byte_offsets))
        
        else:
            siemplify.LOGGER.info("No pending use cases to stream in this iteration.")
            siemplify.set_scoped_job_context_property("active_stream", False)
        
        siemplify.LOGGER.info("--- Iteration Replay Summary ---")
        if replayed_counts_per_type:
            for log_type, count in replayed_counts_per_type.items():
                siemplify.LOGGER.info(f"Log Type: {log_type} | Events Replayed: {count}")
        else:
            siemplify.LOGGER.info("No events replayed in this iteration.")
        
        siemplify.LOGGER.info("Finished Iteration")
        
    except Exception as e:
        siemplify.LOGGER.error("General error performing Job {}".format(SCRIPT_NAME))
        siemplify.LOGGER.exception(e)
        raise

    siemplify.end_script()


if __name__ == "__main__":
    main()