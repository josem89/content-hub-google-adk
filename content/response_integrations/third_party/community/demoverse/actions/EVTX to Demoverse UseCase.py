from SiemplifyAction import SiemplifyAction
from SiemplifyUtils import output_handler
from ScriptResult import EXECUTION_STATE_COMPLETED, EXECUTION_STATE_FAILED,EXECUTION_STATE_TIMEDOUT
from evtxUtils import *
from constants import DEFAULT_DATETIME_FORMAT


INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "EVTX to Demoverse UseCase"


#testing2ß

@output_handler
def main():
    siemplify = SiemplifyAction()
    siemplify.script_name = SCRIPT_NAME
    
    siemplify.LOGGER.info(
        "================= Main - Param Init ================="
    )
    source_path = siemplify.extract_action_param("EVTX Zip File Path", print_value=True)
    dst_path = siemplify.extract_action_param("Target Directory", print_value=True)

    status = EXECUTION_STATE_COMPLETED  # used to flag back to siemplify system, the action final status
    output_message = "Succesfully created Demoverse usecase events' files"  # human readable message, showed in UI as the action result
    result_value = dst_path  # Set a simple result value, used for playbook if\else and placeholders.
    

    siemplify.LOGGER.info("----------------- Main - Started -----------------")

    try:
        
        json_result = {}
        failed_files = []
        empty_files = []
        successful_files = []
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"The file {source_path} does not exist.")
        
        if os.path.exists(dst_path):
            siemplify.LOGGER.info("Destination path Already Exists - Deleting Existing Files to avoid conflicts")
            if os.path.exists(f'{dst_path}/zip_files'):
                delete_legacy_files(f'{dst_path}/zip_files')
            if os.path.exists(f'{dst_path}/EVENTS'):
                delete_legacy_files(f'{dst_path}/EVENTS')

        else:
            os.makedirs(dst_path, exist_ok=True)

        try:
            extracted_path = extract_zip(source_path, dst_path)
        except Exception as e:
            siemplify.LOGGER.error("Failed to extract Zip")
            siemplify.LOGGER.exception(e)
            raise

        if os.listdir(extracted_path):
          extract_evt_events = []
          for file in os.listdir(extracted_path):
            try:
                file_path = os.path.join(extracted_path, file)
                if os.path.isfile(file_path):
                    file_events = extract_events(file_path)
                    
                if file_events:
                    extract_evt_events.extend(file_events)
                    successful_files.append({"file":file, "eventCount":len(file_events)})

                else:
                    siemplify.LOGGER.error(f"The file {file} is empty.")
                    empty_files.append(file)

            except Exception as e:
                siemplify.LOGGER.error(f"Failed processing file {file}: {e}")
                failed_files.append(file)

    
        
        if extract_evt_events:
            
            final_event_list, earliest_timestamp, latest_timestamp = update_log_times(extract_evt_events, siemplify.LOGGER)
            time_window = latest_timestamp - earliest_timestamp
        
            total_seconds = time_window.total_seconds()


            events_payload = {}

            for event in final_event_list:
                event_type = check_evtx_type(event)
                if event_type not in events_payload:
                    events_payload[event_type] = []
                events_payload[event_type].append(event)  
    
            if not os.path.exists(f"{dst_path}/EVENTS"):
                os.mkdir(f"{dst_path}/EVENTS")
          
            for event_type, events in events_payload.items():
                with open(f"{dst_path}/EVENTS/{event_type}.log", "w") as f:
                    for event in events:
                        f.write(event.replace("\n",'') + "\n")
            days = time_window.days
            hours = int(total_seconds // 3600) % 24  # Hours within the current day
            minutes = int(total_seconds // 60) % 60  # Minutes within the current hour
            seconds = int(total_seconds) % 60  # Seconds within the current minute
            json_result["firstLogTime"] = int(earliest_timestamp.timestamp())
            json_result["lastLogTime"] = int(latest_timestamp.timestamp()) + 1
            json_result["timeWindow"] = {
                "days": days,
                "hours": hours,
                "minutes": minutes,
                "seconds": seconds
            }
            json_result["logTypeEntriesCount"] = {}
            for key,value in events_payload.items():
                json_result["logTypeEntriesCount"][key] = len(value)
            json_result["useCasePath"] = result_value

            
            json_result['failed_files']= failed_files if failed_files else []
            
            json_result['empty_files']= empty_files if empty_files else []
            
            json_result['successful_files'] = successful_files if successful_files else []
            siemplify.result.add_result_json(json_result)
            
            if not successful_files:
                
                output_message = "No events found when extracting events"
                result_value = False
                status = EXECUTION_STATE_FAILED
        else:
            output_message = "No events found when extracting events" 
            result_value = False

        json_result['failed_files']= failed_files if failed_files else []
            
        json_result['empty_files']= empty_files if empty_files else []
            
        json_result['successful_files'] = successful_files if successful_files else []
        siemplify.result.add_result_json(json_result)


    except Exception as e:
        siemplify.LOGGER.error(f"An error occurred: {e}")
        siemplify.LOGGER.exception(e)
        status = EXECUTION_STATE_FAILED  # used to flag back to siemplify system, the action final status
        output_message = "Failed to create Demoverse usecase events' files"  # human readable message, showed in UI as the action result
        result_value = False  # Set a simple result value, used for playbook if\else and placeholders.



    siemplify.LOGGER.info("----------------- Main - Ended -----------------")
    siemplify.LOGGER.info("\n  status: {}\n  result_value: {}\n  output_message: {}".format(status,result_value, output_message))
    
    siemplify.end(output_message, result_value, status)


    

if __name__ == "__main__":
    main()
