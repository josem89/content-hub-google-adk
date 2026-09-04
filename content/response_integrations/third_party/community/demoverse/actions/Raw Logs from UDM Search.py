from SiemplifyAction import SiemplifyAction
from SiemplifyUtils import output_handler
from ScriptResult import EXECUTION_STATE_COMPLETED, EXECUTION_STATE_FAILED,EXECUTION_STATE_TIMEDOUT
from GoogleChronicleManagerV2 import GoogleChronicleManagerV2
import json, os, base64
from datetime import datetime
from ScriptResult import EXECUTION_STATE_COMPLETED, EXECUTION_STATE_FAILED, EXECUTION_STATE_TIMEDOUT
from constants import SEARCH_DATETIME_FORMAT
import zipfile, shutil

#test
INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "Raw Logs from UDM Search"
ROOT_FOLDER = "/tmp/EVENTS"


def force_makedirs(path):
    """
    Creates a directory and its parents. If the directory already exists,
    it will be removed first, effectively overwriting it.

    Args:
        path (str): The path to the directory to create.
    """
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
            print(f"Removed existing directory: {path}")
        except OSError as e:
            print(f"Error removing existing directory '{path}': {e}")
            return

    try:
        os.makedirs(path)
        print(f"Successfully created directory: {path}")
    except OSError as e:
        print(f"Error creating directory '{path}': {e}")


def create_events_file(events, log_type):
    with open(f"{ROOT_FOLDER}/{log_type}.log", "w") as f:
        for event in events:
            f.write(event + "\n")


def create_zip_and_encode_base64(folder_path):
    """
    Creates a zip archive of the given folder and returns the Base64 encoded string of the zip file.

    Args:
        folder_path (str): The path to the folder you want to zip.

    Returns:
        str: The Base64 encoded string of the zip file, or None if an error occurs.
    """
    if not os.path.isdir(folder_path):
        print(f"Error: Folder not found at '{folder_path}'")
        return None

    zip_filename = f"archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    try:
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, folder_path)
                    zipf.write(file_path, relative_path)

        with open(zip_filename, 'rb') as zip_file:
            zip_bytes = zip_file.read()
            base64_encoded_zip = base64.b64encode(zip_bytes).decode('utf-8')

        os.remove(zip_filename)  # Clean up the temporary zip file
        return base64_encoded_zip

    except Exception as e:
        print(f"An error occurred: {e}")
        if os.path.exists(zip_filename):
            os.remove(zip_filename)
        return None


def is_valid_date_format(date_string, format_string):
  """
  Validates if a given string matches a specified date format.

  Args:
    date_string: The string to validate.
    format_string: The expected date format string (e.g., '%Y-%m-%d').

  Returns:
    True if the string matches the format, False otherwise.
  """
  try:
    datetime.strptime(date_string, format_string)
    return True
  except ValueError:
    return False

@output_handler
def main():
    siemplify = SiemplifyAction()
    siemplify.script_name = SCRIPT_NAME # In order to use the SiemplifyLogger, you must assign a name to the script.
    
    status = EXECUTION_STATE_COMPLETED  # used to flag back to siemplify system, the action final status
    output_message = "Succesfully Created ZIP folder from UDM Query"  # human readable message, showed in UI as the action result
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
    query = siemplify.extract_action_param(param_name="Udm Query", print_value=True)
    start_time = siemplify.extract_action_param(param_name="Start Time", print_value=True, default_value = None, input_type=str)
    end_time = siemplify.extract_action_param(param_name="End Time", print_value=True, default_value = None, input_type=str)
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
        
        if not is_valid_date_format(start_time, SEARCH_DATETIME_FORMAT):
            raise ValueError("Start Time must match format %Y-%m-%dT%H:%M:%SZ")
        
        if not end_time:
            end_time = datetime.now().strftime(SEARCH_DATETIME_FORMAT)
        
        if not is_valid_date_format(end_time, SEARCH_DATETIME_FORMAT):
            raise ValueError("Start Time must match format %Y-%m-%dT%H:%M:%SZ")
        

        
        chronicle_manager = GoogleChronicleManagerV2(instance=instance, region=region, verify_ssl=True,
                                                     siemplify_logger=siemplify.LOGGER,
                                                     workload_identity_email=workload_identity_email, **creds)
        
        chronicle_manager.test_connectivity()

        events = chronicle_manager.execute_udm_query(query, start_time, end_time)

        logs_data = {}
        udm_events = []
        

        if events:
            for event in events:

                event_data = event.get('udm').get('metadata')
                log_type = event_data.get('logType')
    
                if log_type == "UDM":
                    udm_events.append(json.dumps(event.get('udm')).replace('\n', '').replace('\r', ''))
                    continue
                
                if logs_data.get(log_type) is None:
                    logs_data[log_type] = []
                logs_data[log_type].append(event_data.get('id'))


            force_makedirs(ROOT_FOLDER)
            
            if udm_events:
                create_events_file(udm_events, 'UDM')

            for log_type, event_ids in logs_data.items():
                raw_logs = []
                
                
  
                for i in range(0, len(event_ids), 200):
                    raw_logs += chronicle_manager.find_raw_logs(event_ids[i:i+200])
            
                create_events_file(raw_logs, log_type)

            contents = create_zip_and_encode_base64(ROOT_FOLDER)
            if not contents:
                raise "There was an errror creating the Files"
            

            siemplify.result.add_attachment("Raw Events Folder", "EVENTS.zip", contents, None)

            shutil.rmtree(ROOT_FOLDER)


        
        else:
            siemplify.LOGGER.info("No results from UDM query")
            output_message = "No results from UDM Query"
            result_value = False
    
    except Exception as e:
        siemplify.LOGGER.error("General error performing Job {}".format(SCRIPT_NAME))
        siemplify.LOGGER.exception(e)
        status = EXECUTION_STATE_FAILED
        result_value = False
        output_message = f"Failed Executing action: {e}"        
        raise e

    siemplify.end(output_message, result_value, status)


if __name__ == "__main__":
    main()
