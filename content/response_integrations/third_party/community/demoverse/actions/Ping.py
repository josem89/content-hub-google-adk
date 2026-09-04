import Parser
from GoogleChronicleManagerV2 import GoogleChronicleManagerV2
from SiemplifyAction import SiemplifyAction
import json
from ScriptResult import EXECUTION_STATE_COMPLETED, EXECUTION_STATE_FAILED


INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "Ping"
  

def main():
    siemplify = SiemplifyAction()
    siemplify.script_name = SCRIPT_NAME # In order to use the SiemplifyLogger, you must assign a name to the script.
    
    status = EXECUTION_STATE_COMPLETED  # used to flag back to siemplify system, the action final status
    output_message = "Succesfully connected to Chronicle API"  # human readable message, showed in UI as the action result
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

    siemplify.LOGGER.info("----------------- Main - Started -----------------")
    siemplify.LOGGER.info("----Version 2.4---")

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

        chronicle_manager = GoogleChronicleManagerV2(instance=instance, region=region, verify_ssl=True,
                                                     siemplify_logger=siemplify.LOGGER,
                                                     workload_identity_email=workload_identity_email, **creds)

        chronicle_manager.test_connectivity()
    
        
    except Exception as e:
        siemplify.LOGGER.error("General error connecting to Chronicle API")
        siemplify.LOGGER.exception(e)
        status = EXECUTION_STATE_FAILED
        result_value = False
        output_message = "Failed to connect to Chronicle API"        
        raise e

    siemplify.end(output_message, result_value, status)


if __name__ == "__main__":
    main()