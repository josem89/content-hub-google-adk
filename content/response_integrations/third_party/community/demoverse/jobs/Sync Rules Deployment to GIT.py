from GCSManager import GCSManager
from GoogleChronicleManagerV2 import GoogleChronicleManagerV2
from SiemplifyJob import SiemplifyJob
from constants import LOGSTORY_RULES_PATH
import os, json

INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "Sync Rules from Chronicle"
WHITE_LIST_FILES = ['.DS_Store']
RULE_STATUS_FILE = "rule_status.json"




def remove_empty_lines(text):
    return "\n".join([line.rstrip() for line in text.splitlines() if line.strip()])

def write_rule_to_gcs(gcs_manager, rule, rules_base_path, siemplify):
    """Writes a Chronicle rule content to a file in the GCS bucket."""
    blob_name = os.path.join(rules_base_path, f"{rule.name}")
    try:
        gcs_manager.upload_file_content(blob_name, rule.content)
        return True
    except Exception as e:
        siemplify.LOGGER.error(f"Failed to write rule '{rule.name}' to GCS: {e}")
        return False

def main():
    siemplify = SiemplifyJob()
    siemplify.script_name = SCRIPT_NAME

    # INIT ACTION PARAMETERS:
    bucket_name = siemplify.extract_job_param(param_name="GCS Bucket Name", print_value=True, default_value=None)
    prefix = siemplify.extract_job_param(param_name="GCS Prefix", print_value=True, default_value=None)
    gcs_sa_key = siemplify.extract_job_param(param_name="GCS SA Key", print_value=False, default_value=None)
    gcs_workload_identity_email = siemplify.extract_job_param(param_name="GCS Workload Identity Email", print_value=False, default_value=None)
    chronicle_sa = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Chronicle SA", print_value=False)
    instance = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Instance Name", print_value=True).replace(' ', '')
    verify_ssl = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Verify SSL", print_value=True, input_type=bool)
    region = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Region", print_value=False).replace(' ', '')
    workload_identity_email = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Workload Identity Email", print_value=False)

    siemplify.LOGGER.info("Starting Job Execution")

    gcs_manager = None
    rule_statuses = {}

    creds = {}
    if chronicle_sa and chronicle_sa != 'None':
        try:
            creds = json.loads(chronicle_sa)
        except Exception as e:
            siemplify.LOGGER.error("Unable to parse credentials as JSON. Please validate creds.")
            siemplify.LOGGER.exception(e)
            raise

    try:
        gcs_manager = GCSManager(bucket_name=bucket_name, prefix=prefix, siemplify=siemplify, sa_key=gcs_sa_key, workload_identity_email=gcs_workload_identity_email)

        chronicle_manager = GoogleChronicleManagerV2(instance=instance, region=region, verify_ssl=verify_ssl,
                                                     siemplify_logger=siemplify.LOGGER,
                                                     workload_identity_email=workload_identity_email, **creds)

        chronicle_manager.test_connectivity()

        siemplify.LOGGER.info("Fetching all rules from Chronicle instance...")
        all_chronicle_rules = chronicle_manager.list_rules()    
        
        
        path = LOGSTORY_RULES_PATH
            
        logstory_rules = {}
        blobs = gcs_manager.list_files(prefix=path)
        for blob_name in blobs:
            rule_file = os.path.basename(blob_name)
            if rule_file not in WHITE_LIST_FILES and rule_file:
                rule_data = gcs_manager.get_file_content_as_use_case_data(blob_name, 
                    'rulesV2', rule_file)
                logstory_rules[rule_file] = rule_data

        if all_chronicle_rules:
            siemplify.LOGGER.info(f"Found {len(all_chronicle_rules)} rules in Chronicle.")

            # Determine the base path for rules in GCS
            logstory_rules_base_path = LOGSTORY_RULES_PATH
            default_rules_base_path = 'Demo/rules'

            for rule in all_chronicle_rules:
                siemplify.LOGGER.info(f"Processing rule: {rule.name} (ID: {rule.id})")
                siemplify.LOGGER.info([key for key in rule.__dict__])
                
                # Fetch the detailed rule content
                
                if rule.content:
                         
                    if rule.name in logstory_rules:
                        if remove_empty_lines(rule.content) != remove_empty_lines(logstory_rules.get(rule.name).content):                     
                            write_successful = write_rule_to_gcs(gcs_manager, rule, logstory_rules_base_path, siemplify)
                    else:
                        write_successful = write_rule_to_gcs(gcs_manager, rule, default_rules_base_path, siemplify)

                    # Determine the rule status
                    if rule.deployment.get("enabled") and rule.deployment.get("alerting"):
                        status = "alerting"
                    elif rule.deployment.get("enabled") and not rule.deployment.get("alerting"):
                        status = "live"
                    else:
                        status = "disabled"
                    rule_statuses[rule.name] = status

                else:
                    siemplify.LOGGER.warn(f"Could not fetch detailed content for rule: {rule.name} (ID: {rule.id})")
                    rule_statuses[rule.name] = "error_fetching_content"

            

            # In GCS, we don't commit/push. We just upload the status file.
            siemplify.LOGGER.info("Uploading rule status to GCS...")
            try:
                status_content = json.dumps(rule_statuses, indent=4)
                gcs_manager.upload_file_content(RULE_STATUS_FILE, status_content)
                siemplify.LOGGER.info(f"Rule statuses written to '{RULE_STATUS_FILE}' in GCS.")
            except Exception as e:
                siemplify.LOGGER.error(f"Failed to write rule status to '{RULE_STATUS_FILE}': {e}")

        else:
            siemplify.LOGGER.info("No rules found in the Chronicle instance.")

    except Exception as e:
        siemplify.LOGGER.error(f"General error performing Job {SCRIPT_NAME}")
        siemplify.LOGGER.exception(e)
        raise

    siemplify.end_script()

if __name__ == "__main__":
    main()