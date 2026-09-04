
from GCSManager import GCSManager
from GoogleChronicleManagerV2 import GoogleChronicleManagerV2
from SiemplifyJob import SiemplifyJob
from constants import LOGSTORY_RULES_PATH, DEFAULT_RULES_PATH
import os, json, re

#V2 uses Chronicle API instead of Backstory 
INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "Sync Detection Rules"
WHITE_LIST_FILES = ['.DS_Store']

def remove_empty_lines(text):
    """
    Removes empty lines and trailing whitespace from a multi-line string.
    Args:
        text (str): The string to process.
    Returns:
        str: The cleaned string.
    """
    return "\n".join([line.rstrip() for line in text.splitlines() if line.strip()])

def create_rules_set(parsed_rules, existing_rules, siemplify):
    """
    Compares rules from Git with existing rules in Chronicle to determine their status.
    """
    rules_set = []
    
    for pending_rule in parsed_rules:
        if existing_rules:
            chronicle_rule = next((rule for rule in existing_rules if rule.name == pending_rule.name), None)

            if chronicle_rule:            
                siemplify.LOGGER.info(f"Rule '{pending_rule.name}' already exists in Chronicle. Checking for content differences.")
                pending_rule.exists = True
                pending_rule.id = chronicle_rule.id
                pending_rule.deployment = chronicle_rule.deployment
                # Compare content to see if an update is needed
                if remove_empty_lines(pending_rule.content) == remove_empty_lines(chronicle_rule.content):
                    siemplify.LOGGER.info(f"Rule '{pending_rule.name}' content is already in sync.")
                    pending_rule.is_synced = True
            else:
                siemplify.LOGGER.info(f"Rule '{pending_rule.name}' is new and will be created in Chronicle.")

        rules_set.append(pending_rule)
    
    return rules_set

def add_namespace_filter(namespace, rule_content):
    """
    Dynamically injects a namespace filter into a YARA-L rule's 'events' section.
    This ensures the rule only runs on logs from a specific namespace (e.g., "LogStory").
    """

    pattern = r'\$([a-zA-Z0-9_]+)\.(metadata|principal|target|network|security_result)'
    matches = re.findall(pattern,rule_content)
    namespace_condition = str()
    event_names = list()

    if not matches:
        return None

    for _match in matches:
        # Collect unique event variable names (e.g., $udm, $network)
        if _match[0] not in event_names:
            event_names.append(_match[0])    

    for name in event_names:
        namespace_condition += f'\n    ${name}.metadata.base_labels.namespaces = "{namespace}"'
    
    return rule_content.replace('events:',f'events:{namespace_condition}')


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
    chronicle_sa = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Chronicle SA", print_value=False)
    instance = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Instance Name", print_value=True).replace(' ', '')
    verify_ssl = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Verify SSL", print_value=True, input_type=bool)
    rules_white_list = siemplify.extract_job_param(param_name="Rules", print_value=False, default_value=None, is_mandatory=True)
    live_rule = siemplify.extract_job_param(param_name="Live Rule", print_value=True, default_value=True, input_type=bool)
    alerting_rule = siemplify.extract_job_param(param_name="Alerting Rule", print_value=True, default_value=True, input_type=bool)
    region = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Region", print_value=False).replace(' ', '')
    workload_identity_email = siemplify.extract_configuration_param(provider_name=INTEGRATION_NAME, param_name="Workload Identity Email", print_value=False)
    siemplify.LOGGER.info("Starting Job Execution")
    
    gcs_manager = None
    
    creds = {}
    if chronicle_sa and chronicle_sa != 'None':
        try:
            creds = json.loads(chronicle_sa)
        except Exception as e:
            siemplify.LOGGER.error("Unable to parse credentials as JSON. Please validate creds.")
            siemplify.LOGGER.exception(e)
            raise
    
    if rules_white_list and rules_white_list != "ALL_RULES":
        try:
            rules_white_list = rules_white_list.split(",")
        except Exception as e:
            siemplify.LOGGER.error("Unable to parse rules white list")
            siemplify.LOGGER.exception(e)
            raise ValueError("You must provide a valid list of Rules")
    
    elif not rules_white_list:
        raise ValueError("You must provide a valid list of Rules")


    
    try:
        
        gcs_manager = GCSManager(bucket_name=bucket_name, prefix=prefix, siemplify=siemplify, sa_key=gcs_sa_key, workload_identity_email=gcs_workload_identity_email)
        
        
        chronicle_manager = GoogleChronicleManagerV2(instance=instance, region=region, verify_ssl=verify_ssl,
                                                     siemplify_logger=siemplify.LOGGER,
                                                     workload_identity_email=workload_identity_email, **creds)
        
        
        
        chronicle_manager.test_connectivity()
        siemplify.LOGGER.info("Latest Fix: Rule Deployment Status")
        siemplify.LOGGER.info("Reading Source Repo for available Rules")
        
        # --- Discover all rule files from the Git repository ---
        available_rules = []
        
        blobs = gcs_manager.list_files(prefix="")
        top_folders = set()
        for blob in blobs:
            parts = blob.split('/')
            if len(parts) > 1:
                top_folders.add(parts[0])

        for _folder in top_folders:
            if _folder == "LogStory":
                prefix_path = LOGSTORY_RULES_PATH
            else:
                prefix_path = f'{_folder}/rules'

            siemplify.LOGGER.info(f"Scanning for rules with prefix: {prefix_path}")
            rule_blobs = [b for b in blobs if b.startswith(prefix_path)]
            for blob_name in rule_blobs:
                rule_file = os.path.basename(blob_name)
                if rule_file not in WHITE_LIST_FILES and rule_file:
                    rule = {'name': rule_file, 'path': blob_name}
                    available_rules.append(rule)
        
        # --- Filter rules based on the job's whitelist parameter ---
        filtered_rules = []
        if rules_white_list != "ALL_RULES":
            filtered_rules = [rule for rule in available_rules if rule.get('name') in rules_white_list]
        else:
            filtered_rules = available_rules
        
        
        siemplify.LOGGER.info(f"There are {len(filtered_rules)} relevant rules from Repo")
        
        siemplify.LOGGER.info("Getting Existing Rules from Chronicle Instance")
        
        # --- Read content for each filtered rule and prepare it for Chronicle ---
        siemplify.LOGGER.info("--- Preparing Rules from GCS ---")
        parsed_rules = []
        
        deployment = {"enabled": live_rule, "alerting": alerting_rule}

        for rule in filtered_rules:
            try:
                siemplify.LOGGER.info(f"Parsing rule file: {rule.get('name')}")
                rule_data = gcs_manager.get_file_content_as_use_case_data(rule.get('path'), 
                        'rules', rule.get('name'))
                if rule_data:
                    # For LogStory rules, dynamically add a namespace filter to scope them correctly
                    if LOGSTORY_RULES_PATH in rule.get('path'):
                        rule_data.content = add_namespace_filter('LogStory',rule_data.content)
                        if not rule_data.content:
                            siemplify.LOGGER.warn(f"Couldnt add Namespace matching to rule {rule_data.name}, rule will be skipped")
                            continue
                    parsed_rules.append(rule_data)
                                
            
            except Exception as e:
                siemplify.LOGGER.error(f"Failed processing Rule {rule.get('name')}")
                siemplify.LOGGER.error(e)

        # --- Fetch existing rules from Chronicle to compare against ---
        siemplify.LOGGER.info("--- Fetching Existing Rules from Chronicle ---")
        existing_rules = chronicle_manager.list_rules(rules_filter = [rule.name for rule in parsed_rules])
        if existing_rules:
            siemplify.LOGGER.info(f"There are {len(existing_rules)} existing rules in Chronicle")

        # --- Create the final set of rules with their status (new, synced, or needs update) ---
        siemplify.LOGGER.info("--- Comparing GCS Rules with Chronicle Rules ---")
        rules_set = create_rules_set(parsed_rules, existing_rules, siemplify)

        # --- Main synchronization loop: create, update, and manage deployment status ---
        siemplify.LOGGER.info("--- Synchronizing Rules to Chronicle ---")
        for rule in rules_set:
            new_rule = None
            try:
                # Scenario 1: Rule exists but content is different. Update it.
                if rule.exists and not rule.is_synced:
                    siemplify.LOGGER.info(f"Rule '{rule.name}' content has changed. Updating it in Chronicle.")
                    new_rule = chronicle_manager.patch_rule(rule)
                    if new_rule:
                        rule.id = new_rule.get('ruleId')
                    else:
                        continue
                
                # Scenario 2: Rule does not exist. Create it.
                elif not rule.exists:
                    siemplify.LOGGER.info(f"Rule '{rule.name}' is new. Creating it in Chronicle.")
                    new_rule = chronicle_manager.create_rule(rule)
                
                    if new_rule:
                        siemplify.LOGGER.info(f"Rule '{rule.name}' created with id {new_rule.get('ruleId')}")
                        rule.id = new_rule.get('name')
                        rule.deployment = {"enabled": False, "alerting": False}
                    else:
                        continue
                
                # Scenario 3: Rule exists and content is the same. Do nothing with content.
                elif rule.exists and rule.is_synced:
                    siemplify.LOGGER.info(f"Rule '{rule.name}' is already in sync. Skipping content update.")
                
                if rule.id:
                    siemplify.LOGGER.info("Reviewing Deployment Status")
                    # After creating or updating, check if the deployment status (live/alerting) needs to be changed.
                    new_deployment = {}
                    siemplify.LOGGER.info(rule.deployment)
                    if deployment.get("enabled") != rule.deployment.get("enabled"):
                        siemplify.LOGGER.info("Updating Enabled")
                        new_deployment["enabled"] = deployment.get("enabled")
                    if deployment.get("alerting") != rule.deployment.get("alerting"):
                        siemplify.LOGGER.info("Updating Alerting")
                        new_deployment["alerting"] = deployment.get("alerting")

                    if new_deployment:
                        siemplify.LOGGER.info(f"Updating deployment for rule '{rule.name}' to: {json.dumps(new_deployment)}")
                        chronicle_manager.update_rule_deployment(rule.id, new_deployment, rule.name)
                    else:
                        siemplify.LOGGER.info(f"Deployment for rule '{rule.name}' is already correct. Skipping deployment update.")
                        
                
            
            except Exception as e:
                siemplify.LOGGER.error(f"General error syncyng rule {rule.name}")
                siemplify.LOGGER.exception(e)
                
        siemplify.LOGGER.info("--- Job Finished ---")
    except Exception as e:
        siemplify.LOGGER.error("General error performing Job {}".format(SCRIPT_NAME))
        siemplify.LOGGER.exception(e)
        raise

    siemplify.end_script()


if __name__ == "__main__":
    main()