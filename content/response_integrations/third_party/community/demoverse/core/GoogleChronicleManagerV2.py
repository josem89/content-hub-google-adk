# ============================================================================#
# title           :GoogleChronicleManager.py
# description     :This Module contain all Google Chronicle operations functionality
# author          :avital@siemplify.co
# date            :30-09-2020
# python_version  :3.7
# product_version :1.0
# ============================================================================#

# ============================= IMPORTS ===================================== #

import Parser
import requests
import requests.adapters
from utils import decode_base64_blob_to_string
from constants import SECOPS_API_URL, MAX_RETRIES, API_LIMIT_ERROR, INGESTION_ENDPOINTS
from random import randint
from time import sleep
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession, Request
from TIPCommon.rest.auth import get_impersonated_credentials



# ============================= CLASSES ===================================== #


class GoogleChronicleManagerV2(object):
    """
    Google Chronicle Manager
    """

    def __init__(self, workload_identity_email: str = None, type: str = None, project_id: str = None, private_key_id: str = None, private_key: str = None, client_email: str = None,
                 client_id: str = None, auth_uri: str = None, token_uri: str = None, auth_provider_x509_cert_url: str = None,
                 client_x509_cert_url: str = None, api_root: str = SECOPS_API_URL, verify_ssl: bool = False,
                 siemplify_logger=None, instance: str = None, region: str = None, **kwargs):
        self.siemplify_logger = siemplify_logger
        self.region = region
        self.instance = instance
        if self.region in ['dev', 'staging']:
            
            if self.region == 'staging':
                url_prefix = "staging-chronicle.sandbox"
            elif self.region == 'dev':
                url_prefix = "autopush-chronicle.sandbox"
        else:
            url_prefix = f"{self.region}-chronicle"
        self.api_root = api_root.format(url_prefix=url_prefix, instance=self.instance)
        
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        credentials = None

        if workload_identity_email:
            credentials = get_impersonated_credentials(
                target_principal=workload_identity_email,
                target_scopes=scopes
            )
        elif client_email:
            self.creds = {
                "type": type,
                "project_id": project_id,
                "private_key_id": private_key_id,
                "private_key": private_key,
                "client_email": client_email,
                "client_id": client_id,
                "auth_uri": auth_uri,
                "token_uri": token_uri,
                "auth_provider_x509_cert_url": auth_provider_x509_cert_url,
                "client_x509_cert_url": client_x509_cert_url,
                **kwargs
            }
            credentials = service_account.Credentials.from_service_account_info(info=self.creds, scopes=scopes)
        else:
            raise Exception("Either Workload Identity Email or Service Account must be provided for authentication.")

        self.session = AuthorizedSession(credentials)
        self.session.verify = verify_ssl
        self.parser = Parser

        
        

    @staticmethod
    def prepare_auth_request(verify_ssl: bool = True):
        """
        Prepare an authenticated request.

        Note: This method is a duplicate of the same method in the GoogleCloudComputeManager class. The only change is
        that created session is using verify_ssl parameter to allow self-signed certificates.
        """
        auth_request_session = requests.Session()
        auth_request_session.verify = verify_ssl

        # Using an adapter to make HTTP requests robust to network errors.
        # This adapter retries HTTP requests when network errors occur
        # and the requests seems safely retryable.
        retry_adapter = requests.adapters.HTTPAdapter(max_retries=3)
        auth_request_session.mount("https://", retry_adapter)

        # Do not pass `self` as the session here, as it can lead to
        # infinite recursion.
        return Request(auth_request_session)

    
    def test_connectivity(self) -> bool:
        """
        Test connectivity
        """
        try:
            self.get_instance()
            return True
        except Exception as e:
            raise e
    
    def get_instance(self):
        url = self.api_root
        response = self.retry_request('get', url)
        self.validate_response(response, "Failed to Connect to the API")
        return response.json()

    def get_rule_deployment(self,rule):
        url = f"{self.api_root.replace(self.instance, rule)}/deployment"
        response = self.retry_request('get', url)
        self.validate_response(response, f"Failed to Get rule {rule} deployment information")
        return response.json()

    def execute_udm_query(self, query, start_time, end_time, limit=10000):
        url = f'{self.api_root}:udmSearch'
        params = {
            "limit": limit, 
            "query": query, 
            "timeRange.end_time": end_time, 
            "timeRange.start_time": start_time,
            }
    
        r = self.session.get(url, params = params)
        self.validate_response(r, f'Failed to execute udm query')
        return r.json().get('events')

    def find_raw_logs(self, ids):
        params = {'ids':ids}
        url = f'{self.api_root}/legacy:legacyFindRawLogs'
        r = self.session.get(f"{self.api_root}/legacy:legacyFindRawLogs", params = params)
        self.validate_response(r, f'Failed getting raw Logs')
        raw_logs = r.json().get('rawLogs')
  
        return [decode_base64_blob_to_string(rl.get('rawLogs')[0].get('logBytes')) for rl in raw_logs]

    def get_rule(self,rule):
        url = self.api_root.replace(self.instance, rule)      
        response = self.retry_request('get', url)
        self.validate_response(response, f"Failed to Get rule {rule}")
        rule_data = response.json()
        rule_data["deployment"] = self.get_rule_deployment(rule)
        return rule_data

    
    def list_rules(self, limit=1000, rules_filter = None):
        
        url = f"{self.api_root}/rules?page_size=1000"       
        response = self.retry_request('get', url)
        self.validate_response(response, "Failed to Get rules")
        self.siemplify_logger.info("got a response from the api")
        
        rules_list = response.json().get("rules")
        if rules_filter and rules_list:
            rules_list = [rule for rule in rules_list if rule.get("displayName") in rules_filter]  
            self.siemplify_logger.info("Created the filtered list")
        if not rules_list:
            return []
        rules = [self.get_rule(rule.get('name')) for rule in rules_list]
        self.siemplify_logger.info("Got Rules from API")
        return self.parser.parse_chronicle_rules_v2(rules)

                
    
    def create_rule(self, rule):
        url= f'{self.api_root}/rules'
        body = {"text": rule.content}
        response = self.retry_request('post', url, body= body)
        self.validate_response(response, f"Failed to create rule {rule.name}")
        return response.json()
    
    def patch_rule(self, rule):
        url= self.api_root.replace(self.instance, rule.id)
        body = {"text": rule.content}
        response = self.retry_request('patch', url, body= body)
        self.validate_response(response, f"Failed to patch rule {rule.name}")
        return response.json()
    
    def update_rule_deployment(self, rule_id, deployment, rule_name):
        url= f'{self.api_root.replace(self.instance, rule_id)}/deployment'
        params = {"updateMask":[key for key in deployment]}
        response = self.retry_request('patch', url, params =params, body= deployment)
        self.validate_response(response, f"Failed to update rule deployment for {rule_name}")   
            

    def import_logs(self, logs):
        url = f"{self.api_root}/logTypes/{logs.log_type}/logs:import"
        body = {
            
            "inline_source": {
                "logs":logs.logs,
                "forwarder":logs.fwd_id
            }
        }
        
        #response = self.session.post(url, data = json.dumps(body))
        response = self.retry_request('post', url, body=body, is_import_logs=True)
        self.validate_response(response, f"Failed to process unstructured logs Batch")       
           
    
    def import_events(self, events):
        url = f"{self.api_root}/events:import"
        body = {
            "inline_source":{
                "events":[ {"udm":event} for event in events]
            }
        }
        response = self.retry_request('post', url, body=body, is_import_logs=True)
        #response = self.session.post(url, json = body)
        self.validate_response(response, "Failed to process UDM Logs")
        
        
    def retry_request(self, method, request_url, params=None, body=None, is_import_logs=False):  

        """
        If received API limitation error, will retry the request given times
        :param method: {str} The method of the request (GET, POST, PUT, DELETE, PATCH)
        :param request_url: {str} The request url
        :param params: {dict} Parameters to use in the request
        :param body: {dict} The json payload of the request
        :return: {Response}
        """
        response = self.session.request(method, request_url, params=params, json=body)
        if response.status_code == API_LIMIT_ERROR:
            for i in range(MAX_RETRIES):
                sleep(randint(1, 3))
                response = self.session.request(method, request_url, params=params, json=body)
                if response.status_code == API_LIMIT_ERROR:
                    continue
                break
        if response.status_code == 403 and is_import_logs:
            self.siemplify_logger.info("403 For import Logs - Retrying Request")
            for i in range(MAX_RETRIES):
                sleep(randint(3, 5))
                response = self.session.request(method, request_url, params=params, json=body)
                if response.status_code == 403:
                    continue
                break

        return response
    
    def validate_response(self, r, error_msg):
        try:
            r.raise_for_status()
        except Exception as e:
            self.siemplify_logger.error(error_msg)
            try:
                self.siemplify_logger.error(f"Response Content: {r.content}")
            except Exception as logger_err:
                self.siemplify_logger.error(f"Failed to log response content: {logger_err}")
            self.siemplify_logger.exception(e)
            raise e
