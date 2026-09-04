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
import json
import requests
import requests.adapters
from math import ceil
from constants import CHRONICLE_API_URL, MAX_RETRIES, API_LIMIT_ERROR, INGESTION_ENDPOINTS
from random import randint
from time import sleep
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession, Request



# ============================= CLASSES ===================================== #


class GoogleChronicleManager(object):
    """
    Google Chronicle Manager
    """

    def __init__(self, type: str, project_id: str, private_key_id: str, private_key: str, client_email: str,
                 client_id: str, auth_uri: str, token_uri: str, auth_provider_x509_cert_url: str,
                 client_x509_cert_url: str, api_root: str = CHRONICLE_API_URL, verify_ssl: bool = False,
                 siemplify_logger=None, **kwargs):
        self.siemplify_logger = siemplify_logger
        self.api_root = api_root
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
        if self.api_root in INGESTION_ENDPOINTS:
            credentials = service_account.Credentials.from_service_account_info(info=self.creds, scopes=["https://www.googleapis.com/auth/malachite-ingestion"])
        else:
            credentials = service_account.Credentials.from_service_account_info(info=self.creds, scopes=["https://www.googleapis.com/auth/chronicle-backstory"])
        #self.session = AuthorizedSession(credentials, auth_request=self.prepare_auth_request(verify_ssl))
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
            self.list_rules(limit = 1)
            return True
        except Exception as e:
            raise e
    
    def list_rules(self, limit=100):
        url = f"{self.api_root}/v2/detect/rules?page_size=1000"       
        response = self.retry_request('get', url)
        self.validate_response(response, "Failed to Get rules")
        return self.parser.parse_chronicle_rules(response.json().get("rules"))

                
    
    def create_rule(self, rule):
        url= f'{self.api_root}/v2/detect/rules'
        body = {"ruleText": rule.content}
        response = self.session.post(url, json = body)
        self.validate_response(response, f"Failed to create rule {rule.name}")
        return response.json()
    
    def create_rule_version(self, rule):
        url= f'{self.api_root}/v2/detect/rules/{rule.id}:createVersion'
        body = {"ruleText": rule.content}
        response = self.session.post(url, json = body)
        self.validate_response(response, f"Failed to create new rule version for rule {rule.name}")
        return response.json()
    
    def set_live_rule(self, rule_id):
        url= f'{self.api_root}/v2/detect/rules/{rule_id}:enableLiveRule'
        response = self.retry_request('post',url)
        self.validate_response(response, f"Failed to enable live status")
    
    def disable_live_rule(self, rule_id):
        url= f'{self.api_root}/v2/detect/rules/{rule_id}:disableLiveRule'
        response = self.retry_request('post',url)
        self.validate_response(response, f"Failed to disable live status")
    
    def set_alerting_rule(self, rule_id):
        url= f'{self.api_root}/v2/detect/rules/{rule_id}:enableAlerting'
        response = self.retry_request('post',url)
        self.validate_response(response, f"Failed to enable Alerting status")   
    
    def disable_alerting_rule(self, rule_id):
        url= f'{self.api_root}/v2/detect/rules/{rule_id}:disableAlerting'
        response = self.retry_request('post',url)
        self.validate_response(response, f"Failed to disable Alerting status")
    
    def test_connectivity_ingestion(self) -> bool:
        """
        Test connectivity
        """
        try:
            self.list_log_types()
            return True
        except Exception as e:
            raise e
    
    def list_log_types(self):
        
        request_url = f"{self.api_root}/v2/logtypes"
        response = self.session.get(request_url)
        response.raise_for_status()
        

    def push_unstructured_logs(self, entries, customer_id, log_type, name_space = None, usecase = None):
        url = f"{self.api_root}/v2/unstructuredlogentries:batchCreate"
        body = {
            "customerId":customer_id,
            "logType": log_type,
            "entries": entries
        }
        
        if name_space:
            body['namespace'] = name_space
        
        if usecase:
            body['labels'] = [{"key":"replayedFrom", "value":"Demoverse"},{"key":"sourceUsecase", "value":usecase}]
        
        response = self.session.post(url, data = json.dumps(body))
        
        if response.status_code == 400 and response.reason == 'Bad Request':
           
           if len(entries) >=10:
               self.siemplify_logger.info(f"API rejected the batch. Retrying with smaller size")
               new_size = ceil(len(entries)/2)
               sub_sets = [entries[x:x+new_size] for x in range(0, len(entries), new_size)]
           
               for _set in sub_sets:    
                    self.push_unstructured_logs(_set, customer_id, log_type, name_space)
           else:
               self.validate_response(response, f"Failed to process unstructured logs Batch")
        
        else:
            
            self.validate_response(response, f"Failed to process unstructured logs Batch")    
        
    
    
        
    
    def push_udm_events(self, events, customer_id):
        url = f"{self.api_root}/v2/udmevents:batchCreate"
        body = {
            "customerId":customer_id,
            "events": events
        }
        response = self.session.post(url, json = body)
        self.validate_response(response, "Failed to process UDM Logs")
        
        
    def retry_request(self, method, request_url, params=None, body=None):  

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
        return response
    
    def validate_response(self, r, error_msg):
        try:
            r.raise_for_status()
        except Exception as e:
            self.siemplify_logger.error(error_msg)
            self.siemplify_logger.exception(e)

        

   