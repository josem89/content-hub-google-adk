from SiemplifyJob import SiemplifyJob
from urllib.parse import urljoin

INTEGRATION_NAME = "CUSTOM_INTEGRATION"
SCRIPT_NAME = "JobTemplate"

ENDPOINTS = {
    "get_mp_integrations":"/api/external/v1/store/GetIntegrationsStoreData",
    "install_integration":"/api/external/v1/store/DownloadAndInstallIntegrationFromLocalStore"
}

INSTALLATION_STATUS = {
    "installed": 1,
    "not_installed":0,
    "update_available":2
}

def get_url(api_root, url_id, **kwargs):
    return urljoin(api_root, ENDPOINTS[url_id].format(**kwargs))

def get_mp_integrations(siemplify,status):

    url = get_url(siemplify.API_ROOT, 'get_mp_integrations')
    r = siemplify.session.get(url)
    r.raise_for_status()
    integrations = r.json().get('integrations')
    return [i for i in integrations 
        if not i.get('isCustom') and
        i.get('status') == INSTALLATION_STATUS.get(status) ]

def install_integration(siemplify, integration_id, integration_version, is_certified=True):
        payload = {
            "name": integration_id,
            "identifier": integration_id,
            "version": integration_version,
            "isCertified": is_certified
        }
        url = get_url(siemplify.API_ROOT, 'install_integration')
        try:
            r = siemplify.session.post(url, json=payload)
            r.raise_for_status()
            return True
        except Exception as e:
            siemplify.LOGGER.error(f'Failed installing integration {integration_id}')
            siemplify.LOGGER.error(e)
            return False
        

        

    

def main():
    siemplify = SiemplifyJob()
    siemplify.script_name = SCRIPT_NAME # In order to use the SiemplifyLogger, you must assign a name to the script.



    # INIT ACTION PARAMETERS:
    whitelist_str = siemplify.extract_job_param(param_name="Whitelist", print_value=True)


    try:
        
        whitelist = []
        if whitelist_str:
            whitelist = whitelist_str.split(',')
            siemplify.LOGGER.info(f"Following integrations will not be updated: {','.join(whitelist)}")
            
        
        integrations = get_mp_integrations(siemplify, 'update_available')        
        relevant_integrations = [i for i in integrations 
            if i.get('displayName') not in whitelist]
        
        if relevant_integrations:
            siemplify.LOGGER.info(f"Found {len(relevant_integrations)} relevant integration with an available update")

            failed_integrations = []
            successful_integrations = []
            for integration in relevant_integrations:
                success = install_integration(siemplify, integration.get("identifier"), integration.get('version'))

                if success:
                    successful_integrations.append(integration.get("displayName"))
                else:
                    failed_integrations.append(integration.get("displayName"))
            if successful_integrations:
                siemplify.LOGGER.info(f"Successfully updated integrations {','.join(successful_integrations)}")
            if failed_integrations:
                siemplify.LOGGER.info(f"Failed updating integrations {','.join(failed_integrations)}")
        
        else:
            siemplify.LOGGER.info("Nothing to update")

        siemplify.LOGGER.info("Job Finished")

    except Exception as e:
        siemplify.LOGGER.error("General error performing Job {}".format(SCRIPT_NAME))
        siemplify.LOGGER.exception(e)
        raise

    siemplify.end_script()


if __name__ == "__main__":
    main()