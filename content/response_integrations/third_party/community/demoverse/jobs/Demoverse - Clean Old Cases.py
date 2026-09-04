from SiemplifyJob import SiemplifyJob
from datetime import timedelta, datetime
from SiemplifyUtils import unix_now
from constants import SEARCH_CASE_PAYLOAD, SEARCH_DATETIME_FORMAT

INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "Clean Old Cases"

def calculate_case_time(time):
    formats = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ'] 
    for fmt in formats:
        try:
            return datetime.strptime(time, fmt)
        except ValueError:
            pass  # If the format doesn't match, try the next one

def calculate_reference_time(max_hours):

    current_time =  datetime.fromtimestamp(unix_now()/1000)
    reference_time = current_time - timedelta(hours=max_hours)

    return int(datetime.timestamp(reference_time))

def bulk_close_cases(cases, siemplify):
    url = f'{siemplify.API_ROOT}/external/v1/cases-queue/bulk-operations/ExecuteBulkCloseCase'
    body =  {
                "casesIds": cases,
                "closeComment": 'Closed by Demoverse Job',
                "closeReason": 2,
                "rootCause": "Lab test"
            }
    r = siemplify.session.post(url, json=body)
    r.raise_for_status()
    return True

def search_cases(envs=None, time=None, siemplify = None, tags=None):

    url = f'{siemplify.API_ROOT}/external/v1/search/CaseSearchEverything'
    body = SEARCH_CASE_PAYLOAD
    body['endTime'] = datetime.strftime(datetime.fromtimestamp(time), SEARCH_DATETIME_FORMAT)
    body['environments'] = envs
    relevant_cases = list()
    reference_times = list()
    is_more_pages = True
    page = 0
    while is_more_pages:
        body['requestedPage'] = page
        r = siemplify.session.post(url, json=body)
        r.raise_for_status()
        if not len(r.json().get('results')):
            is_more_pages = False
            continue
        if tags:
            for tag in tags:
                relevant_cases += [_case['id'] for _case in r.json().get('results') 
                    if tag not in _case['tags']]
                reference_times += [calculate_case_time(_case['time']) for _case in r.json().get('results')
                    if _case['id'] in relevant_cases]
                
        else:
            relevant_cases += [_case['id'] for _case in r.json().get('results')]
            reference_times += [calculate_case_time(_case['time']) for _case in r.json().get('results')]
        page += 1
    if relevant_cases:
        return relevant_cases, int(datetime.timestamp(max(reference_times)))*1000
    else:
        return [], None

def main():
    siemplify = SiemplifyJob()
    siemplify.script_name = SCRIPT_NAME # In order to use the SiemplifyLogger, you must assign a name to the script.
    

    # INIT ACTION PARAMETERS:
    envs_str = siemplify.extract_job_param(param_name="Relevant Environments", print_value=True, 
        is_mandatory = True)

    max_hours = siemplify.extract_job_param(param_name="Max Hours Back", print_value=True, 
        is_mandatory = True, default_value =48, input_type = int)
    
    tags_str = siemplify.extract_job_param(param_name="Tags Whitelist", print_value=True, 
        is_mandatory = False, default_value =None, input_type = str)
    

    try:
        envs = envs_str.split(',')

        if tags_str:
            tags = tags_str.split(',')
        else:
            tags = None

        reference_time = calculate_reference_time(max_hours)
        last_successful_time = int(siemplify.fetch_timestamp()/1000)
        if not last_successful_time or reference_time > last_successful_time:
            last_successful_time = reference_time
        
        siemplify.LOGGER.info(f"Searching cases up to {datetime.fromtimestamp(last_successful_time)} in envs {','.join(envs)}")
        relevant_cases, end_time = search_cases(envs=envs, time=last_successful_time, siemplify = siemplify, tags=tags)
        
        if relevant_cases:
            siemplify.LOGGER.info(f"Found {len(relevant_cases)} relevant cases")
            siemplify.LOGGER.info(f"Closing relevant cases using:\n  Reason: MAINTENANCE\n  Root Cause: Lab Test")
            result = bulk_close_cases(relevant_cases, siemplify)
            if result:
                siemplify.save_timestamp(new_timestamp=end_time)
                siemplify.LOGGER.info("Succesfully Closed Cases")
        else:
            siemplify.LOGGER.info("No relevant cases found")
            siemplify.LOGGER.info("Nothing to do")
    except Exception as e:
        siemplify.LOGGER.error("General error performing Job {}".format(SCRIPT_NAME))
        siemplify.LOGGER.exception(e)
        raise

    siemplify.end_script()


if __name__ == "__main__":
    main()