from SiemplifyJob import SiemplifyJob
from datetime import timedelta, datetime
from SiemplifyUtils import unix_now
from constants import SEARCH_CASE_PAYLOAD, SEARCH_DATETIME_FORMAT
import json
INTEGRATION_NAME = "Demoverse"
SCRIPT_NAME = "Tag Cases With Labels"

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

def get_case_alerts(case_id, siemplify):
    url = f'{siemplify.API_ROOT}/external/v1/dynamic-cases/GetCaseDetails/{case_id}'
    r = siemplify.session.get(url)
    r.raise_for_status()
    return [alert.get('identifier') for alert in r.json().get('alertCards')]

def parse_events(events, siemplify):
    labels = []
    for event in events:
        fields = []
        for field in event.get('fields'):
            if field.get('groupName') == 'Default':
                fields.extend(field.get("items"))
        
        if fields:
            labels.extend([f"{item.get('originalName').replace('event_metadata_ingestionLabels_','')}:{item.get('value')}"\
             for item in fields if 'event_metadata_ingestionLabels' in item.get('originalName')])
    
    return set(labels)


def get_labels(alert, case_id, siemplify):
    url = f'{siemplify.API_ROOT}/external/v1/dynamic-cases/GetAlertEvents'
    body = {
        "alertIdentifier": alert,
        "caseId": case_id
    }
    r = siemplify.session.post(url, json=body)
    r.raise_for_status()
    
    return parse_events(r.json(), siemplify)

def process_case(case_id, siemplify):
    case_alerts = get_case_alerts(case_id, siemplify)
    labels = {}
    for alert in case_alerts:
        labels['alert'] = get_labels(alert, case_id, siemplify)
    if not labels:
        return None
    return labels

def search_cases(envs=None, time=None, siemplify = None):

    url = f'{siemplify.API_ROOT}/external/v1/search/CaseSearchEverything'
    body = SEARCH_CASE_PAYLOAD
    body['endTime'] = datetime.now().strftime(SEARCH_DATETIME_FORMAT)
    body['startTime'] = datetime.fromtimestamp(time).strftime(SEARCH_DATETIME_FORMAT)
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
        
        relevant_cases += [_case.get('id') for _case in r.json().get('results')]
        reference_times += [calculate_case_time(_case['time']) for _case in r.json().get('results')]
        page += 1
    if relevant_cases:
        return relevant_cases, int(datetime.timestamp(max(reference_times)))*1000
    else:
        return [], None

def add_tag(_case, alert, tag, siemplify):
    url = f'{siemplify.API_ROOT}/external/v1/dynamic-cases/AddCaseTag'
    body = {
        "caseId": _case,
        "alertIdentifier": alert,
        "tag": tag
    }
    r = siemplify.session.post(url, json=body)
    r.raise_for_status()
    
    return True

def add_tags_to_case(_case, labels, siemplify):
    for alert,values in labels.items():
        for label in values:
            add_tag(_case, alert, label, siemplify)
    return True

def main():
    siemplify = SiemplifyJob()
    siemplify.script_name = SCRIPT_NAME # In order to use the SiemplifyLogger, you must assign a name to the script.
    

    # INIT ACTION PARAMETERS:
    envs_str = siemplify.extract_job_param(param_name="Relevant Environments", print_value=True, 
        is_mandatory = False)

    max_hours = siemplify.extract_job_param(param_name="Max Hours Back", print_value=True, 
        is_mandatory = True, default_value =48, input_type = int)
    
    successful_cases = []
    
    try:
        
        if envs_str:
            envs = envs_str.split(',')
        else:
            envs = []

        reference_time = calculate_reference_time(max_hours)
        last_successful_time = int(siemplify.fetch_timestamp()/1000)
        
        #processed_ids_str = siemplify.get_scoped_job_context_property("processed_cases")
        processed_ids_str = None
        if processed_ids_str:
            processed_cases = processed_ids_str.split(',')
        else:
            processed_cases = list()

        
        if not last_successful_time or reference_time > last_successful_time:
            last_successful_time = reference_time
        
        siemplify.LOGGER.info(f"Searching cases Since {datetime.fromtimestamp(last_successful_time)}")
        relevant_cases, end_time = search_cases(envs=envs, time=last_successful_time, siemplify = siemplify)
        

        if relevant_cases:
            siemplify.LOGGER.info(f"Found {len(relevant_cases)} relevant cases")
            siemplify.LOGGER.info(json.dumps(relevant_cases[0]))
            for _case in relevant_cases:
                try:
                    case_labels = process_case(_case, siemplify)
                    result = add_tags_to_case(_case, case_labels, siemplify)
                    successful_cases.append(_case)
                except Exception as e:
                    siemplify.LOGGER.error(f"Failed processing case {_case}:{e}")
            
            if successful_cases:  
                siemplify.save_timestamp(new_timestamp=end_time)
                siemplify.LOGGER.info(f"Succesfully Added Tags to {len(successful_cases)} cases")
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