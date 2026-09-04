from constants import DATETIMEMAP, UDMTIMEMAP, TIME_MAP_PATH, DEFAULT_DATETIME_FORMAT, STREAM_TIME_UNITS
from datetime import datetime, timedelta
import re, copy, base64

def create_stream_batch_events(base_events, base_timestamp, base_timestamp_first, 
            reference_time, last_replayed_time, last_file_time, logger=None):
    """
    Processes a list of UDM events for incremental streaming.

    This function calculates a time window based on the last processed event and the current job execution time.
    It finds all events within this window, updates their timestamps to simulate a live stream, and returns them.

    Args:
        base_events (list): The full list of original UDM events.
        base_timestamp (datetime): The timestamp of the very last event in the original data.
        base_timestamp_first (datetime): The timestamp of the very first event in the original data.
        reference_time (datetime): The current time of the job execution.
        last_replayed_time (datetime): The *new* timestamp that was assigned to the last event in the previous run.
        last_file_time (datetime): The *original* timestamp of the last event processed in the previous run.
        logger: The Siemplify logger instance.

    Returns:
        tuple: A tuple containing:
            - list: The list of UDM events with updated timestamps for the current batch.
            - datetime: The latest *original* timestamp from the events processed in this batch.
            - datetime: The latest *replayed* timestamp from the events processed in this batch.
    """
    # Calculate how much "replayed" time has passed since the last run.
    stream_time_delta = reference_time - last_replayed_time    
    # Determine the end of the time window for this run. It's the start time plus the delta, capped by the absolute last event time.
    max_timestamp = last_file_time + stream_time_delta if (last_file_time + stream_time_delta) <= base_timestamp else base_timestamp

    # Lists to hold the results for this batch.
    updated_events = []
    replayed_timestamps = []
    file_timestamps = []
    
    for event in base_events:
        # Deepcopy to avoid modifying the original event data.
        new_event = copy.deepcopy(event)
        
        dateformat = str(UDMTIMEMAP["dateformat"])
        key = None
            
        if event.get('metadata').get('event_timestamp'):
            key = 'event_timestamp'
        elif event.get('metadata').get('eventTimestamp'):
            key = 'eventTimestamp'
        
        if key:
            # Parse the original event timestamp.
            event_time = event.get('metadata').get(key)
            event_time = re.sub(r'\.\d+', '', event_time)
                
            if event_time:
                
                event_time = datetime.strptime(event_time, dateformat)
                # Check if the event falls within the processing window for this run.
                if event_time <= max_timestamp and event_time > last_file_time:
                    
                    # Calculate the time difference from the last processed event to preserve the original event cadence.
                    time_delta = event_time - last_file_time
                    # Create the new timestamp by adding the delta to the last replayed time.
                    new_time = last_replayed_time + time_delta
                    new_event_timestamp = new_time.strftime(dateformat)

                    # Update the timestamp in the new event object.
                    new_event['metadata'][key] = new_event_timestamp
                    
                    # Collect timestamps and the updated event.
                    file_timestamps.append(event_time)
                    replayed_timestamps.append(new_time)
                    updated_events.append(new_event)
    
    return updated_events, max(file_timestamps) if file_timestamps else None, max(replayed_timestamps) if replayed_timestamps else None

def create_stream_batch(base_entries, base_timestamp, base_timestamp_first, 
    reference_time, last_replayed_time, last_file_time, logger=None):
    """
    Processes a list of unstructured log entries for incremental streaming.

    This function is similar to `create_stream_batch_events` but works on raw text logs. It finds timestamps
    in the log strings, checks if they fall within the processing window, and replaces them to simulate a live stream.

    Args:
        base_entries (list): The full list of original log entries (as dicts with 'log_text').
        base_timestamp (datetime): The timestamp of the very last log in the original data.
        base_timestamp_first (datetime): The timestamp of the very first log in the original data.
        reference_time (datetime): The current time of the job execution.
        last_replayed_time (datetime): The *new* timestamp assigned to the last log in the previous run.
        last_file_time (datetime): The *original* timestamp of the last log processed in the previous run.
        logger: The Siemplify logger instance.

    Returns:
        tuple: A tuple containing:
            - list: The list of log entries with updated timestamps for the current batch.
            - datetime: The latest *original* timestamp from the logs processed in this batch.
            - datetime: The latest *replayed* timestamp from the logs processed in this batch.
    """
    # Calculate how much "replayed" time has passed since the last run.
    stream_time_delta = reference_time - last_replayed_time
    # Determine the end of the time window for this run.
    max_timestamp = last_file_time + stream_time_delta if (last_file_time + stream_time_delta) <= base_timestamp else base_timestamp  

    updated_entries = []
    replayed_timestamps = []
    file_timestamps = []
      
    for log in base_entries:
        new_log = {}
        log_text = log["log_text"]
        is_relevant = False
        # Iterate through known timestamp patterns to find a match in the log text.
        for mapping in DATETIMEMAP:
            event_time_matches = re.findall(mapping.get('pattern'), log_text)
            event_time_matches = remove_duplicates(event_time_matches)    
            if event_time_matches:
                for _match in event_time_matches:
                    # Try all possible date formats for the matched pattern.
                    for dateformat in mapping.get('formats'):
                        try:
                            if dateformat == '%s':
                                event_timestamp = datetime.fromtimestamp(int(_match))
                                                            
                            elif dateformat == '%ms':
                                event_timestamp = datetime.fromtimestamp(int(_match)/1000)
                                                                    
                            else:
                                event_timestamp = datetime.strptime(_match,dateformat)

                            # Check if the parsed timestamp is within the processing window.
                            if event_timestamp and event_timestamp <= max_timestamp and event_timestamp > last_file_time:
                                # Calculate the time delta to preserve the original event cadence.
                                time_delta = event_timestamp - last_file_time
                                # Create the new timestamp.
                                new_time = last_replayed_time + time_delta
                                # Handle special case for millisecond epoch format.
                                if dateformat == '%ms':
                                    new_event_timestamp = f"{new_time.strftime('%s')}000"      
                                else:
                                    new_event_timestamp = new_time.strftime(dateformat)

                                # Replace the old timestamp string in the log with the new one.
                                log_text = log_text.replace(_match, new_event_timestamp)
                                is_relevant = True
                                file_timestamps.append(event_timestamp)
                                replayed_timestamps.append(new_time)
                                break
                            
                        except:
                            pass
        
        # If a timestamp was successfully updated, add the modified log to the results.
        if is_relevant:
            new_log['log_text'] = log_text
            updated_entries.append(new_log)

    return updated_entries, max(file_timestamps) if file_timestamps else None, max(replayed_timestamps) if replayed_timestamps else None

def create_logs_from_batch(batch, timestamp, labels):
    """
    Converts a batch of processed log entries into the format required by the Chronicle `logs:import` API.

    Args:
        batch (UnstructuredLogs): An object containing the log entries and metadata.
        timestamp (str): An optional timestamp to assign to all logs in the batch.
        labels (dict): A dictionary of labels to add to each log.

    Returns:
        list: A list of log objects formatted for the Chronicle API.
    """
    logs = []
    for log in batch.entries:
        # Explicit namespace and source folder mapping
        namespace_map = {
            "Cymbal": "Demoverse",
            "Demo": "Demoverse",
            "LogStory": "Demoverse"
        }
        folder_map = {
            "Cymbal": "Demo",
            "Demo": "Demo",
            "LogStory": "LogStory"
        }

        chronicle_namespace = namespace_map.get(batch.name_space, batch.name_space)
        source_folder_val = folder_map.get(batch.name_space, batch.name_space)

        # The log text must be base64 encoded.
        log = {
            "data" : base64.b64encode(log.get('log_text').encode('utf-8')).decode('utf-8'),
            "environment_namespace": chronicle_namespace,
            "labels": {
                "replayedFrom":{
                    "value":"Demoverse"
                },
                "sourceUsecase":{
                    "value": batch.usecase
                },
                "sourceFolder":{
                    "value": source_folder_val
                }
            }
        }

        # Assign a specific entry time if provided.
        if timestamp:
            log["log_entry_time"] = timestamp

        if labels:
            # Add any additional labels.
            log['labels'].update(labels)

        logs.append(log)
    
    return logs


def remove_duplicates(matches):
    """
    Removes duplicate items from a list.

    Args:
        matches (list): A list that may contain duplicates.

    Returns:
        list: A new list with unique items.
    """
    unique_set = set(matches)
    return list(unique_set)

def get_base_timestamp_logstory(entries, mapping):
    """
    Finds the earliest and latest timestamps in LogStory logs using a specific mapping.

    Args:
        entries (list): A list of log entries.
        mapping (dict): A mapping dictionary that defines the timestamp patterns and formats for this log type.

    Returns:
        tuple: A tuple containing (latest_timestamp, earliest_timestamp), or (None, None) if no timestamps are found.
    """

    events_timestamps = []

    for log in entries:
        log_text = log["log_text"]

        for timestamp in mapping.get('timestamps'):

            _match = re.search(timestamp.get('pattern'), log_text)
            
            if _match:

                if timestamp.get('epoch'):
                    events_timestamps.append(datetime.fromtimestamp(int(_match.group(timestamp.get('group')))))
                else:
                    events_timestamps.append(datetime.strptime(_match.group(timestamp.get('group')),timestamp.get('dateformat')))
        
    if events_timestamps:
        return max(events_timestamps), min(events_timestamps)
    else:
        return None, None

def decode_base64_blob_to_string(base64_blob):
    """
    Decodes a Base64 blob to a string.

    Args:
    base64_blob: The Base64 blob to decode.
    Returns:
    The decoded string.
    """
    base64_bytes = base64_blob.encode('ascii')
    decoded_bytes = base64.b64decode(base64_bytes)
    decoded_string = decoded_bytes.decode('utf-8')
    return decoded_string
   
                


def get_base_timestamp(entries, usecase_mapping=None, logger=None):
    """
    Finds the earliest and latest timestamps in a list of unstructured log entries.

    It iterates through a generic set of patterns (DATETIMEMAP) to find and parse timestamps.
    It can also filter timestamps to be within a specific range defined by `usecase_mapping`.

    Args:
        entries (list): A list of log entries.
        usecase_mapping (dict, optional): A dict with 'first' and 'last' timestamps to constrain the search.
        logger: The Siemplify logger instance.

    Returns:
        tuple: A tuple containing (latest_timestamp, earliest_timestamp), or (None, None) if no timestamps are found.
    """
    events_timestamps = []
    
    for log in entries:
        event_timestamp = None
        log_text = log["log_text"]
            
        # Iterate through all known generic timestamp patterns.
        for mapping in DATETIMEMAP:
            event_time_matches = re.findall(mapping.get('pattern'), log_text)
            
            if event_time_matches:
                
                event_time_matches = remove_duplicates(event_time_matches)
                
                # For each match, try all possible date formats.
                for _match in event_time_matches:
                    for dateformat in mapping.get('formats'):
                        try:
                            if dateformat == '%s':
                                event_timestamp = datetime.fromtimestamp(int(_match))
                            
                            elif dateformat == '%ms':
                                event_timestamp = datetime.fromtimestamp(int(_match)/1000)
                                    
                            else:
                                
                                event_timestamp = datetime.strptime(_match,dateformat)
                            
                            # If a use case mapping is provided, ensure the timestamp falls within its range.
                            if usecase_mapping:
                                
                                if event_timestamp <= datetime.strptime(usecase_mapping.get('last'), DEFAULT_DATETIME_FORMAT) and event_timestamp >= datetime.strptime(usecase_mapping.get('first'), DEFAULT_DATETIME_FORMAT):
                                    
                                    events_timestamps.append(event_timestamp)
                                    break
                            # Otherwise, perform a basic sanity check.
                            else:
                                current_time = datetime.now()
                                # Discard timestamps that are in the future.
                                if event_timestamp > current_time:
                                    raise
                                events_timestamps.append(event_timestamp)
                                break
                            
                        except:
                            pass
    
                        

    if events_timestamps:
        return max(events_timestamps), min(events_timestamps)
    else:
        return None, None
                    
def get_base_timestamp_udm(events):
    """
    Finds the earliest and latest timestamps in a list of UDM events.

    Args:
        events (list): A list of UDM event dictionaries.

    Returns:
        tuple: A tuple containing (latest_timestamp, earliest_timestamp), or (None, None) if no timestamps are found.
    """
    events_timestamps = []
    
    for event in events:
        
        dateformat = str(UDMTIMEMAP["dateformat"])
        key = None    
        if event.get('metadata').get('event_timestamp'):
            key = 'event_timestamp'
        elif event.get('metadata').get('eventTimestamp'):
            key = 'eventTimestamp'
        
        if key:
            event_timestamp = event.get('metadata').get(key)
            event_timestamp = re.sub(r'\.\d+', '', event_timestamp)
            event_timestamp = datetime.strptime(event_timestamp, dateformat)
            
            if event_timestamp:
                events_timestamps.append(event_timestamp)

    return max(events_timestamps), min(events_timestamps)

def update_events(base_events, base_timestamp, new_base_timestamp):
    """
    Updates timestamps for a list of UDM events for a "replay" job.

    This function shifts all event timestamps so that the latest event aligns with `new_base_timestamp`,
    while preserving the relative time differences between all events.

    Args:
        base_events (list): The original list of UDM events.
        base_timestamp (datetime): The timestamp of the latest event in the original data.
        new_base_timestamp (datetime): The target time for the latest event to be replayed to.

    Returns:
        list: A new list of events with updated timestamps.
    """
    updated_events = []
    for event in base_events:
        new_event = copy.deepcopy(event)
        
        dateformat = str(UDMTIMEMAP["dateformat"])
        key = None
            
        if event.get('metadata').get('event_timestamp'):
            key = 'event_timestamp'
        elif event.get('metadata').get('eventTimestamp'):
            key = 'eventTimestamp'
        
        if key:
            event_time = event.get('metadata').get(key)
            event_time = re.sub(r'\.\d+', '', event_time)
                
            if event_time:
                event_time = datetime.strptime(event_time, dateformat)
                # Calculate how far in the past this event was from the latest event.
                time_delta = base_timestamp - event_time
                # Apply that same delta to the new base time to get the replayed time.
                new_time = new_base_timestamp - time_delta
                new_event_timestamp = new_time.strftime(dateformat)

                new_event['metadata'][key] = new_event_timestamp
        
        updated_events.append(new_event)
    
    return updated_events
        
def update_entries(base_entries, base_timestamp, new_base_timestamp, logger=None):
    """
    Updates timestamps for a list of unstructured logs for a "replay" job.

    This function shifts all log timestamps so that the latest log aligns with `new_base_timestamp`,
    while preserving the relative time differences between all logs.

    Args:
        base_entries (list): The original list of log entries.
        base_timestamp (datetime): The timestamp of the latest log in the original data.
        new_base_timestamp (datetime): The target time for the latest log to be replayed to.
        logger: The Siemplify logger instance.

    Returns:
        list: A new list of log entries with updated timestamps in their text.
    """
    updated_entries = []  
    for log in base_entries:
        new_log = {}
        log_text = log["log_text"]
        
        for mapping in DATETIMEMAP:
            event_time_matches = re.findall(mapping.get('pattern'), log_text)
            event_time_matches = remove_duplicates(event_time_matches)    
            if event_time_matches:
                for _match in event_time_matches:
                    for dateformat in mapping.get('formats'):
                        try:
                            # Parse the timestamp and perform a sanity check.
                            if dateformat == '%s':
                                event_timestamp = datetime.fromtimestamp(int(_match))
                                if event_timestamp.year < (new_base_timestamp.year - 5) or event_timestamp > new_base_timestamp:
                                    raise
                            
                            elif dateformat == '%ms':
                                event_timestamp = datetime.fromtimestamp(int(_match)/1000)
                                
                                if event_timestamp.year < (new_base_timestamp.year - 5)  or event_timestamp > new_base_timestamp:
                                    raise
                                    
                            else:
                                event_timestamp = datetime.strptime(_match,dateformat)

                            if event_timestamp and event_timestamp <= new_base_timestamp:
                                # Calculate how far in the past this log was from the latest log.
                                time_delta = base_timestamp - event_timestamp
                                # Apply that same delta to the new base time.
                                new_time = new_base_timestamp - time_delta
                                
                            # Handle special case for millisecond epoch format.
                            if dateformat == '%ms':
                               new_event_timestamp = f"{new_time.strftime('%s')}000"      
                            else:
                                new_event_timestamp = new_time.strftime(dateformat)

                               
                            log_text = log_text.replace(_match, new_event_timestamp)
                            
                            break
                            
                        except:
                            pass
        
        new_log['log_text'] = log_text
        updated_entries.append(new_log)
    
    return updated_entries


def calculate_new_stream(last_successful_stream_ms, wait_time, reference_time):
    """
    Determines if a new stream can be started based on the configured wait time.

    Args:
        last_successful_stream_ms (int): The timestamp (in ms) when the last stream completed.
        wait_time (str): The configured wait time string (e.g., "1h", "30m").
        reference_time (datetime): The current time of the job execution.

    Returns:
        bool: True if the wait time has passed, False otherwise.
    """
    last_stream_datetime = datetime.fromtimestamp(last_successful_stream_ms/1000)
    time_delta_last_stream = reference_time - last_stream_datetime

    wait_time_delta = None
    match = re.match(r'(\d+)(\w+)', wait_time)
    if match:
        value = int(match.group(1))
        unit = STREAM_TIME_UNITS.get(match.group(2))
        kwargs = {unit: value}
        wait_time_delta = timedelta(**kwargs)
    
    if wait_time_delta:
        return True if time_delta_last_stream >= wait_time_delta else False
    
    return False


        
def calculate_time_from_batch_size(init_batchsize, reference_time):
    """
    Calculates a start time in the past based on an initial batch size string.

    This is used to set the starting point for the very first run of a new stream.

    Args:
        init_batchsize (str): The initial batch size string (e.g., "5m", "2h").
        reference_time (datetime): The current time of the job execution.

    Returns:
        float: A Unix timestamp representing the calculated start time, or None if parsing fails.
    """
    match = re.match(r'(\d+)(\w+)', init_batchsize)
    if match:
        value = int(match.group(1))
        unit = STREAM_TIME_UNITS.get(match.group(2))
        kwargs = {unit: value}
        time_delta = timedelta(**kwargs)
        return (reference_time - time_delta).timestamp()
    else:
        return None


def extract_timestamp_from_line(line, is_udm=False, name_space=None, logstory_mapping=None, usecase_mapping=None):
    """Extracts timestamp from a single line string. Returns (event_timestamp, match_str, dateformat, key_if_udm)."""
    if not line or not line.strip():
        return None, None, None, None
    
    if is_udm:
        import json
        try:
            event = json.loads(line)
            dateformat = str(UDMTIMEMAP["dateformat"])
            key = None
            if event.get('metadata', {}).get('event_timestamp'):
                key = 'event_timestamp'
            elif event.get('metadata', {}).get('eventTimestamp'):
                key = 'eventTimestamp'
            
            if key:
                event_time_str = event.get('metadata').get(key)
                clean_time = re.sub(r'\.\d+', '', event_time_str)
                event_timestamp = datetime.strptime(clean_time, dateformat)
                return event_timestamp, event_time_str, dateformat, key
        except:
            pass
        return None, None, None, None

    if name_space == 'LogStory' and logstory_mapping:
        for timestamp in logstory_mapping.get('timestamps', []):
            _match = re.search(timestamp.get('pattern'), line)
            if _match:
                try:
                    match_str = _match.group(timestamp.get('group'))
                    if timestamp.get('epoch'):
                        event_timestamp = datetime.fromtimestamp(int(match_str))
                        return event_timestamp, match_str, '%s', None
                    else:
                        dateformat = timestamp.get('dateformat')
                        event_timestamp = datetime.strptime(match_str, dateformat)
                        return event_timestamp, match_str, dateformat, None
                except:
                    pass
        return None, None, None, None

    for mapping in DATETIMEMAP:
        event_time_matches = re.findall(mapping.get('pattern'), line)
        if event_time_matches:
            event_time_matches = remove_duplicates(event_time_matches)
            for _match in event_time_matches:
                for dateformat in mapping.get('formats'):
                    try:
                        if dateformat == '%s':
                            event_timestamp = datetime.fromtimestamp(int(_match))
                        elif dateformat == '%ms':
                            event_timestamp = datetime.fromtimestamp(int(_match)/1000)
                        else:
                            event_timestamp = datetime.strptime(_match, dateformat)
                        
                        if usecase_mapping:
                            if event_timestamp <= datetime.strptime(usecase_mapping.get('last'), DEFAULT_DATETIME_FORMAT) and event_timestamp >= datetime.strptime(usecase_mapping.get('first'), DEFAULT_DATETIME_FORMAT):
                                return event_timestamp, _match, dateformat, None
                        else:
                            if event_timestamp <= datetime.now():
                                return event_timestamp, _match, dateformat, None
                    except:
                        pass
    return None, None, None, None


def transform_stream_line(line, event_time, last_file_time, last_replayed_time, match_str, dateformat, key_if_udm=None, is_udm=False):
    """Transforms the timestamp in a single line string to simulate live streaming."""
    time_delta = event_time - last_file_time
    new_time = last_replayed_time + time_delta
    
    if is_udm and key_if_udm:
        import json
        try:
            event = json.loads(line)
            new_event_timestamp = new_time.strftime(str(UDMTIMEMAP["dateformat"]))
            event['metadata'][key_if_udm] = new_event_timestamp
            return json.dumps(event), new_time
        except:
            return line, new_time

    # For unstructured logs, shift ALL timestamps in the line by the same offset
    # to maintain alignment and ensure the main timestamp is correctly shifted.
    offset = last_replayed_time - last_file_time
    updated_line = line
    
    for mapping in DATETIMEMAP:
        event_time_matches = re.findall(mapping.get('pattern'), updated_line)
        if event_time_matches:
            event_time_matches = remove_duplicates(event_time_matches)
            for _match in event_time_matches:
                for fmt in mapping.get('formats'):
                    try:
                        if fmt == '%s':
                            event_timestamp = datetime.fromtimestamp(int(_match))
                        elif fmt == '%ms':
                            event_timestamp = datetime.fromtimestamp(int(_match)/1000)
                        else:
                            event_timestamp = datetime.strptime(_match, fmt)
                        
                        if event_timestamp:
                            shifted_time = event_timestamp + offset
                            if fmt == '%ms':
                                new_event_timestamp = f"{shifted_time.strftime('%s')}000"
                            elif fmt == '%s':
                                new_event_timestamp = shifted_time.strftime('%s')
                            else:
                                new_event_timestamp = shifted_time.strftime(fmt)
                            
                            updated_line = updated_line.replace(_match, new_event_timestamp)
                            break
                    except:
                        pass
                        
    return updated_line, new_time
