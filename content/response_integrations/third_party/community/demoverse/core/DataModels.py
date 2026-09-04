from utils import get_base_timestamp, get_base_timestamp_udm, update_entries, update_events, get_base_timestamp_logstory, create_logs_from_batch, create_stream_batch, create_stream_batch_events
import json

class UnstructuredLogs(object):
    def __init__(self, content, log_type, new_base_time, name_space, logstory_mapping, usecase_mapping = None, usecase_name = None, logger = None):
        self.base_entries = [{'log_text': log} for log in content.splitlines()]
        self.base_timestamp, self.base_timestamp_first = get_base_timestamp(self.base_entries, usecase_mapping=usecase_mapping) if name_space != "LogStory" else get_base_timestamp_logstory(self.base_entries, logstory_mapping)
        self.new_base_timestamp = new_base_time
        self.customer_id = None
        self.log_type = log_type
        self.api = "unstructured"
        self.name_space = name_space
        self.entries = None
        self.usecase = usecase_name
        self.logger = logger
        self.last_file_timestamp = None
    
    def get_updated_logs(self):
        self.entries = update_entries(self.base_entries, self.base_timestamp, self.new_base_timestamp, logger=self.logger)
    
    def get_logs_for_stream(self, reference_time, last_replayed_time, last_file_time):
       self.entries, self.last_file_time, self.last_replayed_time = create_stream_batch(self.base_entries, self.base_timestamp, self.base_timestamp_first, 
            reference_time, last_replayed_time, last_file_time, logger=self.logger)
        
    
class UdmEvents(object):
    def __init__(self, content, new_base_time, name_space, usecase_name, logger= None, is_parsed = False, labels = None):
        if not is_parsed:
            self.base_events = [json.loads(event) for event in content.splitlines()]
        else:
            self.base_events = content
        self.name_space = name_space
        self.usecase = usecase_name
        self.base_events= self.update_labels(self.base_events, labels)
        self.base_timestamp, self.base_timestamp_first = get_base_timestamp_udm(self.base_events)
        self.new_base_timestamp = new_base_time
        self.api = "udmevents"
        self.events = None
        self.last_file_timestamp = None
        self.logger=logger
    
    def get_updated_logs(self):
        self.events = update_events(self.base_events, self.base_timestamp, self.new_base_timestamp)

    def get_events_for_stream(self, reference_time, last_replayed_time, last_file_time):
        self.events, self.last_file_time, self.last_replayed_time = create_stream_batch_events(self.base_events, self.base_timestamp, self.base_timestamp_first, 
            reference_time, last_replayed_time, last_file_time, logger=self.logger)

    def update_labels(self, events, labels):
        updated_events = []
        for event in events:
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

            chronicle_namespace = namespace_map.get(self.name_space, self.name_space)
            source_folder_val = folder_map.get(self.name_space, self.name_space)

            if event['metadata'].get('base_labels'):
                event['metadata']['base_labels']['namespaces'] = [chronicle_namespace]
            else:
                event['metadata']['base_labels'] = {'namespaces':[chronicle_namespace]}
            if event.get('principal'):
                event['principal']['namespace'] = chronicle_namespace
            if event.get('target'):
                event['target']['namespace'] = chronicle_namespace

            new_labels = [
                {"key":"replayedFrom", "value":"Demoverse"},
                {"key":"sourceUsecase", "value":self.usecase},
                {"key":"sourceFolder", "value":source_folder_val}
            ]
            if event['metadata'].get('ingestion_labels'):
                event['metadata']['ingestion_labels'] += new_labels
            else:
                event['metadata']['ingestion_labels'] = new_labels
            if labels:
                event['metadata']['ingestion_labels'] += labels
            updated_events.append(event)
        return updated_events
    
class DetectionRule(object):
    def __init__(self, content, rule_name):
        self.name = rule_name
        self.content = content
        self.exists = False
        self.is_synced = False
        self.id = None
        self.is_live = False
        self.is_alerting = False

class DetectionRuleV2(object):
    def __init__(self, content, rule_name):
        self.name = rule_name
        self.content = content
        self.exists = False
        self.is_synced = False
        self.id = None
        self.deployment = None
        
class ChronicleRule(object):
    def __init__(self, rule):
        self.name = rule.get('ruleName')
        self.content = rule.get('ruleText')
        self.id = rule.get('ruleId')
        self.is_live = False if not rule.get('liveRuleEnabled') else True
        self.is_alerting = False if not rule.get('alertingEnabled') else True

class ChronicleRuleV2(object):
    def __init__(self, rule):
        self.name = rule.get('displayName')
        self.content = rule.get('text')
        self.id = rule.get('name')
        self.deployment = rule.get('deployment')

class SecOpsLog(object):
    def __init__(self, batch, fwd_id, timestamp = None, labels={}):
        self.logs = create_logs_from_batch(batch, timestamp, labels)
        self.fwd_id = fwd_id
        self.log_type = batch.log_type
    
    def add_timestamp(self, timestamp):
        new_logs = [log.update({"log_entry_time":timestamp}) for log in self.logs]
        return new_logs

class SecOpsLogsPayload(object):
    def __init__(self, secops_logs):
        self.logs = secops_logs.logs
        self.fwd_id = secops_logs.fwd_id
        self.log_type = secops_logs.log_type
    

    def extend_logs(self, logs):
        self.logs.extend(logs)


class UnstructuredStreamLogs(object):
    def __init__(self, gcs_manager, path, log_type, new_base_time, name_space, logstory_mapping, usecase_mapping=None, usecase_name=None, start_byte=0, logger=None):
        self.gcs_manager = gcs_manager
        self.path = path
        self.log_type = log_type
        self.new_base_timestamp = new_base_time
        self.name_space = name_space
        self.logstory_mapping = logstory_mapping
        self.usecase_mapping = usecase_mapping
        self.usecase = usecase_name
        self.start_byte = start_byte
        self.logger = logger
        self.api = "unstructured"
        self.file = None

        first_line, last_line = self.gcs_manager.get_blob_first_and_last_line(self.path)
        
        from utils import extract_timestamp_from_line
        res_last = extract_timestamp_from_line(last_line, is_udm=False, name_space=self.name_space, logstory_mapping=self.logstory_mapping, usecase_mapping=self.usecase_mapping)
        res_first = extract_timestamp_from_line(first_line, is_udm=False, name_space=self.name_space, logstory_mapping=self.logstory_mapping, usecase_mapping=self.usecase_mapping)
        
        self.base_timestamp = res_last[0] if res_last else None
        self.base_timestamp_first = res_first[0] if res_first else None

        self.last_file_time = None
        self.last_replayed_time = None

    def stream_and_process_window(self, reference_time, last_replayed_time, last_file_time, batch_callback):
        from utils import extract_timestamp_from_line, transform_stream_line

        stream_time_delta = reference_time - last_replayed_time
        max_timestamp = last_file_time + stream_time_delta if (last_file_time + stream_time_delta) <= self.base_timestamp else self.base_timestamp

        current_byte_offset = self.start_byte
        reader = self.gcs_manager.get_stream_blob_reader(self.path, start_byte=self.start_byte)
        
        buffer_entries = []
        buffer_size = 0
        max_buffer_size = 2 * 1024 * 1024 # 2 MB

        self.last_file_time = last_file_time
        self.last_replayed_time = last_replayed_time
        
        for line, next_offset in reader:
            res = extract_timestamp_from_line(line, is_udm=False, name_space=self.name_space, logstory_mapping=self.logstory_mapping, usecase_mapping=self.usecase_mapping)
            if not res or not res[0]:
                current_byte_offset = next_offset
                continue
            
            event_time, match_str, dateformat, _ = res
            
            if event_time > max_timestamp:
                break
            
            if event_time > last_file_time:
                updated_line, new_time = transform_stream_line(line, event_time, last_file_time, last_replayed_time, match_str, dateformat, is_udm=False)
                
                entry = {'log_text': updated_line}
                buffer_entries.append(entry)
                buffer_size += len(updated_line)

                if event_time > self.last_file_time:
                    self.last_file_time = event_time
                if new_time > self.last_replayed_time:
                    self.last_replayed_time = new_time

                if buffer_size >= max_buffer_size or len(buffer_entries) >= 5000:
                    class DummyBatch:
                        pass
                    batch_obj = DummyBatch()
                    batch_obj.entries = buffer_entries
                    batch_obj.name_space = self.name_space
                    batch_obj.usecase = self.usecase
                    batch_obj.log_type = self.log_type
                    
                    batch_callback(batch_obj)
                    buffer_entries = []
                    buffer_size = 0

            current_byte_offset = next_offset

        if buffer_entries:
            class DummyBatch:
                pass
            batch_obj = DummyBatch()
            batch_obj.entries = buffer_entries
            batch_obj.name_space = self.name_space
            batch_obj.usecase = self.usecase
            batch_obj.log_type = self.log_type
            
            batch_callback(batch_obj)

        return current_byte_offset


class UdmStreamEvents(object):
    def __init__(self, gcs_manager, path, new_base_time, name_space, usecase_name, start_byte=0, logger=None):
        self.gcs_manager = gcs_manager
        self.path = path
        self.new_base_timestamp = new_base_time
        self.name_space = name_space
        self.usecase = usecase_name
        self.start_byte = start_byte
        self.logger = logger
        self.api = "udmevents"
        self.file = None

        first_line, last_line = self.gcs_manager.get_blob_first_and_last_line(self.path)
        
        from utils import extract_timestamp_from_line
        res_last = extract_timestamp_from_line(last_line, is_udm=True)
        res_first = extract_timestamp_from_line(first_line, is_udm=True)
        
        self.base_timestamp = res_last[0] if res_last else None
        self.base_timestamp_first = res_first[0] if res_first else None

        self.last_file_time = None
        self.last_replayed_time = None

    def update_label_single(self, event_dict):
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

        chronicle_namespace = namespace_map.get(self.name_space, self.name_space)
        source_folder_val = folder_map.get(self.name_space, self.name_space)

        if event_dict.get('metadata', {}).get('base_labels'):
            event_dict['metadata']['base_labels']['namespaces'] = [chronicle_namespace]
        else:
            if 'metadata' not in event_dict:
                event_dict['metadata'] = {}
            event_dict['metadata']['base_labels'] = {'namespaces':[chronicle_namespace]}
        if event_dict.get('principal'):
            event_dict['principal']['namespace'] = chronicle_namespace
        if event_dict.get('target'):
            event_dict['target']['namespace'] = chronicle_namespace

        new_labels = [
            {"key":"replayedFrom", "value":"Demoverse"},
            {"key":"sourceUsecase", "value":self.usecase},
            {"key":"sourceFolder", "value":source_folder_val}
        ]
        if event_dict['metadata'].get('ingestion_labels'):
            event_dict['metadata']['ingestion_labels'] += new_labels
        else:
            event_dict['metadata']['ingestion_labels'] = new_labels
        return event_dict

    def stream_and_process_window(self, reference_time, last_replayed_time, last_file_time, batch_callback):
        from utils import extract_timestamp_from_line, transform_stream_line
        import json

        stream_time_delta = reference_time - last_replayed_time
        max_timestamp = last_file_time + stream_time_delta if (last_file_time + stream_time_delta) <= self.base_timestamp else self.base_timestamp

        current_byte_offset = self.start_byte
        reader = self.gcs_manager.get_stream_blob_reader(self.path, start_byte=self.start_byte)
        
        buffer_events = []
        
        self.last_file_time = last_file_time
        self.last_replayed_time = last_replayed_time

        for line, next_offset in reader:
            res = extract_timestamp_from_line(line, is_udm=True)
            if not res or not res[0]:
                current_byte_offset = next_offset
                continue
            
            event_time, match_str, dateformat, key = res
            
            if event_time > max_timestamp:
                break
            
            if event_time > last_file_time:
                updated_line, new_time = transform_stream_line(line, event_time, last_file_time, last_replayed_time, match_str, dateformat, key_if_udm=key, is_udm=True)
                
                try:
                    event_dict = json.loads(updated_line)
                    event_dict = self.update_label_single(event_dict)
                    buffer_events.append(event_dict)
                except:
                    pass

                if event_time > self.last_file_time:
                    self.last_file_time = event_time
                if new_time > self.last_replayed_time:
                    self.last_replayed_time = new_time

                if len(buffer_events) >= 2000:
                    batch_callback(buffer_events)
                    buffer_events = []

            current_byte_offset = next_offset

        if buffer_events:
            batch_callback(buffer_events)

        return current_byte_offset